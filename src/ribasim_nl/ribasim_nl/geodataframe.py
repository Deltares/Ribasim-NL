import importlib
import os
import threading
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from functools import reduce

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from geopandas import GeoDataFrame

from ribasim_nl.geometry import snap_boundaries_to_other_line, split_basin

# the module, since `geopandas.tools.overlay` is shadowed by the function of that name
gpd_overlay = importlib.import_module("geopandas.tools.overlay")


def sorted_sjoin(left: GeoDataFrame, right: GeoDataFrame, **kwargs) -> GeoDataFrame:
    """`left.sjoin(right, **kwargs)`, with the matches of each left row in the row order of `right`.

    `sjoin` keeps the row order of `left`, but orders the matches of a row by the spatial index, which differs
    between platforms. Use this when the result depends on the order of the matches.
    """
    assert kwargs.get("how", "inner") in ("inner", "left")
    left_pos, right_pos = "_sorted_sjoin_left_pos", "_sorted_sjoin_right_pos"
    assert left_pos not in left.columns and right_pos not in right.columns
    joined = left.assign(**{left_pos: np.arange(len(left))}).sjoin(
        right.assign(**{right_pos: np.arange(len(right))}), **kwargs
    )
    return joined.sort_values([left_pos, right_pos], kind="stable").drop(columns=[left_pos, right_pos])


def split_basins(basins_gdf: GeoDataFrame, lines_gdf: GeoDataFrame) -> GeoDataFrame:
    """Split basins by linestrings.

    `basins_gdf` contains basin polygons. `lines_gdf` contains lines to split basins on.

    Be aware (!), end-points of linestrings should be outside the boundary of the basin to split so shapely will find
    two intersection-points. Better not to snap these end-points ón the basin boundary.

    Parameters
    ----------
    basins_gdf : GeoDataFrame
        GeoDataFrame with basins to split
    lines_gdf : GeoDataFrame
        GeoDataFrame with lines to split basins on

    Returns
    -------
    GeoDataFrame
        Split basins
    """
    for line in lines_gdf.explode(index_parts=False).itertuples():
        # filter by spatial index
        idx = np.sort(basins_gdf.sindex.intersection(line.geometry.bounds))
        poly_select_gdf = basins_gdf.iloc[idx][basins_gdf.iloc[idx].intersects(line.geometry)]

        ## filter by intersecting geometry
        poly_select_gdf = poly_select_gdf[poly_select_gdf.intersects(line.geometry)]

        ## filter polygons with two intersection-points only
        poly_select_gdf = poly_select_gdf[
            poly_select_gdf.geometry.boundary.intersection(line.geometry).apply(lambda x: x.geom_type != "Point")
        ]

        ## if there are no polygon-candidates, something is wrong
        if poly_select_gdf.empty:
            print(f"no intersect for {line}. Please make sure it is extended outside the basin on two sides")
            continue
        else:
            ## we create new features
            data = []
            for basin in poly_select_gdf.itertuples():
                kwargs = basin._asdict()
                try:
                    for geom in split_basin(basin.geometry, line.geometry).geoms:
                        kwargs["geometry"] = geom
                        data += [{**kwargs}]
                except ValueError as e:
                    raise ValueError(
                        f"Basin with index {basin.Index} can not be cut by line with index {line.Index} raising Exception: {e}"
                    ) from e

        ## we update basins_gdf with new polygons
        basins_gdf = basins_gdf[~basins_gdf.index.isin(poly_select_gdf.index)]
        basins_gdf = pd.concat(
            [basins_gdf, gpd.GeoDataFrame(data, crs=basins_gdf.crs).set_index("Index")],
            ignore_index=True,
        )
    return basins_gdf


def snap_line_boundaries(gdf: GeoDataFrame, tolerance: float) -> GeoDataFrame:
    """Snap the boundaries of a linestring geodataframe to the other boundaries, or lines within the set that are within tolerance"""
    _gdf = gdf.copy()
    for position in range(len(_gdf)):
        # lines snapped in earlier iterations are used in their snapped form
        line = _gdf.geometry.array[position]

        # select other lines that are within tolerance, sorted since spatial index order differs between platforms
        candidates = np.sort(_gdf.sindex.intersection(line.buffer(tolerance).bounds))
        candidate_lines = np.asarray(_gdf.geometry.array[candidates])
        distance = shapely.distance(candidate_lines, line)
        within_tolerance = (distance < tolerance) & (distance > 0)

        # snap boundaries of other lines to this line
        for candidate, other_line in zip(candidates[within_tolerance], candidate_lines[within_tolerance], strict=True):
            geometry = snap_boundaries_to_other_line(line=other_line, other_line=line, tolerance=tolerance)
            # set by label, which updates all parts of exploded multi-lines
            _gdf.loc[_gdf.index[candidate], "geometry"] = geometry

    return _gdf


def _threaded_overlay_difference(df1: GeoDataFrame, df2: GeoDataFrame) -> GeoDataFrame:
    """`geopandas.tools.overlay._overlay_difference`, with the differences of the rows computed in threads.

    The geometry operations and their order per row are the same, so the result is identical. Each row subtracts
    its intersecting geometries one by one, which can take minutes for a row with hundreds of them.
    Shapely releases the GIL, so threads run these in parallel.
    """
    idx1, idx2 = df2.sindex.query(df1.geometry, predicate="intersects", sort=True)
    neighbours_by_row = np.split(idx2, np.searchsorted(idx1, np.arange(1, len(df1))))
    geoms1, geoms2 = df1.geometry.array, df2.geometry.array

    def difference(row: int) -> shapely.Geometry:
        return reduce(lambda x, y: x.difference(y), [geoms1[row], *geoms2[neighbours_by_row[row]]])

    # submit the rows with the most neighbours first, so the slowest rows don't end up last
    rows = sorted(range(len(df1)), key=lambda row: -len(neighbours_by_row[row]))
    with ThreadPoolExecutor(max_workers=os.process_cpu_count()) as executor:
        futures = {row: executor.submit(difference, row) for row in rows}
        new_g = [futures[row].result() for row in range(len(df1))]

    differences = gpd.GeoSeries(new_g, index=df1.index, crs=df1.crs)
    poly_ix = differences.geom_type.isin(gpd_overlay.POLYGON_GEOM_TYPES)
    differences.loc[poly_ix] = differences[poly_ix].make_valid()
    geom_diff = differences[~differences.is_empty].copy()
    dfdiff = df1[~differences.is_empty].copy()
    dfdiff[dfdiff._geometry_column_name] = geom_diff
    return dfdiff


_threaded_overlay_lock = threading.Lock()


@contextmanager
def _threaded_overlay() -> Iterator[None]:
    """Let `gpd.overlay` compute differences in threads, see `_threaded_overlay_difference`."""
    with _threaded_overlay_lock:
        original = gpd_overlay._overlay_difference
        gpd_overlay._overlay_difference = _threaded_overlay_difference
        try:
            yield
        finally:
            gpd_overlay._overlay_difference = original


def threaded_overlay(df1: GeoDataFrame, df2: GeoDataFrame, **kwargs) -> GeoDataFrame:
    """`gpd.overlay(df1, df2, **kwargs)` with identical results, computing the differences of rows in threads.

    This is much faster for `how` "union", "difference", "symmetric_difference" or "identity" when rows intersect
    many geometries of the other frame.
    """
    with _threaded_overlay():
        return gpd.overlay(df1, df2, **kwargs)


def assign_node_ids_by_largest_overlap(
    areas_gdf: GeoDataFrame, basin_area_gdf: GeoDataFrame, code_col: str = "code"
) -> GeoDataFrame:
    """Basin areas from detailed areas, giving each area code the node_id of the basin area it overlaps most.

    Parts of the union of both that don't overlap a basin area also get the node_id of their area code.
    The result is dissolved by node_id.

    Args:
        areas_gdf: detailed areas, with an area code
        basin_area_gdf: basin areas, with a node_id
        code_col: column in `areas_gdf` with the area code

    Returns
    -------
        GeoDataFrame with columns node_id and geometry
    """
    combined_gdf = threaded_overlay(areas_gdf, basin_area_gdf, how="union", keep_geom_type=True).explode()
    combined_gdf["area"] = combined_gdf.geometry.area

    # node_id of the largest overlap per area code
    non_null_gdf = combined_gdf[combined_gdf["node_id"].notna()]
    largest_area_node_ids = non_null_gdf.loc[non_null_gdf.groupby(code_col)["area"].idxmax(), [code_col, "node_id"]]

    combined_gdf = combined_gdf.merge(largest_area_node_ids, on=code_col, how="left", suffixes=("", "_largest"))
    combined_gdf["node_id"] = combined_gdf["node_id"].fillna(combined_gdf["node_id_largest"])
    combined_gdf = combined_gdf.drop(columns=["node_id_largest"]).drop_duplicates()
    combined_gdf = combined_gdf.dissolve(by="node_id").reset_index()[["node_id", "geometry"]]
    combined_gdf.index.name = "fid"
    return combined_gdf
