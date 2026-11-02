from typing import cast

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from geopandas import GeoDataFrame
from shapely.geometry import LineString

from ribasim_nl.geometry import snap_boundaries_to_other_line, split_basin


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
        line_geometry = cast(LineString, line.geometry)
        # filter by spatial index
        idx = np.sort(basins_gdf.sindex.intersection(line_geometry.bounds))
        poly_select_gdf = basins_gdf.iloc[idx][basins_gdf.iloc[idx].intersects(line_geometry)]

        ## filter by intersecting geometry
        poly_select_gdf = poly_select_gdf[poly_select_gdf.intersects(line_geometry)]

        ## filter polygons with two intersection-points only
        poly_select_gdf = poly_select_gdf[
            poly_select_gdf.geometry.boundary.intersection(line_geometry).apply(lambda x: x.geom_type != "Point")
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
                    for geom in split_basin(basin.geometry, line_geometry).geoms:
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
            _gdf.loc[_gdf.index[candidate], "geometry"] = geometry  # ty: ignore[invalid-assignment, invalid-argument-type]

    return _gdf
