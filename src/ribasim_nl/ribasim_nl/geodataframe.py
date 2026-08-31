import geopandas as gpd
import pandas as pd
from geopandas import GeoDataFrame

from ribasim_nl.geometry import snap_boundaries_to_other_line, split_basin


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
        idx = basins_gdf.sindex.intersection(line.geometry.bounds)
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
    for row in _gdf.itertuples():
        # row_tuples = _gdf.itertuples()
        # row = next(row_tuples)
        line = row.geometry
        # select other lines that are within tolerance
        other_lines = _gdf.iloc[_gdf.sindex.intersection(line.buffer(tolerance).bounds)]
        other_lines = other_lines[(other_lines.distance(line) < tolerance) & (other_lines.distance(line) > 0)]

        # snap boundaries of other lines to this line
        if not other_lines.empty:
            for other_row in other_lines.itertuples():
                geometry = snap_boundaries_to_other_line(line=other_row.geometry, other_line=line, tolerance=tolerance)
                _gdf.loc[other_row.Index, "geometry"] = geometry

    return _gdf
