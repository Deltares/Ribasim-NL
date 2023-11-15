from typing import Literal, Union

import geopandas as gpd
import pandas as pd
from geopandas import GeoDataFrame

from ribasim_nl.geometry import split_basin


def join_by_poly_overlay(
    gdf: GeoDataFrame,
    join_poly_gdf: GeoDataFrame,
    select_by: Union[Literal["poly_area"], None] = None,
) -> GeoDataFrame:
    """Join attributes from join_polygon_gdf to gdf.

    Parameters
    ----------
    gdf : GeoDataFrame
        the GeoDataFrame to join to
    join_poly_gdf : GeoDataFrame
        the join attributes from
    select_by : Union[Literal[&quot;poly_area&quot;], None], optional
        by default an overlay will be returned, effectively cutting all geometries in gdf by the geometries
        in join_poly_gdf. When select_by = "poly_area", the index of gdf will be maintained and a join will be made
        by maximum overlapping area. By default None

    Returns
    -------
    GeoDataFrame
    """
    # remove columns from gdf that are in right_column
    gdf = gdf.copy()
    join_columns = [i for i in join_poly_gdf.columns if i != "geometry"]
    gdf = gdf[[i for i in gdf.columns if i not in join_columns]]

    # columns in output file
    columns = list(gdf.columns) + join_columns
    gdf["_left_index"] = gdf.index

    # create overlay
    overlay_gdf = gpd.overlay(gdf, join_poly_gdf, how="intersection")

    # join to left
    if select_by == "poly_area":
        overlay_gdf["poly_area"] = overlay_gdf.geometry.area
        overlay_gdf.sort_values(by="poly_area", inplace=True)
        overlay_gdf.drop_duplicates(subset="_left_index", keep="last", inplace=True)
        overlay_gdf.sort_values(by="_left_index", inplace=True)
        overlay_gdf.index = gdf.index
        overlay_gdf = overlay_gdf[columns]
        overlay_gdf.loc[:, ["geometry"]] = gdf.geometry
    else:
        overlay_gdf = overlay_gdf[columns]

    return overlay_gdf


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
        poly_select_gdf = basins_gdf.iloc[idx][
            basins_gdf.iloc[idx].intersects(line.geometry)
        ]

        ## filter by intersecting geometry
        poly_select_gdf = poly_select_gdf[poly_select_gdf.intersects(line.geometry)]

        ## filter polygons with two intersection-points only
        poly_select_gdf = poly_select_gdf[
            poly_select_gdf.geometry.boundary.intersection(line.geometry).apply(
                lambda x: False if x.geom_type == "Point" else len(x.geoms) == 2
            )
        ]

        ## if there are no polygon-candidates, something is wrong
        if poly_select_gdf.empty:
            print(
                f"no intersect for {line}. Please make sure it is extended outside the basin on two sides"
            )
        else:
            ## we create 2 new fatures in data
            data = []
            for basin in poly_select_gdf.itertuples():
                kwargs = basin._asdict()
                for geom in split_basin(basin.geometry, line.geometry).geoms:
                    kwargs["geometry"] = geom
                    data += [{**kwargs}]

        ## we update basins_gdf with new polygons
        basins_gdf = basins_gdf[~basins_gdf.index.isin(poly_select_gdf.index)]
        basins_gdf = pd.concat(
            [basins_gdf, gpd.GeoDataFrame(data, crs=basins_gdf.crs).set_index("Index")],
            ignore_index=True,
        )
    return basins_gdf
