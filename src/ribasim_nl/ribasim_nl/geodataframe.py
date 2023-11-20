"""All functions resulting in a geopandas.GeoDataFrame"""
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


def direct_basins(
    basins_gdf: GeoDataFrame,
    network_gdf: GeoDataFrame,
    basin_ident: str,
    link_ident: str,
    drop_duplicates: bool = True,
) -> GeoDataFrame:
    """Find directions of basins 'basins_gdf' (polygons) by 'network_gdf'(linestrings)

    Parameters
    ----------
    basins_gdf : GeoDataFrame
        GeoDataFrame with basins
    network_gdf : GeoDataFrame
        GeoDataFrame with network trough basins
    basin_ident : str
        column in basins_gdf to with unique values ending up in the result as 'us_basin' and 'ds_basin'
    link_ident : str
        column in network_gdf with unique values ending up in the result as 'link_ident'
    drop_duplicates : bool, optional
        Duplicate directions, multiple links between two basins will be removed, by default True

    Returns
    -------
    GeoDataFrame
        Selection of network_gdf representing links between basins in basins_gdf
    """

    def find_intersecting_basins(line):
        """Find intersecting basins on a LineString"""
        intersecting_basins_gdf = basins_gdf.iloc[
            basins_gdf.sindex.intersection(line.bounds)
        ]
        intersecting_basins_gdf = intersecting_basins_gdf[
            intersecting_basins_gdf["geometry"].intersects(line)
        ]
        return intersecting_basins_gdf[basin_ident]

    def find_ident(point):
        """Find basin containing a Point"""
        # filter by spatial index
        containing_basins_gdf = basins_gdf.iloc[
            basins_gdf.sindex.intersection(point.bounds)
        ]

        # filter by contain and take first row
        containing_basins_gdf = containing_basins_gdf[
            containing_basins_gdf["geometry"].contains(point)
        ]

        if containing_basins_gdf.empty:
            return None
        elif len(containing_basins_gdf) == 1:
            return getattr(containing_basins_gdf.iloc[0], basin_ident)
        else:
            print(containing_basins_gdf)

    # list of dicts to be filled with 'us_basin', 'ds_basin' and 'geometry'
    data = []

    # we iterate per basin to determine intersections
    for poly_row in basins_gdf.itertuples():
        # identification for the basin
        poly_ident = getattr(poly_row, basin_ident)

        ## select intersecting lines, use spatial index (and then exact intersection)
        intersecting_network_gdf = network_gdf.iloc[
            network_gdf.sindex.intersection(poly_row.geometry.bounds)
        ]
        intersecting_network_gdf = intersecting_network_gdf[
            intersecting_network_gdf["geometry"].intersects(poly_row.geometry.boundary)
        ]

        # iterate per link, to find basin directions
        for line_row in intersecting_network_gdf.itertuples():
            # find upstream and downstream points
            us_point, ds_point = line_row.geometry.boundary.geoms

            # find upstream and downstream basins
            us_ident = find_ident(us_point)
            ds_ident = find_ident(ds_point)

            # determine if/how a link should be added to data
            if ((us_ident is not None) and (ds_ident is not None)) and (
                us_ident != ds_ident
            ):
                link_id = getattr(line_row, link_ident)

                if poly_ident not in [us_ident, ds_ident]:
                    data += [
                        {
                            "link_ident": link_id,
                            "us_basin": us_ident,
                            "ds_basin": poly_ident,
                            "geometry": line_row.geometry,
                        },
                        {
                            "link_ident": link_id,
                            "us_basin": poly_ident,
                            "ds_basin": ds_ident,
                            "geometry": line_row.geometry,
                        },
                    ]
                elif len(find_intersecting_basins(line_row.geometry)) == 2:
                    data += [
                        {
                            "link_ident": link_id,
                            "us_basin": us_ident,
                            "ds_basin": ds_ident,
                            "geometry": line_row.geometry,
                        }
                    ]
                else:
                    print(f"we skip a case for link: '{link_id}'")

    poly_directions_gdf = gpd.GeoDataFrame(data, crs=basins_gdf.crs)

    # if we found duplicates, we may want to drop them
    if drop_duplicates:
        poly_directions_gdf.drop_duplicates(
            ["us_basin", "ds_basin"], keep="first", inplace=True
        )

    return poly_directions_gdf
