"""All functions resulting in a geopandas.GeoDataFrame"""

from typing import Literal

import geopandas as gpd
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import MultiPolygon, Polygon
from shapely.ops import polylabel

from ribasim_nl.geometry import sort_basins, split_basin
from ribasim_nl.network import Network


def join_by_poly_overlay(
    gdf: GeoDataFrame,
    join_poly_gdf: GeoDataFrame,
    select_by: Literal["poly_area"] | None = None,
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
        # select only largest areas
        overlay_gdf["poly_area"] = overlay_gdf.geometry.area
        overlay_gdf.sort_values(by="poly_area", inplace=True)
        overlay_gdf.drop_duplicates(subset="_left_index", keep="last", inplace=True)
        # add geometries that did not have an overlay
        if len(gdf) != len(overlay_gdf):
            overlay_gdf = pd.concat([overlay_gdf, gdf[~gdf.index.isin(overlay_gdf["_left_index"])]])

        # clean columns and index
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
        poly_select_gdf = basins_gdf.iloc[idx][basins_gdf.iloc[idx].intersects(line.geometry)]

        ## filter by intersecting geometry
        poly_select_gdf = poly_select_gdf[poly_select_gdf.intersects(line.geometry)]

        ## filter polygons with two intersection-points only
        poly_select_gdf = poly_select_gdf[
            poly_select_gdf.geometry.boundary.intersection(line.geometry).apply(lambda x: not x.geom_type == "Point")
        ]

        ## if there are no polygon-candidates, something is wrong
        if poly_select_gdf.empty:
            print(f"no intersect for {line}. Please make sure it is extended outside the basin on two sides")
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
                    )

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
        intersecting_basins_gdf = basins_gdf.iloc[basins_gdf.sindex.intersection(line.bounds)]
        intersecting_basins_gdf = intersecting_basins_gdf[intersecting_basins_gdf["geometry"].intersects(line)]
        return intersecting_basins_gdf[basin_ident]

    def find_ident(point):
        """Find basin containing a Point"""
        # filter by spatial index
        containing_basins_gdf = basins_gdf.iloc[basins_gdf.sindex.intersection(point.bounds)]

        # filter by contain and take first row
        containing_basins_gdf = containing_basins_gdf[containing_basins_gdf["geometry"].contains(point)]

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
        intersecting_network_gdf = network_gdf.iloc[network_gdf.sindex.intersection(poly_row.geometry.bounds)]
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
            if ((us_ident is not None) and (ds_ident is not None)) and (us_ident != ds_ident):
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
        poly_directions_gdf.drop_duplicates(["us_basin", "ds_basin"], keep="first", inplace=True)

    return poly_directions_gdf


def basins_to_points(
    basins_gdf: GeoDataFrame,
    network: Network,
    mask: Polygon | None = None,
    buffer: int | None = None,
) -> GeoDataFrame:
    """Get points within a basin

    Parameters
    ----------
    basins_gdf : GeoDataFrame
        GeoDataFrame with Polygon basins
    network : Network
        Ribasim-NL network to snap points on
    mask : Polygon, optional
        Optional mask to clip basin, by default None
    buffer : int, optional
        Buffer to apply on basin in case no point is found, by default None

    Returns
    -------
    GeoDataFrame
        Points within basin on network
    """
    data = []
    if network is not None:
        links_gdf = network.links

        def select_links(geometry):
            idx = links_gdf.sindex.intersection(geometry.bounds)
            links_select_gdf = links_gdf.iloc[idx]
            links_select_gdf = links_select_gdf[links_select_gdf.within(geometry)]
            return links_select_gdf

    for row in basins_gdf.itertuples():
        # get basin_polygon and centroid
        basin_polygon = row.geometry
        point = basin_polygon.centroid
        node_id = None

        # get links within basin_polygon
        if network is not None:
            # we prefer to find selected links within mask
            if mask is not None:
                masked_basin_polygon = basin_polygon.intersection(mask)
                links_select_gdf = select_links(masked_basin_polygon)

            # if not we try to find links within polygon
            if links_select_gdf.empty:
                links_select_gdf = select_links(basin_polygon)

            # if still not we try to find links within polygon applying a buffer
            if links_select_gdf.empty and (buffer is not None):
                links_select_gdf = select_links(basin_polygon.buffer(buffer))

            # if we selected links, we snap to closest node
            if not links_select_gdf.empty:
                # get link maximum length
                link = links_select_gdf.loc[links_select_gdf.geometry.length.sort_values(ascending=False).index[0]]

                # get distance to upstream and downstream point in the link
                us_point, ds_point = link.geometry.boundary.geoms
                us_dist, ds_dist = (i.distance(point) for i in [us_point, ds_point])

                # choose closest point as basin point
                if us_dist < ds_dist:
                    node_id = getattr(link, "node_from")
                    point = us_point
                else:
                    node_id = getattr(link, "node_to")
                    point = ds_point

        # if we don't snap on network, we make sure point is within polygon
        elif not point.within(basin_polygon):
            # polylabel only works on polygons; we use largest polygon if input is MultiPolygon
            if isinstance(basin_polygon, MultiPolygon):
                basin_polygon = sort_basins(list(basin_polygon.geoms))[-1]
            point = polylabel(basin_polygon)

        attributes = {i: getattr(row, i) for i in basins_gdf.columns}
        attributes["geometry"] = point
        attributes["node_id"] = node_id
        data += [attributes]

    return gpd.GeoDataFrame(data, crs=basins_gdf.crs)
