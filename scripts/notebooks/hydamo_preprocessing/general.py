import datetime
import itertools
import logging
import time
import warnings

import geopandas as gpd
import pandas as pd
from shapely.geometry import LineString, Point

# %% Monitoring


def report_time_interval(start: datetime.datetime, end: datetime.datetime) -> str:
    """
    Report time interval between a given start and end time

    Args:
        start (datetime.datetime): start time
        end (datetime.datetime): end time

    Returns
    -------
        str: Report of time interval in hours, minu|tes and seconds
    """
    temp = end - start
    hours = temp // 3600
    temp = temp - 3600 * hours
    minutes = int(temp // 60)
    seconds = round(temp - 60 * minutes, 2)
    passed_time = ""
    if hours != 0:
        passed_time += f"{hours} hour(s), "
    if minutes != 0:
        passed_time += f"{minutes} minute(s) and "

    passed_time += f"{seconds} seconds"
    return passed_time


# %% Extended geospatial vector operations


def get_endpoints_from_lines(lines: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Extract all unique endpoints of line features from vector data

    Args:
        lines (gpd.GeoDataFrame): GeoDataFrame containing line features

    Returns
    -------
        gpd.GeoDataFrame: GeoDataFrame containing all unique endpoints from
        line features
    """
    lines[["startpoint", "endpoint"]] = lines["geometry"].apply(lambda x: pd.Series([x.coords[0], x.coords[-1]]))
    endpoints = pd.unique(lines[["startpoint", "endpoint"]].values.ravel("K"))
    endpoints = gpd.GeoDataFrame({"coordinates": endpoints})
    endpoints["starting_lines"] = endpoints["coordinates"].apply(
        lambda x: lines["code"][lines["startpoint"] == x].values
    )
    endpoints["ending_lines"] = endpoints["coordinates"].apply(lambda x: lines["code"][lines["endpoint"] == x].values)
    endpoints["starting_line_count"] = endpoints.apply(lambda x: len(list(x["starting_lines"])), axis=1)
    endpoints["ending_line_count"] = endpoints.apply(lambda x: len(list(x["ending_lines"])), axis=1)
    endpoints["connected_line_count"] = endpoints.apply(
        lambda x: x["starting_line_count"] + x["ending_line_count"], axis=1
    )
    endpoints_geometry = endpoints.coordinates.apply(lambda x: Point(x))
    endpoints = endpoints.set_geometry(endpoints_geometry)
    return endpoints


def add_point_to_linestring(point: Point, linestring: LineString) -> LineString:
    """
    Inserts point into a linestring, placing the point next to its nearest neighboring point in a way that minimizes the total length of the linestring.

    Args:
        point (Point): point
        linestring (LineString): linestring

    Returns
    -------
        LineString: resulting linestring
    """
    distances = [point.distance(Point(line_point)) for line_point in linestring.coords]
    index_nearest_neighbour = distances.index(min(distances))
    modified_linestring1 = LineString(
        list(linestring.coords)[: index_nearest_neighbour + 1]
        + [point.coords[0]]
        + list(linestring.coords)[index_nearest_neighbour + 1 :]
    )
    modified_linestring2 = LineString(
        list(linestring.coords)[:index_nearest_neighbour]
        + [point.coords[0]]
        + list(linestring.coords)[index_nearest_neighbour:]
    )
    modified_linestring = (
        modified_linestring1 if modified_linestring1.length < modified_linestring2.length else modified_linestring2
    )
    return (modified_linestring, index_nearest_neighbour)


def split_linestring_by_indices(linestring: LineString, split_indices: list) -> list:
    """
    Divides a linestring into multiple linestrings based on a list that contains the indices of the split points within the linestring.

    Args:
        linestring (LineString): Linestring
        split_indices (list): List of indices of split nodes within the linestring

    Returns
    -------
        list: list of resulting linestrings
    """
    split_linestrings = []
    split_indices = sorted(set([0] + split_indices + [len(linestring.coords) - 1]))
    for i in range(len(split_indices) - 1):
        split_linestrings.append(LineString(linestring.coords[split_indices[i] : split_indices[i + 1] + 1]))

    return split_linestrings


def remove_duplicate_split_lines(lines: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Removes duplicates of split lines from line feature vector dataset.

    Duplicates are removed from a subselection from the line feature dataset that contains
    all line features that have been split.

    Args:
        lines (gpd.GeoDataFrame): Vector data containing line features

    Returns
    -------
        gpd.GeoDataFrame: Vector data containing line features without duplicates
    """
    lines["distance"] = [x.length for x in lines["geometry"]]
    updated_endpoints = get_endpoints_from_lines(lines)
    ending_lines_clean_start = updated_endpoints[(updated_endpoints["starting_lines"].str.len() > 1)]["starting_lines"]
    ending_lines_clean_start = list(itertools.chain.from_iterable(ending_lines_clean_start))
    ending_lines_clean_end = updated_endpoints[(updated_endpoints["ending_lines"].str.len() > 1)]["ending_lines"]
    ending_lines_clean_end = list(itertools.chain.from_iterable(ending_lines_clean_end))
    lines_to_remove = list(pd.unique(pd.Series(ending_lines_clean_start + ending_lines_clean_end)))
    lines = lines[
        ~(
            (lines["distance"] <= 0.5)
            & (lines["preprocessing_split"] == "Opgeknipt")
            & (lines["code"].isin(lines_to_remove))
        )
    ].reset_index(drop=True)

    return lines


def connect_lines_by_endpoints(split_endpoints: gpd.GeoDataFrame, lines: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """
    Connects boundary lines to other lines, based on instructions for each line endpoint.

    Connections are created by inserting the endpoints into their target lines. The target line features are
    split afterwards in order to create nodes at the intersection of the connected linestrings.

    Args:
        split_endpoints (gpd.GeoDataFrame): Dataframe containing line endpoints and instructions
        lines (list): Vector Dataset containing line features

    Returns
    -------
        gpd.GeoDataFrame: line feature dataframe
    """
    listed_lines = list(itertools.chain.from_iterable(split_endpoints["target_lines"]))
    listed_points = list(itertools.chain.from_iterable(split_endpoints["points_to_target_lines"]))
    connections_to_create = pd.DataFrame({"lines": listed_lines, "point": listed_points})
    connections_to_create["inserted"] = False
    grouped_split_points_by_line = connections_to_create.groupby("lines")["point"]
    splits = [{"split_line": line, "split_points": list(points)} for line, points in grouped_split_points_by_line]

    # A dataframe is created to store the resulting linestrings from the splits
    split_lines = gpd.GeoDataFrame(columns=lines.columns)
    split_lines["preprocessing_split"] = None

    for split_action in splits:
        split_link = split_action["split_line"]
        line = lines[lines["code"] == split_link]
        linestring = line["geometry"].values[0]
        nodes_to_add = []
        for node in split_action["split_points"]:
            (modified_linestring, index_nearest_neighbour) = add_point_to_linestring(Point(node), linestring)

            if index_nearest_neighbour == 0 and linestring.coords[0] in list(
                connections_to_create.loc[connections_to_create["inserted"], "point"].values
            ):
                continue

            elif index_nearest_neighbour == len(linestring.coords) - 1 and linestring.coords[-1] in list(
                connections_to_create.loc[connections_to_create["inserted"], "point"].values
            ):
                continue

            linestring = modified_linestring

            connections_to_create["inserted"][connections_to_create["point"] == node] = True

            nodes_to_add += [node]

        # The modified line will be divided in seperate linestrings
        split_indices = [list(linestring.coords).index(node) for node in nodes_to_add]
        split_linestrings = split_linestring_by_indices(linestring, split_indices)

        # Append split linestrings to collection of new lines
        for k, split_linestring in enumerate(split_linestrings):
            snip_line = line.copy()
            snip_line["geometry"] = split_linestring
            snip_line["preprocessing_split"] = "Opgeknipt"
            snip_line["code"] = f"{snip_line['code'].values[0]}-{k}"
            split_lines = pd.concat([split_lines, snip_line], axis=0, join="inner")

    # Remove lines that have been devided from original geodataframe, and append resulting lines
    uneditted_lines = lines[~lines["code"].isin(connections_to_create["lines"])]
    connected_lines = pd.concat([uneditted_lines, split_lines], axis=0, join="outer").reset_index(drop=True)

    # Remove excessive split lines
    lines = remove_duplicate_split_lines(connected_lines)
    return lines


def connect_endpoints_by_buffer(lines: gpd.GeoDataFrame, buffer_distance: float = 0.5) -> gpd.GeoDataFrame:
    """
    Connects boundary line endpoints to neighbouring lines that pass within a specified buffer distance with respect to the the boundary endpoints.

    The boundary endpoints are inserted into the passing linestrings

    Args:
        lines (gpd.GeoDataFrame): Line vector data
        buffer distance (float): Buffer distance for connecting line boundary endpoints, expressed
        in the distance unit of vector line dataset

    Returns
    -------
        gpd.Geo: list of resulting linestrings
    """
    warnings.filterwarnings("ignore")
    start_time = time.time()
    iterations = 0
    unconnected_endpoints_count = 0
    finished = False

    logging.info(f"Detect unconnected endpoints nearby linestrings, buffer distance: {buffer_distance}m")

    while not finished:
        endpoints = get_endpoints_from_lines(lines)

        boundary_endpoints = gpd.GeoDataFrame(
            endpoints[(endpoints["starting_line_count"] == 0) | (endpoints["ending_line_count"] == 0)]
        )
        lines["buffer_geometry"] = lines.geometry.buffer(buffer_distance, join_style="round")

        boundary_endpoints["overlaying_line_buffers"] = [
            lines[lines.buffer_geometry.contains(x)].code.tolist() for x in boundary_endpoints.geometry
        ]
        boundary_endpoints["startpoint_overlaying_line_buffers"] = boundary_endpoints.apply(
            lambda x: [
                x["coordinates"] in list(lines[lines.code == y].endpoint.values) for y in x["overlaying_line_buffers"]
            ],
            axis=1,
        )
        boundary_endpoints["endpoint_overlaying_line_buffers"] = boundary_endpoints.apply(
            lambda x: [
                x["coordinates"] in list(lines[lines.code == y].startpoint.values) for y in x["overlaying_line_buffers"]
            ],
            axis=1,
        )
        boundary_endpoints["start_or_endpoint_overlaying_line_buffers"] = boundary_endpoints.apply(
            lambda x: list(zip(x["startpoint_overlaying_line_buffers"], x["endpoint_overlaying_line_buffers"])), axis=1
        )
        boundary_endpoints["crossed_by_unconnected_lines"] = boundary_endpoints.apply(
            lambda x: True in [True not in y for y in x["start_or_endpoint_overlaying_line_buffers"]], axis=1
        )
        unconnected_endpoints = boundary_endpoints[boundary_endpoints["crossed_by_unconnected_lines"]].reset_index(
            drop=True
        )
        unconnected_endpoints["target_lines"] = unconnected_endpoints.apply(
            lambda x: [
                x["overlaying_line_buffers"][i]
                for i in range(len(x["overlaying_line_buffers"]))
                if x["start_or_endpoint_overlaying_line_buffers"][i] == (False, False)
            ],
            axis=1,
        )
        unconnected_endpoints["points_to_target_lines"] = unconnected_endpoints.apply(
            lambda x: [x["coordinates"]] * len(x["target_lines"]), axis=1
        )
        previous_unconnected_endpoints_count = unconnected_endpoints_count
        unconnected_endpoints_count = len(unconnected_endpoints)
        if iterations == 0:
            unconnected_endpoints_count_total = unconnected_endpoints_count
        logging.info(f"{unconnected_endpoints_count} unconnected endpoints detected nearby intersecting lines")
        if unconnected_endpoints_count != 0 and unconnected_endpoints_count != previous_unconnected_endpoints_count:
            logging.info("Connecting linestrings...")
            lines = connect_lines_by_endpoints(unconnected_endpoints, lines)
            iterations += 1
            logging.info("Linestrings connected, starting new iteration...")
        else:
            lines = lines.drop(["startpoint", "endpoint", "buffer_geometry"], axis=1)
            finished = True

    end_time = time.time()
    passed_time = report_time_interval(start_time, end_time)
    logging.info(
        f"Summary:\n\n\
          Detected unconnected endpoints nearby intersecting lines: {unconnected_endpoints_count_total} \n\
          Connected endpoints: {unconnected_endpoints_count_total - unconnected_endpoints_count} \n\
          Remaining unconnected endpoints: {unconnected_endpoints_count}\n\
          Iterations: {iterations}"
    )
    logging.info(f"Finished within {passed_time}")
    return lines


def add_overlapping_polygons(
    left_geodataframe: gpd.GeoDataFrame,
    right_geodataframe: gpd.GeoDataFrame,
    left_id_column: str,
    right_id_column: str,
):
    """
    Create a column in a left geodataframe where it lists the overlapping polygons from the right geodataframe for each polygon in the left geodataframe.

    The id columns of the right and left dataframe have to be defined.

    Parameters
    ----------
    left_geodataframe : gpd.GeoDataFrame()
       the left geodataframe
    right_geodataframe : gpd.GeoDataFrame()
        the right geodataframe
    left_id_column : str
        the name of the ID column in the left geodataframe
    right_id_column : str
        the name of the ID column in the right geodataframe,
        from which the values will be added to the left geodataframe

    Returns
    -------
    left_geodataframe : TYPE
        the updated left geodataframe with an added column which contains
        insights in the overlapping polygons from the right dataframe

    """
    # Calculate total areas of left and right polygons
    left_geodataframe["left_area"] = left_geodataframe["geometry"].apply(lambda x: x.area)
    right_geodataframe["surface_area"] = right_geodataframe.area
    right_geodataframe["right_geometry"] = right_geodataframe["geometry"]

    # Join left and right polygons
    joined = gpd.sjoin(left_geodataframe, right_geodataframe, how="left", op="intersects")
    joined = joined.loc[:, ~joined.columns.duplicated()].copy()

    # Get overlapping right polygon ids, polygons & areas for left polygons
    grouped = pd.DataFrame(joined.groupby(left_id_column)[right_id_column].unique().reset_index(name=right_id_column))
    grouped["right_geometry"] = (
        joined.groupby(left_id_column)["right_geometry"].unique().reset_index(name="right_geometry")["right_geometry"]
    )
    grouped["right_area"] = (
        joined.groupby(left_id_column)["surface_area"].unique().reset_index(name="right_area")["right_area"]
    )

    # Drop NA values from overlapping polygon info columns
    grouped[right_id_column] = grouped[right_id_column].apply(lambda x: pd.Series(x).dropna().tolist())
    grouped["right_area"] = grouped["right_area"].apply(lambda x: pd.Series(x).dropna().tolist())
    grouped["right_geometry"] = grouped["right_geometry"].apply(lambda x: pd.Series(x).dropna().tolist())

    # Merge
    left_geodataframe = left_geodataframe.merge(
        grouped[[left_id_column, right_id_column, "right_geometry", "right_area"]], on=left_id_column, how="left"
    )

    # Postprocessing
    left_geodataframe["overlapping_areas"] = left_geodataframe.apply(
        lambda x: [y.intersection(x["geometry"]).area for y in x["right_geometry"]], axis=1
    )
    left_geodataframe["overlapping_areas"] = left_geodataframe.apply(
        lambda x: [
            {
                "id": x[right_id_column][i],
                "right_area": x["right_area"][i],
                "overlapping_area": x["overlapping_areas"][i],
                "intersection_length": x["right_geometry"][i].intersection(x["geometry"]).length,
            }
            for i in range(len(x["right_area"]))
        ],
        axis=1,
    )
    left_geodataframe = left_geodataframe.drop(columns=["right_area", "right_geometry"])

    return left_geodataframe


def get_most_overlapping_polygon(
    left_geodataframe: gpd.GeoDataFrame(),
    right_geodataframe: gpd.GeoDataFrame(),
    left_id_column: str,
    right_id_column: str,
):
    """
    Create a column in a left geodataframe that contains IDs of the most overlapping polygon from the right geodataframe based on their geometries.

    The id columns of the left and right dataframe have to be defined.

    Parameters
    ----------
    left_geodataframe : gpd.GeoDataFrame()
       the left geodataframe
    right_geodataframe : gpd.GeoDataFrame()
        the right geodataframe
    left_id_column : str
        the name of the ID column in the left geodataframe
    right_id_column : str
        the name of the ID column in the right geodataframe,
        from which the values will be added to the left geodataframe

    Returns
    -------
    left_geodataframe : TYPE
        the updated left geodataframe

    """
    left_geodataframe = add_overlapping_polygons(left_geodataframe, right_geodataframe, left_id_column, right_id_column)

    left_geodataframe["overlapping_areas"] = left_geodataframe["overlapping_areas"].apply(lambda x: pd.DataFrame(x))

    left_geodataframe["most_overlapping_polygon_id"] = left_geodataframe.apply(
        lambda x: x["overlapping_areas"][
            x["overlapping_areas"]["overlapping_area"] == x["overlapping_areas"]["overlapping_area"].max()
        ]["id"].values[0]
        if len(x["overlapping_areas"]) != 0
        else None,
        axis=1,
    )

    left_geodataframe["most_overlapping_polygon_area"] = left_geodataframe.apply(
        lambda x: x["overlapping_areas"][
            x["overlapping_areas"]["overlapping_area"] == x["overlapping_areas"]["overlapping_area"].max()
        ]["overlapping_area"].values[0]
        if len(x["overlapping_areas"]) != 0
        else None,
        axis=1,
    )

    return left_geodataframe


def get_polygon_with_largest_area(polygons, id_col, area_col):
    if len(polygons) == 0:
        return 0
    else:
        polygons[area_col] = polygons.area
        polygons = polygons[polygons[area_col] == max(polygons[area_col])]
        return polygons[id_col].values[0]


def get_most_overlapping_polygon_from_other_gdf(left_gdf, right_gdf, left_id, right_id):
    if right_id in left_gdf.columns:
        future_right_id = f"{right_id}_2"
        future_left_id = f"{left_id}_1"
    else:
        future_right_id = right_id
        future_left_id = left_id
    combined_gdf = left_gdf.overlay(right_gdf, how="intersection")
    combined_gdf["area_geometry"] = combined_gdf.area
    left_gdf["overlapping_polygons"] = left_gdf[left_id].apply(
        lambda x: combined_gdf[combined_gdf[future_left_id] == x]
    )
    left_gdf["right_id"] = left_gdf["overlapping_polygons"].apply(
        lambda x: get_polygon_with_largest_area(x, future_right_id, "area_geometry")
    )
    left_gdf = left_gdf.drop(columns=["overlapping_polygons"])
    return left_gdf


def get_touching_polygons_from_within_gdf(gdf, id_col):
    id_col_right = f"{id_col}_2"
    right_gdf, left_gdf = gdf.copy(), gdf.copy()
    right_gdf[id_col_right] = right_gdf[id_col]
    right_gdf = right_gdf.drop(columns=[id_col])
    joined_gdf = left_gdf.sjoin(right_gdf, predicate="touches")
    gdf["touching_polygons"] = gdf[id_col].apply(
        lambda x: list(joined_gdf[id_col_right][(joined_gdf[id_col] == x) & (joined_gdf[id_col_right] != x)].unique())
    )
    return gdf


def get_most_adjacent_polygon_within_gdf(left_gdf, left_id, right_gdf=None, right_id=None):
    def get_most_intersecting(gdf, polygon, left_id):
        try:
            gdf = gpd.GeoDataFrame(gdf.drop(columns="geometry"), geometry=gdf["geometry"])
            gdf["geomtry"] = gdf.buffer(0.5)
            gdf["intersection_length"] = gdf["geometry"].apply(lambda x: x.intersection(polygon).boundary.length)
            most_overlapping_polygon = gdf[left_id][
                gdf["intersection_length"] == gdf["intersection_length"].max()
            ].values[0]
            return most_overlapping_polygon
        except Exception:
            return None

    left_gdf = get_touching_polygons_from_within_gdf(left_gdf, left_id)
    if isinstance(right_gdf, gpd.GeoDataFrame):
        left_gdf = get_most_overlapping_polygon_from_other_gdf(left_gdf, right_gdf, left_id, right_id)
        left_gdf["right_id"][left_gdf[left_id] == left_gdf["right_id"]] = None
    left_gdf["touching_polygons"] = left_gdf["touching_polygons"].apply(
        lambda x: pd.DataFrame(left_gdf[left_gdf[left_id].isin(x)])
    )
    left_gdf["touching_polygons"] = left_gdf["touching_polygons"].apply(lambda x: x[x["basin"] is not None])
    left_gdf["touching_polygons"] = left_gdf["touching_polygons"].apply(lambda x: x[x["basin"].notna()])
    if isinstance(right_gdf, gpd.GeoDataFrame):
        left_gdf["touching_polygons"] = left_gdf.apply(
            lambda x: x["touching_polygons"][x["touching_polygons"]["right_id"] == x["right_id"]]
            if x["right_id"] is not None
            else x["touching_polygons"],
            axis=1,
        )
    left_gdf["most_adjacent_polygon"] = left_gdf.apply(
        lambda x: get_most_intersecting(x["touching_polygons"], x["geometry"], left_id), axis=1
    )
    left_gdf = left_gdf.drop(columns=["touching_polygons"])
    return left_gdf
