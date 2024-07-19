import geopandas as gpd
import pandas as pd
import networkx as nx
from shapely.geometry import Polygon, Point, LineString
from shapely.ops import snap, split
import itertools
import os
import time
import logging
import warnings
warnings.filterwarnings("ignore")


# General
def get_passed_time(start, end):
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


def get_endpoints_from_lines(lines):
    lines[["startpoint", "endpoint"]] = lines["geometry"].apply(
        lambda x: pd.Series([x.coords[0], x.coords[-1]])
    )
    endpoints = pd.unique(lines[["startpoint", "endpoint"]].values.ravel("K"))
    endpoints = gpd.GeoDataFrame({"coordinates": endpoints})
    endpoints["starting_lines"] = endpoints["coordinates"].apply(
        lambda x: lines["code"][lines["startpoint"] == x].values
    )
    endpoints["ending_lines"] = endpoints["coordinates"].apply(
        lambda x: lines["code"][lines["endpoint"] == x].values
    )

    endpoints["count_starting_lines"] = endpoints.apply(
        lambda x: len(list(x["starting_lines"])), axis=1
    )
    endpoints["count_ending_lines"] = endpoints.apply(
        lambda x: len(list(x["ending_lines"])), axis=1
    )
    endpoints["count"] = endpoints.apply(
        lambda x: x["count_starting_lines"] + x["count_ending_lines"], axis=1
    )
    endpoints = endpoints.set_geometry(endpoints.coordinates.apply(lambda x: Point(x)))
    return endpoints


def snap_connect_lines_by_endpoints(split_endpoints, lines):
    connections_to_create = pd.DataFrame(
        {
            "lines": list(
                itertools.chain.from_iterable(split_endpoints["target_lines"])
            ),
            "point": list(
                itertools.chain.from_iterable(split_endpoints["points_to_target_lines"])
            ),
        }
    )

    connections_to_create["inserted"] = False

    splits = [
        {
            "split_line": x,
            "split_points": list(
                connections_to_create[connections_to_create["lines"] == x][
                    "point"
                ].values
            ),
        }
        for x in pd.unique(connections_to_create["lines"])
    ]

    split_lines = gpd.GeoDataFrame(columns=lines.columns)

    split_lines["preprocessing_split"] = None

    for split_action in splits:
        split_edge = split_action["split_line"]
        line = lines[lines["code"] == split_edge]

        # if split_edge == 'OAF-H-02041':
        #     break

        linestring = line["geometry"].values[0]
        segments = []
        nodes_to_add = []
        for node in split_action["split_points"]:
            # break

            distances = [
                Point(node).distance(Point(point)) for point in list(linestring.coords)
            ]

            dist1pos = distances.index(min(distances))

            if dist1pos == 0:
                if linestring.coords[0] in list(connections_to_create["point"].values):
                    if (
                        connections_to_create["inserted"][
                            connections_to_create["point"] == linestring.coords[0]
                        ].values[0]
                        == True
                    ):
                        continue

            elif dist1pos == len(linestring.coords) - 1:
                if linestring.coords[len(linestring.coords) - 1] in list(
                    connections_to_create["point"].values
                ):
                    if (
                        connections_to_create["inserted"][
                            connections_to_create["point"]
                            == linestring.coords[len(linestring.coords) - 1]
                        ].values[0]
                        == True
                    ):
                        continue
            # else:

            linestring1 = LineString(
                list(linestring.coords)[: dist1pos + 1]
                + [Point(node).coords[0]]
                + list(linestring.coords)[dist1pos + 1 :]
            )

            linestring2 = LineString(
                list(linestring.coords)[:dist1pos]
                + [Point(node).coords[0]]
                + list(linestring.coords)[dist1pos:]
            )

            linestring = (
                linestring1 if linestring1.length < linestring2.length else linestring2
            )

            connections_to_create["inserted"][
                connections_to_create["point"] == node
            ] = True

            nodes_to_add += [node]

        split_indices = sorted(
            list(
                set(
                    [0]
                    + [list(linestring.coords).index(node) for node in nodes_to_add]
                    + [len(linestring.coords) - 1]
                )
            )
        )

        for i in range(len(split_indices) - 1):
            segments.append(
                LineString(
                    linestring.coords[split_indices[i] : split_indices[i + 1] + 1]
                )
            )

        for k, segment in enumerate(segments):
            snip_line = line.copy()
            snip_line["geometry"] = segment
            snip_line["preprocessing_split"] = "Opgeknipt"
            snip_line["code"] = f'{snip_line["code"].values[0]}-{k}'
            split_lines = pd.concat([split_lines, snip_line], axis=0, join="inner")

    uneditted_lines = lines[~lines["code"].isin(connections_to_create["lines"])]
    connected_lines = pd.concat([uneditted_lines, split_lines], axis=0, join="outer")
    connected_lines.index = range(len(connected_lines))

    connected_lines["distance"] = list(
        map(lambda x: x.length, connected_lines["geometry"])
    )

    updated_endpoints = get_endpoints_from_lines(connected_lines)

    ending_lines_clean_start = list(
        itertools.chain.from_iterable(
            updated_endpoints[(updated_endpoints["starting_lines"].str.len() > 1)][
                "starting_lines"
            ]
        )
    )

    ending_lines_clean_end = list(
        itertools.chain.from_iterable(
            updated_endpoints[(updated_endpoints["ending_lines"].str.len() > 1)][
                "ending_lines"
            ]
        )
    )
    lines_to_remove = list(
        pd.unique(pd.Series(ending_lines_clean_start + ending_lines_clean_end))
    )
    connected_lines = connected_lines[
        ~(
            (connected_lines["distance"] <= 0.5)
            & (connected_lines["preprocessing_split"] == "Opgeknipt")
            & (connected_lines["code"].isin(lines_to_remove))
        )
    ]
    connected_lines.index = range(len(connected_lines))
    lines = connected_lines
    return lines


def connect_endpoints_by_buffer(lines, buffer_distance=0.5):
    start_time = time.time()

    iterations = 0
    unconnected_endpoints_count = 0
    finished = False

    logging.info(
        f"Detect unconnected endpoints nearby linestrings, buffer distance: {buffer_distance}m"
    )

    while not finished:
        endpoints = get_endpoints_from_lines(lines)

        boundary_endpoints = gpd.GeoDataFrame(
            endpoints[
                (endpoints["count_starting_lines"] == 0)
                | (endpoints["count_ending_lines"] == 0)
            ]
        )

        lines["buffer_geometry"] = lines.geometry.buffer(
            buffer_distance, join_style="round"
        )

        boundary_endpoints["overlaying_line_buffers"] = list(
            map(
                lambda x: lines[lines.buffer_geometry.contains(x)].code.tolist(),
                boundary_endpoints.geometry,
            )
        )

        boundary_endpoints[
            "startpoint_overlaying_line_buffers"
        ] = boundary_endpoints.apply(
            lambda x: list(
                map(
                    lambda y: x["coordinates"]
                    in list(lines[lines.code == y].endpoint.values),
                    x["overlaying_line_buffers"],
                )
            ),
            axis=1,
        )

        boundary_endpoints[
            "endpoint_overlaying_line_buffers"
        ] = boundary_endpoints.apply(
            lambda x: list(
                map(
                    lambda y: x["coordinates"]
                    in list(lines[lines.code == y].startpoint.values),
                    x["overlaying_line_buffers"],
                )
            ),
            axis=1,
        )

        boundary_endpoints[
            "start_or_endpoint_overlaying_line_buffers"
        ] = boundary_endpoints.apply(
            lambda x: list(
                zip(
                    x["startpoint_overlaying_line_buffers"],
                    x["endpoint_overlaying_line_buffers"],
                )
            ),
            axis=1,
        )

        boundary_endpoints["crossed_by_unconnected_lines"] = boundary_endpoints.apply(
            lambda x: True
            in [True not in y for y in x["start_or_endpoint_overlaying_line_buffers"]],
            axis=1,
        )

        unconnected_endpoints = boundary_endpoints[
            boundary_endpoints["crossed_by_unconnected_lines"] == True
        ].reset_index(drop=True)

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

        logging.info(f"{unconnected_endpoints_count} unconnected endpoints detected")

        if (
            unconnected_endpoints_count != 0
            and unconnected_endpoints_count != previous_unconnected_endpoints_count
        ):
            logging.info("Snapping unconnected endpoints...")

            lines = snap_connect_lines_by_endpoints(unconnected_endpoints, lines)

            iterations += 1

            logging.info("Endpoints connected, starting new iteration...")

        else:
            lines = lines.drop(["startpoint", "endpoint", "buffer_geometry"], axis=1)

            finished = True

    end_time = time.time()

    passed_time = get_passed_time(start_time, end_time)

    logging.info(f"Finished within {passed_time}")

    logging.info(
        f"Summary:\n\n\
          Detected unconnected endpoints within buffer distance: {unconnected_endpoints_count_total} \n\
          Connected endpoints: {unconnected_endpoints_count_total-unconnected_endpoints_count} \n\
          Remaining unconnected endpoints: {unconnected_endpoints_count}\n\
          Iterations: {iterations}"
    )

    return lines


# %% Generate input data for network


def create_nodes_and_edges_from_hydroobjects(edges, buffer_distance=0.05):
    warnings.filterwarnings("ignore")

    edges[["from_node", "to_node"]] = edges["geometry"].apply(
        lambda x: pd.Series([x.coords[0], x.coords[-1]])
    )
    nodes = pd.unique(edges[["from_node", "to_node"]].values.ravel("K"))
    indexer = dict(zip(nodes, range(len(nodes))))
    edges[["from_node", "to_node"]] = edges[["from_node", "to_node"]].applymap(
        indexer.get
    )
    nodes = gpd.GeoDataFrame(
        map(lambda x: Point(x), nodes), columns=["geometry"]
    ).set_geometry("geometry")
    nodes["node_no"] = nodes.index
    edges.index = range(len(edges))
    edges["edge_no"] = edges.index
    return nodes, edges


# %% Replace Nodes


def replace_nodes_perpendicular_on_edges(nodes, edges, distance=5, crs="EPSG:28992"):
    warnings.filterwarnings("ignore")

    edges["line_geometry"] = edges["geometry"]
    nodes["point_geometry"] = nodes["geometry"]

    edges_buffer = gpd.GeoDataFrame(
        {
            "code": edges["code"],
            "geometry": edges.geometry.buffer(distance, join_style="round"),
        }
    )

    edges_buffer["line_geometry"] = edges_buffer["code"].apply(
        lambda x: edges[edges["code"] == x]["geometry"].values[0]
    )

    merged_dataset = gpd.sjoin(
        nodes,
        edges_buffer,
        op="intersects",
        how="left",
        lsuffix="points",
        rsuffix="lines",
    )

    merged_dataset.rename(
        columns={"geometrypoints": "point_geometry", "geometrylines": "line_geometry"},
        inplace=True,
    )

    merged_dataset = merged_dataset.drop_duplicates(subset=["node_no"], keep="first")

    replaced_nodes = []
    for idx, row in merged_dataset.iterrows():
        try:
            node = row["point_geometry"]
            edge = row["line_geometry"]
            left = edge.parallel_offset(distance, "left")
            replaced_node = snap(node, left, distance * 2)
            replaced_nodes.append(replaced_node)
        except AttributeError:
            continue

    replaced_nodes = gpd.GeoDataFrame(geometry=replaced_nodes, crs=crs)
    return replaced_nodes


# %% Get outlet nodes


def get_outlet_nodes(nodes, edges, crs="EPSG:28992"):
    warnings.filterwarnings("ignore")

    endpoints = get_endpoints_from_lines(edges)

    # Select boundary endpoints from line network
    boundary_endpoints = gpd.GeoDataFrame(
        endpoints[
            (endpoints["count_starting_lines"] == 0)
            & (endpoints["count_ending_lines"] >= 1)
        ]
    )

    edges = edges.drop(["line_geometry", "startpoint", "endpoint"], axis=1)

    outlet_nodes = nodes[nodes["geometry"].isin(boundary_endpoints["geometry"])]

    outlet_nodes = outlet_nodes.drop(["point_geometry"], axis=1)

    outlet_nodes.set_crs(crs)

    return outlet_nodes


# %% Ribasim Lumping


def create_graph_based_on_nodes_edges(
    node: gpd.GeoDataFrame,
    edge: gpd.GeoDataFrame,
    add_edge_length_as_weight: bool = False,
) -> nx.DiGraph:
    """
    create networkx graph from ribasim model.
    input: nodes and edges
    """
    graph = nx.DiGraph()
    if node is not None:
        for i, n in node.iterrows():
            graph.add_node(n.node_id, node_type=n.node_type, pos=(n.geometry.x, n.geometry.y))
    if edge is not None:
        for i, e in edge.iterrows():
            if add_edge_length_as_weight:
                graph.add_edge(e.from_node_id, e.to_node_id, weight=e.geometry.length)
            else:
                graph.add_edge(e.from_node_id, e.to_node_id)
    print(
        f" - create network graph from nodes ({len(node)}x) and edges ({len(edge)}x)"
    )
    return graph


def add_basin_code_from_network_to_nodes_and_edges(
    graph: nx.DiGraph,
    nodes: gpd.GeoDataFrame,
    edges: gpd.GeoDataFrame,
):
    """add basin (subgraph) code to nodes and edges"""
    subgraphs = list(nx.weakly_connected_components(graph))
    if nodes is None or edges is None:
        return None, None
    nodes["basin"] = -1
    edges["basin"] = -1
    for i, subgraph in enumerate(subgraphs):
        node_ids = list(subgraph)
        edges.loc[
            edges["from_node"].isin(node_ids) & edges["to_node"].isin(node_ids),
            "basin",
        ] = (
            i + 1
        )
        nodes.loc[nodes["node_no"].isin(list(subgraph)), "basin"] = i + 1
    print(f" - define numbers Ribasim-Basins ({len(subgraphs)}x) and join edges/nodes")
    return nodes, edges
