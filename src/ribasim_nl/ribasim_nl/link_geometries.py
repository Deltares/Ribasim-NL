# %%
import logging

import geopandas as gpd
import pandas as pd
from networkx import NetworkXNoPath, node_disjoint_paths, shortest_path
from shapely.geometry import LineString
from tqdm import tqdm

from ribasim_nl import Model, Network

logger = logging.getLogger(__name__)


def get_network_node(network, point, max_distance: float = 5):
    node = network.move_node(point, max_distance=0.5, align_distance=10)
    if node is None:
        node = network.add_node(point, max_distance=max_distance, align_distance=10)
    return node


def accept_length(geometry, point1, point2, max_straight_line_ratio: float = 5):
    return geometry.length / point1.distance(point2) < 5


def get_edge_geometry(network, source, target, forbidden_nodes):
    try:
        all_paths = node_disjoint_paths(network.graph_undirected, s=source, t=target)
        all_paths = [i for i in all_paths if not any(_node_id in forbidden_nodes for _node_id in i)]
    except NetworkXNoPath:
        return LineString((network.graph.nodes[source]["geometry"], network.graph.nodes[target]["geometry"]))

    if len(all_paths) == 0:
        path = shortest_path(network.graph_undirected, source=source, target=target)
        if any(_node_id in forbidden_nodes for _node_id in path):
            return LineString((network.graph.nodes[source]["geometry"], network.graph.nodes[target]["geometry"]))
        else:
            return network.path_to_line(path)
    else:
        if len(all_paths) == 1:
            geometry = network.path_to_line(all_paths[0])
        else:
            geometries = gpd.GeoSeries([network.path_to_line(i) for i in all_paths])
            geometry = geometries[geometries.length.idxmin()]

    if geometry.length > 0:
        return geometry
    else:
        return LineString((network.graph.nodes[source]["geometry"], network.graph.nodes[target]["geometry"]))


def fix_link_geometries(
    model: Model, network: Network, max_straight_line_ratio: float = 5, node_ids: list | None = None
):
    """Fix model.link.geometry column by finding routes over network

    Args:
        model (Model): Ribasim_nl Model to be fixed
        network (Network): Ribasim_nl Network with hydroobject line geometries
        max_straight_line_ratio (float, optional): threshold to check line.
         If the new line is `max_straight_line_ratio` times longer than straight line distance we don't accept the geometry.
         Defaults to 5.
    """
    node_df = model.node_table().df
    if node_ids is None:
        node_ids = node_df[node_df.node_type.isin(["LevelBoundary", "Basin"])].index
    for node_id in tqdm(node_ids, desc="fix line geometries"):
        logger.info(f"fixing edges for {node_df.at[node_id, 'node_type']} {node_id}")
        # get basin_node_id
        network_basin_node = get_network_node(network, node_df.at[node_id, "geometry"])
        if network_basin_node is None:
            continue

        # get all upstream connector nodes
        upstream_node_ids = model.upstream_node_id(node_id)
        if isinstance(upstream_node_ids, pd.Series):
            upstream_node_ids = upstream_node_ids.to_list()
        else:
            upstream_node_ids = [upstream_node_ids]

        # get all downstream connector nodes
        downstream_node_ids = model.downstream_node_id(node_id)
        if isinstance(downstream_node_ids, pd.Series):
            downstream_node_ids = downstream_node_ids.to_list()
        else:
            downstream_node_ids = [downstream_node_ids]

        # get all network-equivalents for upstream and downstream
        upstream_nodes = [
            get_network_node(network, node_df.at[i, "geometry"]) for i in upstream_node_ids if i is not None
        ]
        downstream_nodes = [
            get_network_node(network, node_df.at[i, "geometry"]) for i in downstream_node_ids if i is not None
        ]

        # draw edges from upstream nodes
        for idx, network_node in enumerate(upstream_nodes):
            if network_node is None:
                continue
            forbidden_nodes = [i for i in upstream_nodes + downstream_nodes if i != network_node]
            geometry = get_edge_geometry(
                network=network, source=network_node, target=network_basin_node, forbidden_nodes=forbidden_nodes
            )
            if accept_length(
                geometry=geometry,
                point1=node_df.at[node_id, "geometry"],
                point2=node_df.at[upstream_node_ids[idx], "geometry"],
                max_straight_line_ratio=max_straight_line_ratio,
            ):
                mask = (model.edge.df["from_node_id"] == upstream_node_ids[idx]) & (
                    model.edge.df["to_node_id"] == node_id
                )
                model.edge.df.loc[mask, ["geometry"]] = geometry

        # draw edges to downstream nodes
        for idx, network_node in enumerate(downstream_nodes):
            if network_node is None:
                continue
            forbidden_nodes = [i for i in upstream_nodes + downstream_nodes if i != network_node]
            geometry = get_edge_geometry(
                network=network,
                target=network_node,
                source=network_basin_node,
                forbidden_nodes=forbidden_nodes,
            )
            if accept_length(
                geometry=geometry,
                point1=node_df.at[node_id, "geometry"],
                point2=node_df.at[downstream_node_ids[idx], "geometry"],
                max_straight_line_ratio=max_straight_line_ratio,
            ):
                mask = (model.edge.df["to_node_id"] == downstream_node_ids[idx]) & (
                    model.edge.df["from_node_id"] == node_id
                )
                model.edge.df.loc[mask, ["geometry"]] = geometry
