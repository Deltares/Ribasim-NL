"""Lump a detailed network into basins, by splitting it at split nodes.

The detailed network is a directed graph of edges and nodes. Splitting it at the split nodes gives connected
subnetworks, the basins. Each basin gets an area, from the areas its edges cross, and connections to its split nodes
and boundaries. The node IDs follow from the order of these steps, see `ribasim_nl.lumping`.
"""

import logging
from dataclasses import dataclass, field

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import shapely
from shapely.geometry import GeometryCollection, LineString, MultiPolygon, Polygon

from ribasim_nl.lumping.utils import merge_grouped, overlay_intersection, quicksort_order, remove_holes

logger = logging.getLogger(__name__)

# offsets of the graph nodes that replace a split node, or that split an edge
SPLIT_NODE_OFFSET = 999_000_000_000
SPLIT_EDGE_OFFSET = 998_000_000_000


@dataclass
class Network:
    """A detailed network to lump: its edges and nodes, and its structures by object type."""

    edges: gpd.GeoDataFrame
    nodes: gpd.GeoDataFrame
    structures: dict[str, gpd.GeoDataFrame] = field(default_factory=dict)


@dataclass
class LumpedNetwork:
    """The lumped network: the basins with their areas, the split nodes and boundaries, and their connections."""

    basins: gpd.GeoDataFrame
    basin_areas: gpd.GeoDataFrame
    split_nodes: gpd.GeoDataFrame
    boundaries: gpd.GeoDataFrame
    basin_connections: gpd.GeoDataFrame
    boundary_connections: gpd.GeoDataFrame
    areas: gpd.GeoDataFrame


def create_graph(nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame) -> nx.DiGraph:
    """A directed graph of the nodes and edges, in their order."""
    graph = nx.DiGraph()
    for node_no, geometry in zip(nodes["node_no"], nodes.geometry, strict=True):
        graph.add_node(node_no, pos=(geometry.x, geometry.y))
    for from_node, to_node in zip(edges["from_node"], edges["to_node"], strict=True):
        graph.add_edge(from_node, to_node)
    return graph


def split_graph(
    graph: nx.DiGraph, split_nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame
) -> tuple[nx.DiGraph, gpd.GeoDataFrame]:
    """Disconnect the graph at the split nodes.

    A split node on an edge replaces the edge by two edges that end in new nodes. A split node on a node replaces
    the node by a new node for each of its edges. Adds the new graph nodes to the split nodes, as `graph_node_no`.
    """
    split_nodes = split_nodes.copy()
    split_nodes["graph_node_no"] = pd.Series([-1] * len(split_nodes), index=split_nodes.index, dtype=object)

    # split nodes on edges; as in 2024, the edges are taken in their order, which need not be that of the split nodes
    split_nodes_edges = split_nodes[split_nodes["edge_no"] != -1].copy()
    split_edges = edges.loc[edges["edge_no"].isin(split_nodes_edges["edge_no"]), ["from_node", "to_node"]]
    split_edges = [(u, v) for u, v in split_edges.itertuples(index=False) if (u, v) in graph.edges]
    assert len(split_edges) == len(split_nodes_edges), "each split node on an edge needs an edge in the graph"
    new_node_no1 = SPLIT_EDGE_OFFSET + split_nodes_edges["edge_no"].to_numpy() * 1_000 + 1
    new_node_no2 = SPLIT_EDGE_OFFSET + split_nodes_edges["edge_no"].to_numpy() * 1_000 + 2
    graph.remove_edges_from(split_edges)
    for no1, no2, geometry, (upstream, downstream) in zip(
        new_node_no1, new_node_no2, split_nodes_edges.geometry, split_edges, strict=True
    ):
        graph.add_node(no1, pos=(geometry.x, geometry.y))
        graph.add_node(no2, pos=(geometry.x, geometry.y))
        graph.add_edge(upstream, no1)
        graph.add_edge(no2, downstream)
    # as in 2024, only the first new node is recorded, twice
    split_nodes.loc[split_nodes_edges.index, "graph_node_no"] = pd.Series(
        [(no1, no1) for no1 in new_node_no1], index=split_nodes_edges.index, dtype=object
    )

    # split nodes on nodes
    split_nodes_nodes = split_nodes[split_nodes["node_no"] != -1]
    graph_node_no: list[object] = []
    for node_no in split_nodes_nodes["node_no"]:
        if node_no not in graph:
            graph_node_no.append(-1)
            continue
        pos = graph.nodes[node_no]["pos"]
        node_edges = [e for e in graph.edges if node_no in e]
        graph.remove_edges_from(node_edges)
        graph.remove_node(node_no)
        new_nodes = []
        for i, edge in enumerate(node_edges):
            new_node = SPLIT_NODE_OFFSET + node_no * 1_000 + i
            graph.add_node(new_node, pos=pos)
            graph.add_edge(*[n if n != node_no else new_node for n in edge])
            new_nodes.append(new_node)
        graph_node_no.append(tuple(new_nodes))
    split_nodes.loc[split_nodes_nodes.index, "graph_node_no"] = pd.Series(
        graph_node_no, index=split_nodes_nodes.index, dtype=object
    )
    return graph, split_nodes


def add_basins_to_nodes_and_edges(
    graph: nx.DiGraph, nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame, split_nodes: gpd.GeoDataFrame
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Number the connected subgraphs, the basins, and add the basin of each node and edge, or -1."""
    nodes, edges, split_nodes = nodes.copy(), edges.copy(), split_nodes.copy()
    nodes["basin"] = -1
    edges["basin"] = -1
    # map the graph nodes that replaced a split node back to its node
    unsplit = [g == -1 or (not isinstance(g, tuple) and pd.isna(g)) for g in split_nodes["graph_node_no"]]
    split_nodes.loc[unsplit, "graph_node_no"] = pd.Series(
        [(x,) for x in split_nodes.loc[unsplit, "node_no"]], index=split_nodes.index[unsplit], dtype=object
    )
    original_node = {
        g: n for n, gs in zip(split_nodes["node_no"], split_nodes["graph_node_no"], strict=True) for g in gs
    }
    for i, subgraph in enumerate(nx.weakly_connected_components(graph)):
        node_nos = [original_node.get(n, n) for n in subgraph]
        edges.loc[edges["from_node"].isin(node_nos) & edges["to_node"].isin(node_nos), "basin"] = i + 1
        nodes.loc[nodes["node_no"].isin(node_nos), "basin"] = i + 1
    return nodes, edges


def _single_basin(nodes: gpd.GeoDataFrame, node_nos: list) -> bool:
    """Whether the nodes `node_nos` are all in one basin."""
    basins = nodes.loc[nodes["node_no"].isin(node_nos), "basin"].to_numpy()
    return len(basins) > 0 and bool((basins == basins[0]).all())


def check_split_nodes_used(
    split_nodes: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Add the `status` of each split node: False if its neighbouring nodes are all in one basin.

    The `split_type` is the `object_type`, or `no_split` if not used.
    """
    split_nodes = split_nodes.copy()
    split_nodes["status"] = True

    not_used = []
    for node_no in split_nodes.loc[split_nodes["node_no"] != -1, "node_no"]:
        neighbours = list(edges.loc[edges["from_node"] == node_no, "to_node"]) + list(
            edges.loc[edges["to_node"] == node_no, "from_node"]
        )
        if _single_basin(nodes, neighbours):
            not_used.append(node_no)
    split_nodes.loc[split_nodes["node_no"].isin(not_used), "status"] = False

    not_used = []
    for edge_no in split_nodes.loc[split_nodes["edge_no"] != -1, "edge_no"]:
        ends = list(edges.loc[edges["edge_no"] == edge_no, "to_node"]) + list(
            edges.loc[edges["edge_no"] == edge_no, "from_node"]
        )
        if _single_basin(nodes, ends):
            not_used.append(edge_no)
    split_nodes.loc[split_nodes["edge_no"].isin(not_used), "status"] = False

    split_nodes["object_type"] = split_nodes["object_type"].fillna("manual")
    split_nodes["split_type"] = split_nodes["object_type"]
    split_nodes.loc[~split_nodes["status"], "split_type"] = "no_split"
    logger.info(f"{(~split_nodes['status']).sum()} split nodes are not used")
    return split_nodes


def create_basin_areas(
    edges: gpd.GeoDataFrame, areas: gpd.GeoDataFrame, laterals: gpd.GeoDataFrame | None = None
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Assign each area to a basin, and dissolve the areas of each basin into its basin area.

    With `laterals`, an area gets the basin of the first lateral that includes the area code in its ID, and the basin
    areas are exploded. Otherwise it gets the basin with the longest length of edges in the area.
    """
    areas = areas[["code", "area", "geometry"]].copy()
    edges = edges.copy()

    if laterals is not None:
        laterals = laterals.copy()
        laterals["area_code_included"] = [
            next((code for code in areas["code"] if lateral_id.find(code) != -1), None) for lateral_id in laterals["id"]
        ]
        laterals_join = laterals.sjoin(
            gpd.GeoDataFrame(edges[["basin"]], geometry=edges.geometry.buffer(1), crs=edges.crs),
            predicate="intersects",
            how="left",
        ).drop(columns=["index_right"])
        laterals_join["basin"] = laterals_join["basin"].fillna(-1).astype(int)
        area_basin = laterals_join.dropna(subset=["area_code_included"]).groupby("area_code_included", sort=False)
        first_basin = area_basin["basin"].first()
        areas["basin"] = [first_basin.get(code, -1) for code in areas["code"]]
        basin_areas = areas.dissolve(by="basin").explode(index_parts=True).reset_index().drop(columns=["level_1"])
    else:
        edges_sel = edges.loc[edges["basin"] != -1].copy()
        areas_orig = areas.copy()
        # only keep the (multi)polygon of the collections that make_valid can return
        valid_areas = []
        for code, valid in zip(areas["code"], areas.make_valid(), strict=True):
            if isinstance(valid, GeometryCollection):
                valid = next((g for g in valid.geoms if isinstance(g, Polygon | MultiPolygon)), None)
            assert isinstance(valid, Polygon | MultiPolygon), f"area {code} is not a polygon when made valid"
            valid_areas.append(valid)
        areas["geometry"] = valid_areas
        areas = areas.sjoin(edges_sel[["basin", "geometry"]])
        # the length of each edge within each area
        assert edges_sel.index.is_unique
        area_edges = edges_sel.geometry.loc[areas["index_right"]].to_numpy()
        areas["edge_length"] = shapely.length(shapely.intersection(area_edges, areas.geometry.to_numpy()))
        lengths = areas.groupby(by=["area", "basin"], as_index=False).agg({"edge_length": "sum"})
        # the basin with the longest edges in each area
        lengths = lengths.sort_values(by=["area", "edge_length"], ascending=[True, False]).drop_duplicates(
            subset=["area"], keep="first"
        )
        areas = gpd.GeoDataFrame(
            lengths[["area", "basin", "edge_length"]]
            .sort_values(by="area", kind="stable")
            .merge(areas_orig, how="outer", on="area"),
            geometry="geometry",
            crs=edges.crs,
        ).sort_values(by="area", kind="stable")
        areas["geometry"] = areas.make_valid()
        basin_areas = areas.dissolve(by="basin").reset_index().drop(columns=["area"])

    areas["basin"] = areas["basin"].fillna(-1).astype(int)
    basin_areas["basin"] = basin_areas["basin"].fillna(-1).astype(int)
    return areas, basin_areas


def create_basins(graph: nx.DiGraph, nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Locate each basin at its node with the highest closeness centrality.

    A basin of two nodes is located at the centroid of the edge between them.
    """
    centralities: dict = {}
    for component in nx.weakly_connected_components(graph):
        centralities.update(nx.closeness_centrality(graph.subgraph(component).to_undirected()))
    centrality = pd.DataFrame({"node_no": list(centralities.keys()), "centrality": list(centralities.values())})
    centrality = centrality[centrality["node_no"] < 900_000_000_000]
    basins_temp = nodes[["node_no", "basin", "geometry"]].merge(centrality, how="outer", on="node_no")
    basins_temp = basins_temp[basins_temp["basin"] != -1].sort_values(
        by=["basin", "centrality"], ascending=[True, False]
    )
    basins = gpd.GeoDataFrame(basins_temp.groupby(by="basin").first().reset_index(), geometry="geometry", crs=nodes.crs)
    no_nodes_basin = basins_temp.groupby(by="basin")["basin"].count().rename("no_nodes_basin")
    basins = basins.merge(no_nodes_basin, how="left", left_on="basin", right_index=True)

    basins1 = basins[basins["no_nodes_basin"] != 2]
    basins2 = basins[basins["no_nodes_basin"] == 2]
    basins_temp2 = basins_temp[basins_temp["basin"].isin(basins2["basin"])]
    first = basins_temp2.groupby("basin").first().reset_index().rename(columns={"node_no": "node_no1"})
    last = (
        basins_temp2.groupby("basin").last().reset_index()[["basin", "node_no"]].rename(columns={"node_no": "node_no2"})
    )
    pairs = first.merge(last, how="left", on="basin")[["basin", "node_no1", "node_no2"]]
    edge_columns = ["basin", "edge_no", "from_node", "to_node"]
    pairs = pairs.merge(
        edges[edge_columns],
        how="left",
        left_on=["basin", "node_no1", "node_no2"],
        right_on=["basin", "from_node", "to_node"],
    ).fillna(-1)
    pairs = pairs.astype(int)
    pairs = pairs.merge(
        edges[edge_columns],
        how="left",
        left_on=["basin", "node_no2", "node_no1"],
        right_on=["basin", "from_node", "to_node"],
    ).fillna(-1)
    pairs = pairs.astype(int)
    for column in ["edge_no", "from_node", "to_node"]:
        pairs[column] = -1
        pairs.loc[pairs[f"{column}_x"] != -1, column] = pairs[f"{column}_x"]
        pairs.loc[pairs[f"{column}_y"] != -1, column] = pairs[f"{column}_y"]
    pairs = pairs.merge(edges[["edge_no", "geometry"]], how="left", on="edge_no")
    pairs = pairs[
        ((pairs["from_node"] == pairs["node_no1"]) & (pairs["to_node"] == pairs["node_no2"]))
        | ((pairs["from_node"] == pairs["node_no2"]) & (pairs["to_node"] == pairs["node_no1"]))
    ]
    pairs = gpd.GeoDataFrame(pairs, geometry=[g.centroid for g in pairs.geometry], crs=basins.crs)
    basins2_geometry = basins2["geometry"]
    basins2 = basins2.drop(columns=["geometry"]).merge(pairs[["basin", "geometry"]], how="left", on="basin")
    # as in 2024, this aligns on the index, which the merge reset
    basins2["geometry"] = basins2["geometry"].fillna(basins2_geometry)

    basins = gpd.GeoDataFrame(pd.concat([basins1, basins2]), geometry="geometry", crs=nodes.crs)
    return basins[basins.geometry.notna()]


def check_nodes_edges_within_basin_areas(
    nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame, basin_areas: gpd.GeoDataFrame
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Add the `basin_area` that each node intersects and each edge lies within, or -1."""
    nodes = gpd.sjoin(
        nodes.drop(columns=["basin_area"], errors="ignore"), basin_areas[["geometry", "basin"]], how="left"
    )
    nodes = nodes.drop(columns=["index_right"])
    nodes["basin_right"] = nodes["basin_right"].fillna(-1).astype(int)
    nodes = nodes.rename(columns={"basin_left": "basin", "basin_right": "basin_area"})
    nodes["basin_check"] = nodes["basin"] == nodes["basin_area"]

    edges = gpd.sjoin(
        edges.drop(columns=["basin_area"], errors="ignore"),
        basin_areas[["geometry", "basin"]],
        how="left",
        predicate="within",
    ).drop(columns=["index_right"])
    edges = edges.rename(columns={"basin_left": "basin", "basin_right": "basin_area"})
    edges["basin_area"] = edges["basin_area"].fillna(-1).astype(int)
    edges["basin_check"] = edges["basin"] == edges["basin_area"]
    return nodes, edges


def _lines(df: pd.DataFrame, start: str, end: str) -> gpd.GeoSeries:
    """Straight lines from the points in column `start` of `df` to those in column `end`, with the index of `df`."""
    return gpd.GeoSeries([LineString([a, b]) for a, b in zip(df[start], df[end], strict=True)], index=df.index)


def create_basin_connections(
    split_nodes: gpd.GeoDataFrame, edges: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame, basins: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Connect each used split node with the basins up- and downstream of it."""
    conn = split_nodes.rename(columns={"geometry": "geom_split_node"})
    conn = conn[(conn["split_type"] != "no_split") & (conn["split_type"] != "harde_knip")]

    # split nodes on an edge connect the basins of its nodes
    conn_struct = conn.loc[conn["edge_no"] != -1].drop(columns=["node_no", "from_node", "to_node"], errors="ignore")
    conn_struct = merge_grouped(conn_struct, edges[["from_node", "to_node", "edge_no"]], on="edge_no")
    drop = ["from_node", "to_node", "node_no", "edge_no"]
    conn_struct_us = merge_grouped(conn_struct, nodes[["node_no", "basin"]], left_on="from_node", right_on="node_no")
    conn_struct_us = conn_struct_us.drop(columns=drop)
    conn_struct_ds = merge_grouped(conn_struct, nodes[["node_no", "basin"]], left_on="to_node", right_on="node_no")
    conn_struct_ds = conn_struct_ds.drop(columns=drop)

    # split nodes on a node connect the basins of its edges
    conn_nodes = conn.loc[conn["node_no"] != -1].drop(columns=["edge_no", "from_node", "to_node"], errors="ignore")
    edge_columns = ["basin", "from_node", "to_node", "edge_no"]
    conn_nodes_ds = merge_grouped(conn_nodes, edges[edge_columns], left_on="node_no", right_on="from_node")
    conn_nodes_ds = conn_nodes_ds.drop(columns=drop)
    conn_nodes_us = merge_grouped(conn_nodes, edges[edge_columns], left_on="node_no", right_on="to_node")
    conn_nodes_us = conn_nodes_us.drop(columns=drop)

    conn_ds = merge_grouped(pd.concat([conn_nodes_ds, conn_struct_ds]), basins[["basin", "geometry"]], on="basin")
    conn_ds = conn_ds.rename(columns={"geometry": "geom_basin"})
    conn_ds["connection"] = "split_node_to_basin"
    conn_ds["geometry"] = _lines(conn_ds, "geom_split_node", "geom_basin")
    conn_us = merge_grouped(pd.concat([conn_nodes_us, conn_struct_us]), basins[["basin", "geometry"]], on="basin")
    conn_us = conn_us.rename(columns={"geometry": "geom_basin"})
    conn_us["connection"] = "basin_to_split_node"
    conn_us["geometry"] = _lines(conn_us, "geom_basin", "geom_split_node")
    basin_connections = gpd.GeoDataFrame(pd.concat([conn_ds, conn_us]), geometry="geometry", crs=split_nodes.crs)
    return basin_connections.drop_duplicates(keep="first")


def create_boundary_connections(
    boundaries: gpd.GeoDataFrame,
    nodes: gpd.GeoDataFrame,
    edges: gpd.GeoDataFrame,
    basins: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    """Connect each boundary with the basin of its nearest node.

    A FlowBoundary flows into the basin. A LevelBoundary flows into it if its node starts an edge, and out of it if
    it ends one, or both.
    """
    conn = boundaries[["boundary_id", "boundary_type", "geometry"]].sjoin_nearest(
        nodes[["node_no", "geometry", "basin"]]
    )
    conn = conn.drop(columns=["index_right"]).rename(
        columns={"node_no": "boundary_node_no", "geometry": "geom_boundary"}
    )
    conn = conn.merge(basins[["basin", "geometry"]].rename(columns={"geometry": "geom_basin"}), how="left", on="basin")

    flow = conn[conn["boundary_type"] == "FlowBoundary"].copy()
    flow["geometry"] = _lines(flow, "geom_boundary", "geom_basin")
    flow["connection"] = "boundary_to_basin"

    level = conn[conn["boundary_type"] == "LevelBoundary"]
    if not level.empty:
        level_in = level[level["boundary_node_no"].isin(edges["from_node"])].copy()
        level_in["geometry"] = _lines(level_in, "geom_boundary", "geom_basin")
        level_in["connection"] = "boundary_to_basin"
        level_out = level[level["boundary_node_no"].isin(edges["to_node"])].copy()
        level_out["geometry"] = _lines(level_out, "geom_basin", "geom_boundary")
        level_out["connection"] = "basin_to_boundary"
        level = pd.concat([level_in, level_out], ignore_index=True)
        level = level.sort_values(["boundary_id", "basin"]).reset_index(drop=True)
        conn = pd.concat([flow, level]).reset_index(drop=True)
    else:
        conn = flow
    conn = gpd.GeoDataFrame(conn, geometry="geometry", crs=boundaries.crs)
    return conn.iloc[quicksort_order(conn["boundary_id"])]


def remove_basins_from_boundary(
    boundary_connections: gpd.GeoDataFrame,
    basin_connections: gpd.GeoDataFrame,
    basins: gpd.GeoDataFrame,
    nodes: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Remove the basins of boundaries that have split nodes, and connect these boundaries to the split nodes."""
    conn = boundary_connections.merge(
        basin_connections[["basin", "split_node", "geom_split_node"]], how="left", on="basin"
    )
    conn["split_node"] = conn["split_node"].fillna(-1).astype(int)
    excluded = conn[conn["split_node"] == -1]
    nodes = nodes.copy()

    out = conn[(conn["split_node"] != -1) & (conn["connection"] == "basin_to_boundary")].copy()
    if not out.empty:
        out["connection"] = "split_node_to_boundary"
        out["geometry"] = _lines(out, "geom_split_node", "geom_boundary")
        nodes.loc[nodes["basin"].isin(out["basin"]), "basin"] = -1

    into = conn[(conn["split_node"] != -1) & (conn["connection"] == "boundary_to_basin")].copy()
    if not into.empty:
        into["connection"] = "boundary_to_split_node"
        into["geometry"] = _lines(into, "geom_boundary", "geom_split_node")
        nodes.loc[nodes["basin"].isin(into["basin"]), "basin"] = -1

    conn = pd.concat([excluded, into, out])
    basin_connections = basin_connections[~basin_connections["basin"].isin(conn["basin"])]
    basins = basins[~basins["basin"].isin(conn["basin"])]
    return conn, basin_connections, basins, nodes


def remove_boundary_basins(
    basins: gpd.GeoDataFrame,
    basin_connections: gpd.GeoDataFrame,
    boundary_connections: gpd.GeoDataFrame,
    nodes: gpd.GeoDataFrame,
    include_flow_boundary_basins: bool = True,
    include_level_boundary_basins: bool = False,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Remove the basins between boundaries and their split nodes, unless they are included."""
    flow = boundary_connections[boundary_connections["boundary_type"] == "FlowBoundary"]
    level = boundary_connections[boundary_connections["boundary_type"] == "LevelBoundary"]
    if not include_flow_boundary_basins and not flow.empty:
        flow, basin_connections, basins, nodes = remove_basins_from_boundary(flow, basin_connections, basins, nodes)
    if not include_level_boundary_basins and not level.empty:
        level, basin_connections, basins, nodes = remove_basins_from_boundary(level, basin_connections, basins, nodes)

    boundary_connections = pd.concat([flow, level])
    if "split_node" in boundary_connections.columns:
        boundary_connections["split_node"] = boundary_connections["split_node"].fillna(-1).astype(int)
    else:
        boundary_connections["split_node"] = -1
    boundary_connections = boundary_connections.iloc[quicksort_order(boundary_connections["boundary_id"])]
    boundary_connections = boundary_connections.reset_index(drop=True)
    boundary_connections = boundary_connections.drop(
        columns=["geom_boundary", "geom_basin", "geom_split_node"], errors="ignore"
    )
    basin_connections = basin_connections.drop(columns=["geom_basin", "geom_split_node"], errors="ignore")
    return boundary_connections, basin_connections, basins, nodes


def remove_isolated_basins(
    basins: gpd.GeoDataFrame,
    basin_areas: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame,
    basin_connections: gpd.GeoDataFrame,
    boundary_connections: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Remove the basins without connections.

    Their basin areas go to a connected basin in the same basin area, or are removed. As in 2024, their areas get NaN
    as basin, not -1, so `assign_unassigned_areas` does not assign them to a neighbouring basin area.
    """
    connected = np.unique(basin_connections["basin"].tolist() + boundary_connections["basin"].tolist())
    isolated = basins.loc[~basins["basin"].isin(connected), "basin"].to_numpy()
    logger.info(f"Removing {len(isolated)} isolated basins")
    if len(isolated) == 0:
        return basins, basin_areas, areas

    # the basins within each basin area
    joined = basin_areas.sjoin(basins, how="left")
    area_basins = {k: joined.loc[[k], "basin_right"].to_numpy() for k in joined.index.unique()}
    area_basins = {k: v for k, v in area_basins.items() if not np.isnan(v).all()}
    basin_area = {b: k for k, v in area_basins.items() for b in v}
    connected_basins = basins.loc[basins["basin"].isin(connected), "basin"]
    new_basin = {b: area_basins[basin_area[b]] for b in isolated if b in basin_area}
    new_basin = {b: v[np.isin(v, connected_basins)] for b, v in new_basin.items()}
    new_basin = {b: int(v[0]) if len(v) > 0 else -(1000 + b) for b, v in new_basin.items()}

    basins = basins.loc[basins["basin"].isin(connected)]
    basin_areas = basin_areas.copy()
    basin_areas["basin"] = [new_basin.get(b, b) for b in basin_areas["basin"]]
    basin_areas = basin_areas.loc[basin_areas["basin"] >= 0]
    areas = areas.copy()
    isolated_areas = areas["basin"].isin(isolated)
    if isolated_areas.any():
        areas["basin"] = areas["basin"].astype(float)
        areas.loc[isolated_areas, "basin"] = np.nan
    return basins, basin_areas, areas


def regenerate_node_ids(
    boundaries: gpd.GeoDataFrame,
    split_nodes: gpd.GeoDataFrame,
    basins: gpd.GeoDataFrame,
    basin_connections: gpd.GeoDataFrame,
    boundary_connections: gpd.GeoDataFrame,
    basin_areas: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame,
) -> LumpedNetwork:
    """Number the boundaries, then the split nodes, then the basins, and connect them by these node IDs.

    As in 2024, the split nodes are numbered from their number, so a split node and a basin can get the same node ID.
    """
    boundaries, split_nodes, basins = boundaries.copy(), split_nodes.copy(), basins.copy()
    basin_connections, boundary_connections = basin_connections.copy(), boundary_connections.copy()
    n_boundaries = len(boundaries)
    boundaries["boundary_node_id"] = boundaries["boundary_id"]
    split_nodes["split_node_node_id"] = split_nodes["split_node"] + n_boundaries
    n_split_nodes = len(split_nodes)
    basins["basin_node_id"] = basins["basin"] + n_split_nodes + n_boundaries

    basin_connections["split_node_node_id"] = basin_connections["split_node"] + n_boundaries
    basin_connections["basin_node_id"] = basin_connections["basin"] + n_split_nodes + n_boundaries
    to_split_node = basin_connections["connection"] == "basin_to_split_node"
    basin_connections["from_node_id"] = np.where(
        to_split_node, basin_connections["basin_node_id"], basin_connections["split_node_node_id"]
    )
    basin_connections["to_node_id"] = np.where(
        to_split_node, basin_connections["split_node_node_id"], basin_connections["basin_node_id"]
    )

    boundary_connections["boundary_node_id"] = boundary_connections["boundary_id"]
    boundary_connections["split_node_node_id"] = boundary_connections["split_node"] + n_boundaries
    boundary_connections["basin_node_id"] = boundary_connections["basin"] + n_split_nodes + n_boundaries
    # the connection names the node at each end: a basin, a boundary or else a split node
    connection = boundary_connections["connection"].astype(str)
    for end, at_end in (("from_node_id", connection.str.startswith), ("to_node_id", connection.str.endswith)):
        node_id = np.where(
            at_end("basin"),
            boundary_connections["basin_node_id"],
            np.where(
                at_end("boundary"),
                boundary_connections["boundary_node_id"],
                boundary_connections["split_node_node_id"],
            ),
        )
        boundary_connections[end] = pd.Series(node_id, index=boundary_connections.index).fillna(-1).astype(int)

    basin_areas = gpd.GeoDataFrame(
        merge_grouped(basin_areas, basins[["basin", "basin_node_id"]], on="basin"),
        geometry="geometry",
        crs=basin_areas.crs,
    )
    basin_areas["basin_node_id"] = basin_areas["basin_node_id"].astype(int)
    areas = areas.copy()
    areas["basin_node_id"] = [b + n_split_nodes + n_boundaries if b > 0 else -1 for b in areas["basin"]]

    return LumpedNetwork(
        basins=basins.drop_duplicates(keep="first"),
        basin_areas=basin_areas.drop_duplicates(keep="first"),
        split_nodes=split_nodes,
        boundaries=boundaries.drop_duplicates(keep="first"),
        basin_connections=basin_connections.drop_duplicates(keep="first"),
        boundary_connections=boundary_connections.drop_duplicates(keep="first"),
        areas=areas.drop_duplicates(keep="first"),
    )


def assign_unassigned_areas(
    areas: gpd.GeoDataFrame, basin_areas: gpd.GeoDataFrame, drainage_areas: gpd.GeoDataFrame | None = None
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Assign the areas without basin to the neighbouring basin area they share most with, in the same drainage area.

    This repeats while it assigns areas. As in 2024, the areas are intersected with `overlay_intersection` and sorted
    with `quicksort_order`, and a basin area that gains areas gets the `basin_node_id` of its smallest area, which is
    -1 if that area had no basin before.
    """
    areas, basin_areas = areas.copy(), basin_areas.copy()
    areas["geometry"] = areas.make_valid()
    if drainage_areas is None:
        areas["drainage_area_code"] = -1
        basin_areas["drainage_area_code"] = -1
    else:
        drainage = drainage_areas[["code", "geometry"]].rename(columns={"code": "drainage_area_code"})
        areas = overlay_intersection(areas, drainage)
        areas["geom_area"] = areas.geometry.area
        areas = areas.iloc[quicksort_order(areas["geom_area"])].drop_duplicates(subset="area", keep="last")
        areas = areas.drop(columns=["geom_area"])
        basin_areas = overlay_intersection(basin_areas, drainage)
        basin_areas["geom_area"] = basin_areas.geometry.area
        basin_areas = basin_areas.iloc[quicksort_order(basin_areas["geom_area"])].drop_duplicates(
            subset="basin", keep="last"
        )
        basin_areas = basin_areas.drop(columns=["geom_area"])

    unassigned = areas.loc[areas["basin"] == -1]
    while len(unassigned) > 0:
        n_unassigned = len(unassigned)
        neighbours = unassigned.copy()
        neighbours["geometry"] = neighbours.geometry.buffer(1)
        neighbours = overlay_intersection(
            neighbours.drop(columns=["basin"]),
            basin_areas[["basin", "drainage_area_code", "geometry"]].rename(
                columns={"basin": "new_basin", "drainage_area_code": "new_drainage_area_code"}
            ),
        )
        neighbours = neighbours[neighbours["drainage_area_code"] == neighbours["new_drainage_area_code"]].copy()
        neighbours["geom_area"] = neighbours.geometry.area
        neighbours = neighbours.iloc[quicksort_order(neighbours["geom_area"])].drop_duplicates(
            subset="area", keep="last"
        )
        logger.info(f"Assigning {len(neighbours)} of {n_unassigned} unassigned areas")

        areas = areas.merge(neighbours[["area", "new_basin"]], how="left", on="area")
        areas["new_basin"] = areas["new_basin"].fillna(-1).astype(int)
        areas.loc[areas["basin"] == -1, "basin"] = areas.loc[areas["basin"] == -1, "new_basin"]
        areas = areas.drop(columns=["new_basin"])
        basin_areas = areas[areas["basin"] != -1].dissolve(by="basin").reset_index().drop(columns=["area"])
        basin_areas["basin"] = basin_areas["basin"].astype(int)

        unassigned = areas.loc[areas["basin"] == -1]
        if len(unassigned) == n_unassigned:
            break
    basin_areas["basin"] = basin_areas["basin"].fillna(-1).astype(int)
    return areas, basin_areas


def lump_network(
    network: Network,
    split_nodes: gpd.GeoDataFrame,
    boundaries: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame,
    drainage_areas: gpd.GeoDataFrame | None = None,
    laterals: gpd.GeoDataFrame | None = None,
    remove_isolated: bool = False,
    include_flow_boundary_basins: bool = True,
    include_level_boundary_basins: bool = False,
    remove_holes_min_area: float = 10.0,
    assign_unassigned: bool = True,
) -> LumpedNetwork:
    """Lump the network into basins at the split nodes, and connect them with the split nodes and boundaries.

    Parameters
    ----------
    network : Network
        The detailed network.
    split_nodes : gpd.GeoDataFrame
        The split nodes, with the `node_no` or `edge_no` they are on, or -1.
    boundaries : gpd.GeoDataFrame
        The boundaries, with their `boundary_id`, `boundary_name` and `boundary_type`.
    areas : gpd.GeoDataFrame
        The areas that make up the basin areas, with their `code` and number `area`.
    drainage_areas : gpd.GeoDataFrame | None
        The drainage areas, with their `code`, within which unassigned areas are assigned.
    laterals : gpd.GeoDataFrame | None
        The laterals, to assign areas to basins with, instead of the edges in each area.
    remove_isolated : bool
        Remove the basins without connections.
    include_flow_boundary_basins, include_level_boundary_basins : bool
        Keep the basins between a boundary and its split nodes.
    remove_holes_min_area : float
        Remove the holes in the basin areas with an area up to this, in m².
    assign_unassigned : bool
        Assign the areas without basin to a neighbouring basin area.
    """
    graph = create_graph(network.nodes, network.edges)
    graph, split_nodes = split_graph(graph, split_nodes, network.edges)
    nodes, edges = add_basins_to_nodes_and_edges(graph, network.nodes, network.edges, split_nodes)
    split_nodes = check_split_nodes_used(split_nodes, nodes, edges)
    basins = create_basins(graph, nodes, edges)
    logger.info(f"{len(basins)} basins")
    areas, basin_areas = create_basin_areas(edges, areas, laterals)
    nodes, edges = check_nodes_edges_within_basin_areas(nodes, edges, basin_areas)
    basin_connections = create_basin_connections(split_nodes, edges, nodes, basins)
    boundary_connections = create_boundary_connections(boundaries, nodes, edges, basins)
    # only the used split nodes are numbered
    split_nodes = split_nodes[~split_nodes["split_type"].isin(["no_split", "harde_knip"])]
    boundary_connections, basin_connections, basins, nodes = remove_boundary_basins(
        basins,
        basin_connections,
        boundary_connections,
        nodes,
        include_flow_boundary_basins=include_flow_boundary_basins,
        include_level_boundary_basins=include_level_boundary_basins,
    )
    if remove_isolated:
        basins, basin_areas, areas = remove_isolated_basins(
            basins, basin_areas, areas, basin_connections, boundary_connections
        )
    basin_areas = remove_holes(basin_areas, min_area=remove_holes_min_area)
    lumped = regenerate_node_ids(
        boundaries, split_nodes, basins, basin_connections, boundary_connections, basin_areas, areas
    )
    if assign_unassigned:
        lumped.areas, lumped.basin_areas = assign_unassigned_areas(lumped.areas, lumped.basin_areas, drainage_areas)
    return lumped
