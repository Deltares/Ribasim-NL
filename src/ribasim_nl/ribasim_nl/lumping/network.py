"""Lump a detailed network into basins, by splitting it at split nodes.

The detailed network is a directed graph of edges and nodes. Splitting it at the split nodes gives connected
subnetworks, the basins. Each basin gets connections to its split nodes and boundaries, and an area, from the areas
that drain to it or border its other areas. The node IDs follow from the order of these steps, see `ribasim_nl.lumping`.
"""

import logging
from dataclasses import dataclass, field

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import shapely
from shapely.geometry import GeometryCollection, LineString, MultiPolygon, Polygon
from shapely.geometry.base import BaseGeometry

from ribasim_nl.geometry import polygon_parts, union_polygons
from ribasim_nl.lumping.utils import merge_grouped, quicksort_order

logger = logging.getLogger(__name__)

# offsets of the graph nodes that replace a split node, or that split an edge
SPLIT_NODE_OFFSET = 999_000_000_000
SPLIT_EDGE_OFFSET = 998_000_000_000

# the distance within which areas border each other, in m, see `_neighbouring_basins`
NEIGHBOUR_DISTANCE = 1.0


@dataclass
class Network:
    """A detailed network to lump: its edges and nodes, and its structures by object type."""

    edges: gpd.GeoDataFrame
    nodes: gpd.GeoDataFrame
    structures: dict[str, gpd.GeoDataFrame] = field(default_factory=dict)


@dataclass
class LumpedNetwork:
    """The lumped network: the basins, split nodes and boundaries, their connections, and the areas with their basin.

    The `basin` of an area is the one it drains to, see `area_basins`, or -1. `assign_areas` assigns the others.
    """

    basins: gpd.GeoDataFrame
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


def _valid_polygon(code: object, geometry: BaseGeometry | None) -> Polygon | MultiPolygon | None:
    """The geometry of area `code` made valid, without the lines and points that `make_valid` can add."""
    if geometry is None:
        return None
    valid = shapely.make_valid(geometry)
    if isinstance(valid, GeometryCollection):
        valid = next((g for g in valid.geoms if isinstance(g, Polygon | MultiPolygon)), None)
    assert isinstance(valid, Polygon | MultiPolygon), f"area {code} has no polygon when made valid"
    return valid


def area_basins(
    edges: gpd.GeoDataFrame, areas: gpd.GeoDataFrame, laterals: gpd.GeoDataFrame | None = None
) -> gpd.GeoDataFrame:
    """The areas, made valid, with the `basin` that each drains to, or -1.

    With `laterals`, an area drains to the basin of the first lateral that includes the area code in its ID. Otherwise
    it drains to the basin with the longest length of edges in the area, the lowest basin of equal lengths.
    """
    areas = areas[["code", "area", "geometry"]].copy()
    areas["geometry"] = gpd.GeoSeries(
        [_valid_polygon(c, g) for c, g in zip(areas["code"], areas.geometry, strict=True)],
        index=areas.index,
        crs=areas.crs,
    )

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
    else:
        edges_sel = edges.loc[edges["basin"] != -1]
        assert edges_sel.index.is_unique
        joined = areas.sjoin(edges_sel[["basin", "geometry"]])
        # the length of each edge within each area
        area_edges = edges_sel.geometry.loc[joined["index_right"]].to_numpy()
        joined["edge_length"] = shapely.length(shapely.intersection(area_edges, joined.geometry.to_numpy()))
        lengths = joined.groupby(by=["area", "basin"], as_index=False).agg({"edge_length": "sum"})
        lengths = lengths.sort_values(by=["area", "edge_length", "basin"], ascending=[True, False, True], kind="stable")
        basin = lengths.drop_duplicates(subset="area").set_index("area")["basin"]
        areas["basin"] = areas["area"].map(basin)

    areas["basin"] = areas["basin"].fillna(-1).astype(int)
    return areas


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
    """Remove the basins of boundaries that have split nodes, and connect these boundaries to the split nodes.

    The basins of boundaries without split node are kept, with `boundary_without_split_node`. The 2024 lumping
    removed them too, leaving their boundary connected to no node, and the fix scripts added them back.
    """
    conn = boundary_connections.merge(
        basin_connections[["basin", "split_node", "geom_split_node"]], how="left", on="basin"
    )
    conn["split_node"] = conn["split_node"].fillna(-1).astype(int)
    excluded = conn[conn["split_node"] == -1]
    nodes, basins = nodes.copy(), basins.copy()
    basins["boundary_without_split_node"] |= basins["basin"].isin(excluded["basin"])

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

    removed = pd.concat([into, out])["basin"]
    conn = pd.concat([excluded, into, out])
    basin_connections = basin_connections[~basin_connections["basin"].isin(removed)]
    basins = basins[~basins["basin"].isin(removed)]
    return conn, basin_connections, basins, nodes


def remove_boundary_basins(
    basins: gpd.GeoDataFrame,
    basin_connections: gpd.GeoDataFrame,
    boundary_connections: gpd.GeoDataFrame,
    nodes: gpd.GeoDataFrame,
    include_flow_boundary_basins: bool = True,
    include_level_boundary_basins: bool = False,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Remove the basins between boundaries and their split nodes, unless they are included.

    The basins of boundaries without split node are kept, see `remove_basins_from_boundary`.
    """
    flow = boundary_connections[boundary_connections["boundary_type"] == "FlowBoundary"]
    level = boundary_connections[boundary_connections["boundary_type"] == "LevelBoundary"]
    basins = basins.assign(boundary_without_split_node=False)
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
    basins: gpd.GeoDataFrame, basin_connections: gpd.GeoDataFrame, boundary_connections: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
    """Remove the basins without connections."""
    connected = basins["basin"].isin(basin_connections["basin"]) | basins["basin"].isin(boundary_connections["basin"])
    logger.info(f"Removing {(~connected).sum()} isolated basins")
    return basins.loc[connected]


def regenerate_node_ids(
    boundaries: gpd.GeoDataFrame,
    split_nodes: gpd.GeoDataFrame,
    basins: gpd.GeoDataFrame,
    basin_connections: gpd.GeoDataFrame,
    boundary_connections: gpd.GeoDataFrame,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Number the boundaries, then the split nodes, then the basins, and connect them by these node IDs.

    As in 2024, the split nodes are numbered from their number, so a split node and a basin can get the same node ID.
    `ribasim_nl.lumping.model.lumped_model` gives these basins another node ID. Returns the boundaries, split nodes,
    basins, basin connections and boundary connections.
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

    return (
        boundaries.drop_duplicates(keep="first"),
        split_nodes,
        basins.drop_duplicates(keep="first"),
        basin_connections.drop_duplicates(keep="first"),
        boundary_connections.drop_duplicates(keep="first"),
    )


def _drainage_area_codes(areas: gpd.GeoDataFrame, drainage_areas: gpd.GeoDataFrame | None) -> np.ndarray:
    """The code of the drainage area that each area overlaps most, or None."""
    codes = np.full(len(areas), None, dtype=object)
    if drainage_areas is None:
        return codes
    drainage = shapely.make_valid(drainage_areas.geometry.to_numpy())
    i_area, i_drainage = shapely.STRtree(drainage).query(areas.geometry.to_numpy(), predicate="intersects")
    overlap = shapely.area(shapely.intersection(areas.geometry.to_numpy()[i_area], drainage[i_drainage]))
    # the largest overlap of each area, the first drainage area of equal overlaps
    order = np.lexsort((i_drainage, -overlap, i_area))
    first = order[np.r_[True, np.diff(i_area[order]) != 0]] if len(order) else order
    codes[i_area[first]] = drainage_areas["code"].to_numpy()[i_drainage[first]]
    return codes


def _neighbouring_basins(areas: gpd.GeoDataFrame, unassigned: np.ndarray) -> pd.DataFrame:
    """The basins that the `unassigned` areas border, with how long a border they share, in m.

    The length of the border is measured as the overlap of the area within `NEIGHBOUR_DISTANCE`, divided by that
    distance, which allows for small gaps and overlaps between the areas. Returns the area number, the basin, the
    length of the border, and that of the border within the drainage area of the area.
    """
    geometry = areas.geometry.to_numpy()
    assigned = np.flatnonzero(areas["basin"].to_numpy() != -1)
    buffered = shapely.buffer(geometry[unassigned], NEIGHBOUR_DISTANCE)
    i_unassigned, i_assigned = shapely.STRtree(geometry[assigned]).query(buffered, predicate="intersects")
    rows, neighbours = unassigned[i_unassigned], assigned[i_assigned]
    border = shapely.area(shapely.intersection(buffered[i_unassigned], geometry[neighbours])) / NEIGHBOUR_DISTANCE
    drainage = areas["drainage_area"].to_numpy()
    same_drainage = (drainage[rows] == drainage[neighbours]) & pd.notna(drainage[rows])
    pairs = pd.DataFrame(
        {
            "row": rows,
            "basin": areas["basin"].to_numpy()[neighbours],
            "border": border,
            "drainage_border": np.where(same_drainage, border, 0.0),
        }
    )
    return pairs.groupby(["row", "basin"], as_index=False)[["border", "drainage_border"]].sum()


def _basin_of_longest_border(pairs: pd.DataFrame) -> pd.Series:
    """The basin with the longest border within the drainage area for each row, else the longest border."""
    pairs = pairs[pairs["border"] > 0]
    pairs = pairs.assign(in_drainage=pairs.groupby("row")["drainage_border"].transform("max") > 0)
    pairs = pairs.assign(length=np.where(pairs["in_drainage"], pairs["drainage_border"], pairs["border"]))
    pairs = pairs.sort_values(["row", "length", "basin"], ascending=[True, False, True], kind="stable")
    return pairs.drop_duplicates(subset="row").set_index("row")["basin"]


def _fill_gaps(basin_areas: gpd.GeoDataFrame, max_gap_area: float) -> gpd.GeoDataFrame:
    """Give each gap of at most `max_gap_area` m² between the basin areas to the one sharing the longest border."""
    union = union_polygons(basin_areas.geometry.to_numpy())
    gaps = [Polygon(ring) for part in shapely.get_parts(union) for ring in part.interiors]
    gaps = [gap for gap in gaps if gap.area <= max_gap_area]
    if not gaps:
        return basin_areas
    geometry = basin_areas.geometry.to_numpy()
    buffered = shapely.buffer(np.array(gaps), NEIGHBOUR_DISTANCE)
    i_gap, i_basin = shapely.STRtree(geometry).query(buffered, predicate="intersects")
    border = shapely.area(shapely.intersection(buffered[i_gap], geometry[i_basin]))
    order = np.lexsort((i_basin, -border, i_gap))
    first = order[np.r_[True, np.diff(i_gap[order]) != 0]]
    gaps_of_row: dict[int, list[Polygon]] = {}
    for gap, row in zip(i_gap[first], i_basin[first], strict=True):
        gaps_of_row.setdefault(int(row), []).append(gaps[gap])
    filled = list(geometry)
    for row, gap_polygons in gaps_of_row.items():
        filled[row] = MultiPolygon(polygon_parts(union_polygons(np.array([geometry[row], *gap_polygons]))))
    basin_areas = basin_areas.set_geometry(gpd.GeoSeries(filled, index=basin_areas.index, crs=basin_areas.crs))
    logger.info(f"Filled {len(gaps)} gaps between basin areas")
    return basin_areas


def _assign_areas_to_basins_within(areas: gpd.GeoDataFrame, basins: gpd.GeoDataFrame, drained: np.ndarray) -> None:
    """Give groups of areas without basin that contain the location of one basin without area to that basin, in place.

    The areas that drain to the same basin, which is not in `basins`, form a group, and each other area a group of its
    own. `drained` holds the basin that each area drains to, or -1.
    """
    unassigned = np.flatnonzero(areas["basin"].to_numpy() == -1)
    group = np.where(drained[unassigned] == -1, -2 - unassigned, drained[unassigned])
    without_area = basins[~basins["basin"].isin(areas["basin"])]
    i_basin, i_area = shapely.STRtree(areas.geometry.to_numpy()[unassigned]).query(
        without_area.geometry.to_numpy(), predicate="within"
    )
    candidates = pd.DataFrame({"group": group[i_area], "basin": without_area["basin"].to_numpy()[i_basin]})
    candidates = candidates.drop_duplicates()
    one = candidates.groupby("group")["basin"].transform("size") == 1
    basin_of_group = candidates[one].set_index("group")["basin"]
    assign = np.isin(group, basin_of_group.index)
    logger.info(f"Assigning {assign.sum()} areas to the basin without area within them")
    basin = areas["basin"].to_numpy().copy()
    basin[unassigned[assign]] = basin_of_group.loc[group[assign]].to_numpy()
    areas["basin"] = basin


def assign_areas(
    areas: gpd.GeoDataFrame,
    basins: pd.DataFrame,
    drainage_areas: gpd.GeoDataFrame | None = None,
    max_gap_area: float = 0.0,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Assign every area to a basin, and make the basin areas from the areas of each basin.

    An area keeps the basin it drains to, see `area_basins`, if that basin is in `basins`. With the geometry of
    `basins`, the areas that drain to the same other basin, or to none, go to the one basin without area within them.
    The other areas go to the basin with whose areas they share the longest border, see `_neighbouring_basins`,
    counting only the areas in the same drainage area if they border any. This repeats, so areas also reach a basin
    through other areas. Areas that border no area of a basin go to the nearest. The gaps between the basin areas up to
    `max_gap_area` m² go to the basin area they share the longest border with.

    Returns the areas with their `basin`, and the basin areas, with their `basin`, as MultiPolygons.
    """
    areas = areas[areas.geometry.notna()].reset_index(drop=True)
    drained = areas["basin"].to_numpy()
    areas["basin"] = areas["basin"].where(areas["basin"].isin(basins["basin"]), -1)
    areas["drainage_area"] = _drainage_area_codes(areas, drainage_areas)
    assert areas["basin"].ne(-1).any(), "no area drains to a basin"
    if isinstance(basins, gpd.GeoDataFrame):
        _assign_areas_to_basins_within(areas, basins, drained)

    while (unassigned := np.flatnonzero(areas["basin"].to_numpy() == -1)).size > 0:
        basin = _basin_of_longest_border(_neighbouring_basins(areas, unassigned))
        if basin.empty:
            break
        logger.info(f"Assigning {len(basin)} of {len(unassigned)} areas to the basin they border")
        areas.loc[basin.index, "basin"] = basin.to_numpy()

    if unassigned.size > 0:
        geometry = areas.geometry.to_numpy()
        assigned = np.flatnonzero(areas["basin"].to_numpy() != -1)
        i_unassigned, i_assigned = shapely.STRtree(geometry[assigned]).query_nearest(geometry[unassigned])
        # the first of equally near areas
        order = np.lexsort((i_assigned, i_unassigned))
        nearest = pd.Series(
            areas["basin"].to_numpy()[assigned[i_assigned[order]]], index=unassigned[i_unassigned[order]]
        )
        nearest = nearest[~nearest.index.duplicated()]
        logger.warning(f"Assigning {len(nearest)} areas that border no area of a basin to the nearest")
        areas.loc[nearest.index, "basin"] = nearest.to_numpy()

    basin_areas = areas.groupby("basin")["geometry"].apply(lambda parts: union_polygons(parts.to_numpy()))
    basin_areas = gpd.GeoDataFrame(
        {"basin": basin_areas.index.to_numpy()},
        geometry=[MultiPolygon(polygon_parts(g)) for g in basin_areas.to_numpy()],
        crs=areas.crs,
    )
    if max_gap_area > 0:
        basin_areas = _fill_gaps(basin_areas, max_gap_area)
    return areas, basin_areas


def fill_holes(
    basin_areas: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame,
    drainage_areas: gpd.GeoDataFrame | None = None,
    tolerance: float = 0.001,
) -> gpd.GeoDataFrame:
    """Give the parts of `areas` that no basin area covers to a basin area.

    The uncovered part of an area goes to the basin area that covers most of that area. The areas that no basin area
    covers are assigned as in `assign_areas`, to the basin area they share the longest border with, preferably in the
    same drainage area. Slivers, polygons on average narrower than `tolerance`, are left out. Returns one basin area
    per `node_id`, as MultiPolygons.
    """
    areas = areas[areas.geometry.notna()]
    geometry = np.array([_valid_polygon(c, g) for c, g in zip(areas["code"], areas.geometry, strict=True)])
    basin_geometry = basin_areas.geometry.to_numpy()
    node_ids = basin_areas["node_id"].to_numpy()

    # the node covering most of each area
    i_area, i_basin = shapely.STRtree(basin_geometry).query(geometry, predicate="intersects")
    cover = pd.DataFrame(
        {
            "area": i_area,
            "node_id": node_ids[i_basin],
            "overlap": shapely.area(shapely.intersection(geometry[i_area], basin_geometry[i_basin])),
        }
    )
    cover = cover.groupby(["area", "node_id"], as_index=False)["overlap"].sum()
    cover = cover[cover["overlap"] > tolerance * shapely.length(geometry[cover["area"]])]
    cover = cover.sort_values(["area", "overlap", "node_id"], ascending=[True, False, True], kind="stable")
    covering_node = cover.drop_duplicates(subset="area").set_index("area")["node_id"]

    # the uncovered parts of the areas, with that node, or -1
    uncovered = shapely.difference(geometry, union_polygons(basin_geometry))
    holes = [(i, part) for i, g in enumerate(uncovered.tolist()) for part in polygon_parts(g, tolerance)]
    logger.info(f"Filling {len(holes)} holes of {sum(p.area for _, p in holes) / 1e6:.2f} km2")
    parts = basin_areas[["node_id", "geometry"]].explode(index_parts=False)
    frame = gpd.GeoDataFrame(
        {
            "basin": [*parts["node_id"], *(covering_node.get(i, -1) for i, _ in holes)],
            "code": None,
        },
        geometry=[*parts.geometry, *(p for _, p in holes)],
        crs=basin_areas.crs,
    )
    frame["area"] = np.arange(len(frame))
    _, filled = assign_areas(frame, pd.DataFrame({"basin": np.unique(node_ids)}), drainage_areas)
    return filled.rename(columns={"basin": "node_id"})


def lump_network(
    network: Network,
    split_nodes: gpd.GeoDataFrame,
    boundaries: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame,
    laterals: gpd.GeoDataFrame | None = None,
    remove_isolated: bool = False,
    include_flow_boundary_basins: bool = True,
    include_level_boundary_basins: bool = False,
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
    laterals : gpd.GeoDataFrame | None
        The laterals, to assign areas to basins with, instead of the edges in each area.
    remove_isolated : bool
        Remove the basins without connections.
    include_flow_boundary_basins, include_level_boundary_basins : bool
        Keep the basins between a boundary and its split nodes.
    """
    graph = create_graph(network.nodes, network.edges)
    graph, split_nodes = split_graph(graph, split_nodes, network.edges)
    nodes, edges = add_basins_to_nodes_and_edges(graph, network.nodes, network.edges, split_nodes)
    split_nodes = check_split_nodes_used(split_nodes, nodes, edges)
    basins = create_basins(graph, nodes, edges)
    logger.info(f"{len(basins)} basins")
    areas = area_basins(edges, areas, laterals)
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
        basins = remove_isolated_basins(basins, basin_connections, boundary_connections)
    boundaries, split_nodes, basins, basin_connections, boundary_connections = regenerate_node_ids(
        boundaries, split_nodes, basins, basin_connections, boundary_connections
    )
    return LumpedNetwork(
        basins=basins,
        split_nodes=split_nodes,
        boundaries=boundaries,
        basin_connections=basin_connections,
        boundary_connections=boundary_connections,
        areas=areas,
    )
