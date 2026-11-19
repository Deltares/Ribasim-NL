"""Prepare a model for allocation, see https://ribasim.org/concept/allocation.html.

Ribasim solves an allocation problem per subnetwork, the nodes with the same `subnetwork_id`. All nodes of a
subnetwork must be connected. The primary network, with ID 1, connects to the secondary networks only through
allocation-controlled Pumps and Outlets, and secondary networks cannot connect to each other directly.
"""

import networkx as nx
import pandas as pd
from ribasim import Model

# Ribasim checks the connectivity of a subnetwork over the flow and control links, without Observation nodes
UNCONNECTED_NODE_TYPES = ("Observation",)
# Subnetwork 1 is the primary network. Starting at 2 makes every subnetwork independent of the others.
FIRST_SUBNETWORK_ID = 2


def connectivity_graph(model: Model) -> nx.Graph:
    """Return the graph over which Ribasim checks the connectivity of subnetworks, without Junctions.

    Like `simplify_graph!` in the Ribasim core, a connected group of Junctions with exactly one Basin as a neighbor
    connects all its other neighbors with that Basin, in any direction. Another group connects the nodes that flow
    into it with the nodes it flows to along the direction of its links. Junctions with rows in Junction / static stay
    in the simulation, which is not supported here.
    """
    node_df = model.node.df
    assert node_df is not None, "The model has no nodes"
    link_df = model.link.df
    assert link_df is not None, "The model has no links"
    missing = set(link_df["from_node_id"]).union(link_df["to_node_id"]) - set(node_df.index)
    assert not missing, f"Links connect to nodes that are not in the Node table: {sorted(missing)[:10]}"
    assert model.junction.static.df is None or model.junction.static.df.empty, "Junction / static is not supported"

    node_type = node_df["node_type"]
    junctions = set(node_df.index[node_type == "Junction"])
    graph = nx.Graph()
    graph.add_nodes_from(node_df.index[~node_type.isin((*UNCONNECTED_NODE_TYPES, "Junction"))])
    control = link_df[link_df["link_type"] == "control"]
    assert junctions.isdisjoint({*control["from_node_id"], *control["to_node_id"]}), "Junctions with control links"
    graph.add_edges_from(zip(control["from_node_id"], control["to_node_id"], strict=True))

    flow = link_df[link_df["link_type"] == "flow"]
    downstream = nx.DiGraph(zip(flow["from_node_id"], flow["to_node_id"], strict=True))
    graph.add_edges_from(
        (f, t)
        for f, t in zip(flow["from_node_id"], flow["to_node_id"], strict=True)
        if f not in junctions and t not in junctions
    )
    for group in nx.connected_components(downstream.to_undirected().subgraph(junctions)):
        inflows = [(n, j) for j in group for n in downstream.predecessors(j) if n not in junctions]
        outflows = [(j, n) for j in group for n in downstream.successors(j) if n not in junctions]
        basins = {n for n, _ in inflows} | {n for _, n in outflows}
        basins = {n for n in basins if node_type[n] == "Basin"}
        if len(basins) == 1:
            (basin,) = basins
            graph.add_edges_from((basin, n) for n, _ in inflows if n != basin)
            graph.add_edges_from((basin, n) for _, n in outflows if n != basin)
            continue
        group_downstream = downstream.subgraph(group)
        for from_id, junction in inflows:
            reached = nx.descendants(group_downstream, junction) | {junction}
            graph.add_edges_from((from_id, to_id) for j, to_id in outflows if j in reached)
    return graph


def connected_subnetwork_ids(model: Model) -> pd.Series:
    """Return a subnetwork ID for each node, one per part of the model that is connected, largest first.

    Parts of the same size are ordered by their smallest node ID. Observation nodes get no subnetwork ID, and
    Junctions get the smallest subnetwork ID of the nodes next to their Junction tree.
    """
    node_df = model.node.df
    assert node_df is not None, "The model has no nodes"
    components = sorted(nx.connected_components(connectivity_graph(model)), key=lambda c: (-len(c), min(c)))
    subnetwork_id = pd.Series(pd.NA, index=node_df.index, dtype="Int32", name="subnetwork_id")
    for i, component in enumerate(components):
        subnetwork_id[list(component)] = FIRST_SUBNETWORK_ID + i

    link_df = model.link.df
    assert link_df is not None, "The model has no links"
    flow = link_df[link_df["link_type"] == "flow"]
    junctions = set(node_df.index[node_df["node_type"] == "Junction"])
    trees = nx.Graph(zip(flow["from_node_id"], flow["to_node_id"], strict=True))
    for tree in nx.connected_components(trees.subgraph(junctions)):
        neighbors = {n for j in tree for n in trees.neighbors(j)} - junctions
        ids = subnetwork_id[sorted(neighbors)].dropna()
        if not ids.empty:
            subnetwork_id[sorted(tree)] = ids.min()
    return subnetwork_id


def enable_allocation(model: Model) -> None:
    """Enable allocation, with an independent subnetwork per part of the model that is connected.

    This is a first version without a primary network, so the parts that are connected, which are mostly a single
    nationwide network, each form a single allocation problem.
    """
    assert model.node.df is not None, "The model has no nodes"
    model.node.df["subnetwork_id"] = connected_subnetwork_ids(model)
    model.experimental.allocation = True
