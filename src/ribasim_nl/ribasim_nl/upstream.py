# %%
from collections import deque

import networkx as nx


def upstream_nodes(graph: nx.DiGraph, node_id: int, stop_at_inlet: bool = False):
    """Efficiently find all upstream nodes in a directed graph starting from a given node,
    stopping traversal at nodes stopping at the next inlet.

    Parameters
    ----------
    - graph (nx.DiGraph): The directed graph.
    - start_node (int): The node to start the search from.
    - stop_at_inlet (bool): To stop at the next inlet(s)

    Returns
    -------
    - set: A set of all upstream nodes excluding the starting node.
    """  # noqa: D205
    visited = set()  # To keep track of visited nodes
    node_ids = set({node_id})  # To store the result

    # BFS using a deque
    queue = deque([node_id])

    while queue:
        current_node = queue.popleft()

        # Avoid re-visiting nodes
        if current_node in visited:
            continue
        visited.add(current_node)

        # Check predecessors (upstream neighbors)
        for predecessor in graph.predecessors(current_node):
            if predecessor not in visited:
                node_ids.add(predecessor)

            # Stop traversal if 'node_type is 'level boundary' or '
            if not graph.nodes[predecessor].get("node_type") == "LevelBoundary":
                # Stop traversal if `function` is inlet (and we check on it)
                if (not stop_at_inlet) | (not graph.nodes[predecessor].get("function") == "inlet"):
                    queue.append(predecessor)

    return node_ids
