# %%
from collections import deque

import networkx as nx


def downstream_nodes(
    graph: nx.DiGraph,
    node_id: int,
    stop_at_outlet: bool = False,
    stop_at_node_type: str | None = None,
    stop_at_node_ids: list[int] = [],
):
    """Efficiently find all downstream nodes in a directed graph starting from a given node,
    stopping traversal at nodes stopping at the next outlet.

    Parameters
    ----------
    - graph (nx.DiGraph): The directed graph.
    - node_id: The node to start the search from.
    - stop_at_outlet (bool): To stop at the next inlet(s)
    - stop_at_node_type (str | None): To stop at a specific node type (e.g., 'Basin', 'LevelBoundary')
    - stop_at_node_ids (list[int]): List of node IDs at which to stop traversal.

    Returns
    -------
    - set: A set of all downstream nodes excluding the starting node.
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

        # Check successors (downstream neighbors)
        for successor in graph.successors(current_node):
            if successor not in visited:
                node_ids.add(successor)

                # Determine if we should queue the successor for further exploration
                queue_successor = True

                # if we want to stop at outlet and the successor is an outlet
                if stop_at_outlet and graph.nodes[successor].get("function") == "outlet":
                    queue_successor = False

                # if we stop at a specific node type and the successor matches that type
                if stop_at_node_type is not None and graph.nodes[successor].get("node_type") == stop_at_node_type:
                    queue_successor = False

                # if the successor is in the list of node IDs to stop at
                if successor in stop_at_node_ids:
                    queue_successor = False

                if queue_successor:
                    queue.append(successor)

    return node_ids


# %%
