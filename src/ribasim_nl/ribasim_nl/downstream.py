from collections import deque

import networkx as nx


def downstream_nodes(graph: nx.DiGraph, node_id: int, stop_at_outlet: bool = False):
    """Efficiently find all downstream nodes in a directed graph starting from a given node,
    stopping traversal at nodes stopping at the next outlet.

    Parameters
    ----------
    - graph (nx.DiGraph): The directed graph.
    - start_node: The node to start the search from.
    - stop_at_outlet (bool): To stop at the next inlet(s)

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

                # Stop traversal if 'function' is 'outlet'
                if (not stop_at_outlet) | (not graph.nodes[successor].get("function") == "outlet"):
                    queue.append(successor)

    return node_ids
