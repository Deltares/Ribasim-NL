"""Network helpers for coupling-level checks."""

import pandas as pd

from ribasim_nl.coupling_level_common import as_int


def first_non_junction(
    node_id: int,
    graph: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
    max_iter: int = 50,
) -> tuple[int | None, int | None, str | None]:
    """Follow one flow branch through Junction nodes until a real node is found."""
    current_node_id = int(node_id)
    seen_node_ids = {current_node_id}
    first_link_id = None

    for _ in range(max_iter):
        links = graph.get(current_node_id, [])
        if len(links) == 0:
            return None, first_link_id, "geen link"
        if len(links) > 1:
            return None, first_link_id, f"meerdere links: {links}"

        link_id, next_node_id = links[0]
        if first_link_id is None:
            first_link_id = int(link_id)
        next_node_id = int(next_node_id)
        if next_node_id in seen_node_ids:
            return None, first_link_id, f"cyclus via node {next_node_id}"

        next_node_type = node_type_by_id.get(next_node_id)
        if next_node_type != "Junction":
            return next_node_id, first_link_id, None

        current_node_id = next_node_id
        seen_node_ids.add(current_node_id)

    return None, first_link_id, f"geen niet-Junction node binnen {max_iter} stappen"


def flow_link_graphs(
    link_df: pd.DataFrame,
) -> tuple[dict[int, list[tuple[int, int]]], dict[int, list[tuple[int, int]]]]:
    """Build outgoing and incoming flow-link lookups by node_id."""
    flow_link_df = link_df[link_df["link_type"].fillna("flow").eq("flow")].copy()
    outgoing_flow_links: dict[int, list[tuple[int, int]]] = {}
    for from_node_id, rows in flow_link_df.groupby("from_node_id"):
        outgoing_flow_links[as_int(from_node_id)] = [
            (as_int(row.link_id), as_int(row.to_node_id)) for row in rows.itertuples(index=False)
        ]

    incoming_flow_links: dict[int, list[tuple[int, int]]] = {}
    for to_node_id, rows in flow_link_df.groupby("to_node_id"):
        incoming_flow_links[as_int(to_node_id)] = [
            (as_int(row.link_id), as_int(row.from_node_id)) for row in rows.itertuples(index=False)
        ]

    return outgoing_flow_links, incoming_flow_links
