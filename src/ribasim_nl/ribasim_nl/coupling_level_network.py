"""Network helpers for coupling-level checks."""

from collections.abc import Mapping
from typing import Any

import pandas as pd

from ribasim_nl.coupling_level_common import as_int


def first_non_junction(
    node_id: int,
    graph: dict[int, list[tuple[int, int]]],
    node_type_by_id: Mapping[Any, str],
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
    flow_link_df = link_df[link_df["link_type"].fillna("flow").eq("flow")]
    outgoing_flow_links: dict[int, list[tuple[int, int]]] = {}
    incoming_flow_links: dict[int, list[tuple[int, int]]] = {}
    for link_id, from_node_id, to_node_id in zip(
        flow_link_df["link_id"], flow_link_df["from_node_id"], flow_link_df["to_node_id"], strict=True
    ):
        outgoing_flow_links.setdefault(as_int(from_node_id), []).append((as_int(link_id), as_int(to_node_id)))
        incoming_flow_links.setdefault(as_int(to_node_id), []).append((as_int(link_id), as_int(from_node_id)))
    return outgoing_flow_links, incoming_flow_links
