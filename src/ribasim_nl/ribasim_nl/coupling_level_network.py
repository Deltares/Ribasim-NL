"""Network helpers for coupling-level checks."""

from __future__ import annotations

import pandas as pd

from ribasim_nl.coupling_level_common import CONTROL_NODE_TYPES, as_int


def first_non_junction(
    node_id: int,
    graph: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
    max_iter: int = 50,
) -> tuple[int | None, int | None, str | None]:
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


def downstream_targets(
    node_id: int,
    outgoing_flow_links: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
    max_iter: int = 50,
) -> list[tuple[int | None, int, str | None]]:
    targets: list[tuple[int | None, int, str | None]] = []

    for first_link_id, next_node_id in outgoing_flow_links.get(int(node_id), []):
        queue: list[tuple[int, set[int]]] = [(int(next_node_id), {int(node_id)})]
        iterations = 0

        while queue and iterations < max_iter:
            iterations += 1
            current_node_id, seen_node_ids = queue.pop(0)
            if current_node_id in seen_node_ids:
                targets.append((None, int(first_link_id), f"cyclus via node {current_node_id}"))
                continue

            next_node_type = node_type_by_id.get(current_node_id)
            if next_node_type != "Junction":
                targets.append((current_node_id, int(first_link_id), None))
                continue

            downstream_links = outgoing_flow_links.get(current_node_id, [])
            if len(downstream_links) == 0:
                targets.append((None, int(first_link_id), "geen downstream flow-link na Junction"))
                continue

            next_seen_node_ids = {*seen_node_ids, current_node_id}
            for _, downstream_node_id in downstream_links:
                queue.append((int(downstream_node_id), next_seen_node_ids))

        if queue:
            targets.append((None, int(first_link_id), f"geen downstream niet-Junction node binnen {max_iter} stappen"))

    return targets


def has_alternative_downstream_route(
    start_node_id: int,
    blocked_node_id: int,
    outgoing_flow_links: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
    max_iter: int = 200,
) -> bool:
    queue = [int(start_node_id)]
    seen_node_ids = {int(start_node_id), int(blocked_node_id)}

    for _ in range(max_iter):
        if not queue:
            return False

        current_node_id = queue.pop(0)
        for _, next_node_id in outgoing_flow_links.get(current_node_id, []):
            next_node_id = int(next_node_id)
            if next_node_id in seen_node_ids:
                continue

            node_type = node_type_by_id.get(next_node_id)
            if node_type != "Junction":
                return True

            seen_node_ids.add(next_node_id)
            queue.append(next_node_id)

    return False


def downstream_basin_for_max_level(
    first_downstream_basin_id: int | None,
) -> tuple[int | None, str | None, str]:
    if first_downstream_basin_id is None:
        return None, "geen eerste downstream Basin", "geen_downstream_basin"

    return int(first_downstream_basin_id), None, "downstream_streefpeil"


def downstream_basin_has_direct_control(
    basin_id: int | None,
    outgoing_flow_links: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
) -> tuple[bool, str | None]:
    if basin_id is None:
        return False, "geen downstream Basin"

    targets = downstream_targets(int(basin_id), outgoing_flow_links, node_type_by_id)
    target_errors = [error for _, _, error in targets if error is not None]
    if target_errors:
        return False, "; ".join(target_errors)

    target_types = [node_type_by_id.get(node_id) for node_id, _, _ in targets if node_id is not None]
    if any(node_type == "ManningResistance" for node_type in target_types):
        return False, "downstream ManningResistance niet gebruikt voor max_downstream_level"
    if any(node_type in CONTROL_NODE_TYPES for node_type in target_types):
        return True, None
    return False, f"geen direct downstream Outlet/Pump gevonden: {target_types}"


def flow_link_graphs(
    link_df: pd.DataFrame,
) -> tuple[dict[int, list[tuple[int, int]]], dict[int, list[tuple[int, int]]]]:
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
