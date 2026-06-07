# pyrefly: ignore-errors
from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

from ribasim_nl.control_layout import control_condition_thresholds, control_layouts

if TYPE_CHECKING:
    from ribasim_nl import Model


CONTROL_NODE_TYPES = {"Outlet", "Pump"}
OPEN_NODE_TYPES = {"Basin", "ManningResistance", "Junction"}
LEVEL_UPDATE_PROTECTION_COLUMN = "meta_level_update_protected"


def _node_type_by_id(model: Model) -> dict[int, str]:
    return model.node.df["node_type"].to_dict()


def _flow_link_df(model: Model) -> pd.DataFrame:
    link_df = model.link.df.copy()
    if "link_id" not in link_df.columns:
        index_name = model.link.df.index.name or "link_id"
        link_df = link_df.reset_index(drop=False)
        if index_name not in link_df.columns and "index" in link_df.columns:
            link_df = link_df.rename(columns={"index": "link_id"})
        elif index_name != "link_id" and index_name in link_df.columns:
            link_df = link_df.rename(columns={index_name: "link_id"})

    if "link_type" in link_df.columns:
        link_df = link_df[link_df["link_type"].fillna("flow").eq("flow")]

    return link_df


def _outgoing_flow_links(model: Model) -> dict[int, list[tuple[int, int]]]:
    link_df = _flow_link_df(model)
    outgoing: dict[int, list[tuple[int, int]]] = defaultdict(list)
    for row in link_df.itertuples(index=False):
        outgoing[int(row.from_node_id)].append((int(row.link_id), int(row.to_node_id)))
    return dict(outgoing)


def _undirected_open_adjacency(
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> dict[int, list[int]]:
    adjacency: dict[int, list[int]] = defaultdict(list)
    for row in link_df.itertuples(index=False):
        from_node_id = int(row.from_node_id)
        to_node_id = int(row.to_node_id)
        if node_type_by_id.get(from_node_id) not in OPEN_NODE_TYPES:
            continue
        if node_type_by_id.get(to_node_id) not in OPEN_NODE_TYPES:
            continue

        adjacency[from_node_id].append(to_node_id)
        adjacency[to_node_id].append(from_node_id)

    return dict(adjacency)


def _open_neighbors(
    node_id: int,
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> list[int]:
    neighbors: list[int] = []
    for row in link_df[link_df["from_node_id"].eq(int(node_id)) | link_df["to_node_id"].eq(int(node_id))].itertuples(
        index=False
    ):
        other_node_id = int(row.to_node_id) if int(row.from_node_id) == int(node_id) else int(row.from_node_id)
        if node_type_by_id.get(other_node_id) in OPEN_NODE_TYPES:
            neighbors.append(other_node_id)
    return sorted(set(neighbors))


def _directed_open_neighbors_from_connector(
    node_id: int,
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> list[int]:
    downstream_neighbors = [
        int(row.to_node_id)
        for row in link_df[link_df["from_node_id"].eq(int(node_id))].itertuples(index=False)
        if node_type_by_id.get(int(row.to_node_id)) in OPEN_NODE_TYPES
    ]
    if downstream_neighbors:
        return sorted(set(downstream_neighbors))

    upstream_neighbors = [
        int(row.from_node_id)
        for row in link_df[link_df["to_node_id"].eq(int(node_id))].itertuples(index=False)
        if node_type_by_id.get(int(row.from_node_id)) in OPEN_NODE_TYPES
    ]
    return sorted(set(upstream_neighbors))


def _open_neighbors_from_connector(
    node_id: int,
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> list[int]:
    neighbors: set[int] = set()
    for row in link_df[link_df["from_node_id"].eq(int(node_id)) | link_df["to_node_id"].eq(int(node_id))].itertuples(
        index=False
    ):
        other_node_id = int(row.to_node_id) if int(row.from_node_id) == int(node_id) else int(row.from_node_id)
        if node_type_by_id.get(other_node_id) in OPEN_NODE_TYPES:
            neighbors.add(other_node_id)

    return sorted(neighbors)


def _open_component(start_node_id: int, adjacency: dict[int, list[int]], max_iter: int) -> set[int]:
    component = {int(start_node_id)}
    queue: deque[int] = deque([int(start_node_id)])

    while queue and len(component) < max_iter:
        node_id = queue.popleft()
        for next_node_id in adjacency.get(node_id, []):
            if next_node_id in component:
                continue
            component.add(next_node_id)
            queue.append(next_node_id)

    if queue:
        raise ValueError(f"Open Manning-component vanaf node {start_node_id} groter dan {max_iter} nodes.")

    return component


def _component_boundary_control_node_ids(
    component_node_ids: set[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> list[int]:
    control_node_ids: set[int] = set()
    for row in link_df.itertuples(index=False):
        from_node_id = int(row.from_node_id)
        to_node_id = int(row.to_node_id)
        if from_node_id in component_node_ids and node_type_by_id.get(to_node_id) in CONTROL_NODE_TYPES:
            control_node_ids.add(to_node_id)
        if to_node_id in component_node_ids and node_type_by_id.get(from_node_id) in CONTROL_NODE_TYPES:
            control_node_ids.add(from_node_id)
    return sorted(control_node_ids)


def _supply_boundary_control_node_ids(
    *,
    boundary_control_node_ids: list[int],
    control_function_by_node_id: dict[int, str | None],
) -> list[int]:
    return sorted(
        int(node_id)
        for node_id in boundary_control_node_ids
        if control_function_by_node_id.get(int(node_id)) in {"inlaat", "doorlaat"}
    )


def _component_boundary_basin_node_ids(
    component_node_ids: set[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> list[int]:
    boundary_basin_node_ids: set[int] = set()
    for row in link_df.itertuples(index=False):
        from_node_id = int(row.from_node_id)
        to_node_id = int(row.to_node_id)
        from_in_component = from_node_id in component_node_ids
        to_in_component = to_node_id in component_node_ids
        if from_in_component == to_in_component:
            continue

        inside_node_id = from_node_id if from_in_component else to_node_id
        outside_node_id = to_node_id if from_in_component else from_node_id
        if node_type_by_id.get(outside_node_id) not in CONTROL_NODE_TYPES:
            continue

        if node_type_by_id.get(inside_node_id) == "Basin":
            boundary_basin_node_ids.add(inside_node_id)

    return sorted(boundary_basin_node_ids)


def _control_function_by_node_id(model: Model, node_type_by_id: dict[int, str]) -> dict[int, str | None]:
    function_by_node_id: dict[int, str | None] = {}
    for node_type in CONTROL_NODE_TYPES:
        component = getattr(model, node_type.lower(), None)
        static_df = getattr(getattr(component, "static", None), "df", None)
        if static_df is None or static_df.empty or "node_id" not in static_df.columns:
            continue

        for connector_node_id, connector_rows in static_df.groupby("node_id", sort=True):
            connector_node_id = int(connector_node_id)
            if node_type_by_id.get(connector_node_id) != node_type:
                continue

            function_by_node_id[connector_node_id] = _connector_function(connector_rows)

    return function_by_node_id


def _upstream_protected_basin_node_ids(
    component_node_ids: set[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
    control_function_by_node_id: dict[int, str | None],
    boundary_control_node_ids: list[int],
) -> set[int]:
    """Return headwater basins without upstream supply/flow-control steering."""
    component_control_functions = {
        control_function_by_node_id.get(int(node_id)) for node_id in boundary_control_node_ids
    }
    if any(function in {"inlaat", "doorlaat"} for function in component_control_functions):
        return set()

    component_node_ids = {int(node_id) for node_id in component_node_ids}
    basin_node_ids = {node_id for node_id in component_node_ids if node_type_by_id.get(node_id) == "Basin"}
    upstream_open_by_node_id: dict[int, set[int]] = {node_id: set() for node_id in component_node_ids}
    upstream_control_by_node_id: dict[int, set[int]] = {node_id: set() for node_id in component_node_ids}

    for row in link_df.itertuples(index=False):
        from_node_id = int(row.from_node_id)
        to_node_id = int(row.to_node_id)
        if to_node_id not in component_node_ids:
            continue

        if from_node_id in component_node_ids:
            upstream_open_by_node_id[to_node_id].add(from_node_id)
        elif node_type_by_id.get(from_node_id) in CONTROL_NODE_TYPES:
            upstream_control_by_node_id[to_node_id].add(from_node_id)

    protected_basin_node_ids: set[int] = set()
    for basin_node_id in basin_node_ids:
        queue: deque[int] = deque([basin_node_id])
        seen_node_ids = {basin_node_id}
        has_upstream_basin = False
        upstream_control_node_ids: set[int] = set()

        while queue:
            node_id = queue.popleft()
            upstream_control_node_ids.update(upstream_control_by_node_id[node_id])

            for upstream_node_id in upstream_open_by_node_id[node_id]:
                if upstream_node_id in seen_node_ids:
                    continue
                if upstream_node_id != basin_node_id and node_type_by_id.get(upstream_node_id) == "Basin":
                    has_upstream_basin = True
                    break
                seen_node_ids.add(upstream_node_id)
                queue.append(upstream_node_id)

            if has_upstream_basin:
                break

        upstream_control_functions = {control_function_by_node_id.get(node_id) for node_id in upstream_control_node_ids}
        has_upstream_supply_or_flow_control = any(
            function in {"inlaat", "doorlaat"} for function in upstream_control_functions
        )
        if not has_upstream_basin and not has_upstream_supply_or_flow_control:
            protected_basin_node_ids.add(basin_node_id)

    return protected_basin_node_ids


def _single_manning_branch_basin_node_ids(
    component_node_ids: set[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
    control_function_by_node_id: dict[int, str | None],
    ignored_supply_control_node_ids: set[int] | None = None,
) -> set[int]:
    protected_basin_node_ids: set[int] = set()
    component_node_ids = {int(node_id) for node_id in component_node_ids}
    ignored_supply_control_node_ids = {int(node_id) for node_id in ignored_supply_control_node_ids or set()}

    for basin_node_id in component_node_ids:
        if node_type_by_id.get(basin_node_id) != "Basin":
            continue

        open_neighbors = []
        boundary_control_node_ids = set()
        for row in link_df[
            link_df["from_node_id"].eq(basin_node_id) | link_df["to_node_id"].eq(basin_node_id)
        ].itertuples(index=False):
            from_node_id = int(row.from_node_id)
            to_node_id = int(row.to_node_id)
            other_node_id = to_node_id if from_node_id == basin_node_id else from_node_id

            if other_node_id in component_node_ids:
                open_neighbors.append(other_node_id)
            if node_type_by_id.get(other_node_id) in CONTROL_NODE_TYPES:
                boundary_control_node_ids.add(other_node_id)

        boundary_supply_control_functions = {
            control_function_by_node_id.get(node_id)
            for node_id in boundary_control_node_ids
            if node_id not in ignored_supply_control_node_ids
        }
        if any(function in {"inlaat", "doorlaat"} for function in boundary_supply_control_functions):
            continue

        if len(set(open_neighbors)) != 1:
            continue

        only_neighbor_id = open_neighbors[0]
        if node_type_by_id.get(only_neighbor_id) == "ManningResistance":
            protected_basin_node_ids.add(basin_node_id)

    return protected_basin_node_ids


def _terminal_manning_branch_basin_node_ids(
    component_node_ids: set[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> set[int]:
    """Return terminal basins that should not be leveled through a Manning route."""
    protected_basin_node_ids: set[int] = set()
    component_node_ids = {int(node_id) for node_id in component_node_ids}

    for basin_node_id in component_node_ids:
        if node_type_by_id.get(basin_node_id) != "Basin":
            continue

        open_neighbor_ids: list[int] = []
        non_open_neighbor_types: set[str | None] = set()
        for row in link_df[
            link_df["from_node_id"].eq(basin_node_id) | link_df["to_node_id"].eq(basin_node_id)
        ].itertuples(index=False):
            from_node_id = int(row.from_node_id)
            to_node_id = int(row.to_node_id)
            other_node_id = to_node_id if from_node_id == basin_node_id else from_node_id

            if other_node_id in component_node_ids and node_type_by_id.get(other_node_id) in OPEN_NODE_TYPES:
                open_neighbor_ids.append(other_node_id)
            else:
                non_open_neighbor_types.add(node_type_by_id.get(other_node_id))

        if len(open_neighbor_ids) != 1:
            continue
        if node_type_by_id.get(open_neighbor_ids[0]) != "ManningResistance":
            continue
        if non_open_neighbor_types - {"TabulatedRatingCurve"}:
            continue

        protected_basin_node_ids.add(basin_node_id)

    return protected_basin_node_ids


def _downstream_targets(
    node_id: int,
    outgoing_flow_links: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
    max_iter: int,
) -> list[tuple[int | None, int, str | None]]:
    targets: list[tuple[int | None, int, str | None]] = []

    for first_link_id, next_node_id in outgoing_flow_links.get(int(node_id), []):
        queue: deque[tuple[int, set[int]]] = deque([(int(next_node_id), {int(node_id)})])
        iterations = 0

        while queue and iterations < max_iter:
            iterations += 1
            current_node_id, seen_node_ids = queue.popleft()
            if current_node_id in seen_node_ids:
                targets.append((None, int(first_link_id), f"cyclus via node {current_node_id}"))
                continue

            node_type = node_type_by_id.get(current_node_id)
            if node_type != "Junction":
                targets.append((current_node_id, int(first_link_id), None))
                continue

            downstream_links = outgoing_flow_links.get(current_node_id, [])
            if not downstream_links:
                targets.append((None, int(first_link_id), "geen downstream flow-link na Junction"))
                continue

            next_seen_node_ids = {*seen_node_ids, current_node_id}
            for _, downstream_node_id in downstream_links:
                queue.append((int(downstream_node_id), next_seen_node_ids))

        if queue:
            targets.append((None, int(first_link_id), f"geen downstream niet-Junction node binnen {max_iter} stappen"))

    return targets


def _enclosed_downstream_basin_routes(
    start_basin_id: int,
    outgoing_flow_links: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
    max_iter: int,
) -> tuple[list[tuple[int, list[int]]], list[str]]:
    queue: deque[tuple[int, list[int]]] = deque([(int(start_basin_id), [int(start_basin_id)])])
    seen_basin_ids = {int(start_basin_id)}
    enclosed_routes: list[tuple[int, list[int]]] = []
    errors: list[str] = []

    while queue and len(seen_basin_ids) < max_iter:
        basin_id, path = queue.popleft()
        targets = _downstream_targets(
            node_id=basin_id,
            outgoing_flow_links=outgoing_flow_links,
            node_type_by_id=node_type_by_id,
            max_iter=max_iter,
        )
        errors.extend(error for _, _, error in targets if error is not None)

        control_targets = [
            node_id
            for node_id, _, error in targets
            if error is None and node_id is not None and node_type_by_id.get(node_id) in CONTROL_NODE_TYPES
        ]
        open_targets = [
            node_id
            for node_id, _, error in targets
            if error is None and node_id is not None and node_type_by_id.get(node_id) not in CONTROL_NODE_TYPES
        ]

        if control_targets and not open_targets:
            enclosed_routes.append((basin_id, path))
            continue

        for target_node_id in open_targets:
            node_type = node_type_by_id.get(target_node_id)
            if node_type == "Basin":
                candidate_basin_ids = [int(target_node_id)]
            else:
                candidate_basin_ids = [
                    int(node_id)
                    for node_id, _, error in _downstream_targets(
                        node_id=int(target_node_id),
                        outgoing_flow_links=outgoing_flow_links,
                        node_type_by_id=node_type_by_id,
                        max_iter=max_iter,
                    )
                    if error is None and node_id is not None and node_type_by_id.get(node_id) == "Basin"
                ]

            for candidate_basin_id in candidate_basin_ids:
                if candidate_basin_id in seen_basin_ids:
                    continue
                seen_basin_ids.add(candidate_basin_id)
                queue.append((candidate_basin_id, [*path, int(target_node_id), candidate_basin_id]))

    if queue:
        errors.append(f"geen eenduidig downstream eindpunt binnen {max_iter} basin-stappen")

    return enclosed_routes, errors


def _target_level_by_basin_id(model: Model, target_level_column: str) -> dict[int, float]:
    area_df = model.basin.area.df
    if target_level_column not in area_df.columns:
        raise KeyError(f"Kolom {target_level_column!r} ontbreekt in Basin / area.")

    return (
        area_df[["node_id", target_level_column]]
        .dropna(subset=["node_id", target_level_column])
        .drop_duplicates(subset=["node_id"], keep="last")
        .set_index("node_id")[target_level_column]
        .astype(float)
        .to_dict()
    )


def _level_boundary_level_by_id(model: Model) -> dict[int, float]:
    level_boundary = getattr(model, "level_boundary", None)
    if level_boundary is None:
        return {}

    for table_name in ["time", "static"]:
        table = getattr(level_boundary, table_name, None)
        df = getattr(table, "df", None)
        if df is None or df.empty or not {"node_id", "level"}.issubset(df.columns):
            continue

        return (
            df[["node_id", "level"]]
            .dropna(subset=["node_id", "level"])
            .groupby("node_id")["level"]
            .min()
            .astype(float)
            .to_dict()
        )

    return {}


def _target_level_for_basin_ids(
    basin_node_ids: set[int],
    target_level_by_basin_id: dict[int, float],
) -> float | None:
    levels = [
        float(target_level_by_basin_id[basin_node_id])
        for basin_node_id in basin_node_ids
        if basin_node_id in target_level_by_basin_id and pd.notna(target_level_by_basin_id[basin_node_id])
    ]
    if not levels:
        return None

    return min(levels)


def _side_basin_ids_and_level(
    node_id: int,
    *,
    open_adjacency: dict[int, list[int]],
    node_type_by_id: dict[int, str],
    target_level_by_basin_id: dict[int, float],
    level_boundary_level_by_id: dict[int, float],
    max_iter: int,
) -> tuple[set[int], float | None]:
    node_id = int(node_id)
    node_type = node_type_by_id.get(node_id)

    if node_type == "LevelBoundary":
        return set(), level_boundary_level_by_id.get(node_id)

    if node_type not in OPEN_NODE_TYPES:
        return set(), None

    if node_type == "Basin":
        basin_ids = {node_id}
    else:
        basin_ids = {
            component_node_id
            for component_node_id in _open_component(
                start_node_id=node_id,
                adjacency=open_adjacency,
                max_iter=max_iter,
            )
            if node_type_by_id.get(component_node_id) == "Basin"
        }

    levels = [
        float(target_level_by_basin_id[basin_id])
        for basin_id in basin_ids
        if basin_id in target_level_by_basin_id and pd.notna(target_level_by_basin_id[basin_id])
    ]
    if not levels:
        return basin_ids, None

    return basin_ids, min(levels)


def _candidate_connector_node_ids(model: Model) -> list[int]:
    candidates: list[int] = []
    for node_type in CONTROL_NODE_TYPES:
        component = getattr(model, node_type.lower(), None)
        if component is None or component.static.df is None:
            continue

        static_df = component.static.df.copy()
        if "node_id" not in static_df.columns:
            continue

        if "control_state" not in static_df.columns:
            continue

        control_state = static_df["control_state"].astype("string").str.lower()
        has_control_state = control_state.notna() & control_state.ne("")
        if not has_control_state.any():
            continue

        flow_rate = pd.to_numeric(static_df.get("flow_rate", 0.0), errors="coerce").fillna(0.0)
        max_flow_rate = pd.to_numeric(static_df.get("max_flow_rate", 0.0), errors="coerce").fillna(0.0)
        active_mask = control_state.eq("aanvoer") & (flow_rate.gt(0.0) | max_flow_rate.gt(0.0))

        candidates.extend(static_df.loc[active_mask, "node_id"].dropna().astype(int).to_list())

    node_type_by_id = _node_type_by_id(model)
    return sorted({node_id for node_id in candidates if node_type_by_id.get(node_id) in CONTROL_NODE_TYPES})


def _control_node_ids_by_connector_node_id(model: Model, node_type_by_id: dict[int, str]) -> dict[int, list[int]]:
    link_df = model.link.df.copy()
    if "link_type" in link_df.columns:
        link_df = link_df[link_df["link_type"].eq("control")]

    control_link_df = link_df[
        link_df["from_node_id"].map(node_type_by_id).eq("DiscreteControl")
        & link_df["to_node_id"].map(node_type_by_id).isin(CONTROL_NODE_TYPES)
    ]
    if control_link_df.empty:
        return {}

    return (
        control_link_df.groupby("to_node_id")["from_node_id"]
        .apply(lambda values: [int(value) for value in values])
        .to_dict()
    )


def _flow_demand_target_node_ids(model: Model, node_type_by_id: dict[int, str]) -> set[int]:
    link_df = model.link.df.copy()
    if "link_type" in link_df.columns:
        link_df = link_df[link_df["link_type"].eq("control")]

    flow_demand_links = link_df[
        link_df["from_node_id"].map(node_type_by_id).eq("FlowDemand")
        & link_df["to_node_id"].map(node_type_by_id).isin(CONTROL_NODE_TYPES)
    ]
    if flow_demand_links.empty:
        return set()

    return set(flow_demand_links["to_node_id"].dropna().astype(int))


def _capacity_is_positive(df: pd.DataFrame) -> pd.Series:
    flow_rate = pd.to_numeric(df.get("flow_rate", 0.0), errors="coerce").fillna(0.0)
    max_flow_rate = pd.to_numeric(df.get("max_flow_rate", 0.0), errors="coerce").fillna(0.0)
    return flow_rate.gt(0.0) | max_flow_rate.gt(0.0)


def _truthy_series(series: pd.Series) -> pd.Series:
    if series.empty:
        return pd.Series(dtype=bool)
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").fillna(0).ne(0)
    return series.astype("string").str.lower().isin({"1", "true", "yes", "ja", "y"})


def _connector_function(connector_rows: pd.DataFrame) -> str | None:
    control_state = connector_rows["control_state"].astype("string").str.lower()
    positive_capacity = _capacity_is_positive(connector_rows)
    has_supply_state = (control_state.eq("aanvoer") & positive_capacity).any()
    has_drain_state = (control_state.eq("afvoer") & positive_capacity).any()

    if has_supply_state and has_drain_state:
        return "doorlaat"
    if has_supply_state:
        return "inlaat"
    if has_drain_state:
        return "uitlaat"
    return None


def _compound_variable_counts(df: pd.DataFrame | None, control_node_id: int) -> dict[int, int]:
    if df is None or df.empty or "compound_variable_id" not in df.columns:
        return {}

    rows = df[df["node_id"].eq(int(control_node_id))].copy()
    if rows.empty:
        return {}

    compound_variable_ids = pd.to_numeric(rows["compound_variable_id"], errors="coerce").dropna().astype(int)
    return compound_variable_ids.value_counts().sort_index().to_dict()


def _logic_pairs(logic_df: pd.DataFrame | None, control_node_id: int) -> set[tuple[str, str]]:
    if logic_df is None or logic_df.empty or not {"truth_state", "control_state"}.issubset(logic_df.columns):
        return set()

    rows = logic_df[logic_df["node_id"].eq(int(control_node_id))].copy()
    if rows.empty:
        return set()

    return {
        (str(row.truth_state), str(row.control_state).lower())
        for row in rows[["truth_state", "control_state"]].itertuples(index=False)
    }


def _validate_control_layout_for_sync(
    *,
    function: str,
    connector_node_id: int,
    control_node_id: int,
    variable_df: pd.DataFrame | None,
    condition_df: pd.DataFrame | None,
    logic_df: pd.DataFrame | None,
) -> None:
    expected = control_layouts().get(function)
    if expected is None:
        return

    expected_variable_counts, expected_condition_counts, expected_logic_pairs = expected
    variable_counts = _compound_variable_counts(variable_df, control_node_id)
    condition_counts = _compound_variable_counts(condition_df, control_node_id)
    logic_pairs = _logic_pairs(logic_df, control_node_id)

    if (
        variable_counts != expected_variable_counts
        or condition_counts != expected_condition_counts
        or logic_pairs != expected_logic_pairs
    ):
        raise ValueError(
            f"{function.capitalize()} #{connector_node_id} met DiscreteControl #{control_node_id} heeft geen "
            "control.py-layout: "
            f"variables={variable_counts} verwacht={expected_variable_counts}; "
            f"conditions={condition_counts} verwacht={expected_condition_counts}; "
            f"logic={sorted(logic_pairs)} verwacht={sorted(expected_logic_pairs)}"
        )


def _level_difference_threshold(model: Model) -> float:
    solver = getattr(model, "solver", None)
    threshold = getattr(solver, "level_difference_threshold", 0.02)
    return float(threshold)


def _control_operating_levels(
    model: Model,
    connector_node_ids: list[int],
    node_type_by_id: dict[int, str],
    *,
    include_functions: set[str] | None = None,
) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    records: list[dict[str, object]] = []
    missing_records: list[dict[str, object]] = []

    for connector_node_id in connector_node_ids:
        node_type = node_type_by_id.get(int(connector_node_id))
        if node_type not in CONTROL_NODE_TYPES:
            continue

        component = getattr(model, node_type.lower(), None)
        static_df = getattr(getattr(component, "static", None), "df", None)
        if static_df is None or static_df.empty or "control_state" not in static_df.columns:
            continue

        connector_rows = static_df[static_df["node_id"].eq(int(connector_node_id))].copy()
        if connector_rows.empty:
            continue

        function = _connector_function(connector_rows)
        if include_functions is not None and function not in include_functions:
            continue

        control_state = connector_rows["control_state"].astype("string").str.lower()
        active_rows = connector_rows[_capacity_is_positive(connector_rows)].copy()
        for row_index, row in active_rows.iterrows():
            state = control_state.loc[row_index]
            if state == "aanvoer":
                level_column = "max_downstream_level"
            elif state == "afvoer":
                level_column = "min_upstream_level"
            else:
                continue

            level = row.get(level_column)
            record = {
                "node_id": int(connector_node_id),
                "node_type": node_type,
                "functie": function,
                "control_state": state,
                "level_column": level_column,
            }
            if pd.isna(level):
                missing_records.append(record)
                continue

            records.append({**record, "level": float(level)})

    return records, missing_records


def _consistent_control_level(
    model: Model,
    connector_node_ids: list[int],
    node_type_by_id: dict[int, str],
    tolerance: float,
) -> tuple[float | None, str, list[int]]:
    level_records, missing_records = _control_operating_levels(
        model=model,
        connector_node_ids=connector_node_ids,
        node_type_by_id=node_type_by_id,
        include_functions={"doorlaat", "uitlaat"},
    )
    if missing_records:
        return None, "bestaande_control_levels_onvolledig", []
    if not level_records:
        return None, "geen_bestaande_control_levels", []

    levels = [record["level"] for record in level_records]
    if max(levels) - min(levels) > tolerance:
        return None, "bestaande_control_levels_niet_eenduidig", []

    source_node_ids = sorted({int(record["node_id"]) for record in level_records})
    return float(levels[0]), "bestaande_control_levels", source_node_ids


def _control_level_by_node_id(
    model: Model,
    connector_node_ids: list[int],
    node_type_by_id: dict[int, str],
    tolerance: float,
) -> dict[int, float]:
    level_records, _ = _control_operating_levels(
        model=model,
        connector_node_ids=connector_node_ids,
        node_type_by_id=node_type_by_id,
        include_functions={"doorlaat", "uitlaat"},
    )
    records_df = pd.DataFrame(level_records)
    if records_df.empty:
        return {}

    level_by_node_id: dict[int, float] = {}
    for node_id, node_records_df in records_df.groupby("node_id"):
        levels = node_records_df["level"].astype(float)
        if levels.max() - levels.min() > tolerance:
            continue
        level_by_node_id[int(node_id)] = float(levels.iloc[0])

    return level_by_node_id


def _control_target_level_by_node_id(
    model: Model,
    connector_node_ids: list[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
    target_level_by_basin_id: dict[int, float],
    tolerance: float,
    max_iter: int,
) -> dict[int, float]:
    open_adjacency = _undirected_open_adjacency(link_df=link_df, node_type_by_id=node_type_by_id)
    level_boundary_level_by_id = _level_boundary_level_by_id(model)
    control_ids_by_connector_id = _control_node_ids_by_connector_node_id(model, node_type_by_id=node_type_by_id)
    variable_df = getattr(getattr(model.discrete_control, "variable", None), "df", None)
    records: list[dict[str, object]] = []

    for connector_node_id in connector_node_ids:
        node_type = node_type_by_id.get(int(connector_node_id))
        if node_type not in CONTROL_NODE_TYPES:
            continue

        component = getattr(model, node_type.lower(), None)
        static_df = getattr(getattr(component, "static", None), "df", None)
        if static_df is None or static_df.empty or "control_state" not in static_df.columns:
            continue

        connector_rows = static_df[static_df["node_id"].eq(int(connector_node_id))].copy()
        if connector_rows.empty:
            continue

        function = _connector_function(connector_rows)
        if function not in {"doorlaat", "uitlaat"}:
            continue

        control_target_level: float | None = None
        if function == "uitlaat" and variable_df is not None and not variable_df.empty:
            listen_node_ids: set[int] = set()
            for control_node_id in control_ids_by_connector_id.get(int(connector_node_id), []):
                variable_rows = variable_df[
                    variable_df["node_id"].eq(int(control_node_id))
                    & variable_df["variable"].astype("string").str.lower().eq("level")
                ].copy()
                variable_rows["weight"] = pd.to_numeric(variable_rows["weight"], errors="coerce")
                variable_rows = variable_rows[variable_rows["weight"].eq(1.0)]
                listen_node_ids.update(variable_rows["listen_node_id"].dropna().astype(int).to_list())

            if len(listen_node_ids) == 1:
                _, control_target_level = _side_basin_ids_and_level(
                    next(iter(listen_node_ids)),
                    open_adjacency=open_adjacency,
                    node_type_by_id=node_type_by_id,
                    target_level_by_basin_id=target_level_by_basin_id,
                    level_boundary_level_by_id=level_boundary_level_by_id,
                    max_iter=max_iter,
                )

        upstream_node_ids = [
            int(row.from_node_id)
            for row in link_df[link_df["to_node_id"].eq(int(connector_node_id))].itertuples(index=False)
        ]
        downstream_node_ids = [
            int(row.to_node_id)
            for row in link_df[link_df["from_node_id"].eq(int(connector_node_id))].itertuples(index=False)
        ]
        if len(upstream_node_ids) != 1 or len(downstream_node_ids) != 1:
            continue

        control_state = connector_rows["control_state"].astype("string").str.lower()
        active_rows = connector_rows[_capacity_is_positive(connector_rows)].copy()
        if control_target_level is not None and not active_rows.empty:
            records.append({"node_id": int(connector_node_id), "level": float(control_target_level)})
            continue

        for row_index in active_rows.index:
            state = control_state.loc[row_index]
            if state == "aanvoer":
                side_node_id = downstream_node_ids[0]
            elif state == "afvoer":
                side_node_id = upstream_node_ids[0]
            else:
                continue

            _, side_level = _side_basin_ids_and_level(
                side_node_id,
                open_adjacency=open_adjacency,
                node_type_by_id=node_type_by_id,
                target_level_by_basin_id=target_level_by_basin_id,
                level_boundary_level_by_id=level_boundary_level_by_id,
                max_iter=max_iter,
            )
            if side_level is None:
                continue

            records.append({"node_id": int(connector_node_id), "level": float(side_level)})

    records_df = pd.DataFrame(records)
    if records_df.empty:
        return {}

    level_by_node_id: dict[int, float] = {}
    for node_id, node_records_df in records_df.groupby("node_id"):
        levels = node_records_df["level"].astype(float)
        if levels.max() - levels.min() > tolerance:
            continue
        level_by_node_id[int(node_id)] = float(levels.iloc[0])

    return level_by_node_id


def _dominant_downstream_control_level(
    model: Model,
    *,
    component_node_ids: set[int],
    boundary_control_node_ids: list[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
    target_level_by_basin_id: dict[int, float],
    tolerance: float,
    max_iter: int,
) -> tuple[float | None, str, list[int]]:
    level_by_node_id = _control_level_by_node_id(
        model=model,
        connector_node_ids=boundary_control_node_ids,
        node_type_by_id=node_type_by_id,
        tolerance=tolerance,
    )
    level_by_node_id.update(
        _control_target_level_by_node_id(
            model=model,
            connector_node_ids=boundary_control_node_ids,
            link_df=link_df,
            node_type_by_id=node_type_by_id,
            target_level_by_basin_id=target_level_by_basin_id,
            tolerance=tolerance,
            max_iter=max_iter,
        )
    )
    if not level_by_node_id:
        return None, "geen_eenduidige_dominante_control_levels", []

    outgoing: dict[int, list[int]] = defaultdict(list)
    for row in link_df.itertuples(index=False):
        outgoing[int(row.from_node_id)].append(int(row.to_node_id))

    boundary_control_node_ids = [node_id for node_id in boundary_control_node_ids if node_id in level_by_node_id]
    boundary_control_node_id_set = set(boundary_control_node_ids)
    starts = list(component_node_ids)

    basin_route_count_by_control_id: dict[int, int] = defaultdict(int)
    open_route_count_by_control_id: dict[int, int] = defaultdict(int)
    for start_node_id in starts:
        queue: deque[int] = deque([int(start_node_id)])
        seen_node_ids = {int(start_node_id)}
        reached_control_node_ids: set[int] = set()

        while queue and len(seen_node_ids) < max_iter:
            current_node_id = queue.popleft()
            for next_node_id in outgoing.get(current_node_id, []):
                if next_node_id in boundary_control_node_id_set:
                    reached_control_node_ids.add(next_node_id)
                    continue
                if next_node_id in component_node_ids and next_node_id not in seen_node_ids:
                    seen_node_ids.add(next_node_id)
                    queue.append(next_node_id)

        for control_node_id in reached_control_node_ids:
            open_route_count_by_control_id[control_node_id] += 1
            if node_type_by_id.get(start_node_id) == "Basin":
                basin_route_count_by_control_id[control_node_id] += 1

    rows = [
        {
            "node_id": node_id,
            "basin_route_count": basin_route_count_by_control_id[node_id],
            "open_route_count": open_route_count_by_control_id[node_id],
            "level": level_by_node_id[node_id],
        }
        for node_id in boundary_control_node_ids
        if open_route_count_by_control_id[node_id] > 0
    ]
    if not rows:
        return None, "geen_dominante_downstream_control", []

    rows_df = pd.DataFrame(rows)
    max_open_route_count = rows_df["open_route_count"].max()
    max_basin_route_count = rows_df.loc[rows_df["open_route_count"].eq(max_open_route_count), "basin_route_count"].max()
    dominant_rows_df = rows_df[
        rows_df["open_route_count"].eq(max_open_route_count) & rows_df["basin_route_count"].eq(max_basin_route_count)
    ].copy()
    level_spread = dominant_rows_df["level"].max() - dominant_rows_df["level"].min()
    if level_spread > tolerance:
        target_level = dominant_rows_df["level"].min()
        dominant_rows_df = dominant_rows_df[(dominant_rows_df["level"] - target_level).abs().le(tolerance)].copy()
        status = "dominante_downstream_control_gelijke_score_laagste_peil"
    else:
        target_level = float(dominant_rows_df["level"].iloc[0])
        status = "dominante_downstream_control"

    return (
        float(target_level),
        status,
        sorted(dominant_rows_df["node_id"].astype(int).to_list()),
    )


def _node_ids_of_type_in_geometry(
    model: Model,
    *,
    node_type: str,
    geometry_df,
    excluded_node_ids: set[int] | None = None,
) -> list[int]:
    import geopandas as gpd

    excluded_node_ids = {int(node_id) for node_id in excluded_node_ids or set()}
    node_df = model.node.df.copy()
    if "node_id" not in node_df.columns:
        index_name = node_df.index.name or "node_id"
        node_df = node_df.reset_index(drop=False)
        if index_name in node_df.columns and index_name != "node_id":
            node_df = node_df.rename(columns={index_name: "node_id"})
        elif "index" in node_df.columns and "node_id" not in node_df.columns:
            node_df = node_df.rename(columns={"index": "node_id"})

    geometry_column = getattr(getattr(node_df, "geometry", None), "name", "geometry")
    if geometry_column not in node_df.columns:
        raise ValueError("Kan Manning-nodes niet ruimtelijk selecteren: model.node.df heeft geen geometry-kolom.")

    crs = getattr(model.node.df, "crs", None)
    node_gdf = gpd.GeoDataFrame(node_df, geometry=geometry_column, crs=crs)
    if getattr(geometry_df, "crs", None) is not None and crs is not None and geometry_df.crs != crs:
        geometry_df = geometry_df.to_crs(crs)

    geometry_union = geometry_df.geometry.union_all()
    selected_gdf = node_gdf[node_gdf["node_type"].eq(node_type) & node_gdf.geometry.within(geometry_union)].copy()
    selected_node_ids = selected_gdf["node_id"].dropna().astype(int).to_list()
    return sorted(node_id for node_id in selected_node_ids if node_id not in excluded_node_ids)


def _nearest_upstream_basin_ids_for_control(
    control_node_id: int,
    *,
    component_node_ids: set[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
    max_iter: int,
) -> list[int]:
    incoming: dict[int, list[int]] = defaultdict(list)
    for row in link_df.itertuples(index=False):
        from_node_id = int(row.from_node_id)
        to_node_id = int(row.to_node_id)
        if from_node_id in component_node_ids or to_node_id == int(control_node_id):
            incoming[to_node_id].append(from_node_id)

    starts = [
        int(node_id)
        for node_id in incoming.get(int(control_node_id), [])
        if int(node_id) in component_node_ids and node_type_by_id.get(int(node_id)) in OPEN_NODE_TYPES
    ]
    if not starts:
        return []

    queue: deque[tuple[int, int]] = deque((node_id, 0) for node_id in starts)
    seen_node_ids = set(starts)
    basin_ids_by_distance: dict[int, list[int]] = defaultdict(list)

    while queue and len(seen_node_ids) < max_iter:
        current_node_id, distance = queue.popleft()
        if node_type_by_id.get(current_node_id) == "Basin":
            basin_ids_by_distance[distance].append(current_node_id)
            continue

        for next_node_id in incoming.get(current_node_id, []):
            next_node_id = int(next_node_id)
            if next_node_id not in component_node_ids or next_node_id in seen_node_ids:
                continue
            if node_type_by_id.get(next_node_id) not in OPEN_NODE_TYPES:
                continue

            seen_node_ids.add(next_node_id)
            queue.append((next_node_id, distance + 1))

    if not basin_ids_by_distance:
        return []

    nearest_distance = min(basin_ids_by_distance)
    return sorted(set(basin_ids_by_distance[nearest_distance]))


def _dominant_downstream_parameterized_target_level(
    *,
    component_node_ids: set[int],
    boundary_control_node_ids: list[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
    target_level_by_basin_id: dict[int, float],
    tolerance: float,
    max_iter: int,
) -> tuple[float | None, str, list[int], list[int]]:
    outgoing: dict[int, list[int]] = defaultdict(list)
    for row in link_df.itertuples(index=False):
        outgoing[int(row.from_node_id)].append(int(row.to_node_id))

    boundary_control_node_id_set = set(boundary_control_node_ids)
    basin_route_count_by_control_id: dict[int, int] = defaultdict(int)
    open_route_count_by_control_id: dict[int, int] = defaultdict(int)
    for start_node_id in component_node_ids:
        queue: deque[int] = deque([int(start_node_id)])
        seen_node_ids = {int(start_node_id)}
        reached_control_node_ids: set[int] = set()

        while queue and len(seen_node_ids) < max_iter:
            current_node_id = queue.popleft()
            for next_node_id in outgoing.get(current_node_id, []):
                if next_node_id in boundary_control_node_id_set:
                    reached_control_node_ids.add(next_node_id)
                    continue
                if next_node_id in component_node_ids and next_node_id not in seen_node_ids:
                    seen_node_ids.add(next_node_id)
                    queue.append(next_node_id)

        for control_node_id in reached_control_node_ids:
            open_route_count_by_control_id[control_node_id] += 1
            if node_type_by_id.get(start_node_id) == "Basin":
                basin_route_count_by_control_id[control_node_id] += 1

    rows: list[dict[str, object]] = []
    for control_node_id in boundary_control_node_ids:
        if open_route_count_by_control_id[control_node_id] == 0:
            continue

        source_basin_ids = _nearest_upstream_basin_ids_for_control(
            control_node_id=control_node_id,
            component_node_ids=component_node_ids,
            link_df=link_df,
            node_type_by_id=node_type_by_id,
            max_iter=max_iter,
        )
        source_levels = [
            float(target_level_by_basin_id[basin_id])
            for basin_id in source_basin_ids
            if basin_id in target_level_by_basin_id and pd.notna(target_level_by_basin_id[basin_id])
        ]
        if not source_levels:
            continue

        rows.append(
            {
                "node_id": int(control_node_id),
                "basin_route_count": basin_route_count_by_control_id[control_node_id],
                "open_route_count": open_route_count_by_control_id[control_node_id],
                "level": min(source_levels),
                "source_basin_ids": source_basin_ids,
            }
        )

    if not rows:
        return None, "geen_dominante_downstream_control_met_basinpeil", [], []

    rows_df = pd.DataFrame(rows)
    max_open_route_count = rows_df["open_route_count"].max()
    max_basin_route_count = rows_df.loc[rows_df["open_route_count"].eq(max_open_route_count), "basin_route_count"].max()
    dominant_rows_df = rows_df[
        rows_df["open_route_count"].eq(max_open_route_count) & rows_df["basin_route_count"].eq(max_basin_route_count)
    ].copy()
    level_spread = dominant_rows_df["level"].max() - dominant_rows_df["level"].min()
    if level_spread > tolerance:
        target_level = dominant_rows_df["level"].min()
        dominant_rows_df = dominant_rows_df[(dominant_rows_df["level"] - target_level).abs().le(tolerance)].copy()
        status = "dominante_downstream_control_gelijke_score_laagste_basinpeil"
    else:
        target_level = float(dominant_rows_df["level"].iloc[0])
        status = "dominante_downstream_control_basinpeil"

    source_basin_ids = sorted(
        {
            int(basin_id)
            for basin_ids in dominant_rows_df["source_basin_ids"].to_list()
            for basin_id in basin_ids
            if int(basin_id) in target_level_by_basin_id
            and abs(float(target_level_by_basin_id[int(basin_id)]) - float(target_level)) <= tolerance
        }
    )
    return (
        float(target_level),
        status,
        sorted(dominant_rows_df["node_id"].astype(int).to_list()),
        source_basin_ids,
    )


def _parse_node_id_list(value: object) -> set[int]:
    if value is None or pd.isna(value):
        return set()
    if isinstance(value, int):
        return {int(value)}
    if isinstance(value, float):
        return set() if pd.isna(value) else {int(value)}

    node_ids: set[int] = set()
    for item in str(value).split(","):
        item = item.strip()
        if not item:
            continue
        node_ids.add(int(float(item)))
    return node_ids


def _set_value_if_changed(
    df: pd.DataFrame,
    row_index: object,
    column: str,
    value: float,
    records: list[dict[str, object]],
    record: dict[str, object],
) -> None:
    old_value = df.at[row_index, column]
    if pd.notna(old_value) and float(old_value) == float(value):
        return

    df.at[row_index, column] = float(value)
    records.append(
        {
            **record,
            "table": record.get("table"),
            "row_index": row_index,
            "column": column,
            "old_value": old_value,
            "new_value": float(value),
        }
    )


def _format_control_node_name(
    *,
    function: str,
    upstream_level: float | None,
    downstream_level: float | None,
    current_name: object,
) -> str | None:
    if pd.notna(current_name) and ":" in str(current_name):
        prefix = str(current_name).split(":", 1)[0].strip()
    else:
        prefix = function

    if function == "uitlaat":
        if upstream_level is None:
            return None
        return f"{prefix}: {float(upstream_level):.2f} [m+NAP]"

    if function in {"inlaat", "doorlaat"}:
        if downstream_level is None:
            return None
        if upstream_level is None:
            return f"{prefix}: {float(downstream_level):.2f} [m+NAP]"
        return f"{prefix}: {float(upstream_level):.2f}/{float(downstream_level):.2f} [m+NAP]"

    return None


def _set_control_node_name_if_changed(
    model: Model,
    *,
    control_node_id: int,
    function: str,
    upstream_level: float | None,
    downstream_level: float | None,
    records: list[dict[str, object]],
    connector_node_id: int,
) -> None:
    node_df = model.node.df
    if node_df is None or "name" not in node_df.columns:
        return

    control_node_id = int(control_node_id)
    row_index = None
    if control_node_id in node_df.index:
        row_index = control_node_id
    elif "node_id" in node_df.columns:
        matches = node_df.index[node_df["node_id"].eq(control_node_id)]
        if len(matches) > 0:
            row_index = matches[0]

    if row_index is None:
        return

    old_name = node_df.at[row_index, "name"]
    new_name = _format_control_node_name(
        function=function,
        upstream_level=upstream_level,
        downstream_level=downstream_level,
        current_name=old_name,
    )
    if new_name is None or (pd.notna(old_name) and str(old_name) == new_name):
        return

    node_df.at[row_index, "name"] = new_name
    records.append(
        {
            "node_id": connector_node_id,
            "control_node_id": control_node_id,
            "table": "Node",
            "row_index": row_index,
            "column": "name",
            "old_value": old_name,
            "new_value": new_name,
        }
    )


def _node_geometry_df(model: Model) -> pd.DataFrame:
    node_df = model.node.df.copy()
    if "node_id" not in node_df.columns:
        index_name = node_df.index.name or "node_id"
        node_df = node_df.reset_index(drop=False)
        if index_name in node_df.columns and index_name != "node_id":
            node_df = node_df.rename(columns={index_name: "node_id"})
        elif "index" in node_df.columns and "node_id" not in node_df.columns:
            node_df = node_df.rename(columns={"index": "node_id"})

    geometry_column = getattr(getattr(node_df, "geometry", None), "name", "geometry")
    if geometry_column not in node_df.columns:
        raise ValueError("Kan geen GPKG schrijven: model.node.df heeft geen geometry-kolom.")

    return node_df[["node_id", geometry_column]].reset_index(drop=True)


def _write_basin_updates_gpkg(
    model: Model,
    updates_df: pd.DataFrame,
    output_gpkg: str | Path,
    *,
    layer: str = "basin_level_updates",
) -> Path | None:
    basin_updates_df = updates_df[updates_df["status"].eq("update") & updates_df["basin_node_id"].notna()].copy()
    if basin_updates_df.empty:
        return None

    import geopandas as gpd

    node_geometry_df = _node_geometry_df(model=model)
    geometry_column = getattr(getattr(node_geometry_df, "geometry", None), "name", "geometry")
    crs = getattr(model.node.df, "crs", None)

    basin_updates_df["basin_node_id"] = basin_updates_df["basin_node_id"].astype(int)
    basin_updates_gdf = basin_updates_df.merge(
        node_geometry_df,
        left_on="basin_node_id",
        right_on="node_id",
        how="left",
        suffixes=("", "_geometry"),
    ).drop(columns=["node_id"], errors="ignore")
    basin_updates_gdf = gpd.GeoDataFrame(basin_updates_gdf, geometry=geometry_column, crs=crs)
    basin_updates_gdf = basin_updates_gdf[basin_updates_gdf.geometry.notna()].copy()
    if basin_updates_gdf.empty:
        return None

    output_gpkg = Path(output_gpkg)
    output_gpkg.parent.mkdir(parents=True, exist_ok=True)
    if output_gpkg.exists():
        try:
            output_gpkg.unlink()
        except PermissionError:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_gpkg = output_gpkg.with_name(f"{output_gpkg.stem}_{timestamp}{output_gpkg.suffix}")
    basin_updates_gdf.to_file(output_gpkg, layer=layer, driver="GPKG")
    return output_gpkg


def _write_control_updates_gpkg(
    model: Model,
    updates_df: pd.DataFrame,
    output_gpkg: str | Path,
    *,
    layer: str = "control_level_updates",
) -> Path | None:
    if updates_df.empty or "node_id" not in updates_df.columns:
        return None

    import geopandas as gpd

    control_updates_df = updates_df.copy()
    if "control_node_id" in control_updates_df.columns:
        control_updates_df["geometry_node_id"] = control_updates_df["control_node_id"]
        control_updates_df["geometry_node_id"] = control_updates_df["geometry_node_id"].fillna(
            control_updates_df["node_id"]
        )
    else:
        control_updates_df["geometry_node_id"] = control_updates_df["node_id"]
    control_updates_df = control_updates_df.dropna(subset=["geometry_node_id"]).copy()
    if control_updates_df.empty:
        return None

    node_geometry_df = _node_geometry_df(model=model)
    geometry_column = getattr(getattr(node_geometry_df, "geometry", None), "name", "geometry")
    crs = getattr(model.node.df, "crs", None)

    control_updates_df["geometry_node_id"] = control_updates_df["geometry_node_id"].astype(int)
    control_updates_gdf = control_updates_df.merge(
        node_geometry_df,
        left_on="geometry_node_id",
        right_on="node_id",
        how="left",
        suffixes=("", "_geometry"),
    ).drop(columns=["node_id_geometry"], errors="ignore")
    control_updates_gdf = gpd.GeoDataFrame(control_updates_gdf, geometry=geometry_column, crs=crs)
    control_updates_gdf = control_updates_gdf[control_updates_gdf.geometry.notna()].copy()
    if control_updates_gdf.empty:
        return None

    output_gpkg = Path(output_gpkg)
    output_gpkg.parent.mkdir(parents=True, exist_ok=True)
    if output_gpkg.exists():
        try:
            output_gpkg.unlink()
        except PermissionError:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_gpkg = output_gpkg.with_name(f"{output_gpkg.stem}_{timestamp}{output_gpkg.suffix}")
    control_updates_gdf.to_file(output_gpkg, layer=layer, driver="GPKG")
    return output_gpkg


def sync_control_levels_for_basin_updates(
    model: Model,
    basin_level_updates: pd.DataFrame,
    *,
    extra_control_node_ids: list[int] | set[int] | None = None,
    protected_control_node_ids: list[int] | set[int] | None = None,
    target_level_column: str = "meta_streefpeil",
    us_target_level_offset_supply: float = -0.04,
    output_path: str | Path | None = None,
    output_gpkg: str | Path | None = None,
    verbose: bool = True,
    max_iter: int = 500,
) -> pd.DataFrame:
    """Synchroniseer bestaande controller-levels nadat Manning-route basins zijn aangepast.

    De basin-correctie wordt alleen gestart door aanvoer/doorlaat-connectors, maar
    alle controllers die aan aangepaste basins hangen moeten daarna consistent zijn.
    Dit raakt dus ook drains/uitlaten rond zo'n aangepast basin. Drain-only gebieden
    worden niet geraakt omdat daar geen basin-update uit `basin_level_updates` komt.
    """
    if basin_level_updates.empty or "basin_node_id" not in basin_level_updates.columns:
        return pd.DataFrame()

    component_basin_node_ids = {
        int(node_id)
        for node_id in basin_level_updates.loc[
            basin_level_updates["status"].isin(["update", "ongewijzigd", "handmatig_peil_behouden"]),
            "basin_node_id",
        ].dropna()
    }
    upstream_protected_basin_node_ids = {
        int(node_id)
        for node_id in basin_level_updates.loc[
            basin_level_updates["status"].isin(
                ["bovenstrooms_zonder_aanvoer_sturing_behouden", "enkele_manning_tak_basin_behouden"]
            ),
            "basin_node_id",
        ].dropna()
    }

    component_connector_node_ids: set[int] = set()
    if "boundary_control_node_ids" in basin_level_updates.columns:
        for value in basin_level_updates["boundary_control_node_ids"].dropna():
            component_connector_node_ids.update(_parse_node_id_list(value))
    component_connector_node_ids.update(int(node_id) for node_id in extra_control_node_ids or [])
    protected_control_node_ids = {int(node_id) for node_id in protected_control_node_ids or []}

    locked_control_node_ids: set[int] = set()
    if {"target_level_basis", "boundary_control_node_ids"}.issubset(basin_level_updates.columns):
        locked_rows = basin_level_updates[basin_level_updates["target_level_basis"].eq("bestaande_control_levels")]
        for value in locked_rows["boundary_control_node_ids"].dropna():
            locked_control_node_ids.update(_parse_node_id_list(value))

    if not component_basin_node_ids and not component_connector_node_ids:
        return pd.DataFrame()

    node_type_by_id = _node_type_by_id(model)
    link_df = _flow_link_df(model)
    open_adjacency = _undirected_open_adjacency(link_df=link_df, node_type_by_id=node_type_by_id)
    target_level_by_basin_id = _target_level_by_basin_id(model, target_level_column)
    level_boundary_level_by_id = _level_boundary_level_by_id(model)
    control_ids_by_connector_id = _control_node_ids_by_connector_node_id(model, node_type_by_id=node_type_by_id)
    flow_demand_connector_node_ids = _flow_demand_target_node_ids(model, node_type_by_id=node_type_by_id)
    component_connector_node_ids -= flow_demand_connector_node_ids

    records: list[dict[str, object]] = []
    touched_connector_node_ids: set[int] = set()
    control_name_levels_by_connector_id: dict[int, tuple[str, float | None, float | None]] = {}
    condition_basin_scope_by_connector_id: dict[int, set[int]] = {}

    for node_type in sorted(CONTROL_NODE_TYPES):
        component = getattr(model, node_type.lower(), None)
        static_df = getattr(getattr(component, "static", None), "df", None)
        if static_df is None or static_df.empty or "control_state" not in static_df.columns:
            continue

        table_name = f"{node_type} / static"
        for connector_node_id, connector_rows in static_df.groupby("node_id", sort=True):
            connector_node_id = int(connector_node_id)
            if connector_node_id in flow_demand_connector_node_ids:
                continue
            if node_type_by_id.get(connector_node_id) != node_type:
                continue

            upstream_node_ids = [
                int(row.from_node_id)
                for row in link_df[link_df["to_node_id"].eq(connector_node_id)].itertuples(index=False)
            ]
            downstream_node_ids = [
                int(row.to_node_id)
                for row in link_df[link_df["from_node_id"].eq(connector_node_id)].itertuples(index=False)
            ]
            if len(upstream_node_ids) != 1 or len(downstream_node_ids) != 1:
                continue

            upstream_basin_ids, upstream_level = _side_basin_ids_and_level(
                upstream_node_ids[0],
                open_adjacency=open_adjacency,
                node_type_by_id=node_type_by_id,
                target_level_by_basin_id=target_level_by_basin_id,
                level_boundary_level_by_id=level_boundary_level_by_id,
                max_iter=max_iter,
            )
            downstream_basin_ids, downstream_level = _side_basin_ids_and_level(
                downstream_node_ids[0],
                open_adjacency=open_adjacency,
                node_type_by_id=node_type_by_id,
                target_level_by_basin_id=target_level_by_basin_id,
                level_boundary_level_by_id=level_boundary_level_by_id,
                max_iter=max_iter,
            )
            upstream_side_protected = (
                bool(upstream_basin_ids) and upstream_basin_ids <= upstream_protected_basin_node_ids
            )
            downstream_side_protected = (
                bool(downstream_basin_ids) and downstream_basin_ids <= upstream_protected_basin_node_ids
            )

            connector_is_component_boundary = connector_node_id in component_connector_node_ids
            in_component_scope = connector_is_component_boundary or bool(
                (upstream_basin_ids | downstream_basin_ids) & component_basin_node_ids
            )
            if not in_component_scope:
                continue
            locked_control = connector_node_id in locked_control_node_ids

            control_state = connector_rows["control_state"].astype("string").str.lower()
            positive_capacity = _capacity_is_positive(connector_rows)
            has_supply_state = (control_state.eq("aanvoer") & positive_capacity).any()
            has_drain_state = (control_state.eq("afvoer") & positive_capacity).any()
            if not (has_supply_state or has_drain_state):
                continue

            if has_supply_state and has_drain_state:
                function = "doorlaat"
            elif has_supply_state:
                function = "inlaat"
            else:
                function = "uitlaat"

            manual_level_protected = connector_node_id in protected_control_node_ids
            if LEVEL_UPDATE_PROTECTION_COLUMN in connector_rows.columns:
                manual_level_protected = manual_level_protected or bool(
                    _truthy_series(connector_rows[LEVEL_UPDATE_PROTECTION_COLUMN]).any()
                )
            if manual_level_protected:
                records.append(
                    {
                        "node_id": connector_node_id,
                        "node_type": node_type,
                        "functie": function,
                        "table": table_name,
                        "column": "control_levels",
                        "old_value": None,
                        "new_value": None,
                        "status": "handmatig_control_level_behouden",
                    }
                )
                continue

            upstream_component_basin_ids = upstream_basin_ids & component_basin_node_ids
            downstream_component_basin_ids = downstream_basin_ids & component_basin_node_ids
            upstream_sync_level = _target_level_for_basin_ids(upstream_component_basin_ids, target_level_by_basin_id)
            if upstream_sync_level is None:
                upstream_sync_level = upstream_level
            downstream_sync_level = _target_level_for_basin_ids(
                downstream_component_basin_ids,
                target_level_by_basin_id,
            )
            if downstream_sync_level is None:
                downstream_sync_level = downstream_level

            connector_side_was_synced = bool(upstream_component_basin_ids or downstream_component_basin_ids)
            sync_both_sides = bool(has_supply_state and has_drain_state and connector_side_was_synced)
            sync_min_upstream = (
                upstream_sync_level is not None
                and not upstream_side_protected
                and (bool(upstream_component_basin_ids) or connector_is_component_boundary or sync_both_sides)
            )
            sync_max_downstream = (
                downstream_sync_level is not None
                and has_supply_state
                and not downstream_side_protected
                and (bool(downstream_component_basin_ids) or connector_is_component_boundary or sync_both_sides)
            )
            relevant_condition_basin_ids: set[int] = set()
            if sync_min_upstream:
                relevant_condition_basin_ids.update(upstream_basin_ids)
            if sync_max_downstream and has_supply_state:
                relevant_condition_basin_ids.update(downstream_basin_ids)

            control_name_levels_by_connector_id[connector_node_id] = (
                function,
                upstream_sync_level,
                downstream_sync_level,
            )
            if relevant_condition_basin_ids and not locked_control:
                touched_connector_node_ids.add(connector_node_id)
                condition_basin_scope_by_connector_id[connector_node_id] = relevant_condition_basin_ids
            for row_index, state in control_state.items():
                # Rows without capacity are closed states; syncing their levels can
                # overwrite intentional offsets in flushing controls.
                if not bool(positive_capacity.loc[row_index]):
                    continue

                record = {
                    "node_id": connector_node_id,
                    "node_type": node_type,
                    "functie": function,
                    "control_state": state,
                    "upstream_node_id": upstream_node_ids[0],
                    "downstream_node_id": downstream_node_ids[0],
                    "upstream_basin_node_ids": ",".join(map(str, sorted(upstream_basin_ids))),
                    "downstream_basin_node_ids": ",".join(map(str, sorted(downstream_basin_ids))),
                    "table": table_name,
                }

                if (
                    sync_min_upstream
                    and pd.notna(state)
                    and upstream_sync_level is not None
                    and "min_upstream_level" in static_df.columns
                ):
                    if state == "aanvoer" and has_supply_state:
                        min_upstream_level = float(upstream_sync_level) + us_target_level_offset_supply
                    else:
                        min_upstream_level = float(upstream_sync_level)
                    if locked_control and state != "aanvoer":
                        continue
                    _set_value_if_changed(
                        static_df,
                        row_index,
                        "min_upstream_level",
                        min_upstream_level,
                        records,
                        record,
                    )

                if (
                    sync_max_downstream
                    and pd.notna(state)
                    and has_supply_state
                    and state == "aanvoer"
                    and downstream_sync_level is not None
                    and "max_downstream_level" in static_df.columns
                ):
                    if locked_control:
                        continue
                    _set_value_if_changed(
                        static_df,
                        row_index,
                        "max_downstream_level",
                        float(downstream_sync_level),
                        records,
                        record,
                    )

    variable_df = getattr(getattr(model.discrete_control, "variable", None), "df", None)
    condition_df = getattr(getattr(model.discrete_control, "condition", None), "df", None)
    logic_df = getattr(getattr(model.discrete_control, "logic", None), "df", None)
    level_difference_threshold = _level_difference_threshold(model)
    if variable_df is not None and condition_df is not None:
        for connector_node_id in sorted(touched_connector_node_ids):
            for control_node_id in control_ids_by_connector_id.get(connector_node_id, []):
                function = None
                if connector_node_id in control_name_levels_by_connector_id:
                    function, upstream_level, downstream_level = control_name_levels_by_connector_id[connector_node_id]
                    _set_control_node_name_if_changed(
                        model=model,
                        control_node_id=control_node_id,
                        function=function,
                        upstream_level=upstream_level,
                        downstream_level=downstream_level,
                        records=records,
                        connector_node_id=connector_node_id,
                    )
                    _validate_control_layout_for_sync(
                        function=function,
                        connector_node_id=connector_node_id,
                        control_node_id=control_node_id,
                        variable_df=variable_df,
                        condition_df=condition_df,
                        logic_df=logic_df,
                    )

                variable_rows = variable_df[variable_df["node_id"].eq(control_node_id)].copy()
                if variable_rows.empty:
                    continue

                variable_rows["weight"] = pd.to_numeric(variable_rows["weight"], errors="coerce")
                variable_rows = variable_rows.dropna(subset=["listen_node_id", "compound_variable_id", "weight"])
                relevant_condition_basin_ids = condition_basin_scope_by_connector_id.get(connector_node_id, set())
                for variable_row in variable_rows.itertuples(index=False):
                    listen_node_id = int(variable_row.listen_node_id)
                    listen_basin_ids, listen_level = _side_basin_ids_and_level(
                        listen_node_id,
                        open_adjacency=open_adjacency,
                        node_type_by_id=node_type_by_id,
                        target_level_by_basin_id=target_level_by_basin_id,
                        level_boundary_level_by_id=level_boundary_level_by_id,
                        max_iter=max_iter,
                    )
                    if listen_level is None:
                        continue
                    if not (listen_basin_ids & relevant_condition_basin_ids):
                        continue

                    condition_mask = condition_df["node_id"].eq(control_node_id) & condition_df[
                        "compound_variable_id"
                    ].eq(variable_row.compound_variable_id)
                    if not condition_mask.any():
                        continue

                    condition_rows = condition_df.loc[condition_mask].copy()
                    if "condition_id" in condition_rows.columns:
                        condition_rows = condition_rows.sort_values("condition_id")
                    threshold_values = control_condition_thresholds(
                        layout_key=str(function),
                        compound_variable_id=int(variable_row.compound_variable_id),
                        variable_name=str(variable_row.variable),
                        level_value=float(listen_level),
                        weight=float(variable_row.weight),
                        level_difference_threshold=level_difference_threshold,
                    )
                    if len(threshold_values) != len(condition_rows):
                        raise ValueError(
                            f"{function.capitalize()} #{connector_node_id} met DiscreteControl #{control_node_id} "
                            f"heeft {len(condition_rows)} conditions voor compound_variable_id "
                            f"{int(variable_row.compound_variable_id)}, verwacht {len(threshold_values)}."
                        )

                    for threshold_value, (condition_index, _condition_row) in zip(
                        threshold_values, condition_rows.iterrows(), strict=True
                    ):
                        new_high = float(threshold_value)
                        new_low = float(threshold_value)
                        old_high = condition_df.at[condition_index, "threshold_high"]
                        old_low = condition_df.at[condition_index, "threshold_low"]
                        if (
                            pd.notna(old_high)
                            and float(old_high) == new_high
                            and pd.notna(old_low)
                            and float(old_low) == new_low
                        ):
                            continue

                        condition_df.at[condition_index, "threshold_high"] = new_high
                        condition_df.at[condition_index, "threshold_low"] = new_low
                        records.append(
                            {
                                "node_id": connector_node_id,
                                "control_node_id": control_node_id,
                                "listen_node_id": listen_node_id,
                                "listen_basin_node_ids": ",".join(map(str, sorted(listen_basin_ids))),
                                "table": "DiscreteControl / condition",
                                "row_index": condition_index,
                                "column": "threshold_high,threshold_low",
                                "old_value": f"{old_high},{old_low}",
                                "new_value": f"{new_high},{new_low}",
                            }
                        )

    updates_df = pd.DataFrame(records)
    if output_path is not None and not updates_df.empty:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        updates_df.to_csv(output_path, index=False)
    written_output_gpkg = None
    if output_gpkg is not None:
        written_output_gpkg = _write_control_updates_gpkg(
            model=model,
            updates_df=updates_df,
            output_gpkg=output_gpkg,
        )

    if verbose:
        print("Manning-route controller level updates:", len(updates_df))
        if output_path is not None and not updates_df.empty:
            print(f"Manning-route controller report geschreven: {output_path}")
        if output_gpkg is not None:
            if written_output_gpkg is None:
                print("Manning-route controller GPKG niet geschreven: geen controller-updates met geometrie.")
            else:
                print(f"Manning-route controller GPKG geschreven: {written_output_gpkg}")

    return updates_df


def sync_parameterized_manning_basin_levels(
    model: Model,
    *,
    aanvoergebieden_df=None,
    target_level_column: str = "meta_streefpeil",
    output_gpkg: str | Path | None = None,
    excluded_manning_node_ids: list[int] | set[int] | None = None,
    protected_basin_node_ids: list[int] | set[int] | None = None,
    apply: bool = True,
    update_profile: bool = True,
    update_state: bool = True,
    tolerance: float = 1e-6,
    verbose: bool = True,
    max_iter: int = 500,
) -> pd.DataFrame:
    """Synchroniseer basin-peilen in parameterisatie via gesloten Manning-componenten.

    Deze routine raakt alleen Basin / area, Basin / state en Basin / profile. Outlet,
    Pump en DiscreteControl-tabellen worden niet aangepast. Alleen ManningResistance-nodes
    binnen `aanvoergebieden_df` worden gebruikt als startpunt; het hele open component
    rond zo'n Manning-node wordt daarna gesloten op het dominante benedenstroomse basinpeil.
    Eindbasins met precies een ManningResistance als enige open buur en hooguit
    TabulatedRatingCurve als overige directe verbinding worden beschermd.
    """
    if aanvoergebieden_df is None:
        raise ValueError("aanvoergebieden_df is verplicht voor parameterisatie-Manning-sync.")

    node_type_by_id = _node_type_by_id(model)
    link_df = _flow_link_df(model)
    open_adjacency = _undirected_open_adjacency(link_df=link_df, node_type_by_id=node_type_by_id)
    target_level_by_basin_id = _target_level_by_basin_id(model, target_level_column)
    excluded_manning_node_ids = {int(node_id) for node_id in excluded_manning_node_ids or set()}
    protected_basin_node_ids = {int(node_id) for node_id in protected_basin_node_ids or set()}

    if LEVEL_UPDATE_PROTECTION_COLUMN in model.basin.area.df.columns:
        protected_basin_node_ids.update(
            model.basin.area.df.loc[
                _truthy_series(model.basin.area.df[LEVEL_UPDATE_PROTECTION_COLUMN]),
                "node_id",
            ]
            .dropna()
            .astype(int)
            .to_list()
        )

    start_manning_node_ids = _node_ids_of_type_in_geometry(
        model=model,
        node_type="ManningResistance",
        geometry_df=aanvoergebieden_df,
        excluded_node_ids=excluded_manning_node_ids,
    )

    records: list[dict[str, object]] = []
    component_by_key: dict[tuple[int, ...], set[int]] = {}
    selected_manning_by_component_key: dict[tuple[int, ...], set[int]] = defaultdict(set)
    for manning_node_id in start_manning_node_ids:
        if node_type_by_id.get(int(manning_node_id)) != "ManningResistance":
            continue

        component_node_ids = _open_component(
            start_node_id=int(manning_node_id),
            adjacency=open_adjacency,
            max_iter=max_iter,
        )
        component_key = tuple(sorted(component_node_ids))
        component_by_key[component_key] = component_node_ids
        selected_manning_by_component_key[component_key].add(int(manning_node_id))

    for component_id, component_key in enumerate(sorted(component_by_key), start=1):
        component_node_ids = component_by_key[component_key]
        basin_ids = sorted(node_id for node_id in component_node_ids if node_type_by_id.get(node_id) == "Basin")
        component_manning_node_ids = sorted(
            node_id for node_id in component_node_ids if node_type_by_id.get(node_id) == "ManningResistance"
        )
        selected_manning_node_ids = sorted(selected_manning_by_component_key[component_key])
        boundary_control_node_ids = _component_boundary_control_node_ids(
            component_node_ids=component_node_ids,
            link_df=link_df,
            node_type_by_id=node_type_by_id,
        )

        if not basin_ids:
            records.append(
                {
                    "component_id": component_id,
                    "selected_manning_node_ids": ",".join(map(str, selected_manning_node_ids)),
                    "manning_node_ids": ",".join(map(str, component_manning_node_ids)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "status": "geen_basin_in_manning_component",
                }
            )
            continue

        target_level, target_level_status, target_control_node_ids, target_basin_ids = (
            _dominant_downstream_parameterized_target_level(
                component_node_ids=component_node_ids,
                boundary_control_node_ids=boundary_control_node_ids,
                link_df=link_df,
                node_type_by_id=node_type_by_id,
                target_level_by_basin_id=target_level_by_basin_id,
                tolerance=tolerance,
                max_iter=max_iter,
            )
        )
        if target_level is None:
            records.append(
                {
                    "component_id": component_id,
                    "selected_manning_node_ids": ",".join(map(str, selected_manning_node_ids)),
                    "manning_node_ids": ",".join(map(str, component_manning_node_ids)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "target_level_basis": target_level_status,
                    "status": "target_level_ontbreekt",
                }
            )
            continue

        terminal_manning_branch_basin_node_ids = _terminal_manning_branch_basin_node_ids(
            component_node_ids=component_node_ids,
            link_df=link_df,
            node_type_by_id=node_type_by_id,
        )
        changed_any_basin = False
        for basin_id in basin_ids:
            old_level = target_level_by_basin_id.get(int(basin_id))
            if int(basin_id) in protected_basin_node_ids:
                status = "handmatig_peil_behouden"
                changed_any_basin = True
            elif int(basin_id) in terminal_manning_branch_basin_node_ids:
                status = "manning_eindbasin_behouden"
                changed_any_basin = True
            elif pd.notna(old_level) and abs(float(old_level) - float(target_level)) <= tolerance:
                status = "ongewijzigd"
            else:
                status = "update"
                changed_any_basin = True

            records.append(
                {
                    "component_id": component_id,
                    "selected_manning_node_ids": ",".join(map(str, selected_manning_node_ids)),
                    "manning_node_ids": ",".join(map(str, component_manning_node_ids)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "target_control_node_ids": ",".join(map(str, target_control_node_ids)),
                    "target_basin_node_ids": ",".join(map(str, target_basin_ids)),
                    "target_level_basis": target_level_status,
                    "basin_node_id": int(basin_id),
                    "old_level": old_level,
                    "new_level": target_level,
                    "status": status,
                }
            )

        if not changed_any_basin:
            records.append(
                {
                    "component_id": component_id,
                    "selected_manning_node_ids": ",".join(map(str, selected_manning_node_ids)),
                    "manning_node_ids": ",".join(map(str, component_manning_node_ids)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "target_control_node_ids": ",".join(map(str, target_control_node_ids)),
                    "target_basin_node_ids": ",".join(map(str, target_basin_ids)),
                    "target_level_basis": target_level_status,
                    "status": "geen_level_afwijking",
                }
            )

    updates_df = pd.DataFrame(records)
    if updates_df.empty or "basin_node_id" not in updates_df.columns:
        return updates_df

    update_rows = updates_df[updates_df["status"].eq("update")].copy()
    conflicting = update_rows.groupby("basin_node_id")["new_level"].nunique().loc[lambda series: series.gt(1)]
    if not conflicting.empty:
        raise ValueError(f"Tegenstrijdige parameterisatie-Manning peilen voor basins: {conflicting.index.to_list()}")

    update_rows = update_rows.drop_duplicates(subset=["basin_node_id"], keep="last")
    profile_shift_by_basin_id: dict[int, float] = {}
    if apply and not update_rows.empty:
        if update_profile and model.basin.profile.df is not None:
            for row in update_rows.itertuples(index=False):
                if pd.isna(row.old_level) or pd.isna(row.new_level):
                    continue
                basin_id = int(row.basin_node_id)
                profile_mask = model.basin.profile.df["node_id"].eq(basin_id)
                if not profile_mask.any():
                    continue

                level_shift = float(row.new_level) - float(row.old_level)
                if abs(level_shift) > tolerance:
                    model.basin.profile.df.loc[profile_mask, "level"] = (
                        model.basin.profile.df.loc[profile_mask, "level"].astype(float) + level_shift
                    )
                    profile_shift_by_basin_id[basin_id] = level_shift

        level_by_basin_id = update_rows.set_index("basin_node_id")["new_level"].astype(float).to_dict()
        mask = model.basin.area.df["node_id"].isin(level_by_basin_id)
        model.basin.area.df.loc[mask, target_level_column] = model.basin.area.df.loc[mask, "node_id"].map(
            level_by_basin_id
        )
        if update_state:
            model.basin.state.df = model.basin.area.df[["node_id", target_level_column]].rename(
                columns={target_level_column: "level"}
            )

    if profile_shift_by_basin_id:
        basin_row_mask = updates_df["basin_node_id"].notna()
        updates_df.loc[basin_row_mask, "profile_level_shift"] = updates_df.loc[basin_row_mask, "basin_node_id"].map(
            profile_shift_by_basin_id
        )

    written_output_gpkg = None
    if output_gpkg is not None:
        written_output_gpkg = _write_basin_updates_gpkg(
            model=model,
            updates_df=updates_df,
            output_gpkg=output_gpkg,
            layer="parameterized_manning_basin_level_updates",
        )

    if verbose:
        print("Parameterisatie Manning-route basin level updates:", update_rows["basin_node_id"].nunique())
        if profile_shift_by_basin_id:
            print("Parameterisatie Manning-route basin profielen verschoven:", len(profile_shift_by_basin_id))
        if output_gpkg is not None:
            if written_output_gpkg is None:
                print("Parameterisatie Manning-route GPKG niet geschreven: geen basin-updates met geometrie.")
            else:
                print(f"Parameterisatie Manning-route GPKG geschreven: {written_output_gpkg}")

    return updates_df


def sync_basin_levels_along_manning_routes(
    model: Model,
    *,
    connector_node_ids: list[int] | None = None,
    start_basin_node_ids: list[int] | None = None,
    protected_basin_node_ids: list[int] | None = None,
    protected_control_node_ids: list[int] | set[int] | None = None,
    target_level_column: str = "meta_streefpeil",
    output_path: str | Path | None = None,
    basin_output_gpkg: str | Path | None = None,
    control_output_path: str | Path | None = None,
    control_output_gpkg: str | Path | None = None,
    extra_control_node_ids: list[int] | set[int] | None = None,
    apply: bool = True,
    update_profile: bool = True,
    update_state: bool = True,
    update_control_levels: bool = True,
    prefer_existing_control_levels: bool = True,
    prefer_dominant_downstream_control_level: bool = True,
    require_manning_resistance: bool = True,
    require_downstream_supply_control: bool = True,
    protect_upstream_basins_without_supply_control: bool = True,
    treat_flow_demand_as_supply_control: bool = False,
    inspect_all_connector_sides: bool = False,
    control_level_tolerance: float = 1e-6,
    verbose: bool = True,
    max_iter: int = 500,
) -> pd.DataFrame:
    """Zet basin-peilen in open Manning-componenten gelijk aan het laagste componentpeil.

    Basin, ManningResistance en Junction worden als open/tweerichtings verbindingen behandeld om een
    Manning-component te kunnen doorlopen. Outlet en Pump begrenzen zo'n component. Componenten zonder
    ManningResistance of zonder benedenstroomse inlaat/doorlaat worden standaard overgeslagen.
    Bovenstroomse basins zonder aanvoerende sturing worden standaard behouden. Handmatig beschermde basins
    worden gerapporteerd maar niet overschreven. FlowDemand-gestuurde targets tellen alleen als aanvoerende
    sturing wanneer `treat_flow_demand_as_supply_control=True`; hun eigen static/controller-levels blijven
    beschermd. Standaard wordt alleen de hydraulisch downstream open zijde van een connector gestart; alle
    open zijden inspecteren kan met `inspect_all_connector_sides=True`.
    """
    node_type_by_id = _node_type_by_id(model)
    link_df = _flow_link_df(model)
    open_adjacency = _undirected_open_adjacency(link_df=link_df, node_type_by_id=node_type_by_id)
    target_level_by_basin_id = _target_level_by_basin_id(model, target_level_column)
    flow_demand_connector_node_ids = _flow_demand_target_node_ids(model, node_type_by_id=node_type_by_id)
    ignored_supply_control_node_ids = set() if treat_flow_demand_as_supply_control else flow_demand_connector_node_ids
    control_function_by_node_id = _control_function_by_node_id(model, node_type_by_id=node_type_by_id)
    protected_basin_node_ids = {int(node_id) for node_id in protected_basin_node_ids or []}
    if connector_node_ids is None and start_basin_node_ids is None:
        connector_node_ids = _candidate_connector_node_ids(model)
    if connector_node_ids is not None:
        connector_node_ids = [
            int(node_id) for node_id in connector_node_ids if int(node_id) not in ignored_supply_control_node_ids
        ]

    starts: list[tuple[int | None, int]] = []
    for connector_node_id in connector_node_ids or []:
        if inspect_all_connector_sides:
            open_neighbors = _open_neighbors_from_connector(
                node_id=int(connector_node_id),
                link_df=link_df,
                node_type_by_id=node_type_by_id,
            )
        else:
            open_neighbors = _directed_open_neighbors_from_connector(
                node_id=int(connector_node_id),
                link_df=link_df,
                node_type_by_id=node_type_by_id,
            )

        if len(open_neighbors) == 1:
            starts.append((int(connector_node_id), open_neighbors[0]))
        elif inspect_all_connector_sides and open_neighbors:
            starts.extend((int(connector_node_id), open_neighbor) for open_neighbor in open_neighbors)
        else:
            starts.append((int(connector_node_id), -1))

    starts.extend((None, int(start_basin_node_id)) for start_basin_node_id in start_basin_node_ids or [])

    records: list[dict[str, object]] = []
    component_connectors_by_key: dict[tuple[int, ...], set[int]] = {}
    component_start_by_key: dict[tuple[int, ...], int] = {}
    for connector_node_id, start_node_id in starts:
        if start_node_id < 0:
            records.append(
                {
                    "connector_node_id": connector_node_id,
                    "start_node_id": None,
                    "status": "geen_eenduidige_open_component_start",
                }
            )
            continue

        if node_type_by_id.get(start_node_id) not in OPEN_NODE_TYPES:
            records.append(
                {
                    "connector_node_id": connector_node_id,
                    "start_node_id": start_node_id,
                    "status": "start_node_is_niet_open",
                }
            )
            continue

        component_node_ids = _open_component(
            start_node_id=start_node_id,
            adjacency=open_adjacency,
            max_iter=max_iter,
        )
        component_key = tuple(sorted(component_node_ids))
        component_connectors_by_key.setdefault(component_key, set())
        if connector_node_id is not None:
            component_connectors_by_key[component_key].add(int(connector_node_id))
        component_start_by_key.setdefault(component_key, int(start_node_id))

    for component_id, component_key in enumerate(sorted(component_connectors_by_key), start=1):
        component_node_ids = set(component_key)
        connector_node_ids_for_component = sorted(component_connectors_by_key[component_key])
        has_manning_resistance = any(
            node_type_by_id.get(node_id) == "ManningResistance" for node_id in component_node_ids
        )
        if require_manning_resistance and not has_manning_resistance:
            records.append(
                {
                    "component_id": component_id,
                    "connector_node_ids": ",".join(map(str, connector_node_ids_for_component)),
                    "start_node_id": component_start_by_key.get(component_key),
                    "status": "geen_manning_resistance_in_component",
                }
            )
            continue

        boundary_control_node_ids = _component_boundary_control_node_ids(
            component_node_ids=component_node_ids,
            link_df=link_df,
            node_type_by_id=node_type_by_id,
        )
        boundary_control_node_ids = [
            node_id for node_id in boundary_control_node_ids if node_id not in ignored_supply_control_node_ids
        ]
        supply_boundary_control_node_ids = _supply_boundary_control_node_ids(
            boundary_control_node_ids=boundary_control_node_ids,
            control_function_by_node_id=control_function_by_node_id,
        )
        if require_downstream_supply_control and not supply_boundary_control_node_ids:
            records.append(
                {
                    "component_id": component_id,
                    "connector_node_ids": ",".join(map(str, connector_node_ids_for_component)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "status": "geen_inlaat_of_doorlaat",
                }
            )
            continue

        basin_ids = sorted(node_id for node_id in component_node_ids if node_type_by_id.get(node_id) == "Basin")
        upstream_protected_basin_node_ids = (
            _upstream_protected_basin_node_ids(
                component_node_ids=component_node_ids,
                link_df=link_df,
                node_type_by_id=node_type_by_id,
                control_function_by_node_id=control_function_by_node_id,
                boundary_control_node_ids=boundary_control_node_ids,
            )
            if protect_upstream_basins_without_supply_control
            else set()
        )
        single_manning_branch_basin_node_ids = _single_manning_branch_basin_node_ids(
            component_node_ids=component_node_ids,
            link_df=link_df,
            node_type_by_id=node_type_by_id,
            control_function_by_node_id=control_function_by_node_id,
            ignored_supply_control_node_ids=ignored_supply_control_node_ids,
        )
        boundary_basin_ids = _component_boundary_basin_node_ids(
            component_node_ids=component_node_ids,
            link_df=link_df,
            node_type_by_id=node_type_by_id,
        )
        target_level: float | None = None
        target_basin_ids: list[int] = []
        target_level_basis = "geen_target_level"
        target_control_node_ids: list[int] = []
        control_level_status = "niet_gecontroleerd"

        if prefer_existing_control_levels:
            control_level, control_level_status, target_control_node_ids = _consistent_control_level(
                model=model,
                connector_node_ids=boundary_control_node_ids,
                node_type_by_id=node_type_by_id,
                tolerance=control_level_tolerance,
            )
            if control_level is not None:
                target_level = control_level
                target_level_basis = "bestaande_control_levels"

        if target_level is None and prefer_dominant_downstream_control_level:
            control_level, control_level_status, target_control_node_ids = _dominant_downstream_control_level(
                model=model,
                component_node_ids=component_node_ids,
                boundary_control_node_ids=boundary_control_node_ids,
                link_df=link_df,
                node_type_by_id=node_type_by_id,
                target_level_by_basin_id=target_level_by_basin_id,
                tolerance=control_level_tolerance,
                max_iter=max_iter,
            )
            if control_level is not None:
                target_level = control_level
                target_level_basis = "dominante_downstream_control"

        if target_level is None:
            level_source_basin_ids = [
                basin_id
                for basin_id in boundary_basin_ids
                if basin_id in target_level_by_basin_id and pd.notna(target_level_by_basin_id[basin_id])
            ]
            target_level_basis = "laagste_grens_basin_peil"
            if not level_source_basin_ids:
                level_source_basin_ids = [
                    basin_id
                    for basin_id in basin_ids
                    if basin_id in target_level_by_basin_id and pd.notna(target_level_by_basin_id[basin_id])
                ]
                target_level_basis = "laagste_component_basin_peil"

            if level_source_basin_ids:
                target_level = min(float(target_level_by_basin_id[basin_id]) for basin_id in level_source_basin_ids)
                target_basin_ids = [
                    basin_id
                    for basin_id in level_source_basin_ids
                    if float(target_level_by_basin_id[basin_id]) == target_level
                ]

        if target_level is None:
            records.append(
                {
                    "component_id": component_id,
                    "connector_node_ids": ",".join(map(str, connector_node_ids_for_component)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "control_level_status": control_level_status,
                    "status": "target_level_ontbreekt",
                }
            )
            continue

        changed_any_basin = False
        for basin_id in basin_ids:
            old_level = target_level_by_basin_id.get(basin_id)
            protected_status = None
            if basin_id in upstream_protected_basin_node_ids:
                protected_status = "bovenstrooms_zonder_aanvoer_sturing_behouden"
            elif basin_id in single_manning_branch_basin_node_ids:
                protected_status = "enkele_manning_tak_basin_behouden"

            if protected_status is not None:
                changed_any_basin = True
                records.append(
                    {
                        "component_id": component_id,
                        "connector_node_ids": ",".join(map(str, connector_node_ids_for_component)),
                        "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                        "target_basin_node_ids": ",".join(map(str, target_basin_ids)),
                        "target_control_node_ids": ",".join(map(str, target_control_node_ids)),
                        "target_level_basis": target_level_basis,
                        "control_level_status": control_level_status,
                        "basin_node_id": basin_id,
                        "old_level": old_level,
                        "new_level": target_level,
                        "status": protected_status,
                    }
                )
                continue

            if pd.notna(old_level) and abs(float(old_level) - float(target_level)) <= control_level_tolerance:
                records.append(
                    {
                        "component_id": component_id,
                        "connector_node_ids": ",".join(map(str, connector_node_ids_for_component)),
                        "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                        "target_basin_node_ids": ",".join(map(str, target_basin_ids)),
                        "target_control_node_ids": ",".join(map(str, target_control_node_ids)),
                        "target_level_basis": target_level_basis,
                        "control_level_status": control_level_status,
                        "basin_node_id": basin_id,
                        "old_level": old_level,
                        "new_level": target_level,
                        "status": "ongewijzigd",
                    }
                )
                continue

            changed_any_basin = True
            if basin_id in protected_basin_node_ids:
                records.append(
                    {
                        "component_id": component_id,
                        "connector_node_ids": ",".join(map(str, connector_node_ids_for_component)),
                        "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                        "target_basin_node_ids": ",".join(map(str, target_basin_ids)),
                        "target_control_node_ids": ",".join(map(str, target_control_node_ids)),
                        "target_level_basis": target_level_basis,
                        "control_level_status": control_level_status,
                        "basin_node_id": basin_id,
                        "old_level": old_level,
                        "new_level": target_level,
                        "status": "handmatig_peil_behouden",
                    }
                )
                continue

            records.append(
                {
                    "component_id": component_id,
                    "connector_node_ids": ",".join(map(str, connector_node_ids_for_component)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "target_basin_node_ids": ",".join(map(str, target_basin_ids)),
                    "target_control_node_ids": ",".join(map(str, target_control_node_ids)),
                    "target_level_basis": target_level_basis,
                    "control_level_status": control_level_status,
                    "basin_node_id": basin_id,
                    "old_level": old_level,
                    "new_level": target_level,
                    "status": "update",
                }
            )
        if not changed_any_basin:
            records.append(
                {
                    "component_id": component_id,
                    "connector_node_ids": ",".join(map(str, connector_node_ids_for_component)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "target_basin_node_ids": ",".join(map(str, target_basin_ids)),
                    "target_control_node_ids": ",".join(map(str, target_control_node_ids)),
                    "target_level_basis": target_level_basis,
                    "control_level_status": control_level_status,
                    "status": "geen_level_afwijking",
                }
            )

    updates_df = pd.DataFrame(records)
    if updates_df.empty or "basin_node_id" not in updates_df.columns:
        return updates_df

    update_rows = updates_df[updates_df["status"].eq("update")].copy()
    conflicting = update_rows.groupby("basin_node_id")["new_level"].nunique().loc[lambda series: series.gt(1)]
    if not conflicting.empty:
        raise ValueError(f"Tegenstrijdige Manning-route peilen voor basins: {conflicting.index.to_list()}")

    update_rows = update_rows.drop_duplicates(subset=["basin_node_id"], keep="last")
    profile_shift_by_basin_id: dict[int, float] = {}
    if apply:
        if update_profile and model.basin.profile.df is not None and "basin_node_id" in updates_df.columns:
            profile_rows = updates_df[
                updates_df["status"].isin(["update", "ongewijzigd", "handmatig_peil_behouden"])
                & updates_df["basin_node_id"].notna()
            ].copy()
            profile_rows["desired_profile_level"] = profile_rows["new_level"]
            protected_mask = profile_rows["status"].eq("handmatig_peil_behouden")
            profile_rows.loc[protected_mask, "desired_profile_level"] = profile_rows.loc[protected_mask, "old_level"]
            profile_rows = profile_rows.dropna(subset=["desired_profile_level"]).drop_duplicates(
                subset=["basin_node_id"], keep="last"
            )

            profile_top_by_basin_id = model.basin.profile.df.groupby("node_id")["level"].max()
            for row in profile_rows.itertuples(index=False):
                basin_id = int(row.basin_node_id)
                if basin_id not in profile_top_by_basin_id.index:
                    continue

                level_shift = float(row.desired_profile_level) - float(profile_top_by_basin_id.at[basin_id])
                if level_shift == 0.0:
                    continue

                profile_mask = model.basin.profile.df["node_id"].eq(basin_id)
                model.basin.profile.df.loc[profile_mask, "level"] = (
                    model.basin.profile.df.loc[profile_mask, "level"].astype(float) + level_shift
                )
                profile_shift_by_basin_id[basin_id] = level_shift

        if not update_rows.empty:
            level_by_basin_id = update_rows.set_index("basin_node_id")["new_level"].astype(float).to_dict()
            mask = model.basin.area.df["node_id"].isin(level_by_basin_id)
            model.basin.area.df.loc[mask, target_level_column] = model.basin.area.df.loc[mask, "node_id"].map(
                level_by_basin_id
            )
        if update_state:
            model.basin.state.df = model.basin.area.df[["node_id", target_level_column]].rename(
                columns={target_level_column: "level"}
            )
        if update_control_levels:
            if control_output_path is None and output_path is not None:
                output_path_for_controls = Path(output_path).with_name(Path(output_path).stem + "_control_levels.csv")
            else:
                output_path_for_controls = control_output_path
            sync_control_levels_for_basin_updates(
                model=model,
                basin_level_updates=updates_df,
                extra_control_node_ids=extra_control_node_ids,
                protected_control_node_ids=protected_control_node_ids,
                target_level_column=target_level_column,
                output_path=output_path_for_controls,
                output_gpkg=control_output_gpkg,
                verbose=verbose,
                max_iter=max_iter,
            )

    if profile_shift_by_basin_id:
        basin_row_mask = updates_df["basin_node_id"].notna()
        updates_df.loc[basin_row_mask, "profile_level_shift"] = updates_df.loc[basin_row_mask, "basin_node_id"].map(
            profile_shift_by_basin_id
        )

    if output_path is not None:
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        updates_df.to_csv(output_path, index=False)
    written_basin_output_gpkg = None
    if basin_output_gpkg is not None:
        written_basin_output_gpkg = _write_basin_updates_gpkg(
            model=model,
            updates_df=updates_df,
            output_gpkg=basin_output_gpkg,
        )

    if verbose:
        protected_rows = updates_df[updates_df["status"].eq("handmatig_peil_behouden")]
        print("Manning-route basin level updates:", update_rows["basin_node_id"].nunique())
        if profile_shift_by_basin_id:
            print("Manning-route basin profielen verschoven:", len(profile_shift_by_basin_id))
        if not protected_rows.empty:
            print("Manning-route handmatige basin-peilen behouden:", protected_rows["basin_node_id"].nunique())
        if output_path is not None:
            print(f"Manning-route report geschreven: {output_path}")
        if basin_output_gpkg is not None:
            if written_basin_output_gpkg is None:
                print("Manning-route basin GPKG niet geschreven: geen basin-updates met geometrie.")
            else:
                print(f"Manning-route basin GPKG geschreven: {written_basin_output_gpkg}")

    return updates_df


def sync_full_control_manning_levels(
    model: Model,
    *,
    output_dir: str | Path | None = None,
    write_reports: bool = False,
    protected_basin_node_ids: list[int] | None = None,
    protected_control_node_ids: list[int] | set[int] | None = None,
    **kwargs,
) -> pd.DataFrame:
    """Synchroniseer Manning-route levels voor full-control notebooks.

    De notebooks gebruiken dezelfde basin- en controller-sync, maar schrijven de
    GPKG-rapporten alleen bij de expliciete rapportstap. Pre-write syncs blijven
    daardoor stil en schrijven geen tijdelijke CSV-bestanden.
    """
    basin_output_gpkg = None
    control_output_gpkg = None
    if write_reports:
        if output_dir is None:
            raise ValueError("output_dir is verplicht wanneer write_reports=True.")
        output_dir = Path(output_dir)
        basin_output_gpkg = output_dir / "manning_level_basin_updates.gpkg"
        control_output_gpkg = output_dir / "manning_level_control_updates.gpkg"

    return sync_basin_levels_along_manning_routes(
        model=model,
        basin_output_gpkg=basin_output_gpkg,
        control_output_gpkg=control_output_gpkg,
        protected_basin_node_ids=protected_basin_node_ids,
        protected_control_node_ids=protected_control_node_ids,
        **kwargs,
    )
