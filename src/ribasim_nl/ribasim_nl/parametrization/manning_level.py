# pyrefly: ignore-errors
from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from ribasim_nl import Model


CONTROL_NODE_TYPES = {"Outlet", "Pump"}
OPEN_NODE_TYPES = {"Basin", "ManningResistance", "Junction"}


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


def _capacity_is_positive(df: pd.DataFrame) -> pd.Series:
    flow_rate = pd.to_numeric(df.get("flow_rate", 0.0), errors="coerce").fillna(0.0)
    max_flow_rate = pd.to_numeric(df.get("max_flow_rate", 0.0), errors="coerce").fillna(0.0)
    return flow_rate.gt(0.0) | max_flow_rate.gt(0.0)


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


def _dominant_downstream_control_level(
    model: Model,
    *,
    component_node_ids: set[int],
    boundary_control_node_ids: list[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
    tolerance: float,
    max_iter: int,
) -> tuple[float | None, str, list[int]]:
    level_by_node_id = _control_level_by_node_id(
        model=model,
        connector_node_ids=boundary_control_node_ids,
        node_type_by_id=node_type_by_id,
        tolerance=tolerance,
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

    component_connector_node_ids: set[int] = set()
    if "boundary_control_node_ids" in basin_level_updates.columns:
        for value in basin_level_updates["boundary_control_node_ids"].dropna():
            component_connector_node_ids.update(_parse_node_id_list(value))
    component_connector_node_ids.update(int(node_id) for node_id in extra_control_node_ids or [])

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

    records: list[dict[str, object]] = []
    touched_connector_node_ids: set[int] = set()
    control_name_levels_by_connector_id: dict[int, tuple[str, float | None, float | None]] = {}

    for node_type in sorted(CONTROL_NODE_TYPES):
        component = getattr(model, node_type.lower(), None)
        static_df = getattr(getattr(component, "static", None), "df", None)
        if static_df is None or static_df.empty or "control_state" not in static_df.columns:
            continue

        table_name = f"{node_type} / static"
        for connector_node_id, connector_rows in static_df.groupby("node_id", sort=True):
            connector_node_id = int(connector_node_id)
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
            in_component_scope = connector_node_id in component_connector_node_ids or bool(
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

            control_name_levels_by_connector_id[connector_node_id] = (function, upstream_level, downstream_level)
            if not locked_control:
                touched_connector_node_ids.add(connector_node_id)
            for row_index, state in control_state.items():
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

                if pd.notna(state) and upstream_level is not None and "min_upstream_level" in static_df.columns:
                    if state == "aanvoer" and has_supply_state:
                        min_upstream_level = float(upstream_level) + us_target_level_offset_supply
                    else:
                        min_upstream_level = float(upstream_level)
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
                    pd.notna(state)
                    and has_supply_state
                    and state == "aanvoer"
                    and downstream_level is not None
                    and "max_downstream_level" in static_df.columns
                ):
                    if locked_control:
                        continue
                    _set_value_if_changed(
                        static_df,
                        row_index,
                        "max_downstream_level",
                        float(downstream_level),
                        records,
                        record,
                    )

    variable_df = getattr(getattr(model.discrete_control, "variable", None), "df", None)
    condition_df = getattr(getattr(model.discrete_control, "condition", None), "df", None)
    if variable_df is not None and condition_df is not None:
        for connector_node_id in sorted(touched_connector_node_ids):
            for control_node_id in control_ids_by_connector_id.get(connector_node_id, []):
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

                variable_rows = variable_df[variable_df["node_id"].eq(control_node_id)].copy()
                if variable_rows.empty:
                    continue

                variable_rows["weight"] = pd.to_numeric(variable_rows["weight"], errors="coerce")
                variable_rows = variable_rows.dropna(subset=["listen_node_id", "compound_variable_id", "weight"])
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
                    if connector_node_id not in component_connector_node_ids and not (
                        listen_basin_ids & component_basin_node_ids
                    ):
                        continue

                    threshold_value = float(listen_level) * float(variable_row.weight)
                    condition_mask = condition_df["node_id"].eq(control_node_id) & condition_df[
                        "compound_variable_id"
                    ].eq(variable_row.compound_variable_id)
                    if not condition_mask.any():
                        continue

                    condition_rows = condition_df.loc[condition_mask].copy()
                    if "condition_id" in condition_rows.columns:
                        condition_rows = condition_rows.sort_values("condition_id")
                    base_high = condition_rows.iloc[0]["threshold_high"]
                    base_low = condition_rows.iloc[0]["threshold_low"]

                    for condition_index, condition_row in condition_rows.iterrows():
                        high_offset = (
                            0.0
                            if pd.isna(base_high) or pd.isna(condition_row["threshold_high"])
                            else float(condition_row["threshold_high"]) - float(base_high)
                        )
                        low_offset = (
                            0.0
                            if pd.isna(base_low) or pd.isna(condition_row["threshold_low"])
                            else float(condition_row["threshold_low"]) - float(base_low)
                        )
                        new_high = threshold_value + high_offset
                        new_low = threshold_value + low_offset
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


def sync_basin_levels_along_manning_routes(
    model: Model,
    *,
    connector_node_ids: list[int] | None = None,
    start_basin_node_ids: list[int] | None = None,
    protected_basin_node_ids: list[int] | None = None,
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
    control_level_tolerance: float = 1e-6,
    verbose: bool = True,
    max_iter: int = 500,
) -> pd.DataFrame:
    """Zet basin-peilen in open Manning-componenten gelijk aan het laagste componentpeil.

    Basin, ManningResistance en Junction worden als open/tweerichtings verbindingen
    behandeld. Outlet en Pump begrenzen zo'n component. Handmatig beschermde basins
    worden gerapporteerd maar niet overschreven.
    """
    node_type_by_id = _node_type_by_id(model)
    link_df = _flow_link_df(model)
    open_adjacency = _undirected_open_adjacency(link_df=link_df, node_type_by_id=node_type_by_id)
    target_level_by_basin_id = _target_level_by_basin_id(model, target_level_column)
    protected_basin_node_ids = {int(node_id) for node_id in protected_basin_node_ids or []}
    if connector_node_ids is None and start_basin_node_ids is None:
        connector_node_ids = _candidate_connector_node_ids(model)

    starts: list[tuple[int | None, int]] = []
    for connector_node_id in connector_node_ids or []:
        open_neighbors = _directed_open_neighbors_from_connector(
            node_id=int(connector_node_id),
            link_df=link_df,
            node_type_by_id=node_type_by_id,
        )
        if len(open_neighbors) == 1:
            starts.append((int(connector_node_id), open_neighbors[0]))
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
        boundary_control_node_ids = _component_boundary_control_node_ids(
            component_node_ids=component_node_ids,
            link_df=link_df,
            node_type_by_id=node_type_by_id,
        )
        basin_ids = sorted(node_id for node_id in component_node_ids if node_type_by_id.get(node_id) == "Basin")
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
