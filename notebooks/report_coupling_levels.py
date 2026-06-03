from __future__ import annotations

import argparse
import math
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

CONTROL_NODE_TYPES = {"Outlet", "Pump"}
STATIC_TABLE_BY_NODE_TYPE = {
    "Outlet": "Outlet / static",
    "Pump": "Pump / static",
}
RWS_AUTHORITY = "Rijkswaterstaat"
SKIP_LEVEL_UPDATE_AUTHORITIES = {"WetterskipFryslan"}
SKIP_MIN_UPSTREAM_UPDATE_NODE_IDS = {3800291}
DEFAULT_UPSTREAM_SUPPLY_OFFSET = -0.04
DEFAULT_RWS_PROFILE_OFFSET = 0.1
FLOW_DEMAND_CONTROL_THRESHOLD_OFFSET = 0.02
FLOW_DEMAND_RWS_PROFILE_MIN_UPSTREAM_OFFSET = 0.01
FLOW_DEMAND_RWS_CONTROL_THRESHOLD_OFFSET = 0.03


def database_gpkg_path(toml_file: Path) -> Path:
    model_dir = toml_file.parent
    input_database_gpkg = model_dir / "input" / "database.gpkg"
    legacy_database_gpkg = model_dir / "database.gpkg"
    if input_database_gpkg.exists():
        return input_database_gpkg
    if legacy_database_gpkg.exists():
        return legacy_database_gpkg
    raise FileNotFoundError(
        f"Kan geen database.gpkg vinden voor model {toml_file}. "
        f"Gezocht in {input_database_gpkg} en {legacy_database_gpkg}."
    )


def resolve_output_gpkg(toml_file: Path, output_gpkg: Path | None) -> Path:
    if output_gpkg is None:
        return toml_file.with_name("coupling_level_report.gpkg")
    if output_gpkg.is_absolute():
        return output_gpkg
    return toml_file.parent / output_gpkg


def resolve_writable_gpkg(output_gpkg: Path, label: str) -> Path:
    if output_gpkg.exists():
        try:
            output_gpkg.unlink()
        except PermissionError:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_gpkg = output_gpkg.with_name(f"{output_gpkg.stem}_{timestamp}{output_gpkg.suffix}")
            print(f"{label} is in gebruik; schrijf naar alternatief bestand: {output_gpkg}", flush=True)
    output_gpkg.parent.mkdir(parents=True, exist_ok=True)
    return output_gpkg


def normalize_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def positive(value: object) -> bool:
    try:
        if value is None:
            return False
        number = float(value)
        return math.isfinite(number) and number > 0.0
    except (TypeError, ValueError):
        return False


def read_table(con: sqlite3.Connection, table: str) -> pd.DataFrame:
    return pd.read_sql_query(f'SELECT * FROM "{table}"', con)  # noqa: S608


def table_exists(con: sqlite3.Connection, table: str) -> bool:
    row = con.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ? LIMIT 1",
        (table,),
    ).fetchone()
    return row is not None


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

    target_types = [node_type_by_id.get(node_id) for node_id, _, _ in targets]
    if any(node_type == "ManningResistance" for node_type in target_types):
        return False, "downstream ManningResistance niet gebruikt voor max_downstream_level"
    if any(node_type in CONTROL_NODE_TYPES for node_type in target_types):
        return True, None
    return False, f"geen direct downstream Outlet/Pump gevonden: {target_types}"


def static_row_has_capacity(row: pd.Series) -> bool:
    return positive(row.get("flow_rate")) or positive(row.get("max_flow_rate"))


def classify_functions(static_df: pd.DataFrame, flow_demand_inlet_nodes: set[int] | None = None) -> dict[int, str]:
    flow_demand_inlet_nodes = flow_demand_inlet_nodes or set()
    control_state = static_df["control_state"].astype("string").str.lower()
    capacity = static_df.apply(static_row_has_capacity, axis=1)
    aanvoer_nodes = set(static_df.loc[control_state.eq("aanvoer") & capacity, "node_id"].astype(int))
    aanvoer_nodes |= flow_demand_inlet_nodes
    afvoer_nodes = set(static_df.loc[control_state.eq("afvoer") & capacity, "node_id"].astype(int))
    node_ids = set(static_df["node_id"].astype(int))

    functions = dict.fromkeys(node_ids, "dicht")
    for node_id in aanvoer_nodes - afvoer_nodes:
        functions[node_id] = "inlaat"
    for node_id in afvoer_nodes - aanvoer_nodes:
        functions[node_id] = "uitlaat"
    for node_id in aanvoer_nodes & afvoer_nodes:
        functions[node_id] = "doorlaat"
    return functions


def capacity_text(rows: pd.DataFrame) -> str:
    parts = []
    for row in rows.itertuples(index=False):
        values = []
        for column in ["flow_rate", "min_flow_rate", "max_flow_rate"]:
            value = getattr(row, column, None)
            if pd.notna(value):
                values.append(f"{column}={float(value):g}")
        parts.append(f"{getattr(row, 'control_state', None)} ({', '.join(values)})")
    return "; ".join(parts)


def build_report(database_path: Path, upstream_supply_offset: float, rws_profile_offset: float, tolerance: float):
    con = sqlite3.connect(database_path)
    con.row_factory = sqlite3.Row

    node_df = read_table(con, "Node")
    link_df = read_table(con, "Link")
    basin_area_df = read_table(con, "Basin / area")
    basin_state_df = read_table(con, "Basin / state")
    basin_profile_df = read_table(con, "Basin / profile")

    node_by_id = node_df.set_index("node_id").to_dict("index")
    node_type_by_id = node_df.set_index("node_id")["node_type"].to_dict()
    basin_authority_by_id = node_df.set_index("node_id")["meta_waterbeheerder"].to_dict()
    basin_name_by_id = node_df.set_index("node_id")["name"].to_dict()
    streefpeil_by_basin_id = normalize_numeric(basin_area_df.set_index("node_id")["meta_streefpeil"])
    state_level_by_basin_id = normalize_numeric(basin_state_df.set_index("node_id")["level"])
    min_profile_by_basin_id = basin_profile_df.groupby("node_id")["level"].min()

    flow_link_df = link_df[link_df["link_type"].fillna("flow").eq("flow")].copy()
    outgoing_flow_links = (
        flow_link_df.groupby("from_node_id")[["link_id", "to_node_id"]]
        .apply(lambda rows: [(int(row.link_id), int(row.to_node_id)) for row in rows.itertuples()])
        .to_dict()
    )
    incoming_flow_links = (
        flow_link_df.groupby("to_node_id")[["link_id", "from_node_id"]]
        .apply(lambda rows: [(int(row.link_id), int(row.from_node_id)) for row in rows.itertuples()])
        .to_dict()
    )
    flow_demand_target_info: dict[int, dict[str, object]] = {}
    positive_flow_demand_target_node_ids: set[int] = set()
    flow_demand_controlled_node_ids: set[int] = set()
    flow_demand_parts = []
    for flow_demand_table in ["FlowDemand / time", "FlowDemand / static"]:
        if table_exists(con, flow_demand_table):
            flow_demand_df = read_table(con, flow_demand_table)
            flow_demand_df["demand"] = normalize_numeric(flow_demand_df["demand"])
            flow_demand_parts.append(flow_demand_df[["node_id", "demand"]])
    if flow_demand_parts:
        flow_demand_df = pd.concat(flow_demand_parts, ignore_index=True)
        flow_demand_node_ids = set(flow_demand_df["node_id"].astype(int))
        positive_flow_demand_ids = set(
            flow_demand_df.groupby("node_id")["demand"].max().loc[lambda series: series.gt(0.0)].index.astype(int)
        )
        control_link_df = link_df[
            link_df["link_type"].fillna("").eq("control")
            & link_df["from_node_id"].astype(int).isin(flow_demand_node_ids)
            & link_df["to_node_id"].astype(int).map(node_type_by_id).isin(CONTROL_NODE_TYPES)
        ].copy()
        max_demand_by_node_id = flow_demand_df.groupby("node_id")["demand"].max().to_dict()
        for target_node_id, rows in control_link_df.groupby("to_node_id"):
            target_flow_demand_node_ids = sorted({int(node_id) for node_id in rows["from_node_id"]})
            max_demands = [
                float(max_demand_by_node_id[node_id])
                for node_id in target_flow_demand_node_ids
                if pd.notna(max_demand_by_node_id.get(node_id))
            ]
            max_demand = max(max_demands) if max_demands else np.nan
            positive_flow_demand_target = any(
                node_id in positive_flow_demand_ids for node_id in target_flow_demand_node_ids
            )
            if positive_flow_demand_target:
                positive_flow_demand_target_node_ids.add(int(target_node_id))
            flow_demand_controlled_node_ids.add(int(target_node_id))
            flow_demand_target_info[int(target_node_id)] = {
                "flow_demand_inlaat": positive_flow_demand_target,
                "flow_demand_controlled": True,
                "flow_demand_node_ids": ",".join(str(node_id) for node_id in target_flow_demand_node_ids),
                "flow_demand_max_demand": max_demand,
            }

    static_parts = []
    for node_type, table in STATIC_TABLE_BY_NODE_TYPE.items():
        static_df = read_table(con, table)
        static_df["node_type"] = node_type
        static_df["static_table"] = table
        static_parts.append(static_df)
    static_df = pd.concat(static_parts, ignore_index=True)
    static_df["table_fid"] = static_df["fid"]

    static_df = static_df.merge(
        node_df[
            [
                "node_id",
                "node_type",
                "name",
                "meta_waterbeheerder",
                "meta_code_waterbeheerder",
                "meta_node_id_waterbeheerder",
            ]
        ],
        on=["node_id", "node_type"],
        how="inner",
    )
    static_df["flow_demand_inlaat"] = static_df["node_id"].astype(int).isin(positive_flow_demand_target_node_ids)
    static_df["flow_demand_controlled"] = static_df["node_id"].astype(int).isin(flow_demand_controlled_node_ids)
    static_df["flow_demand_node_ids"] = (
        static_df["node_id"]
        .astype(int)
        .map(lambda node_id: flow_demand_target_info.get(int(node_id), {}).get("flow_demand_node_ids"))
    )
    static_df["flow_demand_max_demand"] = normalize_numeric(
        static_df["node_id"]
        .astype(int)
        .map(lambda node_id: flow_demand_target_info.get(int(node_id), {}).get("flow_demand_max_demand"))
    )
    functions = classify_functions(static_df, flow_demand_inlet_nodes=positive_flow_demand_target_node_ids)
    static_df["functie"] = static_df["node_id"].astype(int).map(functions)
    static_df["control_state_lower"] = static_df["control_state"].astype("string").str.lower()

    records = []
    leak_records = []
    outlet_inlet_records = []
    outlet_inlet_node_ids = set()
    for row in static_df.itertuples(index=False):
        node_id = int(row.node_id)
        row_dict = row._asdict()
        control_state = "" if pd.isna(row.control_state_lower) else str(row.control_state_lower).lower()
        flow_demand_inlaat = bool(row.flow_demand_inlaat)
        flow_demand_controlled = bool(row.flow_demand_controlled)
        min_upstream_update_skipped_node = node_id in SKIP_MIN_UPSTREAM_UPDATE_NODE_IDS
        static_capacity = static_row_has_capacity(row_dict)
        active_aanvoer_capacity = control_state == "aanvoer" and static_capacity
        inactive_flow_demand_aanvoer = control_state == "aanvoer" and flow_demand_inlaat and not static_capacity
        upstream_id, upstream_link_id, upstream_error = first_non_junction(
            node_id, incoming_flow_links, node_type_by_id
        )
        downstream_id, downstream_link_id, downstream_error = first_non_junction(
            node_id, outgoing_flow_links, node_type_by_id
        )

        upstream_node_type = node_type_by_id.get(upstream_id)
        downstream_node_type = node_type_by_id.get(downstream_id)
        upstream_authority = basin_authority_by_id.get(upstream_id)
        downstream_authority = basin_authority_by_id.get(downstream_id)
        level_update_skipped_authority = row.meta_waterbeheerder in SKIP_LEVEL_UPDATE_AUTHORITIES
        upstream_streefpeil = streefpeil_by_basin_id.get(upstream_id, np.nan)
        downstream_streefpeil = streefpeil_by_basin_id.get(downstream_id, np.nan)
        upstream_min_profile = min_profile_by_basin_id.get(upstream_id, np.nan)

        max_basin_id, max_error, max_basis = downstream_basin_for_max_level(
            first_downstream_basin_id=downstream_id if downstream_node_type == "Basin" else None
        )
        downstream_basin_has_control, downstream_basin_control_error = downstream_basin_has_direct_control(
            basin_id=max_basin_id,
            outgoing_flow_links=outgoing_flow_links,
            node_type_by_id=node_type_by_id,
        )
        direct_downstream_targets = (
            downstream_targets(int(max_basin_id), outgoing_flow_links, node_type_by_id)
            if max_basin_id is not None
            else []
        )
        direct_downstream_control_node_ids = [
            int(node_id)
            for node_id, _, error in direct_downstream_targets
            if error is None and node_type_by_id.get(node_id) in CONTROL_NODE_TYPES
        ]
        direct_downstream_control_functions = [
            functions.get(node_id, "onbekend") for node_id in direct_downstream_control_node_ids
        ]
        expected_max_downstream = streefpeil_by_basin_id.get(max_basin_id, np.nan)

        expected_min_upstream = np.nan
        min_basis = "niet_gecontroleerd"
        discrete_control_sync_level = np.nan
        if flow_demand_controlled and upstream_node_type == "Basin":
            if upstream_authority == RWS_AUTHORITY and pd.notna(upstream_min_profile):
                discrete_control_sync_level = upstream_min_profile + FLOW_DEMAND_RWS_CONTROL_THRESHOLD_OFFSET
            elif pd.notna(upstream_streefpeil):
                discrete_control_sync_level = upstream_streefpeil + FLOW_DEMAND_CONTROL_THRESHOLD_OFFSET

        if upstream_node_type == "Basin":
            if (
                flow_demand_controlled
                and control_state == "aanvoer"
                and row.functie in ["inlaat", "doorlaat"]
                and pd.notna(upstream_min_profile)
                and upstream_authority == RWS_AUTHORITY
            ):
                expected_min_upstream = upstream_min_profile + FLOW_DEMAND_RWS_PROFILE_MIN_UPSTREAM_OFFSET
                min_basis = "flow_demand_rws_min_profile_level_plus_offset"
            elif flow_demand_controlled and control_state == "aanvoer" and row.functie in ["inlaat", "doorlaat"]:
                expected_min_upstream = (
                    upstream_streefpeil + upstream_supply_offset if pd.notna(upstream_streefpeil) else np.nan
                )
                min_basis = "flow_demand_upstream_streefpeil_plus_aanvoer_offset"
            elif row.functie == "uitlaat":
                expected_min_upstream = upstream_streefpeil
                min_basis = "upstream_streefpeil"
            elif active_aanvoer_capacity and row.functie in ["inlaat", "doorlaat"]:
                expected_min_upstream = (
                    upstream_streefpeil + upstream_supply_offset if pd.notna(upstream_streefpeil) else np.nan
                )
                min_basis = "upstream_streefpeil_plus_aanvoer_offset"
            elif inactive_flow_demand_aanvoer and row.functie in ["inlaat", "doorlaat"]:
                expected_min_upstream = (
                    upstream_streefpeil + upstream_supply_offset if pd.notna(upstream_streefpeil) else np.nan
                )
                min_basis = "flow_demand_upstream_streefpeil_plus_aanvoer_offset"
            elif control_state == "afvoer" and row.functie in ["inlaat", "doorlaat"]:
                expected_min_upstream = upstream_streefpeil
                min_basis = "upstream_streefpeil"

        rws_to_model = (
            upstream_node_type == "Basin"
            and downstream_node_type == "Basin"
            and upstream_authority == RWS_AUTHORITY
            and pd.notna(downstream_authority)
            and downstream_authority != RWS_AUTHORITY
        )
        rws_inlet_profile_update_allowed = (
            rws_to_model
            and row.functie == "inlaat"
            and (control_state == "aanvoer" or flow_demand_inlaat)
            and pd.notna(upstream_min_profile)
            and downstream_authority not in SKIP_LEVEL_UPDATE_AUTHORITIES
            and not level_update_skipped_authority
            and not flow_demand_controlled
            and not min_upstream_update_skipped_node
        )
        rws_inlet_profile_min_upstream = (
            upstream_min_profile + rws_profile_offset if rws_inlet_profile_update_allowed else np.nan
        )
        if rws_inlet_profile_update_allowed:
            expected_min_upstream = rws_inlet_profile_min_upstream
            min_basis = "rijkswaterstaat_min_profile_level_plus_offset"

        if min_upstream_update_skipped_node:
            expected_min_upstream = row.min_upstream_level
            min_basis = "huidige_waarde_node_uitgesloten_van_min_upstream_update"

        current_min_upstream = row.min_upstream_level
        if pd.isna(expected_min_upstream) and pd.notna(current_min_upstream):
            expected_min_upstream = current_min_upstream
            min_basis = "huidige_waarde_gebruikt_omdat_streefpeil_ontbreekt"

        max_report_allowed = (
            active_aanvoer_capacity
            and row.functie in ["inlaat", "doorlaat"]
            and row.node_type in CONTROL_NODE_TYPES
            and max_basin_id is not None
        )
        max_downstream_afwijking = (
            max_report_allowed
            and pd.notna(expected_max_downstream)
            and (
                pd.isna(row.max_downstream_level)
                or not np.isclose(float(row.max_downstream_level), float(expected_max_downstream), atol=tolerance)
            )
        )
        min_upstream_afwijking = (
            upstream_node_type == "Basin"
            and pd.notna(expected_min_upstream)
            and not np.isclose(float(row.min_upstream_level), float(expected_min_upstream), atol=tolerance)
        )
        rws_inlet_profile_min_upstream_afwijking = rws_inlet_profile_update_allowed and (
            pd.isna(row.min_upstream_level)
            or not np.isclose(float(row.min_upstream_level), float(rws_inlet_profile_min_upstream), atol=tolerance)
        )

        record = {
            **row_dict,
            "upstream_link_id": upstream_link_id,
            "upstream_node_id": upstream_id,
            "upstream_node_type": upstream_node_type,
            "upstream_basin_authority": upstream_authority,
            "upstream_basin_name": basin_name_by_id.get(upstream_id),
            "upstream_basin_streefpeil": upstream_streefpeil,
            "upstream_basin_state_level": state_level_by_basin_id.get(upstream_id, np.nan),
            "upstream_basin_min_profile_level": upstream_min_profile,
            "downstream_link_id": downstream_link_id,
            "downstream_node_id": downstream_id,
            "downstream_node_type": downstream_node_type,
            "downstream_basin_authority": downstream_authority,
            "downstream_basin_name": basin_name_by_id.get(downstream_id),
            "downstream_basin_streefpeil": downstream_streefpeil,
            "max_downstream_level_basin_id": max_basin_id,
            "max_downstream_level_basin_authority": basin_authority_by_id.get(max_basin_id),
            "max_downstream_level_basin_error": max_error,
            "max_downstream_level_direct_control": bool(downstream_basin_has_control),
            "max_downstream_level_direct_control_error": downstream_basin_control_error,
            "max_downstream_level_direct_control_node_ids": ",".join(
                str(node_id) for node_id in direct_downstream_control_node_ids
            ),
            "max_downstream_level_direct_control_functions": ",".join(direct_downstream_control_functions),
            "max_downstream_level_check_basis": max_basis,
            "gecheckte_max_downstream_level": expected_max_downstream,
            "verschil_max_downstream_level": row.max_downstream_level - expected_max_downstream,
            "max_downstream_level_afwijking": bool(max_downstream_afwijking),
            "max_downstream_level_update_allowed": bool(
                max_report_allowed
                and row.functie in ["inlaat", "doorlaat"]
                and max_downstream_afwijking
                and not level_update_skipped_authority
                and not flow_demand_controlled
            ),
            "gecheckte_max_downstream_level_update": expected_max_downstream,
            "max_downstream_level_update_basis": max_basis,
            "gecheckte_min_upstream_level": expected_min_upstream,
            "gecheckte_discrete_control_level": discrete_control_sync_level,
            "verschil_min_upstream_level": row.min_upstream_level - expected_min_upstream,
            "min_upstream_level_check_basis": min_basis,
            "min_upstream_level_afwijking": bool(min_upstream_afwijking),
            "rws_to_model": bool(rws_to_model),
            "level_update_skipped_authority": bool(level_update_skipped_authority),
            "min_upstream_update_skipped_node": bool(min_upstream_update_skipped_node),
            "rws_inlet_profile_update_allowed": bool(rws_inlet_profile_update_allowed),
            "rws_inlet_profile_min_upstream": rws_inlet_profile_min_upstream,
            "rws_inlet_profile_min_upstream_afwijking": bool(rws_inlet_profile_min_upstream_afwijking),
            "upstream_check_error": upstream_error,
            "downstream_check_error": downstream_error,
        }
        records.append(record)

        upstream_targets = downstream_targets(node_id, incoming_flow_links, node_type_by_id)
        upstream_basin_targets = [
            (int(upstream_target_id), int(upstream_target_link_id))
            for upstream_target_id, upstream_target_link_id, upstream_target_error in upstream_targets
            if upstream_target_error is None
            and upstream_target_id is not None
            and node_type_by_id.get(upstream_target_id) == "Basin"
        ]
        if (
            row.node_type == "Outlet"
            and functions.get(node_id) == "inlaat"
            and len(upstream_basin_targets) == 1
            and node_id not in outlet_inlet_node_ids
        ):
            upstream_basin_id, upstream_basin_link_id = upstream_basin_targets[0]
            if has_alternative_downstream_route(
                start_node_id=upstream_basin_id,
                blocked_node_id=node_id,
                outgoing_flow_links=outgoing_flow_links,
                node_type_by_id=node_type_by_id,
            ):
                continue

            outlet_inlet_node_ids.add(node_id)
            node_row = node_by_id[node_id]
            outlet_inlet_records.append(
                {
                    "node_id": node_id,
                    "node_type": row.node_type,
                    "functie": functions.get(node_id),
                    "meta_waterbeheerder": node_row.get("meta_waterbeheerder"),
                    "meta_code_waterbeheerder": node_row.get("meta_code_waterbeheerder"),
                    "meta_node_id_waterbeheerder": node_row.get("meta_node_id_waterbeheerder"),
                    "name": node_row.get("name"),
                    "upstream_basin_id": upstream_basin_id,
                    "upstream_link_id": upstream_basin_link_id,
                    "upstream_basin_authority": basin_authority_by_id.get(upstream_basin_id),
                    "upstream_basin_name": basin_name_by_id.get(upstream_basin_id),
                    "upstream_basin_streefpeil": streefpeil_by_basin_id.get(upstream_basin_id, np.nan),
                    "reden": (
                        "Outlet is inlaat met precies een bovenstrooms Basin en dat Basin heeft geen andere "
                        "doorstroomtak."
                    ),
                }
            )

    rows_by_node = dict(tuple(static_df.groupby("node_id")))
    for node_id, rows in rows_by_node.items():
        upstream_id, upstream_link_id, _ = first_non_junction(int(node_id), incoming_flow_links, node_type_by_id)
        downstream_id, downstream_link_id, _ = first_non_junction(int(node_id), outgoing_flow_links, node_type_by_id)
        upstream_authority = basin_authority_by_id.get(upstream_id)
        downstream_authority = basin_authority_by_id.get(downstream_id)
        if not (
            node_type_by_id.get(upstream_id) == "Basin"
            and node_type_by_id.get(downstream_id) == "Basin"
            and upstream_authority == RWS_AUTHORITY
            and pd.notna(downstream_authority)
            and downstream_authority != RWS_AUTHORITY
        ):
            continue
        afvoer_rows = rows[
            rows["control_state"].astype("string").str.lower().eq("afvoer")
            & rows.apply(static_row_has_capacity, axis=1)
        ]
        if afvoer_rows.empty:
            continue
        node_row = node_by_id[int(node_id)]
        leak_records.append(
            {
                "node_id": int(node_id),
                "node_type": node_row.get("node_type"),
                "functie": functions[int(node_id)],
                "meta_waterbeheerder": node_row.get("meta_waterbeheerder"),
                "meta_code_waterbeheerder": node_row.get("meta_code_waterbeheerder"),
                "meta_node_id_waterbeheerder": node_row.get("meta_node_id_waterbeheerder"),
                "name": node_row.get("name"),
                "upstream_link_id": upstream_link_id,
                "upstream_basin_id": upstream_id,
                "upstream_basin_name": basin_name_by_id.get(upstream_id),
                "downstream_link_id": downstream_link_id,
                "downstream_basin_id": downstream_id,
                "downstream_basin_authority": downstream_authority,
                "downstream_basin_name": basin_name_by_id.get(downstream_id),
                "afvoer_rows": capacity_text(afvoer_rows),
            }
        )

    report_df = pd.DataFrame(records)
    deviations_df = report_df[
        report_df["max_downstream_level_afwijking"] | report_df["min_upstream_level_afwijking"]
    ].copy()
    allowed_updates_df = report_df[report_df["rws_inlet_profile_min_upstream_afwijking"]].copy()
    direct_min_upstream_updates_df = report_df[
        report_df["min_upstream_level_afwijking"]
        & report_df["upstream_node_type"].eq("Basin")
        & report_df["gecheckte_min_upstream_level"].notna()
        & ~report_df["rws_inlet_profile_min_upstream_afwijking"]
        & ~report_df["level_update_skipped_authority"]
    ].copy()
    if not direct_min_upstream_updates_df.empty:
        direct_min_upstream_updates_df["direct_min_upstream_level_update_allowed"] = True
        direct_min_upstream_updates_df["direct_min_upstream_level_update_basis"] = "direct_upstream_basin_streefpeil"
    leaks_df = pd.DataFrame(leak_records)
    outlet_inlets_df = pd.DataFrame(outlet_inlet_records)
    return (
        report_df,
        deviations_df,
        allowed_updates_df,
        direct_min_upstream_updates_df,
        leaks_df,
        outlet_inlets_df,
    )


def add_geometry(database_path: Path, df: pd.DataFrame) -> gpd.GeoDataFrame:
    node_gdf = gpd.read_file(database_path, layer="Node", engine="pyogrio", fid_as_index=True).reset_index(
        names="node_id"
    )
    node_gdf = node_gdf[["node_id", "geometry"]]
    if df.empty:
        return gpd.GeoDataFrame(df, geometry=[], crs=node_gdf.crs)
    gdf = node_gdf.merge(df, on="node_id", how="inner")
    return gpd.GeoDataFrame(gdf, geometry="geometry", crs=node_gdf.crs)


def prepare_gpkg_layer(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Avoid writing source fid values as the GeoPackage feature id."""
    gdf = gdf.copy()
    if "fid" in gdf.columns:
        gdf = gdf.rename(columns={"fid": "source_fid"})
    return gdf.reset_index(drop=True)


def write_report_gpkg(
    database_path: Path,
    output_gpkg: Path,
    deviations_df: pd.DataFrame,
    allowed_updates_df: pd.DataFrame,
    direct_min_upstream_updates_df: pd.DataFrame,
    leaks_df: pd.DataFrame,
    outlet_inlets_df: pd.DataFrame,
) -> Path:
    output_gpkg = resolve_writable_gpkg(output_gpkg, "Rapport-GPKG")

    deviations_gdf = add_geometry(database_path, deviations_df)
    max_gdf = deviations_gdf[deviations_gdf["max_downstream_level_afwijking"].fillna(False)].copy()
    min_gdf = deviations_gdf[deviations_gdf["min_upstream_level_afwijking"].fillna(False)].copy()
    allowed_updates_gdf = add_geometry(database_path, allowed_updates_df)
    direct_min_upstream_updates_gdf = add_geometry(database_path, direct_min_upstream_updates_df)
    leaks_gdf = add_geometry(database_path, leaks_df)
    outlet_inlets_gdf = add_geometry(database_path, outlet_inlets_df)

    prepare_gpkg_layer(deviations_gdf).to_file(output_gpkg, layer="level_afwijkingen", driver="GPKG")
    prepare_gpkg_layer(max_gdf).to_file(output_gpkg, layer="max_downstream_afwijkingen", driver="GPKG")
    prepare_gpkg_layer(min_gdf).to_file(output_gpkg, layer="min_upstream_afwijkingen", driver="GPKG")
    prepare_gpkg_layer(allowed_updates_gdf).to_file(output_gpkg, layer="toegestane_rws_inlaat_updates", driver="GPKG")
    prepare_gpkg_layer(direct_min_upstream_updates_gdf).to_file(
        output_gpkg, layer="toegestane_directe_min_upstream_updates", driver="GPKG"
    )
    prepare_gpkg_layer(leaks_gdf).to_file(output_gpkg, layer="verdachte_rws_lekken", driver="GPKG")
    prepare_gpkg_layer(outlet_inlets_gdf).to_file(output_gpkg, layer="verdachte_outlet_als_inlaat", driver="GPKG")
    return output_gpkg


def write_suspicious_gpkg(
    database_path: Path,
    output_gpkg: Path,
    leaks_df: pd.DataFrame,
    outlet_inlets_df: pd.DataFrame,
) -> Path:
    output_gpkg = resolve_writable_gpkg(output_gpkg, "Verdachte-punten-GPKG")

    prepare_gpkg_layer(add_geometry(database_path, leaks_df)).to_file(
        output_gpkg, layer="verdachte_rws_lekken", driver="GPKG"
    )
    prepare_gpkg_layer(add_geometry(database_path, outlet_inlets_df)).to_file(
        output_gpkg, layer="verdachte_outlet_als_inlaat", driver="GPKG"
    )
    return output_gpkg


def backup_database(database_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = database_path.with_name(f"{database_path.stem}_before_report_coupling_levels_{timestamp}.gpkg")
    shutil.copy2(database_path, backup_path)
    return backup_path


def apply_level_updates(
    database_path: Path,
    min_upstream_updates_df: pd.DataFrame,
    direct_min_upstream_updates_df: pd.DataFrame,
    max_downstream_updates_df: pd.DataFrame,
) -> tuple[Path | None, int, int, int]:
    if min_upstream_updates_df.empty and direct_min_upstream_updates_df.empty and max_downstream_updates_df.empty:
        return None, 0, 0, 0

    backup_path = backup_database(database_path)
    con = sqlite3.connect(database_path)
    min_update_count = 0
    max_update_count = 0
    condition_update_count = 0
    synced_conditions: set[tuple[int, int, float]] = set()

    def update_discrete_control_conditions(
        target_node_id: int,
        listen_node_id: int | None,
        level_value: float,
    ) -> int:
        if listen_node_id is None:
            return 0

        control_node_ids = [
            int(row[0])
            for row in con.execute(
                'SELECT from_node_id FROM "Link" WHERE link_type = "control" AND to_node_id = ?',
                (int(target_node_id),),
            ).fetchall()
        ]
        if not control_node_ids:
            return 0

        update_count = 0
        for control_node_id in control_node_ids:
            variable_rows = con.execute(
                (
                    'SELECT compound_variable_id, weight FROM "DiscreteControl / variable" '
                    "WHERE node_id = ? AND listen_node_id = ?"
                ),
                (control_node_id, int(listen_node_id)),
            ).fetchall()
            seen_compound_variable_ids = set()
            for compound_variable_id, weight in variable_rows:
                compound_variable_id = int(compound_variable_id)
                if compound_variable_id in seen_compound_variable_ids:
                    continue
                seen_compound_variable_ids.add(compound_variable_id)

                threshold_value = float(level_value) * float(weight)
                condition_rows = con.execute(
                    (
                        'SELECT fid, threshold_high, threshold_low FROM "DiscreteControl / condition" '
                        "WHERE node_id = ? AND compound_variable_id = ? ORDER BY condition_id"
                    ),
                    (control_node_id, compound_variable_id),
                ).fetchall()
                if not condition_rows:
                    continue

                base_high = condition_rows[0][1]
                base_low = condition_rows[0][2]
                for fid, threshold_high, threshold_low in condition_rows:
                    high_offset = (
                        0.0 if base_high is None or threshold_high is None else float(threshold_high) - float(base_high)
                    )
                    low_offset = (
                        0.0 if base_low is None or threshold_low is None else float(threshold_low) - float(base_low)
                    )
                    con.execute(
                        (
                            'UPDATE "DiscreteControl / condition" SET threshold_high = ?, threshold_low = ? '
                            "WHERE fid = ?"
                        ),
                        (threshold_value + high_offset, threshold_value + low_offset, int(fid)),
                    )
                    update_count += 1

        return update_count

    def sync_once(target_node_id: int, listen_node_id: int | None, level_value: float) -> None:
        nonlocal condition_update_count
        if listen_node_id is None or pd.isna(level_value):
            return
        key = (int(target_node_id), int(listen_node_id), round(float(level_value), 9))
        if key in synced_conditions:
            return
        synced_conditions.add(key)
        condition_update_count += update_discrete_control_conditions(
            target_node_id=int(target_node_id),
            listen_node_id=int(listen_node_id),
            level_value=float(level_value),
        )

    for row in min_upstream_updates_df.itertuples(index=False):
        table = row.static_table
        fid = int(row.table_fid)
        node_id = int(row.node_id)
        value = float(row.rws_inlet_profile_min_upstream)
        con.execute(f'UPDATE "{table}" SET min_upstream_level = ? WHERE fid = ?', (value, fid))  # noqa: S608
        if pd.notna(row.upstream_node_id) and pd.notna(row.upstream_basin_streefpeil):
            sync_once(
                target_node_id=node_id,
                listen_node_id=int(row.upstream_node_id),
                level_value=float(row.upstream_basin_streefpeil),
            )
        min_update_count += 1

    for row in direct_min_upstream_updates_df.itertuples(index=False):
        table = row.static_table
        fid = int(row.table_fid)
        node_id = int(row.node_id)
        value = float(row.gecheckte_min_upstream_level)
        con.execute(f'UPDATE "{table}" SET min_upstream_level = ? WHERE fid = ?', (value, fid))  # noqa: S608
        control_value = (
            float(row.gecheckte_discrete_control_level)
            if pd.notna(getattr(row, "gecheckte_discrete_control_level", np.nan))
            else float(row.upstream_basin_streefpeil)
            if pd.notna(row.upstream_basin_streefpeil)
            else value
        )
        sync_once(
            target_node_id=node_id,
            listen_node_id=int(row.upstream_node_id) if pd.notna(row.upstream_node_id) else None,
            level_value=control_value,
        )
        min_update_count += 1

    for row in max_downstream_updates_df.itertuples(index=False):
        table = row.static_table
        fid = int(row.table_fid)
        node_id = int(row.node_id)
        value = float(row.gecheckte_max_downstream_level_update)
        con.execute(f'UPDATE "{table}" SET max_downstream_level = ? WHERE fid = ?', (value, fid))  # noqa: S608
        sync_once(
            target_node_id=node_id,
            listen_node_id=int(row.downstream_node_id) if pd.notna(row.downstream_node_id) else None,
            level_value=value,
        )
        max_update_count += 1

    con.commit()
    con.close()
    return backup_path, min_update_count, max_update_count, condition_update_count


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rapporteer coupling-level afwijkingen en verdachte RWS-lekken. Schrijft standaard alleen GPKG-rapportage."
        )
    )
    parser.add_argument("--toml-file", type=Path, required=True, help="Pad naar Ribasim TOML-model.")
    parser.add_argument(
        "--output-gpkg",
        type=Path,
        default=None,
        help="Output-GPKG. Relatief pad wordt naast de TOML geplaatst.",
    )
    parser.add_argument(
        "--verdachte-output-gpkg",
        type=Path,
        default=None,
        help="Apart GPKG met alleen verdachte punten. Relatief pad wordt naast de TOML geplaatst.",
    )
    parser.add_argument("--tolerance", type=float, default=1e-6)
    parser.add_argument("--upstream-supply-offset", type=float, default=DEFAULT_UPSTREAM_SUPPLY_OFFSET)
    parser.add_argument("--rws-profile-offset", type=float, default=DEFAULT_RWS_PROFILE_OFFSET)
    parser.add_argument(
        "--apply-rws-inlet-min-upstream",
        action="store_true",
        help=(
            "Pas alleen min_upstream_level aan voor RWS -> regionaal model inlaten "
            "op basis van min(Basin / profile.level) + rws-profile-offset. "
            "FlowDemand-gestuurde Outlet/Pump-nodes krijgen specifieke doorlaatregels."
        ),
    )
    parser.add_argument(
        "--apply-max-downstream-level",
        action="store_true",
        help=(
            "Pas max_downstream_level aan voor aanvoer-rijen van inlaten en doorlaten op basis van "
            "het directe downstream Basin. FlowDemand-gestuurde nodes worden hierbij overgeslagen."
        ),
    )
    parser.add_argument(
        "--apply-direct-min-upstream-level",
        action="store_true",
        help=(
            "Pas min_upstream_level aan voor alle afwijkende rijen met een direct upstream Basin. "
            "Er wordt niet via ManningResistance/Junction doorgelopen."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_path = database_gpkg_path(args.toml_file)
    output_gpkg = resolve_output_gpkg(args.toml_file, args.output_gpkg)
    verdachte_output_gpkg = (
        resolve_output_gpkg(args.toml_file, args.verdachte_output_gpkg)
        if args.verdachte_output_gpkg is not None
        else None
    )

    (
        _report_df,
        deviations_df,
        allowed_updates_df,
        direct_min_upstream_updates_df,
        leaks_df,
        outlet_inlets_df,
    ) = build_report(
        database_path=database_path,
        upstream_supply_offset=args.upstream_supply_offset,
        rws_profile_offset=args.rws_profile_offset,
        tolerance=args.tolerance,
    )
    output_gpkg = write_report_gpkg(
        database_path=database_path,
        output_gpkg=output_gpkg,
        deviations_df=deviations_df,
        allowed_updates_df=allowed_updates_df,
        direct_min_upstream_updates_df=direct_min_upstream_updates_df,
        leaks_df=leaks_df,
        outlet_inlets_df=outlet_inlets_df,
    )
    if verdachte_output_gpkg is not None:
        verdachte_output_gpkg = write_suspicious_gpkg(
            database_path=database_path,
            output_gpkg=verdachte_output_gpkg,
            leaks_df=leaks_df,
            outlet_inlets_df=outlet_inlets_df,
        )

    max_downstream_updates_df = deviations_df[deviations_df["max_downstream_level_update_allowed"].fillna(False)].copy()
    backup_path = None
    min_update_count = 0
    max_update_count = 0
    condition_update_count = 0
    apply_direct_min_upstream = args.apply_direct_min_upstream_level or args.apply_max_downstream_level
    if args.apply_rws_inlet_min_upstream or apply_direct_min_upstream or args.apply_max_downstream_level:
        backup_path, min_update_count, max_update_count, condition_update_count = apply_level_updates(
            database_path=database_path,
            min_upstream_updates_df=allowed_updates_df
            if args.apply_rws_inlet_min_upstream
            else allowed_updates_df.iloc[0:0],
            direct_min_upstream_updates_df=(
                direct_min_upstream_updates_df
                if apply_direct_min_upstream
                else direct_min_upstream_updates_df.iloc[0:0]
            ),
            max_downstream_updates_df=(
                max_downstream_updates_df if args.apply_max_downstream_level else max_downstream_updates_df.iloc[0:0]
            ),
        )

    print(f"Model-TOML: {args.toml_file}")
    print(f"Model-GPKG: {database_path}")
    print(f"Rapport-GPKG: {output_gpkg}")
    if verdachte_output_gpkg is not None:
        print(f"Verdachte-punten-GPKG: {verdachte_output_gpkg}")
    print(f"Level-afwijkingen: {len(deviations_df)}")
    print(f"Toegestane RWS-inlaat min_upstream updates: {len(allowed_updates_df)}")
    print(f"Toegestane directe min_upstream updates: {len(direct_min_upstream_updates_df)}")
    print(f"Toegestane inlaat/doorlaat max_downstream updates: {len(max_downstream_updates_df)}")
    print(f"Verdachte RWS-lekken: {len(leaks_df)}")
    print(f"Verdachte Outlet-inlaten met enkel bovenstrooms Basin: {len(outlet_inlets_df)}")
    if backup_path is not None:
        print(f"Backup database: {backup_path}")
        print(f"Toegepaste min_upstream_level updates: {min_update_count}")
        print(f"Toegepaste max_downstream_level updates: {max_update_count}")
        print(f"Gesynchroniseerde DiscreteControl-condition thresholds: {condition_update_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
