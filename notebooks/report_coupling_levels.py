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
DEFAULT_UPSTREAM_SUPPLY_OFFSET = -0.04
DEFAULT_RWS_PROFILE_OFFSET = 0.1


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
        current_node_id = int(next_node_id)
        seen_node_ids = {int(node_id), current_node_id}
        error = None

        for _ in range(max_iter):
            next_node_type = node_type_by_id.get(current_node_id)
            if next_node_type != "Junction":
                targets.append((current_node_id, int(first_link_id), None))
                break

            downstream_links = outgoing_flow_links.get(current_node_id, [])
            if len(downstream_links) == 0:
                error = "geen downstream flow-link na Junction"
                break
            if len(downstream_links) > 1:
                error = f"meerdere downstream flow-links na Junction: {downstream_links}"
                break

            _, current_node_id = downstream_links[0]
            current_node_id = int(current_node_id)
            if current_node_id in seen_node_ids:
                error = f"cyclus via node {current_node_id}"
                break
            seen_node_ids.add(current_node_id)
        else:
            error = f"geen downstream niet-Junction node binnen {max_iter} stappen"

        if error is not None:
            targets.append((None, int(first_link_id), error))

    return targets


def downstream_basin_for_max_level(
    first_downstream_basin_id: int | None,
    outgoing_flow_links: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
    follow_manning_until_control: bool,
    max_iter: int = 50,
) -> tuple[int | None, int | None, str | None, str]:
    if first_downstream_basin_id is None:
        return None, None, "geen eerste downstream Basin", "geen_downstream_basin"

    current_basin_id = int(first_downstream_basin_id)
    if not follow_manning_until_control:
        targets = downstream_targets(current_basin_id, outgoing_flow_links, node_type_by_id)
        target_errors = [error for _, _, error in targets if error is not None]
        if target_errors:
            return current_basin_id, None, "; ".join(target_errors), "downstream_streefpeil"

        manning_targets = [
            (node_id, link_id) for node_id, link_id, _ in targets if node_type_by_id.get(node_id) == "ManningResistance"
        ]
        if len(manning_targets) > 1:
            return (
                current_basin_id,
                None,
                f"meerdere downstream ManningResistance-nodes: {manning_targets}",
                ("downstream_streefpeil"),
            )
        manning_link_id = int(manning_targets[0][1]) if manning_targets else None
        basis = (
            "downstream_streefpeil_manning_niet_doorlopen" if manning_link_id is not None else "downstream_streefpeil"
        )
        return current_basin_id, manning_link_id, None, basis

    seen_basin_ids = {current_basin_id}
    first_manning_link_id = None
    for _ in range(max_iter):
        targets = downstream_targets(current_basin_id, outgoing_flow_links, node_type_by_id)
        target_errors = [error for _, _, error in targets if error is not None]
        if target_errors:
            return current_basin_id, first_manning_link_id, "; ".join(target_errors), "downstream_streefpeil"

        control_targets = [
            (node_id, link_id) for node_id, link_id, _ in targets if node_type_by_id.get(node_id) in CONTROL_NODE_TYPES
        ]
        if control_targets:
            basis = (
                "downstream_streefpeil"
                if first_manning_link_id is None
                else "downstream_streefpeil_na_manning_tot_basin_met_outlet_pump"
            )
            return current_basin_id, first_manning_link_id, None, basis

        manning_targets = [
            (node_id, link_id) for node_id, link_id, _ in targets if node_type_by_id.get(node_id) == "ManningResistance"
        ]
        if not manning_targets:
            basis = (
                "downstream_streefpeil"
                if first_manning_link_id is None
                else "downstream_streefpeil_na_manning_tot_basin_zonder_manning"
            )
            return current_basin_id, first_manning_link_id, None, basis
        if len(manning_targets) > 1:
            return (
                current_basin_id,
                first_manning_link_id,
                (f"meerdere downstream ManningResistance-nodes: {manning_targets}"),
                "downstream_streefpeil",
            )

        manning_node_id, manning_link_id = manning_targets[0]
        if first_manning_link_id is None:
            first_manning_link_id = int(manning_link_id)

        next_basin_id, _, error = first_non_junction(int(manning_node_id), outgoing_flow_links, node_type_by_id)
        if error is not None:
            return current_basin_id, first_manning_link_id, error, "downstream_streefpeil"
        if node_type_by_id.get(next_basin_id) != "Basin":
            return (
                current_basin_id,
                first_manning_link_id,
                (f"downstream van ManningResistance is geen Basin maar {node_type_by_id.get(next_basin_id)}"),
                "downstream_streefpeil",
            )

        current_basin_id = int(next_basin_id)
        if current_basin_id in seen_basin_ids:
            return (
                current_basin_id,
                first_manning_link_id,
                f"cyclus via Basin {current_basin_id}",
                "downstream_streefpeil",
            )
        seen_basin_ids.add(current_basin_id)

    return (
        current_basin_id,
        first_manning_link_id,
        (f"geen max_downstream_level Basin gevonden binnen {max_iter} stappen"),
        "downstream_streefpeil",
    )


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
    if table_exists(con, "FlowDemand / time"):
        flow_demand_time_df = read_table(con, "FlowDemand / time")
        flow_demand_time_df["demand"] = normalize_numeric(flow_demand_time_df["demand"])
        positive_flow_demand_ids = set(
            flow_demand_time_df.groupby("node_id")["demand"].max().loc[lambda series: series.gt(0.0)].index.astype(int)
        )
        control_link_df = link_df[
            link_df["link_type"].fillna("").eq("control")
            & link_df["from_node_id"].astype(int).isin(positive_flow_demand_ids)
            & link_df["to_node_id"].astype(int).map(node_type_by_id).isin(CONTROL_NODE_TYPES)
        ].copy()
        max_demand_by_node_id = flow_demand_time_df.groupby("node_id")["demand"].max().to_dict()
        for target_node_id, rows in control_link_df.groupby("to_node_id"):
            flow_demand_node_ids = sorted({int(node_id) for node_id in rows["from_node_id"]})
            flow_demand_target_info[int(target_node_id)] = {
                "flow_demand_inlaat": True,
                "flow_demand_node_ids": ",".join(str(node_id) for node_id in flow_demand_node_ids),
                "flow_demand_max_demand": max(
                    max_demand_by_node_id.get(node_id, np.nan) for node_id in flow_demand_node_ids
                ),
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
    static_df["flow_demand_inlaat"] = static_df["node_id"].astype(int).isin(flow_demand_target_info)
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
    functions = classify_functions(static_df, flow_demand_inlet_nodes=set(flow_demand_target_info))
    static_df["functie"] = static_df["node_id"].astype(int).map(functions)
    static_df["control_state_lower"] = static_df["control_state"].astype("string").str.lower()

    records = []
    leak_records = []
    for row in static_df.itertuples(index=False):
        node_id = int(row.node_id)
        row_dict = row._asdict()
        control_state = "" if pd.isna(row.control_state_lower) else str(row.control_state_lower).lower()
        row_has_capacity = positive(row.flow_rate) or positive(row.max_flow_rate)
        flow_demand_inlaat = bool(row.flow_demand_inlaat)
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
        upstream_streefpeil = streefpeil_by_basin_id.get(upstream_id, np.nan)
        downstream_streefpeil = streefpeil_by_basin_id.get(downstream_id, np.nan)
        upstream_min_profile = min_profile_by_basin_id.get(upstream_id, np.nan)

        max_basin_id, manning_link_id, max_error, max_basis = downstream_basin_for_max_level(
            first_downstream_basin_id=downstream_id if downstream_node_type == "Basin" else None,
            outgoing_flow_links=outgoing_flow_links,
            node_type_by_id=node_type_by_id,
            follow_manning_until_control=False,
        )
        downstream_basin_has_control, downstream_basin_control_error = downstream_basin_has_direct_control(
            basin_id=max_basin_id,
            outgoing_flow_links=outgoing_flow_links,
            node_type_by_id=node_type_by_id,
        )
        expected_max_downstream = streefpeil_by_basin_id.get(max_basin_id, np.nan)

        expected_min_upstream = np.nan
        min_basis = "niet_gecontroleerd"
        if upstream_node_type == "Basin":
            if control_state == "aanvoer" and row.functie in ["inlaat", "doorlaat"] and row_has_capacity:
                expected_min_upstream = (
                    upstream_streefpeil + upstream_supply_offset if pd.notna(upstream_streefpeil) else np.nan
                )
                min_basis = "upstream_streefpeil_plus_aanvoer_offset"
            elif control_state == "afvoer" and row.functie in ["uitlaat", "doorlaat"] and row_has_capacity:
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
        )
        rws_inlet_profile_min_upstream = (
            upstream_min_profile + rws_profile_offset if rws_inlet_profile_update_allowed else np.nan
        )
        if rws_inlet_profile_update_allowed:
            expected_min_upstream = rws_inlet_profile_min_upstream
            min_basis = "rijkswaterstaat_min_profile_level_plus_offset"

        current_min_upstream = row.min_upstream_level
        if pd.isna(expected_min_upstream) and pd.notna(current_min_upstream):
            expected_min_upstream = current_min_upstream
            min_basis = "huidige_waarde_gebruikt_omdat_streefpeil_ontbreekt"

        max_allowed = (
            control_state == "aanvoer"
            and row.functie == "inlaat"
            and row.node_type in CONTROL_NODE_TYPES
            and downstream_basin_has_control
        )
        max_downstream_afwijking = (
            max_allowed
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
            "max_downstream_level_manning_link_id": manning_link_id,
            "max_downstream_level_basin_error": max_error,
            "max_downstream_level_direct_control": bool(downstream_basin_has_control),
            "max_downstream_level_direct_control_error": downstream_basin_control_error,
            "max_downstream_level_check_basis": max_basis,
            "gecheckte_max_downstream_level": expected_max_downstream,
            "verschil_max_downstream_level": row.max_downstream_level - expected_max_downstream,
            "max_downstream_level_afwijking": bool(max_downstream_afwijking),
            "gecheckte_min_upstream_level": expected_min_upstream,
            "verschil_min_upstream_level": row.min_upstream_level - expected_min_upstream,
            "min_upstream_level_check_basis": min_basis,
            "min_upstream_level_afwijking": bool(min_upstream_afwijking),
            "rws_to_model": bool(rws_to_model),
            "rws_inlet_profile_update_allowed": bool(rws_inlet_profile_update_allowed),
            "rws_inlet_profile_min_upstream": rws_inlet_profile_min_upstream,
            "rws_inlet_profile_min_upstream_afwijking": bool(rws_inlet_profile_min_upstream_afwijking),
            "upstream_check_error": upstream_error,
            "downstream_check_error": downstream_error,
        }
        records.append(record)

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
    leaks_df = pd.DataFrame(leak_records)
    return report_df, deviations_df, allowed_updates_df, leaks_df


def add_geometry(database_path: Path, df: pd.DataFrame) -> gpd.GeoDataFrame:
    node_gdf = gpd.read_file(database_path, layer="Node", engine="pyogrio", fid_as_index=True).reset_index(
        names="node_id"
    )
    node_gdf = node_gdf[["node_id", "geometry"]]
    if df.empty:
        return gpd.GeoDataFrame(df, geometry=[], crs=node_gdf.crs)
    gdf = node_gdf.merge(df, on="node_id", how="inner")
    return gpd.GeoDataFrame(gdf, geometry="geometry", crs=node_gdf.crs)


def write_report_gpkg(
    database_path: Path,
    output_gpkg: Path,
    deviations_df: pd.DataFrame,
    allowed_updates_df: pd.DataFrame,
    leaks_df: pd.DataFrame,
) -> None:
    if output_gpkg.exists():
        output_gpkg.unlink()

    deviations_gdf = add_geometry(database_path, deviations_df)
    max_gdf = deviations_gdf[deviations_gdf["max_downstream_level_afwijking"].fillna(False)].copy()
    min_gdf = deviations_gdf[deviations_gdf["min_upstream_level_afwijking"].fillna(False)].copy()
    allowed_updates_gdf = add_geometry(database_path, allowed_updates_df)
    leaks_gdf = add_geometry(database_path, leaks_df)

    deviations_gdf.to_file(output_gpkg, layer="level_afwijkingen", driver="GPKG")
    max_gdf.to_file(output_gpkg, layer="max_downstream_afwijkingen", driver="GPKG")
    min_gdf.to_file(output_gpkg, layer="min_upstream_afwijkingen", driver="GPKG")
    allowed_updates_gdf.to_file(output_gpkg, layer="toegestane_rws_inlaat_updates", driver="GPKG")
    leaks_gdf.to_file(output_gpkg, layer="verdachte_rws_lekken", driver="GPKG")


def backup_database(database_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = database_path.with_name(f"{database_path.stem}_before_report_coupling_levels_{timestamp}.gpkg")
    shutil.copy2(database_path, backup_path)
    return backup_path


def apply_level_updates(
    database_path: Path,
    min_upstream_updates_df: pd.DataFrame,
    max_downstream_updates_df: pd.DataFrame,
) -> tuple[Path | None, int, int]:
    if min_upstream_updates_df.empty and max_downstream_updates_df.empty:
        return None, 0, 0

    backup_path = backup_database(database_path)
    con = sqlite3.connect(database_path)
    min_update_count = 0
    max_update_count = 0

    for row in min_upstream_updates_df.itertuples(index=False):
        table = row.static_table
        fid = int(row.table_fid)
        value = float(row.rws_inlet_profile_min_upstream)
        con.execute(f'UPDATE "{table}" SET min_upstream_level = ? WHERE fid = ?', (value, fid))  # noqa: S608
        min_update_count += 1

    for row in max_downstream_updates_df.itertuples(index=False):
        table = row.static_table
        fid = int(row.table_fid)
        value = float(row.gecheckte_max_downstream_level)
        con.execute(f'UPDATE "{table}" SET max_downstream_level = ? WHERE fid = ?', (value, fid))  # noqa: S608
        max_update_count += 1

    con.commit()
    con.close()
    return backup_path, min_update_count, max_update_count


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
    parser.add_argument("--tolerance", type=float, default=1e-6)
    parser.add_argument("--upstream-supply-offset", type=float, default=DEFAULT_UPSTREAM_SUPPLY_OFFSET)
    parser.add_argument("--rws-profile-offset", type=float, default=DEFAULT_RWS_PROFILE_OFFSET)
    parser.add_argument(
        "--apply-rws-inlet-min-upstream",
        action="store_true",
        help=(
            "Pas alleen min_upstream_level aan voor RWS -> regionaal model inlaten "
            "op basis van min(Basin / profile.level) + rws-profile-offset. "
            "FlowDemand-gestuurde inlaten tellen hierbij mee."
        ),
    )
    parser.add_argument(
        "--apply-max-downstream-level",
        action="store_true",
        help=(
            "Pas max_downstream_level aan voor aanvoer-rijen van inlaten als het "
            "directe downstream Basin naar een Outlet/Pump gaat. ManningResistance-routes "
            "en doorlaten worden niet aangepast."
        ),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_path = database_gpkg_path(args.toml_file)
    output_gpkg = resolve_output_gpkg(args.toml_file, args.output_gpkg)

    _, deviations_df, allowed_updates_df, leaks_df = build_report(
        database_path=database_path,
        upstream_supply_offset=args.upstream_supply_offset,
        rws_profile_offset=args.rws_profile_offset,
        tolerance=args.tolerance,
    )
    write_report_gpkg(
        database_path=database_path,
        output_gpkg=output_gpkg,
        deviations_df=deviations_df,
        allowed_updates_df=allowed_updates_df,
        leaks_df=leaks_df,
    )

    max_downstream_updates_df = deviations_df[deviations_df["max_downstream_level_afwijking"].fillna(False)].copy()
    backup_path = None
    min_update_count = 0
    max_update_count = 0
    if args.apply_rws_inlet_min_upstream or args.apply_max_downstream_level:
        backup_path, min_update_count, max_update_count = apply_level_updates(
            database_path=database_path,
            min_upstream_updates_df=allowed_updates_df
            if args.apply_rws_inlet_min_upstream
            else allowed_updates_df.iloc[0:0],
            max_downstream_updates_df=(
                max_downstream_updates_df if args.apply_max_downstream_level else max_downstream_updates_df.iloc[0:0]
            ),
        )

    print(f"Model-TOML: {args.toml_file}")
    print(f"Model-GPKG: {database_path}")
    print(f"Rapport-GPKG: {output_gpkg}")
    print(f"Level-afwijkingen: {len(deviations_df)}")
    print(f"Toegestane RWS-inlaat min_upstream updates: {len(allowed_updates_df)}")
    print(f"Toegestane inlaat max_downstream updates: {len(max_downstream_updates_df)}")
    print(f"Verdachte RWS-lekken: {len(leaks_df)}")
    if backup_path is not None:
        print(f"Backup database: {backup_path}")
        print(f"Toegepaste min_upstream_level updates: {min_update_count}")
        print(f"Toegepaste max_downstream_level updates: {max_update_count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
