"""Report and apply coupling-level corrections for coupled Ribasim models."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ribasim_nl import Model
from ribasim_nl.coupling_level_apply import apply_level_updates
from ribasim_nl.coupling_level_common import (
    CONTROL_NODE_TYPES,
    LEVEL_UPDATE_PROTECTION_COLUMN,
    LIMBURG_AUTHORITY,
    RWS_AUTHORITY,
    SKIP_LEVEL_UPDATE_AUTHORITIES,
    SKIP_LEVEL_UPDATE_NODE_IDS,
    SKIP_MIN_UPSTREAM_UPDATE_NODE_IDS,
    STATIC_TABLE_BY_NODE_TYPE,
    as_float,
    as_int,
    classify_functions,
    database_gpkg_path,
    is_missing,
    is_present,
    normalize_numeric,
    reset_index_to_column,
    resolve_output_gpkg,
    static_row_has_capacity,
    truthy,
)
from ribasim_nl.coupling_level_controls import protected_controller_threshold_updates
from ribasim_nl.coupling_level_network import (
    downstream_basin_for_max_level,
    downstream_basin_has_direct_control,
    downstream_targets,
    first_non_junction,
    flow_link_graphs,
    has_alternative_downstream_route,
)
from ribasim_nl.coupling_level_reporting import (
    not_applied_level_deviations,
    write_report_gpkg,
    write_suspicious_gpkg,
)


@dataclass(frozen=True)
class CouplingLevelSettings:
    upstream_supply_offset: float
    rws_profile_offset: float
    apply_rws_inlet_min_upstream: bool
    apply_max_downstream_level: bool
    apply_direct_min_upstream_level: bool
    manning_n: float | None = None


@dataclass(frozen=True)
class CouplingReportContext:
    node_by_id: dict[int, dict[str, object]]
    node_type_by_id: dict[int, str]
    basin_authority_by_id: dict[int, object]
    basin_name_by_id: dict[int, object]
    streefpeil_by_basin_id: pd.Series
    state_level_by_basin_id: pd.Series
    min_profile_by_basin_id: pd.Series
    outgoing_flow_links: dict[int, list[tuple[int, int]]]
    incoming_flow_links: dict[int, list[tuple[int, int]]]
    static_df: pd.DataFrame
    functions: dict[int, str]


def capacity_text(rows: pd.DataFrame) -> str:
    parts = []
    for row in rows.itertuples(index=False):
        values = []
        for column in ["flow_rate", "min_flow_rate", "max_flow_rate"]:
            value = getattr(row, column, None)
            if is_present(value):
                values.append(f"{column}={as_float(value):g}")
        parts.append(f"{getattr(row, 'control_state', None)} ({', '.join(values)})")
    return "; ".join(parts)


def flow_demand_targets(
    model: Model,
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> tuple[dict[int, dict[str, object]], set[int], set[int]]:
    target_info: dict[int, dict[str, object]] = {}
    positive_target_node_ids: set[int] = set()
    controlled_node_ids: set[int] = set()
    flow_demand_parts = []

    for flow_demand_table in [model.flow_demand.time.df, model.flow_demand.static.df]:
        if flow_demand_table is None or flow_demand_table.empty or "demand" not in flow_demand_table.columns:
            continue
        flow_demand_df = flow_demand_table.copy()
        flow_demand_df["demand"] = normalize_numeric(flow_demand_df["demand"])
        flow_demand_parts.append(flow_demand_df[["node_id", "demand"]])

    if not flow_demand_parts:
        return target_info, positive_target_node_ids, controlled_node_ids

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
    max_demand_by_node_id = {
        as_int(node_id): demand
        for node_id, demand in flow_demand_df.groupby("node_id")["demand"].max().to_dict().items()
    }

    for target_node_id, rows in control_link_df.groupby("to_node_id"):
        target_node_id_int = as_int(target_node_id)
        target_flow_demand_node_ids = sorted({as_int(node_id) for node_id in rows["from_node_id"]})
        max_demands = [
            as_float(max_demand_by_node_id[node_id])
            for node_id in target_flow_demand_node_ids
            if is_present(max_demand_by_node_id.get(node_id))
        ]
        positive_flow_demand_target = any(
            node_id in positive_flow_demand_ids for node_id in target_flow_demand_node_ids
        )
        if positive_flow_demand_target:
            positive_target_node_ids.add(target_node_id_int)
        controlled_node_ids.add(target_node_id_int)
        target_info[target_node_id_int] = {
            "flow_demand_inlaat": positive_flow_demand_target,
            "flow_demand_controlled": True,
            "flow_demand_node_ids": ",".join(str(node_id) for node_id in target_flow_demand_node_ids),
            "flow_demand_max_demand": max(max_demands) if max_demands else np.nan,
        }

    return target_info, positive_target_node_ids, controlled_node_ids


def read_control_static_tables(
    model: Model,
    node_df: pd.DataFrame,
    flow_demand_target_info: dict[int, dict[str, object]],
    positive_flow_demand_target_node_ids: set[int],
    flow_demand_controlled_node_ids: set[int],
) -> pd.DataFrame:
    static_parts = []
    for node_type, table in STATIC_TABLE_BY_NODE_TYPE.items():
        static_table = model.get_component(node_type).static.df
        if static_table is None:
            continue
        static_df = reset_index_to_column(static_table.copy(), "table_fid")
        static_df["node_type"] = node_type
        static_df["static_table"] = table
        static_parts.append(static_df)

    if not static_parts:
        return pd.DataFrame()

    static_df = pd.concat(static_parts, ignore_index=True)
    if LEVEL_UPDATE_PROTECTION_COLUMN not in static_df.columns:
        static_df[LEVEL_UPDATE_PROTECTION_COLUMN] = False
    static_df[LEVEL_UPDATE_PROTECTION_COLUMN] = static_df[LEVEL_UPDATE_PROTECTION_COLUMN].map(truthy)

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
    return static_df


def direct_min_upstream_updates(report_df: pd.DataFrame) -> pd.DataFrame:
    if report_df.empty:
        return report_df.copy()

    updates_df = report_df[
        report_df["min_upstream_level_afwijking"]
        & report_df["upstream_node_type"].eq("Basin")
        & report_df["gecheckte_min_upstream_level"].notna()
        & ~report_df["rws_inlet_profile_min_upstream_afwijking"]
        & ~report_df["level_update_skipped_authority"]
        & ~report_df["flow_demand_controlled"]
        & ~report_df["min_upstream_protected_by_static"]
        & ~report_df["node_id"].isin(SKIP_LEVEL_UPDATE_NODE_IDS)
    ].copy()
    if not updates_df.empty:
        updates_df["direct_min_upstream_level_update_allowed"] = True
        updates_df["direct_min_upstream_level_update_basis"] = "direct_upstream_basin_streefpeil"
    return updates_df


def build_report_context(model: Model) -> CouplingReportContext:
    assert model.node.df is not None
    assert model.link.df is not None
    assert model.basin.area.df is not None
    assert model.basin.state.df is not None
    assert model.basin.profile.df is not None

    node_df = reset_index_to_column(model.node.df.copy(), "node_id")
    link_df = reset_index_to_column(model.link.df.copy(), "link_id")
    basin_area_df = model.basin.area.df.copy()
    basin_state_df = model.basin.state.df.copy()
    basin_profile_df = model.basin.profile.df.copy()

    node_by_id = {
        as_int(node_id): {str(key): value for key, value in row.items()}
        for node_id, row in node_df.set_index("node_id").iterrows()
    }
    node_type_by_id = {
        as_int(node_id): str(node_type)
        for node_id, node_type in node_df.set_index("node_id")["node_type"].to_dict().items()
    }
    basin_authority_by_id = {
        as_int(node_id): authority
        for node_id, authority in node_df.set_index("node_id")["meta_waterbeheerder"].to_dict().items()
    }
    basin_name_by_id = {
        as_int(node_id): name for node_id, name in node_df.set_index("node_id")["name"].to_dict().items()
    }
    outgoing_flow_links, incoming_flow_links = flow_link_graphs(link_df)
    flow_demand_info, positive_flow_demand_ids, flow_demand_controlled_ids = flow_demand_targets(
        model=model,
        link_df=link_df,
        node_type_by_id=node_type_by_id,
    )
    static_df = read_control_static_tables(
        model=model,
        node_df=node_df,
        flow_demand_target_info=flow_demand_info,
        positive_flow_demand_target_node_ids=positive_flow_demand_ids,
        flow_demand_controlled_node_ids=flow_demand_controlled_ids,
    )
    functions = classify_functions(static_df, flow_demand_inlet_nodes=positive_flow_demand_ids)
    static_df["functie"] = static_df["node_id"].astype(int).map(functions)
    static_df["control_state_lower"] = static_df["control_state"].astype("string").str.lower()

    return CouplingReportContext(
        node_by_id=node_by_id,
        node_type_by_id=node_type_by_id,
        basin_authority_by_id=basin_authority_by_id,
        basin_name_by_id=basin_name_by_id,
        streefpeil_by_basin_id=normalize_numeric(basin_area_df.set_index("node_id")["meta_streefpeil"]),
        state_level_by_basin_id=normalize_numeric(basin_state_df.set_index("node_id")["level"]),
        min_profile_by_basin_id=basin_profile_df.groupby("node_id")["level"].min(),
        outgoing_flow_links=outgoing_flow_links,
        incoming_flow_links=incoming_flow_links,
        static_df=static_df,
        functions=functions,
    )


def connector_report_record(
    row: Any,
    context: CouplingReportContext,
    upstream_supply_offset: float,
    rws_profile_offset: float,
    tolerance: float,
) -> dict[str, object]:
    node_id = as_int(row.node_id)
    row_dict = row._asdict()
    control_state = "" if is_missing(row.control_state_lower) else str(row.control_state_lower).lower()
    flow_demand_inlaat = bool(row.flow_demand_inlaat)
    flow_demand_controlled = bool(row.flow_demand_controlled)
    level_update_protected = bool(getattr(row, LEVEL_UPDATE_PROTECTION_COLUMN))
    min_upstream_update_skipped_node = node_id in SKIP_MIN_UPSTREAM_UPDATE_NODE_IDS
    static_capacity = static_row_has_capacity(row_dict)
    active_aanvoer_capacity = control_state == "aanvoer" and static_capacity
    inactive_flow_demand_aanvoer = control_state == "aanvoer" and flow_demand_inlaat and not static_capacity

    upstream_id, upstream_link_id, upstream_error = first_non_junction(
        node_id, context.incoming_flow_links, context.node_type_by_id
    )
    downstream_id, downstream_link_id, downstream_error = first_non_junction(
        node_id, context.outgoing_flow_links, context.node_type_by_id
    )
    upstream_node_type = context.node_type_by_id.get(upstream_id) if upstream_id is not None else None
    downstream_node_type = context.node_type_by_id.get(downstream_id) if downstream_id is not None else None
    upstream_authority = context.basin_authority_by_id.get(upstream_id) if upstream_id is not None else None
    downstream_authority = context.basin_authority_by_id.get(downstream_id) if downstream_id is not None else None
    upstream_basin_name = context.basin_name_by_id.get(upstream_id) if upstream_id is not None else None
    downstream_basin_name = context.basin_name_by_id.get(downstream_id) if downstream_id is not None else None

    rws_to_model_link = (
        upstream_authority == RWS_AUTHORITY
        and is_present(downstream_authority)
        and downstream_authority != RWS_AUTHORITY
    )
    min_upstream_is_coupling_link = rws_to_model_link or (
        is_present(row.meta_waterbeheerder)
        and is_present(upstream_authority)
        and row.meta_waterbeheerder != upstream_authority
    )
    max_downstream_is_coupling_link = (
        is_present(row.meta_waterbeheerder)
        and is_present(downstream_authority)
        and row.meta_waterbeheerder != downstream_authority
    )
    min_upstream_protected_by_static = (
        level_update_protected and not min_upstream_is_coupling_link
    ) or node_id in SKIP_LEVEL_UPDATE_NODE_IDS
    max_downstream_protected_by_static = (
        level_update_protected and not max_downstream_is_coupling_link
    ) or node_id in SKIP_LEVEL_UPDATE_NODE_IDS

    upstream_streefpeil = context.streefpeil_by_basin_id.get(upstream_id, np.nan)
    downstream_streefpeil = context.streefpeil_by_basin_id.get(downstream_id, np.nan)
    upstream_min_profile = context.min_profile_by_basin_id.get(upstream_id, np.nan)
    level_update_skipped_authority = row.meta_waterbeheerder in SKIP_LEVEL_UPDATE_AUTHORITIES

    max_basin_id, max_error, max_basis = downstream_basin_for_max_level(
        first_downstream_basin_id=downstream_id if downstream_node_type == "Basin" else None
    )
    downstream_basin_has_control, downstream_basin_control_error = downstream_basin_has_direct_control(
        basin_id=max_basin_id,
        outgoing_flow_links=context.outgoing_flow_links,
        node_type_by_id=context.node_type_by_id,
    )
    direct_downstream_targets = (
        downstream_targets(as_int(max_basin_id), context.outgoing_flow_links, context.node_type_by_id)
        if max_basin_id is not None
        else []
    )
    direct_downstream_control_node_ids = [
        as_int(target_node_id)
        for target_node_id, _, error in direct_downstream_targets
        if error is None
        and target_node_id is not None
        and context.node_type_by_id.get(as_int(target_node_id)) in CONTROL_NODE_TYPES
    ]
    direct_downstream_control_functions = [
        context.functions.get(target_node_id, "onbekend") for target_node_id in direct_downstream_control_node_ids
    ]

    expected_max_downstream = context.streefpeil_by_basin_id.get(max_basin_id, np.nan)
    expected_min_upstream = np.nan
    min_basis = "niet_gecontroleerd"

    if upstream_node_type == "Basin":
        if row.functie == "uitlaat":
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
        and is_present(downstream_authority)
        and downstream_authority != RWS_AUTHORITY
    )
    limburg_rws_flow_demand_min_upstream = (
        flow_demand_controlled
        and rws_to_model
        and row.functie == "inlaat"
        and (
            (is_present(row.meta_waterbeheerder) and str(row.meta_waterbeheerder) == LIMBURG_AUTHORITY)
            or (is_present(downstream_authority) and str(downstream_authority) == LIMBURG_AUTHORITY)
        )
    )
    rws_inlet_profile_update_allowed = (
        rws_to_model
        and row.functie == "inlaat"
        and (control_state == "aanvoer" or flow_demand_inlaat)
        and is_present(upstream_min_profile)
        and downstream_authority not in SKIP_LEVEL_UPDATE_AUTHORITIES
        and not level_update_skipped_authority
        and (not flow_demand_controlled or limburg_rws_flow_demand_min_upstream)
        and not min_upstream_update_skipped_node
        and not min_upstream_protected_by_static
        and node_id not in SKIP_LEVEL_UPDATE_NODE_IDS
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
    if flow_demand_controlled:
        if not rws_inlet_profile_update_allowed:
            expected_min_upstream = row.min_upstream_level
            min_basis = "huidige_waarde_flow_demand_beschermd"
        expected_max_downstream = row.max_downstream_level
        max_basis = "huidige_waarde_flow_demand_beschermd"
    if min_upstream_protected_by_static:
        expected_min_upstream = row.min_upstream_level
        min_basis = "huidige_waarde_handmatig_level_beschermd"
    if max_downstream_protected_by_static:
        expected_max_downstream = row.max_downstream_level
        max_basis = "huidige_waarde_handmatig_level_beschermd"
    if pd.isna(expected_min_upstream) and pd.notna(row.min_upstream_level):
        expected_min_upstream = row.min_upstream_level
        min_basis = "huidige_waarde_gebruikt_omdat_streefpeil_ontbreekt"

    max_report_allowed = (
        active_aanvoer_capacity
        and row.functie in ["inlaat", "doorlaat"]
        and row.node_type in CONTROL_NODE_TYPES
        and max_basin_id is not None
        and max_downstream_is_coupling_link
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
        and min_upstream_is_coupling_link
        and pd.notna(expected_min_upstream)
        and not np.isclose(float(row.min_upstream_level), float(expected_min_upstream), atol=tolerance)
    )
    rws_inlet_profile_min_upstream_afwijking = rws_inlet_profile_update_allowed and (
        pd.isna(row.min_upstream_level)
        or not np.isclose(float(row.min_upstream_level), float(rws_inlet_profile_min_upstream), atol=tolerance)
    )

    return {
        **row_dict,
        "upstream_link_id": upstream_link_id,
        "upstream_node_id": upstream_id,
        "upstream_node_type": upstream_node_type,
        "upstream_basin_authority": upstream_authority,
        "upstream_basin_name": upstream_basin_name,
        "upstream_basin_streefpeil": upstream_streefpeil,
        "upstream_basin_state_level": context.state_level_by_basin_id.get(upstream_id, np.nan),
        "upstream_basin_min_profile_level": upstream_min_profile,
        "min_upstream_is_coupling_link": bool(min_upstream_is_coupling_link),
        "rws_to_model_link": bool(rws_to_model_link),
        "downstream_link_id": downstream_link_id,
        "downstream_node_id": downstream_id,
        "downstream_node_type": downstream_node_type,
        "downstream_basin_authority": downstream_authority,
        "downstream_basin_name": downstream_basin_name,
        "downstream_basin_streefpeil": downstream_streefpeil,
        "max_downstream_is_coupling_link": bool(max_downstream_is_coupling_link),
        "max_downstream_level_basin_id": max_basin_id,
        "max_downstream_level_basin_authority": (
            context.basin_authority_by_id.get(max_basin_id) if max_basin_id is not None else None
        ),
        "max_downstream_level_basin_error": max_error,
        "max_downstream_level_direct_control": bool(downstream_basin_has_control),
        "max_downstream_level_direct_control_error": downstream_basin_control_error,
        "max_downstream_level_direct_control_node_ids": ",".join(
            str(target_node_id) for target_node_id in direct_downstream_control_node_ids
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
            and not max_downstream_protected_by_static
            and node_id not in SKIP_LEVEL_UPDATE_NODE_IDS
        ),
        "gecheckte_max_downstream_level_update": expected_max_downstream,
        "max_downstream_level_update_basis": max_basis,
        "gecheckte_min_upstream_level": expected_min_upstream,
        "verschil_min_upstream_level": row.min_upstream_level - expected_min_upstream,
        "min_upstream_level_check_basis": min_basis,
        "min_upstream_level_afwijking": bool(min_upstream_afwijking),
        "rws_to_model": bool(rws_to_model),
        "level_update_skipped_authority": bool(level_update_skipped_authority),
        "min_upstream_update_skipped_node": bool(min_upstream_update_skipped_node),
        "level_update_protected": bool(level_update_protected),
        "min_upstream_protected_by_static": bool(min_upstream_protected_by_static),
        "max_downstream_protected_by_static": bool(max_downstream_protected_by_static),
        "rws_inlet_profile_update_allowed": bool(rws_inlet_profile_update_allowed),
        "limburg_rws_flow_demand_min_upstream": bool(limburg_rws_flow_demand_min_upstream),
        "rws_inlet_profile_min_upstream": rws_inlet_profile_min_upstream,
        "rws_inlet_profile_min_upstream_afwijking": bool(rws_inlet_profile_min_upstream_afwijking),
        "upstream_check_error": upstream_error,
        "downstream_check_error": downstream_error,
    }


def outlet_inlet_record(row: Any, context: CouplingReportContext) -> dict[str, object] | None:
    node_id = as_int(row.node_id)
    upstream_targets = downstream_targets(node_id, context.incoming_flow_links, context.node_type_by_id)
    upstream_basin_targets = [
        (as_int(target_id), as_int(link_id))
        for target_id, link_id, error in upstream_targets
        if error is None and target_id is not None and context.node_type_by_id.get(target_id) == "Basin"
    ]
    if row.node_type != "Outlet" or context.functions.get(node_id) != "inlaat" or len(upstream_basin_targets) != 1:
        return None

    upstream_basin_id, upstream_basin_link_id = upstream_basin_targets[0]
    if has_alternative_downstream_route(
        start_node_id=upstream_basin_id,
        blocked_node_id=node_id,
        outgoing_flow_links=context.outgoing_flow_links,
        node_type_by_id=context.node_type_by_id,
    ):
        return None

    node_row = context.node_by_id[node_id]
    return {
        "node_id": node_id,
        "node_type": row.node_type,
        "functie": context.functions.get(node_id),
        "meta_waterbeheerder": node_row.get("meta_waterbeheerder"),
        "meta_code_waterbeheerder": node_row.get("meta_code_waterbeheerder"),
        "meta_node_id_waterbeheerder": node_row.get("meta_node_id_waterbeheerder"),
        "name": node_row.get("name"),
        "upstream_basin_id": upstream_basin_id,
        "upstream_link_id": upstream_basin_link_id,
        "upstream_basin_authority": context.basin_authority_by_id.get(upstream_basin_id),
        "upstream_basin_name": context.basin_name_by_id.get(upstream_basin_id),
        "upstream_basin_streefpeil": context.streefpeil_by_basin_id.get(upstream_basin_id, np.nan),
        "reden": ("Outlet is inlaat met precies een bovenstrooms Basin en dat Basin heeft geen andere doorstroomtak."),
    }


def rws_leak_records(context: CouplingReportContext) -> list[dict[str, object]]:
    records = []
    for node_id, rows in context.static_df.groupby("node_id"):
        node_id_int = as_int(node_id)
        upstream_id, upstream_link_id, _ = first_non_junction(
            node_id_int, context.incoming_flow_links, context.node_type_by_id
        )
        downstream_id, downstream_link_id, _ = first_non_junction(
            node_id_int, context.outgoing_flow_links, context.node_type_by_id
        )
        upstream_authority = context.basin_authority_by_id.get(upstream_id) if upstream_id is not None else None
        downstream_authority = context.basin_authority_by_id.get(downstream_id) if downstream_id is not None else None
        if not (
            (context.node_type_by_id.get(upstream_id) if upstream_id is not None else None) == "Basin"
            and (context.node_type_by_id.get(downstream_id) if downstream_id is not None else None) == "Basin"
            and upstream_authority == RWS_AUTHORITY
            and is_present(downstream_authority)
            and downstream_authority != RWS_AUTHORITY
        ):
            continue

        afvoer_rows = rows[
            rows["control_state"].astype("string").str.lower().eq("afvoer")
            & rows.apply(static_row_has_capacity, axis=1)
        ]
        if afvoer_rows.empty:
            continue

        node_row = context.node_by_id[node_id_int]
        records.append(
            {
                "node_id": node_id_int,
                "node_type": node_row.get("node_type"),
                "functie": context.functions[node_id_int],
                "meta_waterbeheerder": node_row.get("meta_waterbeheerder"),
                "meta_code_waterbeheerder": node_row.get("meta_code_waterbeheerder"),
                "meta_node_id_waterbeheerder": node_row.get("meta_node_id_waterbeheerder"),
                "name": node_row.get("name"),
                "upstream_link_id": upstream_link_id,
                "upstream_basin_id": upstream_id,
                "upstream_basin_name": context.basin_name_by_id.get(upstream_id) if upstream_id is not None else None,
                "downstream_link_id": downstream_link_id,
                "downstream_basin_id": downstream_id,
                "downstream_basin_authority": downstream_authority,
                "downstream_basin_name": (
                    context.basin_name_by_id.get(downstream_id) if downstream_id is not None else None
                ),
                "afvoer_rows": capacity_text(afvoer_rows),
            }
        )
    return records


def report_tables(
    report_records: list[dict[str, object]],
    leak_records: list[dict[str, object]],
    outlet_inlet_records: list[dict[str, object]],
) -> dict[str, pd.DataFrame]:
    report_df = pd.DataFrame(report_records)
    if report_df.empty:
        deviations_df = report_df.copy()
        allowed_updates_df = report_df.copy()
    else:
        deviations_df = report_df[
            report_df["max_downstream_level_afwijking"] | report_df["min_upstream_level_afwijking"]
        ].copy()
        allowed_updates_df = report_df[report_df["rws_inlet_profile_min_upstream_afwijking"]].copy()

    return {
        "report": report_df,
        "deviations": deviations_df,
        "rws_inlet_min_upstream_updates": allowed_updates_df,
        "direct_min_upstream_updates": direct_min_upstream_updates(report_df),
        "rws_leaks": pd.DataFrame(leak_records),
        "outlet_inlets": pd.DataFrame(outlet_inlet_records),
    }


def build_report(
    model: Model,
    upstream_supply_offset: float,
    rws_profile_offset: float,
    tolerance: float,
) -> dict[str, pd.DataFrame]:
    context = build_report_context(model)
    records = [
        connector_report_record(row, context, upstream_supply_offset, rws_profile_offset, tolerance)
        for row in context.static_df.itertuples(index=False)
    ]
    outlet_inlets = [
        record
        for row in context.static_df.itertuples(index=False)
        if (record := outlet_inlet_record(row, context)) is not None
    ]
    return report_tables(records, rws_leak_records(context), outlet_inlets)


def run_coupling_level_report(
    toml_file: Path,
    settings: CouplingLevelSettings,
    output_gpkg: Path | None = None,
    verdachte_output_gpkg: Path | None = None,
    tolerance: float = 1e-6,
) -> None:
    model = Model.read(toml_file)
    assert model.node.df is not None
    database_path = database_gpkg_path(model, toml_file)
    output_gpkg = resolve_output_gpkg(toml_file, output_gpkg)
    verdachte_output_gpkg = resolve_output_gpkg(toml_file, verdachte_output_gpkg) if verdachte_output_gpkg else None

    tables = build_report(
        model=model,
        upstream_supply_offset=settings.upstream_supply_offset,
        rws_profile_offset=settings.rws_profile_offset,
        tolerance=tolerance,
    )
    protected_controller_updates = protected_controller_threshold_updates(
        model=model,
        report_df=tables["report"],
        tolerance=tolerance,
    )
    not_applied = not_applied_level_deviations(
        deviations_df=tables["deviations"],
        allowed_updates_df=tables["rws_inlet_min_upstream_updates"],
        direct_min_upstream_updates_df=tables["direct_min_upstream_updates"],
    )

    output_gpkg = write_report_gpkg(
        node_df=model.node.df,
        output_gpkg=output_gpkg,
        deviations_df=tables["deviations"],
        allowed_updates_df=tables["rws_inlet_min_upstream_updates"],
        direct_min_upstream_updates_df=tables["direct_min_upstream_updates"],
        not_applied_df=not_applied,
        protected_controller_updates_df=protected_controller_updates,
        leaks_df=tables["rws_leaks"],
        outlet_inlets_df=tables["outlet_inlets"],
    )
    if verdachte_output_gpkg is not None:
        verdachte_output_gpkg = write_suspicious_gpkg(
            node_df=model.node.df,
            output_gpkg=verdachte_output_gpkg,
            not_applied_df=not_applied,
            protected_controller_updates_df=protected_controller_updates,
            leaks_df=tables["rws_leaks"],
            outlet_inlets_df=tables["outlet_inlets"],
        )

    max_downstream_updates = tables["deviations"][
        tables["deviations"].get("max_downstream_level_update_allowed", pd.Series(dtype=bool)).fillna(False)
    ].copy()
    apply_direct_min_upstream = settings.apply_direct_min_upstream_level or settings.apply_max_downstream_level
    apply_level_changes = (
        settings.apply_rws_inlet_min_upstream or apply_direct_min_upstream or settings.apply_max_downstream_level
    )

    backup_path = None
    min_update_count = 0
    max_update_count = 0
    condition_update_count = 0
    manning_update_count = 0
    if apply_level_changes or settings.manning_n is not None:
        backup_path, min_update_count, max_update_count, condition_update_count, manning_update_count = (
            apply_level_updates(
                model=model,
                toml_file=toml_file,
                database_path=database_path,
                min_upstream_updates_df=(
                    tables["rws_inlet_min_upstream_updates"]
                    if settings.apply_rws_inlet_min_upstream
                    else tables["rws_inlet_min_upstream_updates"].iloc[0:0]
                ),
                direct_min_upstream_updates_df=(
                    tables["direct_min_upstream_updates"]
                    if apply_direct_min_upstream
                    else tables["direct_min_upstream_updates"].iloc[0:0]
                ),
                max_downstream_updates_df=(
                    max_downstream_updates if settings.apply_max_downstream_level else max_downstream_updates.iloc[0:0]
                ),
                protected_controller_updates_df=(
                    protected_controller_updates if apply_level_changes else protected_controller_updates.iloc[0:0]
                ),
                manning_n=settings.manning_n,
            )
        )

    print(f"Model-TOML: {toml_file}")
    print(f"Model-GPKG: {database_path}")
    print(f"Rapport-GPKG: {output_gpkg}")
    if verdachte_output_gpkg is not None:
        print(f"Verdachte-punten-GPKG: {verdachte_output_gpkg}")
    print(f"Level-afwijkingen: {len(tables['deviations'])}")
    print(f"Toegestane RWS-inlaat min_upstream updates: {len(tables['rws_inlet_min_upstream_updates'])}")
    print(f"Toegestane directe min_upstream updates: {len(tables['direct_min_upstream_updates'])}")
    print(f"Toegestane inlaat/doorlaat max_downstream updates: {len(max_downstream_updates)}")
    print(f"Coupling/protected controller-threshold updates: {len(protected_controller_updates)}")
    print(f"Verdachte niet-aangepakte level-afwijkingen: {len(not_applied)}")
    print(f"Verdachte RWS-lekken: {len(tables['rws_leaks'])}")
    print(f"Verdachte Outlet-inlaten met enkel bovenstrooms Basin: {len(tables['outlet_inlets'])}")
    if backup_path is not None:
        print(f"Backup database: {backup_path}")
        print(f"Toegepaste min_upstream_level updates: {min_update_count}")
        print(f"Toegepaste max_downstream_level updates: {max_update_count}")
        print(f"Gesynchroniseerde DiscreteControl-condition thresholds: {condition_update_count}")
        if settings.manning_n is not None:
            print(f"Toegepaste ManningResistance manning_n updates: {manning_update_count}")
            print(f"ManningResistance manning_n gezet op: {settings.manning_n}")
