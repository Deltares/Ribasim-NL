"""Check and apply coupling-level corrections for coupled Ribasim models."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ribasim_nl import Model
from ribasim_nl.coupling_level_apply import apply_level_updates
from ribasim_nl.coupling_level_common import (
    AAENMAAS_AUTHORITY,
    CONTROL_NODE_TYPES,
    DEDOMMEL_AUTHORITY,
    LEVEL_UPDATE_PROTECTION_COLUMN,
    LIMBURG_AUTHORITY,
    RWS_AUTHORITY,
    SKIP_LEVEL_UPDATE_AUTHORITIES,
    SKIP_LEVEL_UPDATE_NODE_IDS,
    STATIC_TABLE_BY_NODE_TYPE,
    as_int,
    classify_functions,
    is_missing,
    is_present,
    normalize_numeric,
    reset_index_to_column,
    static_row_has_capacity,
    truthy,
)
from ribasim_nl.coupling_level_controls import protected_controller_threshold_updates
from ribasim_nl.coupling_level_network import (
    first_non_junction,
    flow_link_graphs,
)

FLOW_DEMAND_DIRECT_MIN_UPSTREAM_AUTHORITIES = {AAENMAAS_AUTHORITY, LIMBURG_AUTHORITY}
RWS_FLOW_DEMAND_PROFILE_AUTHORITIES = {AAENMAAS_AUTHORITY, DEDOMMEL_AUTHORITY, LIMBURG_AUTHORITY}


@dataclass(frozen=True)
class CouplingLevelSettings:
    upstream_supply_offset: float
    rws_profile_offset: float
    apply_rws_inlet_min_upstream: bool
    apply_max_downstream_level: bool
    apply_direct_min_upstream_level: bool


@dataclass(frozen=True)
class CouplingLevelContext:
    node_type_by_id: dict[int, str]
    basin_authority_by_id: dict[int, object]
    streefpeil_by_basin_id: pd.Series
    min_profile_by_basin_id: pd.Series
    outgoing_flow_links: dict[int, list[tuple[int, int]]]
    incoming_flow_links: dict[int, list[tuple[int, int]]]
    static_df: pd.DataFrame


def flow_demand_targets(
    model: Model,
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> tuple[set[int], set[int]]:
    """Find connector nodes controlled by a FlowDemand node."""
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
        return positive_target_node_ids, controlled_node_ids

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

    for target_node_id, rows in control_link_df.groupby("to_node_id"):
        target_node_id_int = as_int(target_node_id)
        target_flow_demand_node_ids = sorted({as_int(node_id) for node_id in rows["from_node_id"]})
        positive_flow_demand_target = any(
            node_id in positive_flow_demand_ids for node_id in target_flow_demand_node_ids
        )
        if positive_flow_demand_target:
            positive_target_node_ids.add(target_node_id_int)
        controlled_node_ids.add(target_node_id_int)

    return positive_target_node_ids, controlled_node_ids


def read_control_static_tables(
    model: Model,
    node_df: pd.DataFrame,
    positive_flow_demand_target_node_ids: set[int],
    flow_demand_controlled_node_ids: set[int],
) -> pd.DataFrame:
    """Combine Outlet/Pump static rows with node metadata and FlowDemand flags."""
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
    return static_df


def direct_min_upstream_updates(level_df: pd.DataFrame) -> pd.DataFrame:
    """Select non-RWS min_upstream updates that can be applied directly."""
    if level_df.empty:
        return level_df.copy()

    updates_df = level_df[
        level_df["min_upstream_level_afwijking"]
        & level_df["upstream_node_type"].eq("Basin")
        & level_df["gecheckte_min_upstream_level"].notna()
        & ~level_df["rws_inlet_profile_min_upstream_afwijking"]
        & ~level_df["level_update_skipped_authority"]
        & (
            ~level_df["flow_demand_controlled"]
            | level_df["flow_demand_direct_min_upstream_update_allowed"].fillna(False)
        )
        & ~level_df["min_upstream_protected_by_static"]
        & ~level_df["node_id"].isin(SKIP_LEVEL_UPDATE_NODE_IDS)
    ].copy()
    if not updates_df.empty:
        updates_df["direct_min_upstream_level_update_allowed"] = True
        updates_df["direct_min_upstream_level_update_basis"] = "direct_upstream_basin_streefpeil"
        updates_df.loc[
            updates_df["flow_demand_direct_min_upstream_update_allowed"].fillna(False),
            "direct_min_upstream_level_update_basis",
        ] = "flow_demand_upstream_streefpeil_plus_supply_offset"
    return updates_df


def build_coupling_level_context(model: Model) -> CouplingLevelContext:
    """Prepare lookup tables used by the coupling-level checks."""
    assert model.node.df is not None
    assert model.link.df is not None
    assert model.basin.area.df is not None
    assert model.basin.profile.df is not None

    node_df = reset_index_to_column(model.node.df.copy(), "node_id")
    link_df = reset_index_to_column(model.link.df.copy(), "link_id")
    basin_area_df = model.basin.area.df.copy()
    basin_profile_df = model.basin.profile.df.copy()

    node_type_by_id = {
        as_int(node_id): str(node_type)
        for node_id, node_type in node_df.set_index("node_id")["node_type"].to_dict().items()
    }
    basin_authority_by_id = {
        as_int(node_id): authority
        for node_id, authority in node_df.set_index("node_id")["meta_waterbeheerder"].to_dict().items()
    }
    outgoing_flow_links, incoming_flow_links = flow_link_graphs(link_df)
    positive_flow_demand_ids, flow_demand_controlled_ids = flow_demand_targets(
        model=model,
        link_df=link_df,
        node_type_by_id=node_type_by_id,
    )
    static_df = read_control_static_tables(
        model=model,
        node_df=node_df,
        positive_flow_demand_target_node_ids=positive_flow_demand_ids,
        flow_demand_controlled_node_ids=flow_demand_controlled_ids,
    )
    functions = classify_functions(static_df, flow_demand_inlet_nodes=positive_flow_demand_ids)
    static_df["functie"] = static_df["node_id"].astype(int).map(functions)
    static_df["control_state_lower"] = static_df["control_state"].astype("string").str.lower()

    return CouplingLevelContext(
        node_type_by_id=node_type_by_id,
        basin_authority_by_id=basin_authority_by_id,
        streefpeil_by_basin_id=normalize_numeric(basin_area_df.set_index("node_id")["meta_streefpeil"]),
        min_profile_by_basin_id=basin_profile_df.groupby("node_id")["level"].min(),
        outgoing_flow_links=outgoing_flow_links,
        incoming_flow_links=incoming_flow_links,
        static_df=static_df,
    )


def connector_level_record(
    row: Any,
    context: CouplingLevelContext,
    upstream_supply_offset: float,
    rws_profile_offset: float,
    tolerance: float,
) -> dict[str, object]:
    """Check one Outlet/Pump static row against the linked Basin levels."""
    node_id = as_int(row.node_id)
    row_dict = row._asdict()
    control_state = "" if is_missing(row.control_state_lower) else str(row.control_state_lower).lower()
    flow_demand_inlaat = bool(row.flow_demand_inlaat)
    flow_demand_controlled = bool(row.flow_demand_controlled)
    level_update_protected = bool(getattr(row, LEVEL_UPDATE_PROTECTION_COLUMN))
    static_capacity = static_row_has_capacity(row_dict)
    active_aanvoer_capacity = control_state == "aanvoer" and static_capacity
    inactive_flow_demand_aanvoer = control_state == "aanvoer" and flow_demand_inlaat and not static_capacity

    upstream_id, _, _ = first_non_junction(node_id, context.incoming_flow_links, context.node_type_by_id)
    downstream_id, _, _ = first_non_junction(node_id, context.outgoing_flow_links, context.node_type_by_id)
    upstream_node_type = context.node_type_by_id.get(upstream_id) if upstream_id is not None else None
    downstream_node_type = context.node_type_by_id.get(downstream_id) if downstream_id is not None else None
    upstream_authority = context.basin_authority_by_id.get(upstream_id) if upstream_id is not None else None
    downstream_authority = context.basin_authority_by_id.get(downstream_id) if downstream_id is not None else None
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
    min_upstream_protected_by_static = level_update_protected or node_id in SKIP_LEVEL_UPDATE_NODE_IDS
    max_downstream_protected_by_static = level_update_protected or node_id in SKIP_LEVEL_UPDATE_NODE_IDS

    upstream_streefpeil = context.streefpeil_by_basin_id.get(upstream_id, np.nan)
    upstream_min_profile = context.min_profile_by_basin_id.get(upstream_id, np.nan)
    level_update_skipped_authority = row.meta_waterbeheerder in SKIP_LEVEL_UPDATE_AUTHORITIES

    max_basin_id = downstream_id if downstream_node_type == "Basin" else None
    expected_max_downstream = context.streefpeil_by_basin_id.get(max_basin_id, np.nan)
    expected_min_upstream = np.nan

    if upstream_node_type == "Basin":
        if row.functie == "uitlaat":
            expected_min_upstream = upstream_streefpeil
        elif (active_aanvoer_capacity or inactive_flow_demand_aanvoer) and row.functie in ["inlaat", "doorlaat"]:
            expected_min_upstream = (
                upstream_streefpeil + upstream_supply_offset if pd.notna(upstream_streefpeil) else np.nan
            )
        elif control_state == "afvoer" and row.functie in ["inlaat", "doorlaat"]:
            expected_min_upstream = upstream_streefpeil

    rws_to_model = (
        upstream_node_type == "Basin"
        and downstream_node_type == "Basin"
        and upstream_authority == RWS_AUTHORITY
        and is_present(downstream_authority)
        and downstream_authority != RWS_AUTHORITY
    )
    rws_flow_demand_profile_min_upstream_allowed = (
        flow_demand_controlled
        and rws_to_model
        and row.functie == "inlaat"
        and (
            (
                is_present(row.meta_waterbeheerder)
                and str(row.meta_waterbeheerder) in RWS_FLOW_DEMAND_PROFILE_AUTHORITIES
            )
            or (is_present(downstream_authority) and str(downstream_authority) in RWS_FLOW_DEMAND_PROFILE_AUTHORITIES)
        )
    )
    rws_inlet_profile_update_allowed = (
        rws_to_model
        and row.functie == "inlaat"
        and (control_state == "aanvoer" or flow_demand_inlaat)
        and is_present(upstream_min_profile)
        and downstream_authority not in SKIP_LEVEL_UPDATE_AUTHORITIES
        and not level_update_skipped_authority
        and (not flow_demand_controlled or rws_flow_demand_profile_min_upstream_allowed)
        and not min_upstream_protected_by_static
        and node_id not in SKIP_LEVEL_UPDATE_NODE_IDS
    )
    rws_inlet_profile_min_upstream = (
        upstream_min_profile + rws_profile_offset if rws_inlet_profile_update_allowed else np.nan
    )
    if rws_inlet_profile_update_allowed:
        expected_min_upstream = rws_inlet_profile_min_upstream

    flow_demand_direct_min_upstream_authority_match = (
        (
            is_present(row.meta_waterbeheerder)
            and str(row.meta_waterbeheerder) in FLOW_DEMAND_DIRECT_MIN_UPSTREAM_AUTHORITIES
        )
        or (is_present(upstream_authority) and str(upstream_authority) in FLOW_DEMAND_DIRECT_MIN_UPSTREAM_AUTHORITIES)
        or (
            is_present(downstream_authority)
            and str(downstream_authority) in FLOW_DEMAND_DIRECT_MIN_UPSTREAM_AUTHORITIES
        )
    )
    flow_demand_direct_min_upstream_update_allowed = (
        flow_demand_controlled
        and row.functie == "inlaat"
        and upstream_node_type == "Basin"
        and flow_demand_direct_min_upstream_authority_match
        and not rws_inlet_profile_update_allowed
        and not level_update_skipped_authority
        and not min_upstream_protected_by_static
        and node_id not in SKIP_LEVEL_UPDATE_NODE_IDS
    )
    if flow_demand_controlled:
        if not (rws_inlet_profile_update_allowed or flow_demand_direct_min_upstream_update_allowed):
            expected_min_upstream = row.min_upstream_level
        expected_max_downstream = row.max_downstream_level
    if min_upstream_protected_by_static:
        expected_min_upstream = row.min_upstream_level
    if max_downstream_protected_by_static:
        expected_max_downstream = row.max_downstream_level
    if flow_demand_direct_min_upstream_update_allowed and is_present(upstream_streefpeil):
        expected_min_upstream = upstream_streefpeil + upstream_supply_offset
    if pd.isna(expected_min_upstream) and pd.notna(row.min_upstream_level):
        expected_min_upstream = row.min_upstream_level

    max_update_candidate = (
        active_aanvoer_capacity
        and row.functie in ["inlaat", "doorlaat"]
        and row.node_type in CONTROL_NODE_TYPES
        and max_basin_id is not None
        and max_downstream_is_coupling_link
    )
    max_downstream_afwijking = (
        max_update_candidate
        and not max_downstream_protected_by_static
        and not level_update_skipped_authority
        and not flow_demand_controlled
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
        "table_fid": row.table_fid,
        "node_id": node_id,
        "node_type": row.node_type,
        "static_table": row.static_table,
        "control_state": row.control_state,
        "functie": row.functie,
        "min_upstream_level": row.min_upstream_level,
        "max_downstream_level": row.max_downstream_level,
        "flow_rate": row_dict.get("flow_rate"),
        "min_flow_rate": row_dict.get("min_flow_rate"),
        "max_flow_rate": row_dict.get("max_flow_rate"),
        "flow_demand_controlled": flow_demand_controlled,
        "upstream_node_id": upstream_id,
        "upstream_node_type": upstream_node_type,
        "upstream_basin_streefpeil": upstream_streefpeil,
        "downstream_node_id": downstream_id,
        "min_upstream_is_coupling_link": bool(min_upstream_is_coupling_link),
        "max_downstream_is_coupling_link": bool(max_downstream_is_coupling_link),
        "max_downstream_level_basin_id": max_basin_id,
        "gecheckte_max_downstream_level": expected_max_downstream,
        "max_downstream_level_afwijking": bool(max_downstream_afwijking),
        "max_downstream_level_update_allowed": bool(max_downstream_afwijking),
        "gecheckte_max_downstream_level_update": expected_max_downstream,
        "gecheckte_min_upstream_level": expected_min_upstream,
        "min_upstream_level_afwijking": bool(min_upstream_afwijking),
        "level_update_skipped_authority": bool(level_update_skipped_authority),
        "min_upstream_protected_by_static": bool(min_upstream_protected_by_static),
        "rws_inlet_profile_update_allowed": bool(rws_inlet_profile_update_allowed),
        "rws_flow_demand_profile_min_upstream_allowed": bool(rws_flow_demand_profile_min_upstream_allowed),
        "limburg_rws_flow_demand_min_upstream": bool(rws_flow_demand_profile_min_upstream_allowed),
        "flow_demand_direct_min_upstream_update_allowed": bool(flow_demand_direct_min_upstream_update_allowed),
        "rws_inlet_profile_min_upstream": rws_inlet_profile_min_upstream,
        "rws_inlet_profile_min_upstream_afwijking": bool(rws_inlet_profile_min_upstream_afwijking),
    }


def coupling_level_tables(
    level_records: list[dict[str, object]],
) -> dict[str, pd.DataFrame]:
    """Split checked rows into the update tables used by apply_level_updates."""
    level_df = pd.DataFrame(level_records)
    if level_df.empty:
        level_deviation_df = level_df.copy()
        allowed_updates_df = level_df.copy()
    else:
        level_deviation_df = level_df[
            level_df["max_downstream_level_afwijking"] | level_df["min_upstream_level_afwijking"]
        ].copy()
        allowed_updates_df = level_df[level_df["rws_inlet_profile_min_upstream_afwijking"]].copy()

    return {
        "levels": level_df,
        "deviations": level_deviation_df,
        "rws_inlet_min_upstream_updates": allowed_updates_df,
        "direct_min_upstream_updates": direct_min_upstream_updates(level_df),
    }


def build_coupling_level_tables(
    model: Model,
    upstream_supply_offset: float,
    rws_profile_offset: float,
    tolerance: float,
) -> dict[str, pd.DataFrame]:
    """Run all connector checks for a model and return update tables."""
    context = build_coupling_level_context(model)
    records = [
        connector_level_record(
            row,
            context,
            upstream_supply_offset,
            rws_profile_offset,
            tolerance,
        )
        for row in context.static_df.itertuples(index=False)
    ]
    return coupling_level_tables(records)


def run_coupling_level_check(
    toml_file: Path,
    settings: CouplingLevelSettings,
    tolerance: float = 1e-6,
) -> None:
    """Apply configured coupling-level corrections to a TOML model."""
    model = Model.read(toml_file)
    assert model.node.df is not None

    tables = build_coupling_level_tables(
        model=model,
        upstream_supply_offset=settings.upstream_supply_offset,
        rws_profile_offset=settings.rws_profile_offset,
        tolerance=tolerance,
    )
    protected_controller_updates = protected_controller_threshold_updates(
        model=model,
        level_df=tables["levels"],
        tolerance=tolerance,
    )

    max_downstream_updates = tables["deviations"][
        tables["deviations"].get("max_downstream_level_update_allowed", pd.Series(dtype=bool)).fillna(False)
    ].copy()
    apply_direct_min_upstream = settings.apply_direct_min_upstream_level or settings.apply_max_downstream_level
    apply_level_changes = (
        settings.apply_rws_inlet_min_upstream or apply_direct_min_upstream or settings.apply_max_downstream_level
    )

    if apply_level_changes:
        apply_level_updates(
            model=model,
            toml_file=toml_file,
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
        )
