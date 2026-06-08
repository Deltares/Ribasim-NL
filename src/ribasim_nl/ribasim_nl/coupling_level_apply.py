"""Apply explicitly allowed coupling-level corrections to a Ribasim model."""

from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

import pandas as pd

from ribasim_nl import Model
from ribasim_nl.control_layout import control_condition_thresholds, control_layout_key
from ribasim_nl.coupling_level_common import (
    STATIC_TABLE_BY_NODE_TYPE,
    as_float,
    as_int,
    control_node_ids_by_target_node_id,
    control_node_name,
    flow_demand_controlled_node_ids,
    is_missing,
    is_present,
    model_level_difference_threshold,
)
from ribasim_nl.coupling_level_controls import (
    has_intentional_named_layout_mismatch,
    validate_control_layout_for_sync,
)


def backup_database(database_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = database_path.with_name(f"{database_path.stem}_before_report_coupling_levels_{timestamp}.gpkg")
    shutil.copy2(database_path, backup_path)
    return backup_path


def protected_table_copy(
    df: pd.DataFrame | None,
    node_ids: set[int] | None = None,
    excluded_table_fids: set[int] | None = None,
) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    result = df.copy()
    result["__table_fid__"] = result.index
    if node_ids is not None:
        result = result[result["node_id"].astype(int).isin(node_ids)].copy() if node_ids else result.iloc[0:0].copy()
    if excluded_table_fids:
        result = result.loc[~result["__table_fid__"].astype(int).isin(excluded_table_fids)].copy()
    return result.sort_index(axis=0).sort_index(axis=1).reset_index(drop=True)


def assert_table_copy_unchanged(label: str, before: pd.DataFrame, after: pd.DataFrame) -> None:
    try:
        pd.testing.assert_frame_equal(
            before.reset_index(drop=True),
            after.reset_index(drop=True),
            check_dtype=False,
            check_like=False,
        )
    except AssertionError as error:
        raise RuntimeError(f"{label} is onverwacht aangepast door report_coupling_levels.") from error


def protected_apply_table_copies(
    model: Model,
    allowed_static_table_fids_by_table: dict[str, set[int]] | None = None,
) -> dict[str, pd.DataFrame]:
    flow_demand_node_ids = flow_demand_controlled_node_ids(model)
    allowed_static_table_fids_by_table = allowed_static_table_fids_by_table or {}
    table_copies = {
        "DiscreteControl / variable": protected_table_copy(model.discrete_control.variable.df),
        "DiscreteControl / logic": protected_table_copy(model.discrete_control.logic.df),
    }
    for node_type, table in STATIC_TABLE_BY_NODE_TYPE.items():
        static_df = model.get_component(node_type).static.df
        table_copies[f"{table} flow_demand"] = protected_table_copy(
            static_df,
            flow_demand_node_ids,
            excluded_table_fids=allowed_static_table_fids_by_table.get(table, set()),
        )
    return table_copies


def assert_protected_apply_table_copies_unchanged(
    model: Model,
    table_copies: dict[str, pd.DataFrame],
    allowed_static_table_fids_by_table: dict[str, set[int]] | None = None,
) -> None:
    flow_demand_node_ids = flow_demand_controlled_node_ids(model)
    allowed_static_table_fids_by_table = allowed_static_table_fids_by_table or {}
    current_table_copies = {
        "DiscreteControl / variable": protected_table_copy(model.discrete_control.variable.df),
        "DiscreteControl / logic": protected_table_copy(model.discrete_control.logic.df),
    }
    for node_type, table in STATIC_TABLE_BY_NODE_TYPE.items():
        static_df = model.get_component(node_type).static.df
        current_table_copies[f"{table} flow_demand"] = protected_table_copy(
            static_df,
            flow_demand_node_ids,
            excluded_table_fids=allowed_static_table_fids_by_table.get(table, set()),
        )

    for label, before in table_copies.items():
        assert_table_copy_unchanged(label, before, current_table_copies[label])


def set_all_manning_n(model: Model, manning_n: float, tolerance: float = 1e-12) -> int:
    static_df = model.manning_resistance.static.df
    if static_df is None:
        return 0
    if "manning_n" not in static_df.columns:
        raise KeyError("Kolom 'manning_n' ontbreekt in ManningResistance / static.")

    current = pd.to_numeric(static_df["manning_n"], errors="coerce")
    mask = current.isna() | (current.sub(float(manning_n)).abs() > float(tolerance))
    static_df.loc[mask, "manning_n"] = float(manning_n)
    return int(mask.sum())


def allowed_flow_demand_static_update_fids(min_upstream_updates_df: pd.DataFrame) -> dict[str, set[int]]:
    """FlowDemand blijft beschermd, behalve de expliciet toegestane Limburg/RWS min_upstream-correcties."""
    if min_upstream_updates_df.empty:
        return {}

    allowed_df = min_upstream_updates_df[
        min_upstream_updates_df["flow_demand_controlled"].fillna(False)
        & min_upstream_updates_df["rws_inlet_profile_update_allowed"].fillna(False)
        & min_upstream_updates_df["limburg_rws_flow_demand_min_upstream"].fillna(False)
    ].copy()
    if allowed_df.empty:
        return {}

    return {
        str(table): set(rows["table_fid"].dropna().astype(int)) for table, rows in allowed_df.groupby("static_table")
    }


def apply_level_updates(
    model: Model,
    toml_file: Path,
    database_path: Path,
    min_upstream_updates_df: pd.DataFrame,
    direct_min_upstream_updates_df: pd.DataFrame,
    max_downstream_updates_df: pd.DataFrame,
    protected_controller_updates_df: pd.DataFrame,
    *,
    manning_n: float | None = None,
) -> tuple[Path, int, int, int, int]:
    backup_path = backup_database(database_path)
    allowed_static_table_fids_by_table = allowed_flow_demand_static_update_fids(min_upstream_updates_df)
    protected_table_copies = protected_apply_table_copies(model, allowed_static_table_fids_by_table)
    min_update_count = 0
    max_update_count = 0
    condition_update_count = 0
    manning_update_count = 0
    synced_conditions: set[tuple[int, int, float]] = set()
    control_ids_by_target = control_node_ids_by_target_node_id(model)
    static_df_by_table: dict[str, pd.DataFrame] = {}
    for node_type, table in STATIC_TABLE_BY_NODE_TYPE.items():
        static_df = model.get_component(node_type).static.df
        if static_df is None:
            raise ValueError(f"{table} ontbreekt in het model.")
        static_df_by_table[table] = static_df

    def update_discrete_control_conditions(
        target_node_id: int,
        listen_node_id: int | None,
        level_value: float,
        function: str,
        flow_demand_controlled: bool,
    ) -> int:
        if flow_demand_controlled or listen_node_id is None:
            return 0
        variable_df = model.discrete_control.variable.df
        condition_df = model.discrete_control.condition.df
        if variable_df is None or condition_df is None:
            return 0

        control_node_ids = control_ids_by_target.get(as_int(target_node_id), [])
        if not control_node_ids:
            return 0

        update_count = 0
        for control_node_id in control_node_ids:
            control_name = control_node_name(model, control_node_id)
            if has_intentional_named_layout_mismatch(function, control_name):
                continue
            layout_key = control_layout_key(
                function=function,
                flow_demand_controlled=flow_demand_controlled,
                control_name=control_name,
            )
            validate_control_layout_for_sync(
                model,
                target_node_id=as_int(target_node_id),
                control_node_id=as_int(control_node_id),
                function=function,
                flow_demand_controlled=flow_demand_controlled,
            )
            variable_rows = variable_df[
                variable_df["node_id"].astype(int).eq(as_int(control_node_id))
                & variable_df["listen_node_id"].astype(int).eq(as_int(listen_node_id))
            ].copy()
            seen_compound_variable_ids = set()
            for variable_row in variable_rows.itertuples(index=False):
                compound_variable_id = as_int(variable_row.compound_variable_id)
                if compound_variable_id in seen_compound_variable_ids:
                    continue
                seen_compound_variable_ids.add(compound_variable_id)

                condition_mask = condition_df["node_id"].astype(int).eq(as_int(control_node_id)) & condition_df[
                    "compound_variable_id"
                ].astype(int).eq(compound_variable_id)
                condition_rows = condition_df.loc[condition_mask].copy()
                if condition_rows.empty:
                    continue
                condition_rows = condition_rows.sort_values("condition_id")

                threshold_values = control_condition_thresholds(
                    layout_key=layout_key,
                    compound_variable_id=compound_variable_id,
                    variable_name=str(variable_row.variable),
                    level_value=as_float(level_value),
                    weight=as_float(variable_row.weight),
                    level_difference_threshold=model_level_difference_threshold(model),
                )
                if len(threshold_values) != len(condition_rows):
                    raise ValueError(
                        f"{layout_key} #{target_node_id} met DiscreteControl #{control_node_id} heeft "
                        f"{len(condition_rows)} conditions voor compound_variable_id {compound_variable_id}, "
                        f"verwacht {len(threshold_values)}."
                    )

                for threshold_value, condition_index in zip(threshold_values, condition_rows.index, strict=True):
                    condition_df.loc[condition_index, "threshold_high"] = as_float(threshold_value)
                    condition_df.loc[condition_index, "threshold_low"] = as_float(threshold_value)
                    update_count += 1

        return update_count

    def sync_once(
        target_node_id: int,
        listen_node_id: int | None,
        level_value: float,
        function: str,
        flow_demand_controlled: bool,
    ) -> None:
        nonlocal condition_update_count
        if flow_demand_controlled or listen_node_id is None or is_missing(level_value):
            return
        key = (as_int(target_node_id), as_int(listen_node_id), round(as_float(level_value), 9))
        if key in synced_conditions:
            return
        synced_conditions.add(key)
        condition_update_count += update_discrete_control_conditions(
            target_node_id=as_int(target_node_id),
            listen_node_id=as_int(listen_node_id),
            level_value=as_float(level_value),
            function=function,
            flow_demand_controlled=flow_demand_controlled,
        )

    for row in min_upstream_updates_df.itertuples(index=False):
        if bool(getattr(row, "flow_demand_controlled", False)) and not bool(
            getattr(row, "limburg_rws_flow_demand_min_upstream", False)
        ):
            continue
        static_df = static_df_by_table[str(row.static_table)]
        static_df.loc[as_int(row.table_fid), "min_upstream_level"] = as_float(row.rws_inlet_profile_min_upstream)
        if is_present(row.upstream_node_id) and is_present(row.upstream_basin_streefpeil):
            sync_once(
                target_node_id=as_int(row.node_id),
                listen_node_id=as_int(row.upstream_node_id),
                level_value=as_float(row.upstream_basin_streefpeil),
                function=str(row.functie),
                flow_demand_controlled=bool(row.flow_demand_controlled),
            )
        min_update_count += 1

    for row in direct_min_upstream_updates_df.itertuples(index=False):
        if bool(getattr(row, "flow_demand_controlled", False)):
            continue
        value = as_float(row.gecheckte_min_upstream_level)
        static_df = static_df_by_table[str(row.static_table)]
        static_df.loc[as_int(row.table_fid), "min_upstream_level"] = value
        control_value = as_float(row.upstream_basin_streefpeil) if is_present(row.upstream_basin_streefpeil) else value
        sync_once(
            target_node_id=as_int(row.node_id),
            listen_node_id=as_int(row.upstream_node_id) if is_present(row.upstream_node_id) else None,
            level_value=control_value,
            function=str(row.functie),
            flow_demand_controlled=bool(row.flow_demand_controlled),
        )
        min_update_count += 1

    for row in max_downstream_updates_df.itertuples(index=False):
        if bool(getattr(row, "flow_demand_controlled", False)):
            continue
        value = as_float(row.gecheckte_max_downstream_level_update)
        static_df = static_df_by_table[str(row.static_table)]
        static_df.loc[as_int(row.table_fid), "max_downstream_level"] = value
        sync_once(
            target_node_id=as_int(row.node_id),
            listen_node_id=as_int(row.downstream_node_id) if is_present(row.downstream_node_id) else None,
            level_value=value,
            function=str(row.functie),
            flow_demand_controlled=bool(row.flow_demand_controlled),
        )
        max_update_count += 1

    for row in protected_controller_updates_df.itertuples(index=False):
        if bool(getattr(row, "flow_demand_controlled", False)):
            continue
        threshold = as_float(row.gecheckte_threshold)
        condition_df = model.discrete_control.condition.df
        if condition_df is None:
            continue
        condition_df.loc[as_int(row.condition_fid), "threshold_high"] = threshold
        condition_df.loc[as_int(row.condition_fid), "threshold_low"] = threshold
        condition_update_count += 1

    if manning_n is not None:
        manning_update_count = set_all_manning_n(model, manning_n)

    assert_protected_apply_table_copies_unchanged(model, protected_table_copies, allowed_static_table_fids_by_table)
    model.write(toml_file)
    return backup_path, min_update_count, max_update_count, condition_update_count, manning_update_count
