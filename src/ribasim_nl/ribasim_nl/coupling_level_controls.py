"""DiscreteControl checks for coupling-level updates."""

import numpy as np
import pandas as pd

from ribasim_nl import Model
from ribasim_nl.control_layout import control_condition_thresholds, control_layout_key, control_layouts
from ribasim_nl.coupling_level_common import (
    CONTROL_NODE_TYPES,
    LEVEL_UPDATE_PROTECTION_COLUMN,
    STATIC_TABLE_BY_NODE_TYPE,
    as_float,
    as_int,
    classify_functions,
    is_missing,
    is_present,
    model_level_difference_threshold,
    reset_index_to_column,
    static_row_has_capacity,
    truthy,
)


def grouped_compound_counts(df: pd.DataFrame | None, control_node_id: int) -> dict[int, int]:
    """Count rows per compound_variable_id for one DiscreteControl node."""
    if df is None or df.empty or "compound_variable_id" not in df.columns:
        return {}
    rows = df[df["node_id"].astype(int).eq(as_int(control_node_id))]
    if rows.empty:
        return {}
    counts: dict[int, int] = {}
    for compound_variable_id, count in rows.groupby("compound_variable_id").size().items():
        if is_missing(compound_variable_id):
            continue
        counts[as_int(compound_variable_id)] = as_int(count)
    return counts


def logic_pairs(df: pd.DataFrame | None, control_node_id: int) -> set[tuple[str, str]]:
    """Read the truth_state/control_state pairs for one DiscreteControl node."""
    if df is None or df.empty:
        return set()
    rows = df[df["node_id"].astype(int).eq(as_int(control_node_id))]
    return {(str(row.truth_state), str(row.control_state).lower()) for row in rows.itertuples(index=False)}


def explicit_control_layout_from_name(control_name: str | None) -> str | None:
    """Read an explicit layout prefix from a DiscreteControl name."""
    if control_name is None or ":" not in control_name:
        return None
    layout_key = control_name.split(":", 1)[0].strip().lower()
    return layout_key if layout_key in control_layouts() else None


def has_intentional_named_layout_mismatch(function: str, control_name: str | None) -> bool:
    """Check whether a named controller intentionally uses another layout."""
    layout_key = explicit_control_layout_from_name(control_name)
    if layout_key is None:
        return False
    return layout_key != str(function).lower()


def validate_control_layout_for_sync(
    model: Model,
    *,
    target_node_id: int,
    control_node_id: int,
    function: str,
    flow_demand_controlled: bool,
) -> None:
    """Fail when a DiscreteControl does not match the expected control.py layout."""
    control_name = None
    node_df = model.node.df
    if node_df is not None:
        control_node_id = as_int(control_node_id)
        if control_node_id in node_df.index:
            name = node_df.at[control_node_id, "name"]
            control_name = None if is_missing(name) else str(name)
    layout_key = control_layout_key(
        function=function,
        flow_demand_controlled=flow_demand_controlled,
        control_name=control_name,
    )
    expected_layout = control_layouts().get(layout_key)
    if expected_layout is None:
        return

    expected_variable_counts, expected_condition_counts, expected_logic_pairs = expected_layout
    variable_counts = grouped_compound_counts(model.discrete_control.variable.df, control_node_id)
    condition_counts = grouped_compound_counts(model.discrete_control.condition.df, control_node_id)
    found_logic_pairs = logic_pairs(model.discrete_control.logic.df, control_node_id)
    if (
        variable_counts != expected_variable_counts
        or condition_counts != expected_condition_counts
        or found_logic_pairs != expected_logic_pairs
    ):
        raise ValueError(
            f"{layout_key} #{target_node_id} met DiscreteControl #{control_node_id} heeft geen control.py-layout: "
            f"variables={variable_counts} verwacht={expected_variable_counts}; "
            f"conditions={condition_counts} verwacht={expected_condition_counts}; "
            f"logic={sorted(found_logic_pairs)} verwacht={sorted(expected_logic_pairs)}; "
            f"static_functie={function}; control_naam={control_name}"
        )


def protected_static_level_for_condition(
    static_rows: pd.DataFrame,
    layout_key: str,
    compound_variable_id: int,
) -> tuple[float | None, str | None]:
    """Get the protected static level that should drive one condition."""
    return static_level_for_condition(
        static_rows=static_rows,
        layout_key=layout_key,
        compound_variable_id=compound_variable_id,
        protected_only=True,
    )


def static_level_for_condition(
    static_rows: pd.DataFrame,
    layout_key: str,
    compound_variable_id: int,
    *,
    protected_only: bool = False,
) -> tuple[float | None, str | None]:
    """Get the static level that should drive one control.py condition."""
    if static_rows.empty:
        return None, None
    if protected_only and LEVEL_UPDATE_PROTECTION_COLUMN not in static_rows.columns:
        return None, None

    rows = static_rows.copy()
    rows["control_state_lower"] = rows["control_state"].astype("string").str.lower()
    rows["has_capacity"] = rows.apply(static_row_has_capacity, axis=1)
    layout_key = str(layout_key).lower()
    if protected_only:
        rows["level_update_protected"] = rows[LEVEL_UPDATE_PROTECTION_COLUMN].map(truthy)
        rows = rows[rows["level_update_protected"]].copy()

    if layout_key == "inlaat" and compound_variable_id == 1:
        candidates = rows[
            rows["control_state_lower"].eq("aanvoer")
            & rows["has_capacity"]
            & pd.to_numeric(rows["max_downstream_level"], errors="coerce").notna()
        ]
        column = "max_downstream_level"
        basis = "protected_aanvoer_max_downstream_level" if protected_only else "static_aanvoer_max_downstream_level"
    elif (layout_key == "uitlaat" and compound_variable_id == 1) or (
        layout_key == "doorlaat" and compound_variable_id == 1
    ):
        candidates = rows[
            rows["control_state_lower"].eq("afvoer")
            & rows["has_capacity"]
            & pd.to_numeric(rows["min_upstream_level"], errors="coerce").notna()
        ]
        column = "min_upstream_level"
        basis = "protected_afvoer_min_upstream_level" if protected_only else "static_afvoer_min_upstream_level"
    elif layout_key == "doorlaat" and compound_variable_id == 2:
        candidates = rows[
            rows["control_state_lower"].eq("aanvoer")
            & rows["has_capacity"]
            & pd.to_numeric(rows["max_downstream_level"], errors="coerce").notna()
        ]
        column = "max_downstream_level"
        basis = "protected_aanvoer_max_downstream_level" if protected_only else "static_aanvoer_max_downstream_level"
    else:
        return None, None

    values = pd.to_numeric(candidates[column], errors="coerce").dropna()
    if values.empty:
        return None, None
    return as_float(values.iloc[0]), basis


def static_level_layout_key(layout_key: str, function: str) -> str:
    """Use the actual static function when a coupled single-state controller changed function."""
    layout_key = str(layout_key).lower()
    function = str(function).lower()
    if layout_key in {"inlaat", "uitlaat"} and function in {"inlaat", "uitlaat"}:
        return function
    return layout_key


def prefer_matching_listen_node(
    candidates: pd.DataFrame,
    listen_node_id: int | None,
    columns: list[str],
) -> pd.DataFrame:
    """Prefer candidate rows that match the current listen_node_id."""
    if candidates.empty or listen_node_id is None:
        return candidates

    for column in columns:
        if column not in candidates.columns:
            continue
        ids = pd.to_numeric(candidates[column], errors="coerce")
        matched = candidates.loc[ids.eq(as_int(listen_node_id))]
        if not matched.empty:
            return matched
    return candidates


def checked_level_for_condition(
    level_df: pd.DataFrame,
    target_node_id: int,
    listen_node_id: int | None,
    layout_key: str,
    compound_variable_id: int,
) -> tuple[float | None, str | None]:
    """Get the checked coupling level that should drive one condition."""
    if level_df.empty:
        return None, None

    rows = level_df[level_df["node_id"].astype(int).eq(as_int(target_node_id))].copy()
    if rows.empty:
        return None, None

    rows["control_state_lower"] = rows["control_state"].astype("string").str.lower()
    rows["has_capacity"] = rows.apply(static_row_has_capacity, axis=1)
    layout_key = str(layout_key).lower()

    if layout_key == "inlaat" and compound_variable_id == 1:
        candidates = rows[
            rows["control_state_lower"].eq("aanvoer")
            & rows["has_capacity"]
            & rows["max_downstream_is_coupling_link"].fillna(False)
            & pd.to_numeric(rows["gecheckte_max_downstream_level"], errors="coerce").notna()
        ]
        candidates = prefer_matching_listen_node(
            candidates,
            listen_node_id,
            ["max_downstream_level_basin_id", "downstream_node_id"],
        )
        column = "gecheckte_max_downstream_level"
        basis = "coupling_aanvoer_max_downstream_level"
    elif (layout_key == "uitlaat" and compound_variable_id == 1) or (
        layout_key == "doorlaat" and compound_variable_id == 1
    ):
        candidates = rows[
            rows["control_state_lower"].eq("afvoer")
            & rows["has_capacity"]
            & rows["min_upstream_is_coupling_link"].fillna(False)
            & pd.to_numeric(rows["gecheckte_min_upstream_level"], errors="coerce").notna()
        ]
        candidates = prefer_matching_listen_node(candidates, listen_node_id, ["upstream_node_id"])
        column = "gecheckte_min_upstream_level"
        basis = "coupling_afvoer_min_upstream_level"
    elif layout_key == "doorlaat" and compound_variable_id == 2:
        candidates = rows[
            rows["control_state_lower"].eq("aanvoer")
            & rows["has_capacity"]
            & rows["max_downstream_is_coupling_link"].fillna(False)
            & pd.to_numeric(rows["gecheckte_max_downstream_level"], errors="coerce").notna()
        ]
        candidates = prefer_matching_listen_node(
            candidates,
            listen_node_id,
            ["max_downstream_level_basin_id", "downstream_node_id"],
        )
        column = "gecheckte_max_downstream_level"
        basis = "coupling_aanvoer_max_downstream_level"
    else:
        return None, None

    values = pd.to_numeric(candidates[column], errors="coerce").dropna()
    if values.empty:
        return None, None
    return as_float(values.iloc[0]), basis


def protected_controller_threshold_updates(
    model: Model,
    level_df: pd.DataFrame,
    tolerance: float,
    static_level_sync_node_ids: set[int] | None = None,
) -> pd.DataFrame:
    """Find coupling/protected DiscreteControl thresholds that need syncing."""
    assert model.node.df is not None
    assert model.link.df is not None
    static_level_sync_node_ids = static_level_sync_node_ids or set()

    node_df = reset_index_to_column(model.node.df.copy(), "node_id")
    link_df = reset_index_to_column(model.link.df.copy(), "link_id")
    variable_df = model.discrete_control.variable.df
    condition_df = model.discrete_control.condition.df
    if variable_df is None or condition_df is None:
        return pd.DataFrame()

    variable_df = variable_df.copy()
    condition_df = reset_index_to_column(condition_df.copy(), "condition_fid")
    static_dfs = []
    for node_type, table in STATIC_TABLE_BY_NODE_TYPE.items():
        static_table = model.get_component(node_type).static.df
        if static_table is None:
            continue
        static_df = reset_index_to_column(static_table.copy(), "table_fid")
        static_df["node_type"] = node_type
        static_df["static_table"] = table
        static_dfs.append(static_df)

    if not static_dfs:
        return pd.DataFrame()

    static_df = pd.concat(static_dfs, ignore_index=True)
    if LEVEL_UPDATE_PROTECTION_COLUMN not in static_df.columns:
        static_df[LEVEL_UPDATE_PROTECTION_COLUMN] = False

    node_type_by_id = {
        as_int(node_id): str(node_type)
        for node_id, node_type in node_df.set_index("node_id")["node_type"].to_dict().items()
    }
    node_name_by_id = (
        {as_int(node_id): name for node_id, name in node_df.set_index("node_id")["name"].to_dict().items()}
        if "name" in node_df.columns
        else {}
    )
    node_authority_by_id = (
        {
            as_int(node_id): authority
            for node_id, authority in node_df.set_index("node_id")["meta_waterbeheerder"].to_dict().items()
        }
        if "meta_waterbeheerder" in node_df.columns
        else {}
    )
    node_meta_id_by_id = (
        {
            as_int(node_id): meta_id
            for node_id, meta_id in node_df.set_index("node_id")["meta_node_id_waterbeheerder"].to_dict().items()
        }
        if "meta_node_id_waterbeheerder" in node_df.columns
        else {}
    )
    flow_demand_links = link_df[
        link_df["link_type"].fillna("").eq("control")
        & link_df["from_node_id"].map(node_type_by_id).eq("FlowDemand")
        & link_df["to_node_id"].map(node_type_by_id).isin(CONTROL_NODE_TYPES)
    ]
    flow_demand_target_node_ids = set(flow_demand_links["to_node_id"].dropna().astype(int))
    functions = classify_functions(static_df=static_df, flow_demand_inlet_nodes=flow_demand_target_node_ids)

    control_links = link_df[
        link_df["link_type"].fillna("").eq("control")
        & link_df["from_node_id"].map(node_type_by_id).eq("DiscreteControl")
        & link_df["to_node_id"].map(node_type_by_id).isin(CONTROL_NODE_TYPES)
    ]

    records = []
    for control_link in control_links.itertuples(index=False):
        control_node_id = as_int(control_link.from_node_id)
        target_node_id = as_int(control_link.to_node_id)
        sync_from_static_level = target_node_id in static_level_sync_node_ids
        if target_node_id in flow_demand_target_node_ids:
            continue
        if not sync_from_static_level and is_missing(node_authority_by_id.get(target_node_id)):
            continue
        if not sync_from_static_level and is_missing(node_meta_id_by_id.get(target_node_id)):
            continue

        control_name = node_name_by_id.get(control_node_id)
        function = functions.get(target_node_id, "")
        target_authority = node_authority_by_id.get(target_node_id)
        control_authority = node_authority_by_id.get(control_node_id)
        is_coupled_control = (
            is_present(target_authority)
            and is_present(control_authority)
            and str(target_authority) != str(control_authority)
        )
        named_layout_mismatch = has_intentional_named_layout_mismatch(function, control_name)
        if named_layout_mismatch and not (is_coupled_control or sync_from_static_level):
            continue
        layout_key = control_layout_key(
            function=function,
            flow_demand_controlled=False,
            control_name=None if is_missing(control_name) else str(control_name),
        )
        if layout_key not in control_layouts():
            continue

        target_static_rows = static_df[static_df["node_id"].astype(int).eq(target_node_id)]
        has_protected_static = target_static_rows[LEVEL_UPDATE_PROTECTION_COLUMN].map(truthy).any()
        variable_rows = variable_df[variable_df["node_id"].astype(int).eq(control_node_id)].copy()
        for variable_row in variable_rows.itertuples(index=False):
            compound_variable_id = as_int(variable_row.compound_variable_id)
            variable_name = str(variable_row.variable)
            if variable_name.lower() != "level":
                continue

            listen_node_id = as_int(variable_row.listen_node_id)
            listen_authority = node_authority_by_id.get(listen_node_id)
            is_coupled_condition = (
                is_present(target_authority)
                and is_present(listen_authority)
                and str(target_authority) != str(listen_authority)
            )
            source_layout_key = static_level_layout_key(layout_key, function)
            if sync_from_static_level:
                level_value, update_basis = static_level_for_condition(
                    static_rows=target_static_rows,
                    layout_key=source_layout_key,
                    compound_variable_id=compound_variable_id,
                )
            elif is_coupled_condition:
                level_value, update_basis = checked_level_for_condition(
                    level_df=level_df,
                    target_node_id=target_node_id,
                    listen_node_id=listen_node_id,
                    layout_key=layout_key,
                    compound_variable_id=compound_variable_id,
                )
                if level_value is None and is_coupled_control:
                    level_value, update_basis = static_level_for_condition(
                        static_rows=target_static_rows,
                        layout_key=source_layout_key,
                        compound_variable_id=compound_variable_id,
                    )
            elif has_protected_static:
                level_value, update_basis = protected_static_level_for_condition(
                    static_rows=target_static_rows,
                    layout_key=layout_key,
                    compound_variable_id=compound_variable_id,
                )
            elif is_coupled_control:
                level_value, update_basis = static_level_for_condition(
                    static_rows=target_static_rows,
                    layout_key=source_layout_key,
                    compound_variable_id=compound_variable_id,
                )
            else:
                continue
            if level_value is None:
                continue

            condition_rows = condition_df[
                condition_df["node_id"].astype(int).eq(control_node_id)
                & condition_df["compound_variable_id"].astype(int).eq(compound_variable_id)
            ].sort_values("condition_id")
            threshold_values = control_condition_thresholds(
                layout_key=layout_key,
                compound_variable_id=compound_variable_id,
                variable_name=variable_name,
                level_value=as_float(level_value),
                weight=as_float(variable_row.weight),
                level_difference_threshold=model_level_difference_threshold(model),
            )
            if len(threshold_values) != len(condition_rows):
                continue

            for threshold_value, condition_row in zip(
                threshold_values, condition_rows.itertuples(index=False), strict=True
            ):
                current_high = (
                    as_float(condition_row.threshold_high) if is_present(condition_row.threshold_high) else np.nan
                )
                current_low = (
                    as_float(condition_row.threshold_low) if is_present(condition_row.threshold_low) else np.nan
                )
                if (
                    pd.notna(current_high)
                    and pd.notna(current_low)
                    and np.isclose(current_high, as_float(threshold_value), atol=tolerance)
                    and np.isclose(current_low, as_float(threshold_value), atol=tolerance)
                ):
                    continue

                records.append(
                    {
                        "node_id": target_node_id,
                        "node_type": node_type_by_id.get(target_node_id),
                        "functie": function,
                        "meta_waterbeheerder": node_authority_by_id.get(target_node_id),
                        "meta_node_id_waterbeheerder": node_meta_id_by_id.get(target_node_id),
                        "control_node_id": control_node_id,
                        "control_name": control_name,
                        "condition_fid": as_int(condition_row.condition_fid),
                        "compound_variable_id": compound_variable_id,
                        "condition_id": as_int(condition_row.condition_id),
                        "listen_node_id": listen_node_id,
                        "listen_node_authority": listen_authority,
                        "is_coupled_condition": bool(is_coupled_condition),
                        "variable": variable_name,
                        "weight": as_float(variable_row.weight),
                        "protected_static_level": np.nan if is_coupled_condition else as_float(level_value),
                        "coupling_checked_level": as_float(level_value) if is_coupled_condition else np.nan,
                        "threshold_level": as_float(level_value),
                        "threshold_update_basis": update_basis,
                        "huidig_threshold_high": current_high,
                        "huidig_threshold_low": current_low,
                        "gecheckte_threshold": as_float(threshold_value),
                    }
                )

    return pd.DataFrame(records)
