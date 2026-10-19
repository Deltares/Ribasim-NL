"""Ribasim-NL-specific validations that supplement Ribasim validation."""

from collections.abc import Iterator
from typing import TYPE_CHECKING

import numpy as np

from ribasim_nl.profiles import MIN_PROFILE_AREA

if TYPE_CHECKING:
    from pandas.core.groupby import DataFrameGroupBy

    from ribasim_nl.model import Model


def _basin_profile_issues(model: "Model") -> list[str]:
    """Return issues with basin profile levels and areas."""
    basin_profile = model.basin.profile.df
    if basin_profile is None:
        return []

    issues = []
    invalid_levels = basin_profile.loc[~np.isfinite(basin_profile["level"].astype(float))]
    if not invalid_levels.empty:
        node_ids = sorted(map(int, invalid_levels["node_id"].unique()))
        issues.append(f"Basin profile levels must be finite; invalid node IDs: {node_ids}")

    invalid_areas = basin_profile.loc[basin_profile["area"] < MIN_PROFILE_AREA]
    if not invalid_areas.empty:
        node_ids = sorted(map(int, invalid_areas["node_id"].unique()))
        issues.append(f"Basin profile areas must be at least {MIN_PROFILE_AREA} m2; invalid node IDs: {node_ids}")
    return issues


def _discrete_control_condition_issues(model: "Model") -> list[str]:
    """Return issues with DiscreteControl condition thresholds."""
    condition = model.discrete_control.condition.df
    if condition is None or condition.empty:
        return []

    invalid_thresholds = condition["threshold_low"].isna() | condition["threshold_high"].isna()
    invalid_thresholds |= condition["threshold_low"] >= condition["threshold_high"]
    if not invalid_thresholds.any():
        return []

    invalid_conditions = condition.loc[invalid_thresholds]
    table_ids = sorted(map(int, invalid_conditions.index))
    node_ids = sorted(map(int, invalid_conditions["node_id"].unique()))
    return [
        "DiscreteControl conditions must have threshold_low < threshold_high; "
        f"invalid node IDs: {node_ids}; condition table IDs: {table_ids}"
    ]


CONTROLLED_COLUMNS = ["flow_rate", "min_flow_rate", "max_flow_rate", "min_upstream_level", "max_downstream_level"]


def _controlled_static_groups(model: "Model") -> Iterator[tuple[str, "DataFrameGroupBy"]]:
    """Yield the static rows with a control state of Pump and Outlet nodes, grouped by node ID."""
    for node_type in ("Pump", "Outlet"):
        static = model.get_component(node_type).static.df
        if static is None or static.empty:
            continue
        yield node_type, static.dropna(subset=["control_state"]).groupby("node_id")


def _partially_missing_control_state_issues(model: "Model") -> list[str]:
    """Return issues with Pump and Outlet parameters that are missing in some control states only.

    Switching to a control state does not update parameters that are missing in it, so such a
    parameter silently keeps the value of the previous control state. Use an explicit value
    instead, such as `math.inf` for no max_downstream_level.
    """
    issues = []
    for node_type, grouped in _controlled_static_groups(model):
        missing = grouped[CONTROLLED_COLUMNS].agg(lambda values: values.isna().mean())
        is_partial = (missing.gt(0) & missing.lt(1)).any(axis=1)
        node_ids = sorted(map(int, is_partial.index[is_partial]))
        if node_ids:
            issues.append(
                f"{node_type} parameters must be given in all control states or in none; invalid node IDs: {node_ids}"
            )
    return issues


def _inert_control_state_issues(model: "Model") -> list[str]:
    """Return issues with Pump and Outlet nodes that behave the same in every control state.

    DiscreteControl has no effect on such nodes, which typically means that the state
    that should switch a node off (`flow_rate` 0) was overwritten.
    """
    issues = []
    for node_type, grouped in _controlled_static_groups(model):
        is_inert = grouped["control_state"].nunique().gt(1)
        is_inert &= grouped[CONTROLLED_COLUMNS].nunique().le(1).all(axis=1)
        node_ids = sorted(map(int, is_inert.index[is_inert]))
        if node_ids:
            issues.append(
                f"{node_type} nodes must differ between control states, "
                f"otherwise DiscreteControl has no effect; invalid node IDs: {node_ids}"
            )
    return issues


def _flow_boundary_forcing_issues(model: "Model") -> list[str]:
    """Return FlowBoundary forcing coverage, duplication and discharge issues."""
    node_table = model.flow_boundary.node
    if node_table is None:
        return []
    nodes = node_table.df
    if nodes is None or nodes.empty:
        return []
    static = model.flow_boundary.static.df
    time = model.flow_boundary.time.df
    static_ids = set() if static is None else set(static["node_id"])
    time_ids = set() if time is None else set(time["node_id"])
    issues = []
    if overlap := static_ids & time_ids:
        issues.append(f"FlowBoundary nodes must not have both static and time forcing; node IDs: {sorted(overlap)}")
    if missing := set(nodes.index) - (static_ids | time_ids):
        issues.append(f"FlowBoundary nodes must have forcing; missing node IDs: {sorted(missing)}")
    for label, frame, keys in [("static", static, ["node_id"]), ("time", time, ["node_id", "time"])]:
        if frame is None or frame.empty:
            continue
        if frame.duplicated(keys).any():
            issues.append(f"FlowBoundary {label} forcing must have unique {keys}")
        rates = frame["flow_rate"].astype(float)
        if not np.isfinite(rates).all() or rates.lt(0).any():
            issues.append(f"FlowBoundary {label} flow rates must be finite and non-negative")
    return issues


def validate_model(model: "Model") -> None:
    """Validate Ribasim-NL conventions and report all issues together."""
    issues = [
        *_basin_profile_issues(model),
        *_discrete_control_condition_issues(model),
        *_partially_missing_control_state_issues(model),
        *_inert_control_state_issues(model),
        *_flow_boundary_forcing_issues(model),
    ]
    if issues:
        raise ValueError("Ribasim-NL model validation failed:\n- " + "\n- ".join(issues))
