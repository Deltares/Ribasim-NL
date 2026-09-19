"""Ribasim-NL-specific validations that supplement Ribasim validation."""

from typing import TYPE_CHECKING

from ribasim_nl.profiles import MIN_PROFILE_AREA

if TYPE_CHECKING:
    from ribasim_nl.model import Model


def _basin_profile_issues(model: "Model") -> list[str]:
    """Return issues with basin profile areas."""
    basin_profile = model.basin.profile.df
    if basin_profile is None:
        return []

    invalid_profiles = basin_profile.loc[basin_profile["area"] < MIN_PROFILE_AREA]
    if invalid_profiles.empty:
        return []

    node_ids = sorted(map(int, invalid_profiles["node_id"].unique()))
    return [f"Basin profile areas must be at least {MIN_PROFILE_AREA} m2; invalid node IDs: {node_ids}"]


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


def validate_model(model: "Model") -> None:
    """Validate Ribasim-NL conventions and report all issues together."""
    issues = [*_basin_profile_issues(model), *_discrete_control_condition_issues(model)]
    if issues:
        raise ValueError("Ribasim-NL model validation failed:\n- " + "\n- ".join(issues))
