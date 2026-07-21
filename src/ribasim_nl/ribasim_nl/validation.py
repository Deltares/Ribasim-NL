"""Ribasim-NL-specific validations that supplement Ribasim validation."""

from typing import TYPE_CHECKING

from ribasim_nl.profiles import MIN_PROFILE_AREA

if TYPE_CHECKING:
    from ribasim_nl.model import Model


def validate_model(model: "Model") -> None:
    """Validate Ribasim-NL conventions that are stricter than the Ribasim schema."""
    basin_profile = model.basin.profile.df
    if basin_profile is None:
        return

    invalid_profiles = basin_profile.loc[basin_profile["area"] < MIN_PROFILE_AREA]
    if invalid_profiles.empty:
        return

    node_ids = sorted(map(int, invalid_profiles["node_id"].unique()))
    msg = f"Basin profile areas must be at least {MIN_PROFILE_AREA} m2; invalid node IDs: {node_ids}"
    raise ValueError(msg)
