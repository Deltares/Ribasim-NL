"""Regression checks for Basin water-level steady state."""

from typing import TYPE_CHECKING

import pandas as pd
import xarray as xr

if TYPE_CHECKING:
    from ribasim_nl.model import Model

# Basins whose level is allowed to vary at the end of a scenario, by their key, see `ribasim_nl.keys`
ALLOWED_NON_STEADY_STATE_KEYS = {
    "DeDommel": {"wet": {"up:DO85-gm1"}},
    # Boezemgemaal Halfweg pumps from the Basin next to it on the level of the Basin in the middle of the boezem. In
    # the wet scenario it pulls that Basin and its storing Basin about 3 cm below the rest of the boezem every other day
    # or so, also with fixed levels. In the dry scenario Inlaatgemaal AGC supplies the boezem up to 0.5 cm above the
    # level from which Halfweg pumps, so the boezem keeps cycling, also with daily DiscreteControl
    "Rijnland": {
        scenario: {"RIJNLANDSBOEZEM_dummy_id_281/285", "bergend:RIJNLANDSBOEZEM_dummy_id_281/285"}
        for scenario in ("dry", "wet")
    },
    "RijnenIJssel": {"wet": {"up:LV80320076___1"}},
    # This Basin of about 4000 m2 receives about 0.35 m3/s from RWZI Arnhem, with one Pump as its only way out. If its
    # off state is chosen, the level rises up to 2 m until DiscreteControl is next evaluated 6 hours later. It also has
    # short dips of 2 cm or more, which fail this check if they land on the second-to-last day
    "Rivierenland": {"wet": {"driel_dummy_id_30"}},
    "StichtseRijnlanden": {"wet": {"up:SL0008"}},
}


def allowed_non_steady_state_node_ids(model: "Model", authority: str | None, scenario: str | None) -> set[int]:
    """Return the node IDs of the approved level-instability exceptions for a model scenario."""
    keys = ALLOWED_NON_STEADY_STATE_KEYS.get(authority or "", {}).get(scenario or "", set())
    return set(model.node_ids(sorted(keys)))


def steady_state_results(
    level: xr.DataArray, storage: xr.DataArray | None = None, *, tolerance: float = 0.01
) -> pd.DataFrame:
    """Return the final-24-hour steady-state result for every Basin.

    A Basin that reaches zero storage at any point in the simulation is not in
    steady state, even when its final water level is constant.
    """
    if not {"time", "node_id"}.issubset(level.dims):
        msg = "Basin level data must have 'time' and 'node_id' dimensions."
        raise ValueError(msg)
    if tolerance < 0:
        msg = "Steady-state tolerance must be non-negative."
        raise ValueError(msg)

    time_index = pd.DatetimeIndex(level["time"].values)
    if time_index.empty:
        msg = "Basin level data contains no time steps."
        raise ValueError(msg)

    last_time = time_index[-1]
    last_day = level.sel(time=slice(last_time - pd.Timedelta(hours=24), last_time))
    result = pd.concat(
        [
            last_day.mean(dim="time").to_series().rename("mean_level"),
            level.sel(time=last_time).to_series().rename("final_level"),
        ],
        axis=1,
    ).reset_index()
    result["level_deviation"] = (result["final_level"] - result["mean_level"]).abs()
    result["level_steady_state"] = result["level_deviation"].le(tolerance) & result["level_deviation"].notna()
    result["has_positive_storage"] = True
    if storage is not None:
        if not {"time", "node_id"}.issubset(storage.dims):
            msg = "Basin storage data must have 'time' and 'node_id' dimensions."
            raise ValueError(msg)
        result["has_positive_storage"] = storage.min(dim="time").to_series().gt(0).reindex(result["node_id"]).to_numpy()
    result["steady_state"] = result["level_steady_state"] & result["has_positive_storage"]
    return result.sort_values("node_id").reset_index(drop=True)
