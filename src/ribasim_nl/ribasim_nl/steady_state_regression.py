"""Regression checks for Basin water-level steady state."""

import pandas as pd
import xarray as xr

ALLOWED_NON_STEADY_STATE_NODE_IDS = {
    "DeDommel": {"wet": {1708}},
    "RijnenIJssel": {"wet": {835}},
    "StichtseRijnlanden": {"wet": {1877}},
}


def allowed_non_steady_state_node_ids(authority: str | None, scenario: str | None) -> set[int]:
    """Return approved level-instability exceptions for a model scenario."""
    return ALLOWED_NON_STEADY_STATE_NODE_IDS.get(authority, {}).get(scenario, set())


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
