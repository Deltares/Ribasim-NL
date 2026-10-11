"""Find Basins whose level oscillates, for instance due to DiscreteControl that is evaluated only once a day."""

import numpy as np
import pandas as pd


def mean_level_change(level: pd.DataFrame) -> pd.Series:
    """Get the mean absolute level change per day of each Basin.

    :param level: Basin levels [m], with a time index and a column per Basin node ID
    :return: mean absolute level change [m/d], indexed by Basin node ID
    """
    assert isinstance(level.index, pd.DatetimeIndex), "Expected a time index"
    assert len(level) > 1, "Expected at least two time steps"
    days = level.index.to_series().diff().dt.total_seconds().to_numpy()[1:] / 86400
    assert (days > 0).all(), "Expected an increasing time index"
    change = level.diff().iloc[1:].abs().div(days, axis=0)
    return change.mean().rename("level_change")


def oscillating_basins(
    level: pd.DataFrame,
    reference_level: pd.DataFrame | None = None,
    min_change: float = 0.01,
    min_ratio: float = 3.0,
) -> pd.DataFrame:
    """Get the Basins whose level changes more than `min_change` per day on average.

    With a reference, for instance a run with DiscreteControl evaluated every time step, only Basins whose level
    changes at least `min_ratio` times more than in the reference are returned.

    :param level: Basin levels [m], with a time index and a column per Basin node ID
    :param reference_level: Basin levels [m] of the reference run, in the same format
    :param min_change: minimum mean absolute level change [m/d]
    :param min_ratio: minimum ratio of the mean absolute level change to that of the reference
    :return: mean absolute level change [m/d] and that of the reference, indexed by Basin node ID, largest first
    """
    assert min_change >= 0 and min_ratio >= 1, f"Invalid thresholds {min_change=} {min_ratio=}"
    result = mean_level_change(level).to_frame()
    selected = result["level_change"] > min_change
    if reference_level is not None:
        result["reference_level_change"] = mean_level_change(reference_level).reindex(result.index)
        selected &= result["level_change"] > min_ratio * result["reference_level_change"].fillna(np.inf)
    return result[selected].sort_values("level_change", ascending=False)
