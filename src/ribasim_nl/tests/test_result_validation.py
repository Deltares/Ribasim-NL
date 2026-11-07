import numpy as np
import pandas as pd
import pytest
import xarray as xr
from ribasim_nl.goodness_of_fit import compare_observations
from ribasim_nl.result_validation import (
    Limit,
    key_discharge_metrics,
    observation_metrics,
    result_issues,
    solver_metrics,
    validate_results,
)


def test_limit():
    limit = Limit(min=0.0, max=1.0)
    assert limit.issue("x", 0.5) is None
    assert limit.issue("x", -1.0) == "x is -1, below the minimum of 0"
    assert limit.issue("x", 2.0) == "x is 2, above the maximum of 1"
    assert limit.issue("x", np.nan) == "x is nan"
    with pytest.raises(AssertionError, match="Empty limit"):
        Limit(min=1.0, max=1.0)


def test_solver_metrics():
    stats = xr.Dataset(
        {
            "computation_time": ("time", [3_600_000.0, 1_800_000.0]),
            "rhs_calls": ("time", [100, 300]),
            "accepted_timesteps": ("time", [10, 30]),
            "rejected_timesteps": ("time", [0, 10]),
            "dt": ("time", [8640.0, 2880.0]),
        },
        coords={"time": pd.date_range("2020-01-01", periods=2)},
    )
    metrics = solver_metrics(stats)
    assert metrics["computation_time"] == 1.5
    assert metrics["rhs_calls"] == 400
    assert metrics["mean_dt"] == 2 * 86400 / 40
    assert metrics["rejected_fraction"] == 10 / 50


def test_observation_metrics(observation_run):
    comparison = compare_observations(*observation_run)
    metrics = observation_metrics(comparison)
    assert metrics["compared"] == 2
    assert metrics["median_nse"] == pytest.approx(comparison["nse"].mean())
    # a missing key discharge has no KGE, which fails its limit
    assert np.isnan(key_discharge_metrics(comparison)["Lobith"])


def test_validate_results():
    limits = {"solver": {"rhs_calls": Limit(max=10.0), "mean_dt": Limit(min=1.0)}, "level_p99": {"IJsselmeer": Limit()}}
    metrics = {"solver": {"rhs_calls": 5.0, "mean_dt": 2.0}, "level_p99": {"IJsselmeer": 0.0}}
    assert result_issues(metrics, limits) == []
    validate_results(metrics, limits)

    metrics = {"solver": {"rhs_calls": 20.0}, "level_p99": {"IJsselmeer": 0.0}}
    assert result_issues(metrics, limits) == [
        "solver rhs_calls is 20, above the maximum of 10",
        "solver mean_dt is nan",
    ]
    with pytest.raises(ValueError, match="result validation failed"):
        validate_results(metrics, limits)
