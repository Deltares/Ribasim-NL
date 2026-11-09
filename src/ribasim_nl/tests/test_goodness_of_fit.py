import numpy as np
import pandas as pd
import pytest
from ribasim_nl.goodness_of_fit import compare, compare_observations, observed_and_simulated


def test_compare(observed):
    perfect = compare(observed, observed)
    assert perfect == {"days": len(observed), "nse": 1.0, "kge": pytest.approx(1.0), "bias": 0.0}

    biased = compare(observed, 1.5 * observed)
    assert biased["bias"] == pytest.approx(0.5)
    assert biased["kge"] == pytest.approx(1 - np.sqrt(0.5**2 + 0.5**2))
    assert biased["nse"] < 1

    # a constant simulation does not correlate
    constant = compare(observed, pd.Series(observed.mean(), index=observed.index))
    assert constant["nse"] == pytest.approx(0.0, abs=1e-12)
    assert constant["kge"] == pytest.approx(1 - np.sqrt(2))

    # only times with both values count
    short = compare(observed.iloc[:100], observed)
    assert short["days"] == 100
    assert np.isnan(short["nse"]) and np.isnan(short["kge"])


def assert_values_equal(series: pd.Series, expected: pd.Series) -> None:
    """Compare the times and values of two series, also if their times have another resolution."""
    pd.testing.assert_series_equal(series, expected, check_names=False, check_freq=False, check_index_type=False)


def test_observed_and_simulated(observation_run, observed):
    flow, level = observed_and_simulated(*observation_run)
    # the unlinked Observation is left out
    assert (flow.node_id, flow.name, flow.variable, flow.observed_node_id) == (4, "afvoer", "flow_rate", 2)
    assert (level.node_id, level.name, level.variable, level.observed_node_id) == (5, "peil", "level", 1)
    assert_values_equal(flow.observed, observed)
    # the flow rate of the Outlet is that of its outflow link
    assert_values_equal(flow.simulated, observed)
    assert_values_equal(level.simulated, observed + 0.5)


def test_compare_observations(observation_run, observed):
    comparison = compare_observations(*observation_run)
    assert comparison.index.tolist() == [4, 5]
    assert comparison["observed_node_id"].tolist() == [2, 1]
    assert comparison.at[4, "nse"] == 1.0
    assert comparison.at[5, "bias"] == pytest.approx(0.5 / observed.abs().mean())
