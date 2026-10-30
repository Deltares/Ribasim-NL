import numpy as np
import pandas as pd
import pytest
from ribasim_nl.control_diagnostics import mean_level_change, oscillating_basins


@pytest.fixture
def level() -> pd.DataFrame:
    """Basin 1 is constant, Basin 2 alternates by 0.1 m, Basin 3 by 0.02 m every day."""
    time = pd.date_range("2017-01-01", periods=5, freq="D")
    alternating = np.array([0, 1, 0, 1, 0], dtype=float)
    return pd.DataFrame({1: 0.0, 2: 0.1 * alternating, 3: 0.02 * alternating}, index=time)


def test_mean_level_change(level):
    assert mean_level_change(level).to_dict() == pytest.approx({1: 0.0, 2: 0.1, 3: 0.02})


def test_mean_level_change_per_day(level):
    # the same changes over two days are half as fast
    level.index = pd.date_range("2017-01-01", periods=5, freq="2D")
    assert mean_level_change(level)[2] == pytest.approx(0.05)


def test_oscillating_basins(level):
    assert oscillating_basins(level).index.tolist() == [2, 3]
    assert oscillating_basins(level, min_change=0.05).index.tolist() == [2]


def test_oscillating_basins_reference(level):
    reference = level.copy()
    reference[3] = 0.0
    # Basin 2 oscillates as much in the reference
    result = oscillating_basins(level, reference)
    assert result.index.tolist() == [3]
    assert result.at[3, "reference_level_change"] == 0.0
