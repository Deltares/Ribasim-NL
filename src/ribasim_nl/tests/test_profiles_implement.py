import numpy as np
import pandas as pd
import pytest
from ribasim_nl.parametrization.manning_resistance_table import calculate_flow_rate
from ribasim_nl.profiles.implement import BERGEND_REFERENCE_AREA, bergend_manning_static
from scipy.optimize import brentq


def test_bergend_manning_static_scaling():
    area = pd.Series([BERGEND_REFERENCE_AREA, 4 * BERGEND_REFERENCE_AREA], index=[7, 3])
    static = bergend_manning_static(area).set_index("node_id")
    assert static.index.tolist() == [7, 3]
    # the cross section scales with the area, the length is fixed
    assert static.at[3, "profile_width"] == pytest.approx(4 * static.at[7, "profile_width"])
    assert static.at[3, "profile_slope"] == pytest.approx(4 * static.at[7, "profile_slope"])
    assert static.at[3, "length"] == static.at[7, "length"]
    assert (static["manning_n"] == 0.03).all()


def head_difference(area: float, depth: float, discharge: float = 0.01 / 86400) -> float:
    """Head difference [m] over the bergend ManningResistance at a discharge [m/s] over the basin area."""
    static = bergend_manning_static(pd.Series([area], index=[1])).iloc[0]

    def residual(dh: float) -> float:
        slope = dh / static["length"]
        flow_rate = calculate_flow_rate(
            depth, static["profile_width"], static["profile_slope"], static["manning_n"], slope
        )
        return flow_rate - area * discharge

    return brentq(residual, 1e-12, 10.0)


@pytest.mark.parametrize("depth", [0.25, 0.5, 1.0])
def test_bergend_manning_static_head_difference(depth):
    """The head difference at a given specific discharge barely depends on the basin area, above 30 ha."""
    dh = [head_difference(area * 1e4, depth) for area in (30, 100, 1000, 10000)]
    assert max(dh) / min(dh) < 1.4


def test_bergend_manning_static_head_difference_reference():
    # a basin of the reference area at a depth of 0.5 m and a discharge of 10 mm/d
    assert 3e-3 < head_difference(BERGEND_REFERENCE_AREA, 0.5) < 1e-2


@pytest.mark.parametrize("area", [[0.0], [np.nan], [-1.0]])
def test_bergend_manning_static_invalid_area(area):
    with pytest.raises(AssertionError):
        bergend_manning_static(pd.Series(area, index=[1]))
