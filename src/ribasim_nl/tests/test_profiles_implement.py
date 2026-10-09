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
    # width scales with the area, length with its square root
    assert static.at[3, "profile_width"] == pytest.approx(4 * static.at[7, "profile_width"])
    assert static.at[3, "length"] == pytest.approx(2 * static.at[7, "length"])
    assert (static["manning_n"] == 0.03).all()


def test_bergend_manning_static_head_difference():
    """A basin of the reference area has a head difference of about 0.5 mm at a discharge of 10 mm/d."""
    static = bergend_manning_static(pd.Series([BERGEND_REFERENCE_AREA], index=[1])).iloc[0]
    flow_rate = BERGEND_REFERENCE_AREA * 0.01 / 86400

    def residual(dh: float) -> float:
        slope = dh / static["length"]
        q = calculate_flow_rate(1.0, static["profile_width"], static["profile_slope"], static["manning_n"], slope)
        return q - flow_rate

    dh = brentq(residual, 1e-9, 1.0)
    assert 0.3e-3 < dh < 1e-3


@pytest.mark.parametrize("area", [[0.0], [np.nan], [-1.0]])
def test_bergend_manning_static_invalid_area(area):
    with pytest.raises(AssertionError):
        bergend_manning_static(pd.Series(area, index=[1]))
