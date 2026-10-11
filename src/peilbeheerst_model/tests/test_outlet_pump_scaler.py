import pandas as pd
import pytest
from peilbeheerst_model.outlet_pump_scaler import last_result_time


@pytest.mark.parametrize(
    ("endtime", "saveat", "expected"),
    [
        ("2018-01-01", 86400, "2017-12-31"),
        ("2017-01-01 12:00", 3600, "2017-01-01 11:00"),
        # a partial last interval is saved too
        ("2017-01-02 12:00", 86400, "2017-01-02"),
    ],
)
def test_last_result_time(endtime, saveat, expected):
    starttime = pd.Timestamp("2017-01-01")
    assert last_result_time(starttime, pd.Timestamp(endtime), saveat) == pd.Timestamp(expected)


def test_last_result_time_invalid():
    with pytest.raises(AssertionError):
        last_result_time(pd.Timestamp("2017-01-01"), pd.Timestamp("2017-01-01"), 86400)
