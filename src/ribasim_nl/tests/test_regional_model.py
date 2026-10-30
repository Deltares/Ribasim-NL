import numpy as np
import pandas as pd
import xarray as xr
from ribasim_nl.regional_model import ENDTIME, LHM_DAY_START, STARTTIME, read_lhm_budgets


def test_read_lhm_budgets(tmp_path):
    """The LHM days start at LHM_DAY_START, and the day before the start time covers its first hours."""
    time = pd.date_range("2016-12-01", "2020-02-01", freq="D", name="time")
    budgets = xr.Dataset({"precipitation_mmd": ("time", np.arange(len(time), dtype=float))}, coords={"time": time})
    budgets.to_zarr(tmp_path / "budgets.zarr")

    shifted = read_lhm_budgets(tmp_path / "budgets.zarr")
    start = pd.Timestamp(STARTTIME)
    shifted_time = shifted.indexes["time"]
    assert shifted_time[0] == start - pd.Timedelta(days=1) + LHM_DAY_START
    assert shifted_time[0] < start
    assert shifted_time[-1] < pd.Timestamp(ENDTIME)
    # the values stay with their LHM day
    assert shifted["precipitation_mmd"].sel(time=start + LHM_DAY_START).values == time.get_loc(start)
