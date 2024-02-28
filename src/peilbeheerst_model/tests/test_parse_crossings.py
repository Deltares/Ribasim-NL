import pathlib

import numpy as np
import pandas as pd
import pytest

from peilbeheerst_model import ParseCrossings


def _compare_testdata(test_path, check_path, group_stacked=True, filterlayer=None):
    assert test_path.exists()
    assert check_path.exists()

    # Find the crossings
    cross = ParseCrossings(test_path, disable_progress=True)
    res = cross.find_crossings_with_peilgebieden(
        "hydroobject", group_stacked=group_stacked, filterlayer=filterlayer
    )
    if filterlayer is None:
        df_crossings = res
    else:
        _, _, df_crossings = res

    # Reshape dataframe with static test results to be in the same form as
    # the returned crossings.
    test_output = df_crossings[df_crossings.in_use].copy()
    test_output["geom_x"] = np.round(test_output.geometry.x, 8)
    test_output["geom_y"] = np.round(test_output.geometry.y, 8)
    test_output = test_output.drop(columns="geometry", inplace=False)
    test_output = test_output.set_index(["geom_x", "geom_y"]).sort_index()
    test_output = test_output.fillna(pd.NA).drop(columns="match_group")

    # Read the expected output
    check_output = pd.read_csv(check_path, dtype=test_output.dtypes.to_dict())
    check_output = check_output.set_index(["geom_x", "geom_y"]).sort_index()
    check_output = check_output.fillna(pd.NA).drop(columns="match_group")

    # Assert if the data is the same
    assert test_output.equals(check_output)

@pytest.mark.parametrize("gpkg_path", list(map(str, pathlib.Path("tests/data").glob("nofilter_*.gpkg"))))
def test_stacked_nofilter(gpkg_path):
    test_path = pathlib.Path(gpkg_path)
    check_path = test_path.parent.joinpath(f"output_{test_path.stem}.csv")
    _compare_testdata(test_path, check_path, group_stacked=True, filterlayer=None)

@pytest.mark.parametrize("gpkg_path", list(map(str, pathlib.Path("tests/data").glob("nofilter_*.gpkg"))))
def test_stacked_emptyfilter(gpkg_path):
    test_path = pathlib.Path(gpkg_path)
    check_path = test_path.parent.joinpath(f"output_{test_path.stem}.csv")
    _compare_testdata(test_path, check_path, group_stacked=True, filterlayer="duikersifonhevel")

@pytest.mark.parametrize("gpkg_path", list(map(str, pathlib.Path("tests/data").glob("withfilter_*.gpkg"))))
def test_stacked_withfilter(gpkg_path):
    test_path = pathlib.Path(gpkg_path)
    check_path = test_path.parent.joinpath(f"output_{test_path.stem}.csv")
    _compare_testdata(test_path, check_path, group_stacked=True, filterlayer="duikersifonhevel")

# TODO: test structures?

# TODO: test double links?

# TODO: test aggregate_identical_links?

