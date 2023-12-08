import pandas as pd
from ribasim_nl.tables import average_width, cumulative_area, manning_profile


def test_interpolation_simple():
    df_left = pd.DataFrame({"level": [1, 2, 3, 6], "width": [10, 20, 30, 60]})
    df_right = pd.DataFrame({"level": [2, 4], "width": [20, 40]})

    # combine two width-tables
    df = average_width(df_left, df_right)
    assert df.width.to_list() == [10.0, 20.0, 30.0, 40.0, 60.0]

    # compute cumulative areas
    area = cumulative_area(df)
    assert area.to_list() == [0.0, 15.0, 40.0, 75.0, 175.0]

    # calculate manning_profile
    profile_width, profile_slope = manning_profile(df)
    assert profile_width == 10.0
    assert profile_slope == 5.0
