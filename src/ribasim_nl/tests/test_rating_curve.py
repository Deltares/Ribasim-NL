import pandas as pd
from ribasim_nl.rating_curve import flow_distribution_by_level


def test_flow_distribution_by_level_interpolates_and_extrapolates():
    series_a = pd.Series([0.0, 10.0, 20.0], index=pd.Index([0.0, 1.0, 2.0], name="H"), name="a")
    series_b = pd.Series([5.0, 15.0], index=pd.Index([0.5, 1.5], name="H"), name="b")

    result = flow_distribution_by_level([series_a, series_b])

    expected = pd.DataFrame(
        {
            "a": [0.0, 0.5, 0.5, 0.5, 20.0 / 35.0],
            "b": [1.0, 0.5, 0.5, 0.5, 15.0 / 35.0],
        },
        index=pd.Index([0.0, 0.5, 1.0, 1.5, 2.0], name="H"),
    )

    pd.testing.assert_frame_equal(result, expected)


def test_flow_distribution_by_level_uses_largest_h_range():
    short_range = pd.Series([1.0, 3.0], index=pd.Index([1.0, 2.0], name="H"), name="short")
    long_range = pd.Series([2.0, 4.0], index=pd.Index([0.0, 4.0], name="H"), name="long")

    result = flow_distribution_by_level([short_range, long_range])

    assert result.index.to_list() == [0.0, 1.0, 2.0, 3.0, 4.0]
    assert result.columns.to_list() == ["short", "long"]
    assert result.loc[0.0, "short"] == 1.0 / 3.0
    assert result.loc[4.0, "short"] == 3.0 / 7.0


def test_flow_distribution_by_level_requires_unique_names():
    series_a = pd.Series([0.0, 1.0], index=[0.0, 1.0], name="same")
    series_b = pd.Series([1.0, 2.0], index=[0.0, 1.0], name="same")

    try:
        flow_distribution_by_level([series_a, series_b])
    except ValueError as error:
        assert str(error) == "Each Q(H) series must have a unique name."
    else:
        raise AssertionError("Expected ValueError for duplicate series names.")
