import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from ribasim_nl.peilbeheer import (
    COLUMNS,
    DEFAULT_BOUND,
    DEFAULT_TRANSITIONS,
    LEVEL_COLUMNS,
    TRANSITION_COLUMNS,
    Source,
    complete_peilbeheer,
    day_month,
    day_of_year,
    valid_levels,
)
from shapely.geometry import box


def peilgebieden(**columns) -> pd.DataFrame:
    """Peilgebieden with the columns of `COLUMNS`, of which the levels and transitions are unknown by default."""
    n = len(next(iter(columns.values())))
    df = pd.DataFrame({column: [np.nan] * n for column in LEVEL_COLUMNS})
    for column in ("authority", "code", "soort_peilbeheer", *TRANSITION_COLUMNS):
        df[column] = pd.Series([pd.NA] * n, dtype="string")
    for column, values in columns.items():
        df[column] = values
    df["code"] = [str(i) for i in range(n)]
    return df[COLUMNS]


def test_valid_levels():
    levels = pd.Series([0.0, -0.5, 99.0, 9999.0, -1.2, "1.3", "x", None])
    np.testing.assert_array_equal(valid_levels(levels), [0.0, -0.5, np.nan, np.nan, -1.2, 1.3, np.nan, np.nan])
    # in datasets that use 0 for unknown levels
    np.testing.assert_array_equal(
        valid_levels(levels, zero_is_unknown=True), [np.nan, -0.5, np.nan, np.nan, -1.2, 1.3, np.nan, np.nan]
    )


def test_day_of_year():
    assert day_of_year("01-04") == 91
    assert day_of_year("0104") == 91
    assert day_of_year("31-12") == 365
    assert day_of_year("29-02") is None
    assert day_of_year("1-4") is None
    assert day_of_year(pd.NA) is None
    assert day_month(91) == "01-04"


def test_target_level_from_bounds():
    df = complete_peilbeheer(peilgebieden(ondergrens_zomer=[-1.2], bovengrens_zomer=[-1.0]))
    # the winter gets the levels of the summer, without levels of its own
    assert df[LEVEL_COLUMNS].iloc[0].tolist() == pytest.approx([-1.1, -1.2, -1.0, -1.1, -1.2, -1.0])
    assert tuple(df[TRANSITION_COLUMNS].iloc[0]) == DEFAULT_TRANSITIONS


def test_default_and_invalid_bounds():
    df = complete_peilbeheer(
        peilgebieden(
            streefpeil_zomer=[-1.0, -1.0, -1.0],
            streefpeil_winter=[-1.2, -1.2, -1.2],
            # above the target level, more than 0.5 m below it, valid
            ondergrens_winter=[-1.1, -1.8, -1.3],
        )
    )
    assert df["ondergrens_zomer"].tolist() == pytest.approx([-1.0 - DEFAULT_BOUND] * 3)
    assert df["bovengrens_zomer"].tolist() == pytest.approx([-1.0 + DEFAULT_BOUND] * 3)
    assert df["ondergrens_winter"].tolist() == pytest.approx([-1.2 - DEFAULT_BOUND, -1.2 - DEFAULT_BOUND, -1.3])
    assert df["bovengrens_winter"].tolist() == pytest.approx([-1.2 + DEFAULT_BOUND] * 3)


def test_skipped_peilgebieden():
    df = complete_peilbeheer(
        peilgebieden(
            streefpeil_zomer=[np.nan, -1.0, 0.5],
            streefpeil_winter=[np.nan, -2.5, 0.3],
            # one bound is not enough for a target level
            bovengrens_zomer=[1.0, np.nan, np.nan],
        )
    )
    # without target level, and with summer and winter levels too far apart
    assert df["code"].tolist() == ["2"]


@pytest.mark.parametrize(
    ("transitions", "expected"),
    [
        (("01-03", "01-05", "01-09", "01-11"), ("01-03", "01-05", "01-09", "01-11")),
        # only the date at which the level changes starts a transition of 10 days
        (("15-04", pd.NA, "15-09", pd.NA), ("15-04", "24-04", "15-09", "24-09")),
        ((pd.NA, "15-04", pd.NA, "15-09"), ("06-04", "15-04", "06-09", "15-09")),
        # not in the order of the year
        (("01-10", "10-10", "01-04", "10-04"), DEFAULT_TRANSITIONS),
        (("01-01", "10-01", "01-10", "10-10"), DEFAULT_TRANSITIONS),
        ((pd.NA, pd.NA, pd.NA, pd.NA), DEFAULT_TRANSITIONS),
    ],
)
def test_transitions(transitions, expected):
    df = complete_peilbeheer(
        peilgebieden(streefpeil_zomer=[1.0], **{c: [t] for c, t in zip(TRANSITION_COLUMNS, transitions, strict=True)})
    )
    assert tuple(df[TRANSITION_COLUMNS].iloc[0]) == expected


def test_source():
    gdf = gpd.GeoDataFrame(
        {
            "CODE": ["a", "b", "c", "d"],
            "TYPE": [1.0, 2.0, 3.0, 9.0],
            "VAST": [-1.0, 0.0, 0.0, -2.0],
            "ZOMER": [0.0, 0.0, -0.9, 0.0],
            "WINTER": [0.0, 0.0, -1.1, 0.0],
            "ONDER": ["0", "-1.3; -1.25", "0", "0"],
            "BOVEN": ["0", "-1.1;-1.0", "0", "0"],
        },
        geometry=[box(i, 0, i + 1, 1) for i in range(4)],
        crs="EPSG:28992",
    )
    source = Source(
        "test.gpkg",
        code="CODE",
        by="TYPE",
        soort_peilbeheer="TYPE",
        labels={"1": "vast peil"},
        mapping={
            "1": {"streefpeil": "VAST"},
            "2": {"ondergrens": "ONDER", "bovengrens": "BOVEN"},
            "3": {"streefpeil": "VAST", "streefpeil_zomer": "ZOMER", "streefpeil_winter": "WINTER"},
        },
        zero_is_unknown=True,
    )
    df = complete_peilbeheer(source.peilbeheer(gdf).assign(authority="test"))
    # type 9 is not mapped
    assert df["code"].tolist() == ["a", "b", "c"]
    assert df["soort_peilbeheer"].tolist() == ["vast peil", "2", "3"]
    np.testing.assert_allclose(
        df[["streefpeil_zomer", "streefpeil_winter"]], [[-1.0, -1.0], [-1.15, -1.15], [-0.9, -1.1]]
    )
    # the band of all structures: the lowest lower bound, and the highest upper bound
    assert df[["ondergrens_zomer", "bovengrens_zomer"]].iloc[1].tolist() == pytest.approx([-1.3, -1.0])


def test_source_mapping_keys():
    with pytest.raises(AssertionError, match="Unknown levels"):
        Source("test.gpkg", code="CODE", mapping={"*": {"zomerpeil": "ZOMER"}})
    with pytest.raises(AssertionError, match="if and only if"):
        Source("test.gpkg", code="CODE", by="TYPE", mapping={"*": {"streefpeil": "VAST"}})


def test_source_zero_level():
    # a level of 0 is a level like any other, unless the dataset uses it for unknown levels
    gdf = gpd.GeoDataFrame(
        {"CODE": ["a"], "ZOMER": [0.0], "WINTER": [-0.2]}, geometry=[box(0, 0, 1, 1)], crs="EPSG:28992"
    )
    mapping = {"*": {"streefpeil_zomer": "ZOMER", "streefpeil_winter": "WINTER"}}
    df = complete_peilbeheer(Source("test.gpkg", code="CODE", mapping=mapping).peilbeheer(gdf).assign(authority="t"))
    assert df[["streefpeil_zomer", "streefpeil_winter"]].iloc[0].tolist() == pytest.approx([0.0, -0.2])
    source = Source("test.gpkg", code="CODE", mapping=mapping, zero_is_unknown=True)
    df = complete_peilbeheer(source.peilbeheer(gdf).assign(authority="t"))
    assert df[["streefpeil_zomer", "streefpeil_winter"]].iloc[0].tolist() == pytest.approx([-0.2, -0.2])
