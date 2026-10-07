import geopandas as gpd
import pytest
from ribasim import Node
from ribasim.nodes import basin
from ribasim_nl.parametrization.damo_profiles import DAMOProfiles, profile_lines_from_points
from shapely.geometry import LineString, Point, Polygon

from ribasim_nl import Model


@pytest.fixture
def damo_profiles() -> DAMOProfiles:
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.basin.add(Node(1, Point(0, 0)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0])])

    def profile_points(profile_id: str, x: float, z: list[float], codevolgnummer: list[int]) -> list[dict]:
        return [
            {"profiellijnid": profile_id, "codevolgnummer": nr, "geometry": Point(x, y, z_i)}
            for nr, y, z_i in zip(codevolgnummer, (-5, -2, 2, 5), z, strict=True)
        ]

    profile_point_df = gpd.GeoDataFrame(
        # in water, with the two middle points in the water area
        profile_points("a", 0, [1.0, 0.0, 0.0, 1.0], [1, 2, 3, 4])
        # no points in water
        + profile_points("b", 50, [3.0, 2.0, 2.0, 3.0], [1, 2, 3, 4])
        # duplicated codevolgnummer, renumbered along the profile line
        + profile_points("c", 5, [1.0, 0.0, 0.0, 1.0], [1, 1, 2, 2])
        # levels rounded like Python's round(), not numpy's (12.285 -> 12.29)
        + profile_points("d", 8, [13.0, 12.285, 12.4, 13.0], [1, 2, 3, 4])
        # missing codevolgnummer in water, so the width at water level is 0
        + profile_points("e", -8, [1.0, 0.0, 0.0, 1.0], [1, None, 3, 4]),
        crs=28992,
    )
    profile_line_df = gpd.GeoDataFrame(
        {"globalid": ["c", "b", "a", "d", "e"]},
        geometry=[LineString([(x, -5), (x, 5)]) for x in (5, 50, 0, 8, -8)],
        crs=28992,
    )
    water_area_df = gpd.GeoDataFrame(geometry=[Polygon([(-10, -3), (10, -3), (10, 3), (-10, 3)])], crs=28992)
    return DAMOProfiles(
        model=model,
        profile_line_df=profile_line_df,
        profile_point_df=profile_point_df,
        water_area_df=water_area_df,
    )


def test_process_profiles(damo_profiles):
    profiles_df = damo_profiles.process_profiles().set_index("profiel_id")
    assert profiles_df.index.to_list() == ["a", "b", "c", "d", "e"]
    assert profiles_df.loc["d", "bottom_level"] == 12.29
    assert profiles_df.loc["e", "profile_width"] == 1.0
    assert profiles_df.loc["e", "profile_slope"] == 0.5

    # width at water level 4, depth 0.5 (minimum), so the bottom width is 4 - 2 * 0.5 / 0.5
    expected_in_water = {
        "bottom_level": 0.0,
        "water_level": 0.0,
        "invert_level": 1.0,
        "profile_slope": 0.5,
        "profile_width": 2.0,
    }
    for profile_id in ["a", "c"]:
        assert profiles_df.loc[profile_id, list(expected_in_water)].to_dict() == expected_in_water

    # no water: width is a third of the line length, depth from the invert level
    profile_b = profiles_df.loc["b"]
    assert profile_b.bottom_level == 2.0
    assert profile_b.invert_level == 3.0
    assert profile_b.profile_width == pytest.approx(round(10 / 3 / 3, 2))
    assert profile_b.profile_slope == pytest.approx(round(1 / (10 / 3 / 3), 2))


def test_get_profile_level(damo_profiles):
    assert damo_profiles.get_profile_level("a") == 0.0
    assert damo_profiles.get_profile_level("b") == 3.0
    assert damo_profiles.get_profile_level("b", statistic="min") == 2.0


def test_profile_lines_from_points():
    points_df = gpd.GeoDataFrame(
        {
            "profiel": ["x", "x", "x", "y", "y", "y"],
            # profile y has an ambiguous first point
            "codevolgnummer": [2, 1, 3, 1, 1, 2],
        },
        geometry=[Point(0, 1), Point(0, 0), Point(0, 2), Point(5, 0), Point(5, 0), Point(5, 3)],
        crs=28992,
    )
    lines_df = profile_lines_from_points(points_df, profile_id_col="profiel", line_id_col="code")
    assert list(lines_df.columns) == ["code", "geometry"]
    assert lines_df.code.to_list() == ["x", "y"]
    assert lines_df.geometry.iloc[0].equals(LineString([(0, 0), (0, 2)]))
    assert lines_df.geometry.iloc[1].equals(LineString([(5, 0), (5, 3)]))
