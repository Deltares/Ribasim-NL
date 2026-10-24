import geopandas as gpd
import numpy as np
import pytest
import shapely
from ribasim import Node
from ribasim.nodes import basin, manning_resistance
from ribasim_nl.parametrization.manning_resistance_table import (
    crosses_link,
    reach_water_width,
    trapezoid,
    update_manning_resistance_reach,
    update_manning_resistance_static,
)
from shapely.geometry import LineString, Point, box

from ribasim_nl import Model

# a river 60 m wide along the x-axis
RIVER = box(-1000, -30, 1000, 30)


def water_tree(*polygons):
    water = np.asarray(polygons)
    return water, shapely.STRtree(water)


def test_crosses_link():
    link = LineString([(0, 0), (200, 0)])
    # across the link, near its middle
    assert crosses_link(LineString([(100, -10), (100, 10)]), link)
    assert crosses_link(LineString([(100, -10), (110, 10)]), link)
    # a profile of a side ditch that touches the link where the ditch joins, along the link
    assert not crosses_link(LineString([(95, -1), (105, 1)]), link)
    # touching the link with its end
    assert not crosses_link(LineString([(100, 0), (100, 20)]), link)
    assert not crosses_link(LineString([(100, 5), (100, 20)]), link)


def test_reach_water_width():
    water, tree = water_tree(RIVER)
    assert reach_water_width([LineString([(0, 0), (500, 0)])], water, tree) == pytest.approx(60.0)
    # a reach mostly beside the river
    assert reach_water_width([LineString([(0, 0), (100, 0), (100, 400)])], water, tree) == 0.0
    # too short for a cross section away from its ends
    assert reach_water_width([LineString([(0, 0), (50, 0)])], water, tree) == 0.0
    # a ditch of 2 m that lies 3 m beside the link, which follows a hydro-object
    water, tree = water_tree(box(-1000, 3, 1000, 5))
    assert reach_water_width([LineString([(0, 0), (500, 0)])], water, tree) == pytest.approx(2.0)
    assert reach_water_width([LineString([(0, 0), (500, 0)])], water, tree, tolerance=1.0) == 0.0


def test_trapezoid():
    # sides of 2 over a depth of 2: the bottom is 8 m less than the top
    assert trapezoid(60.0, 2.0, 2.0) == pytest.approx((52.0, 2.0))
    # narrow: the bottom is a third of the top, with steeper sides
    assert trapezoid(6.0, 2.0, 2.0) == pytest.approx((2.0, 1.0))


@pytest.fixture
def model() -> Model:
    """Basins along a river, with ManningResistances 11, 12 and 13 between them.

    ManningResistance 13 lies on a ditch beside the river.
    """
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    profile = basin.Profile(level=[0.0, 2.0], area=[100.0, 100.0])
    static = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[2.0], profile_slope=[1.0])
    basins = [(1, Point(0, 0)), (2, Point(400, 0)), (3, Point(800, 0)), (4, Point(800, 400))]
    for node_id, point in basins:
        model.basin.add(Node(node_id, point), [profile])
    for node_id, point, from_id, to_id in [
        (11, Point(200, 0), 1, 2),
        (12, Point(600, 0), 2, 3),
        (13, Point(800, 200), 3, 4),
    ]:
        model.manning_resistance.add(Node(node_id, point), [static])
        model.link.add(model.basin[from_id], model.manning_resistance[node_id])
        model.link.add(model.manning_resistance[node_id], model.basin[to_id])
    # the profile of each ManningResistance, by the link to it
    link_df = model.link.df
    for node_id, profile_id in [(11, "own"), (12, "ditch"), (13, "none")]:
        link_df.loc[link_df.to_node_id == node_id, "meta_profielid_waterbeheerder"] = profile_id
    return model


def test_update_manning_resistance_static(model, tmp_path):
    profiles_gpkg = tmp_path / "profiles.gpkg"
    gpd.GeoDataFrame(
        {"profiel_id": ["own", "ditch"], "profile_width": [40.0, 1.0], "profile_slope": [3.0, 0.5]},
        # across the link of 11, and of a ditch far away from 12
        geometry=[LineString([(100, -30), (100, 30)]), LineString([(500, 500), (500, 510)])],
        crs=28992,
    ).to_file(profiles_gpkg)
    # the river, and a ditch of 4.5 m wide from it to Basin 4
    water_area_df = gpd.GeoDataFrame(geometry=[RIVER, box(797.75, 30, 802.25, 400)], crs=28992)
    update_manning_resistance_static(model, profiles_gpkg=profiles_gpkg, water_area_df=water_area_df)
    static_df = model.manning_resistance.static.df.set_index("node_id")
    # 11 keeps its own DAMO profile
    assert static_df.loc[11, ["profile_width", "profile_slope", "meta_profile_source"]].to_list() == [40.0, 3.0, "DAMO"]
    # 12 has a profile of another watercourse, so it gets the 60 m river width at 2 m depth
    assert static_df.loc[12, ["profile_width", "profile_slope", "meta_profile_source"]].to_list() == [
        52.0,
        2.0,
        "BGT",
    ]
    # 13 on the ditch gets its width of 4.5 m
    assert static_df.loc[13, ["profile_width", "profile_slope", "meta_profile_source"]].to_list() == [
        1.5,
        0.75,
        "BGT",
    ]


def test_update_manning_resistance_reach(model, tmp_path):
    profiles_gpkg = tmp_path / "profiles.gpkg"
    gpd.GeoDataFrame(
        {"profiel_id": ["own"], "profile_width": [40.0], "profile_slope": [3.0]},
        geometry=[LineString([(100, -30), (100, 30)])],
        crs=28992,
    ).to_file(profiles_gpkg)
    # the river, and a ditch of 2 m along the route that the links of 12 will get
    water_area_df = gpd.GeoDataFrame(geometry=[RIVER, box(400, 99, 800, 101)], crs=28992)
    update_manning_resistance_static(model, profiles_gpkg=profiles_gpkg, water_area_df=water_area_df)
    static_df = model.manning_resistance.static.df
    # a ManningResistance with a length and profile of its own, like those of bergend Basins
    static_df.loc[static_df.node_id == 13, "meta_profile_source"] = None
    static_df.loc[static_df.node_id == 13, ["length", "profile_width"]] = [123.0, 7.0]
    before = static_df.set_index("node_id").copy()
    assert before.loc[12, "meta_profile_source"] == "BGT"

    # routing moves 12 and its links along the ditch, 100 m north of the river
    link_df = model.link.df
    link_df.loc[link_df.to_node_id == 12, "geometry"] = LineString([(400, 0), (400, 100), (600, 100)])
    link_df.loc[link_df.from_node_id == 12, "geometry"] = LineString([(600, 100), (800, 100), (800, 0)])
    update_manning_resistance_reach(model, water_area_df)
    after = model.manning_resistance.static.df.set_index("node_id")

    # 11 keeps its DAMO profile, 12 gets the width of its new route, 13 is not touched, and all keep their length
    assert after.loc[11, ["profile_width", "profile_slope"]].to_list() == [40.0, 3.0]
    assert (after["length"] == before["length"]).all()
    assert after.loc[12, ["profile_width", "profile_slope", "meta_profile_source"]].to_list() == [
        pytest.approx(0.67),
        pytest.approx(0.33),
        "BGT",
    ]
    assert after.loc[13, ["length", "profile_width"]].to_list() == [123.0, 7.0]
