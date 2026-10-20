import geopandas as gpd
import pandas as pd
import pytest
from ribasim import Node
from ribasim.nodes import basin, manning_resistance
from ribasim_nl.parametrization.manning_resistance_table import water_widths, widen_to_water_width
from shapely.geometry import LineString, Point, box

from ribasim_nl import Model


@pytest.fixture
def model() -> Model:
    """Basins 1 and 2 in a river along the x-axis, with ManningResistance 3 between them.

    Basin 4 lies beside the river, with ManningResistance 5 between Basins 2 and 4.
    """
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    profile = basin.Profile(level=[0.0, 2.0], area=[100.0, 100.0])
    for node_id, point in [(1, Point(0, 0)), (2, Point(200, 0)), (4, Point(200, 200))]:
        model.basin.add(Node(node_id, point), [profile])
    static = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[2.0], profile_slope=[1.0])
    model.manning_resistance.add(Node(3, Point(100, 0)), [static])
    model.manning_resistance.add(Node(5, Point(200, 100)), [static])
    for from_id, to_id in [(1, 3), (3, 2), (2, 5), (5, 4)]:
        model.link.add(
            model.basin[from_id] if from_id in (1, 2) else model.manning_resistance[from_id],
            model.basin[to_id] if to_id in (2, 4) else model.manning_resistance[to_id],
        )
    return model


@pytest.fixture
def water_area_df() -> gpd.GeoDataFrame:
    """A river 60 m wide along the x-axis."""
    return gpd.GeoDataFrame(geometry=[box(-500, -30, 500, 30)], crs=28992)


def test_water_widths(model, water_area_df):
    widths = water_widths(model, water_area_df)
    # across the river at node 3, and no water at node 5 beside it
    assert widths.to_dict() == {3: pytest.approx(60.0), 5: 0.0}


def test_water_widths_follow_bent_links(model, water_area_df):
    # a link that bends away from the node still gives the direction of the watercourse at the node
    model.link.df.loc[model.link.df.to_node_id == 3, "geometry"] = LineString([(0, 0), (50, 0), (100, 0)])
    assert water_widths(model, water_area_df)[3] == pytest.approx(60.0)


def test_widen_to_water_width():
    static_df = pd.DataFrame({"node_id": [3, 5, 6], "profile_width": [2.0, 2.0, 70.0], "profile_slope": [1.0] * 3})
    water_width = pd.Series({3: 60.0, 5: 0.0, 6: 60.0})
    widened = widen_to_water_width(static_df, water_width, depth=2.0, profile_slope=2.0).set_index("node_id")
    # node 3 is 6 m wide at 2 m depth and becomes 60 m: a bottom of 60 - 2 * 2 * 2 with sides of 2
    assert widened.loc[3].to_dict() == {"profile_width": 52.0, "profile_slope": 2.0}
    # no water, or already wider than the water: unchanged
    assert widened.loc[5].to_dict() == {"profile_width": 2.0, "profile_slope": 1.0}
    assert widened.loc[6].to_dict() == {"profile_width": 70.0, "profile_slope": 1.0}


def test_widen_to_water_width_keeps_a_third_as_bottom():
    # a narrow deep watercourse: the sides of 2 would leave no bottom, so the bottom is a third of the width
    static_df = pd.DataFrame({"node_id": [3], "profile_width": [1.0], "profile_slope": [0.5]})
    widened = widen_to_water_width(static_df, pd.Series({3: 6.0}), depth=2.0, profile_slope=2.0)
    assert widened.loc[0, "profile_width"] == 2.0
    # and the sides reach the 6 m width at 2 m depth
    assert widened.loc[0, "profile_width"] + 2 * widened.loc[0, "profile_slope"] * 2.0 == pytest.approx(6.0)
