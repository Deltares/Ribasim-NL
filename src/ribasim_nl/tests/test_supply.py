import geopandas as gpd
import pytest
from ribasim import Node
from ribasim.nodes import basin, manning_resistance, outlet
from ribasim_nl.supply import SupplyBasin, storing_basins
from shapely.geometry import MultiPolygon, Point, Polygon

from ribasim_nl import Model


def square(x: float, y: float, size: float = 10.0) -> MultiPolygon:
    return MultiPolygon([Polygon([(x, y), (x + size, y), (x + size, y + size), (x, y + size)])])


@pytest.fixture
def model() -> Model:
    """Two Basins connected by an Outlet, of which the first has a storing ('bergend') Basin."""
    model = Model(starttime="2020-01-01", endtime="2020-01-02", crs="EPSG:28992")
    tables = [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])]
    first = model.basin.add(
        Node(1, Point(5, 5), meta_categorie="doorgaand"), [*tables, basin.Area(geometry=[square(0, 0)])]
    )
    second = model.basin.add(
        Node(2, Point(25, 5), meta_categorie="doorgaand"), [*tables, basin.Area(geometry=[square(20, 0)])]
    )
    storing = model.basin.add(
        Node(3, Point(5, 45), meta_categorie="bergend"), [*tables, basin.Area(geometry=[square(0, 0)])]
    )
    manning = model.manning_resistance.add(
        Node(4, Point(5, 25)),
        [manning_resistance.Static(length=[100.0], manning_n=[0.04], profile_width=[10.0], profile_slope=[1.0])],
    )
    connector = model.outlet.add(Node(5, Point(15, 5)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(storing, manning)
    model.link.add(manning, first)
    model.link.add(first, connector)
    model.link.add(connector, second)
    return model


def test_storing_basins(model):
    assert storing_basins(model).to_dict() == {3: 1}


def test_storing_basin_follows_its_basin(model):
    # the supply area holds the nodes of Basins 1 and 2, but not that of storing Basin 3
    supply_area = gpd.GeoDataFrame(geometry=[Polygon([(0, 0), (30, 0), (30, 10), (0, 10)])], crs="EPSG:28992")
    sb = SupplyBasin(model, supply_area)
    areas = sb.exec().set_index("node_id")["meta_aanvoer"]
    assert areas.to_dict() == {1: True, 2: True, 3: False}

    areas = sb.set_aanvoer_off(1).set_index("node_id")["meta_aanvoer"]
    assert areas.to_dict() == {1: False, 2: True, 3: False}
    areas = sb.set_aanvoer_on(1).set_index("node_id")["meta_aanvoer"]
    assert areas.to_dict() == {1: True, 2: True, 3: True}
    areas = sb.set_aanvoer_off(2).set_index("node_id")["meta_aanvoer"]
    assert areas.to_dict() == {1: True, 2: False, 3: True}
