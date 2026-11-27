import geopandas as gpd
import pytest
from ribasim import Node
from ribasim.nodes import basin, flow_demand, outlet, pump
from ribasim_nl.structure_locations import (
    Crossings,
    move_to_structures,
    structure_locations,
)
from shapely.geometry import LineString, Point

from ribasim_nl import Model

CRS = "EPSG:28992"


def crossings(rows: list[dict]) -> Crossings:
    """Crossings on the boundary x = 100 between the areas A (west) and B (east).

    Hydro-object ho_1 crosses it at y = 0, with gemaal g1 30 m behind the crossing. Culvert duiker_1 crosses it at
    y = 50 and ends at hydro-object ho_2, with stuw s1 20 m behind the culvert. Hydro-object ho_3 crosses it at y = 20
    without a structure, and ho_4 at y = 400, with stuw s2 at the crossing.
    """
    lines = {
        "ho_1": LineString([(0, 0), (200, 0)]),
        "duiker_1": LineString([(100, 50), (140, 50)]),
        "ho_2": LineString([(140, 50), (200, 50)]),
        "ho_3": LineString([(0, 20), (200, 20)]),
        "ho_4": LineString([(0, 400), (200, 400)]),
    }
    table = gpd.GeoDataFrame(
        [{"hydroobject": None, "gemaal": None, "stuw": None, "area_from": "A", "area_to": "B"} | row for row in rows],
        crs=CRS,
    )
    return Crossings(
        table,
        gpd.GeoSeries({"g1": Point(130, 0), "g2": Point(100, 150)}, crs=CRS),
        gpd.GeoSeries({"s1": Point(160, 50), "s2": Point(100, 400)}, crs=CRS),
        gpd.GeoSeries(lines, crs=CRS),
    )


def nodes(rows: dict[int, tuple[str, Point]]) -> gpd.GeoDataFrame:
    return gpd.GeoDataFrame(
        {"node_type": [node_type for node_type, _ in rows.values()]},
        geometry=[point for _, point in rows.values()],
        index=list(rows),
        crs=CRS,
    )


PUMP_CROSSING = {"geometry": Point(100, 0), "hydroobject": "ho_1", "gemaal": "g1"}
CULVERT_CROSSING = {"geometry": Point(100, 50), "hydroobject": "duiker_1"}
OPEN_CROSSING = {"geometry": Point(100, 20), "hydroobject": "ho_3"}
STUW_CROSSING = {"geometry": Point(100, 400), "hydroobject": "ho_4", "stuw": "s2"}


def test_pump_to_gemaal():
    table = crossings([PUMP_CROSSING])
    # a Pump gets the gemaal of its crossing, an Outlet does not, and neither does a node that is not on a crossing
    result = structure_locations(nodes({1: ("Pump", Point(100, 0.5)), 2: ("Pump", Point(100, 5))}), table)
    assert result == {1: "g1"}
    assert structure_locations(nodes({1: ("Outlet", Point(100, 0))}), table) == {}
    # a gemaal has one Pump, the nearest one, or the one with the lowest node id
    assert structure_locations(nodes({2: ("Pump", Point(100, 0)), 1: ("Pump", Point(100, 0))}), table) == {1: "g1"}
    # not a gemaal far from the crossing, which the crossings match by the ends of long hydro-objects
    far = crossings([PUMP_CROSSING | {"gemaal": "g2"}])
    assert structure_locations(nodes({1: ("Pump", Point(100, 0))}), far) == {}
    assert structure_locations(nodes({1: ("Pump", Point(100, 0))}), far, max_distance=200.0) == {1: "g2"}


def test_outlet_to_stuw():
    outlets = nodes({1: ("Outlet", Point(100, 20))})
    # the Outlet stands for the crossings between A and B without a structure: the stuw behind the culvert is its own
    table = crossings([OPEN_CROSSING, CULVERT_CROSSING])
    assert structure_locations(outlets, table) == {1: "s1"}
    # not the stuw behind a crossing between other areas, or in the other direction
    for other in ({"area_to": "C"}, {"area_from": "B", "area_to": "A"}):
        assert structure_locations(outlets, crossings([OPEN_CROSSING, CULVERT_CROSSING | other])) == {}
    # not a stuw that is not on the hydro-objects of the crossing, or far from the Outlet
    assert structure_locations(outlets, crossings([OPEN_CROSSING, CULVERT_CROSSING | {"hydroobject": "ho_1"}])) == {}
    table = crossings([OPEN_CROSSING, STUW_CROSSING | {"stuw": None}])
    assert structure_locations(outlets, table) == {}
    assert structure_locations(outlets, table, max_outlet_move=500.0) == {1: "s2"}

    # a stuw has one Outlet: the one whose crossing is matched with it, before a nearer Outlet of another crossing
    table = crossings([OPEN_CROSSING, CULVERT_CROSSING | {"stuw": "s1"}])
    both = nodes({1: ("Outlet", Point(100, 20)), 2: ("Outlet", Point(100, 50))})
    assert structure_locations(both, table) == {2: "s1"}
    table = crossings([OPEN_CROSSING, CULVERT_CROSSING])
    assert structure_locations(both, table) == {2: "s1"}, "the nearest Outlet"


def test_crossings_validation():
    with pytest.raises(AssertionError, match="miss the columns"):
        Crossings(gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=CRS), *[gpd.GeoSeries([], crs=CRS)] * 3)


def test_move_to_structures():
    """Basin 1 -> Pump 3 -> Basin 2, with the Pump on a crossing 30 m from its gemaal, and FlowDemand 4 next to it."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs=CRS)
    for node_id, x in ((1, 0), (2, 200)):
        model.basin.add(
            Node(node_id, Point(x, 0)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])]
        )
    model.pump.add(Node(3, Point(100, 0)), [pump.Static(flow_rate=[1.0])])
    model.outlet.add(Node(5, Point(100, 20)), [outlet.Static(flow_rate=[1.0])])
    model.flow_demand.add(Node(4, Point(110, 0)), [flow_demand.Static(demand=[1.0], demand_priority=[1])])
    model.link.add(model.basin[1], model.pump[3])
    model.link.add(model.pump[3], model.basin[2])
    model.link.add(model.basin[1], model.outlet[5])
    model.link.add(model.outlet[5], model.basin[2])
    model.link.add(model.flow_demand[4], model.pump[3])

    moved = move_to_structures(model, crossings([PUMP_CROSSING, OPEN_CROSSING]))
    assert moved == {3: "g1"}
    assert model.node.df["meta_code_waterbeheerder"].dropna().to_dict() == {3: "g1"}
    points = model.node.df.geometry
    assert points[3].equals(Point(130, 0))
    assert points[4].equals(Point(140, 0)), "The control node stays next to the Pump"
    assert points[5].equals(Point(100, 20)), "An Outlet without a stuw stays on its crossing"
    links = model.link.df.set_index(["from_node_id", "to_node_id"]).geometry
    assert list(links[(1, 3)].coords) == [(0, 0), (130, 0)]
    assert list(links[(4, 3)].coords) == [(140, 0), (130, 0)]
    # a node that is at its structure already is not moved again
    assert move_to_structures(model, crossings([PUMP_CROSSING | {"geometry": Point(130, 0)}])) == {}
    assert model.node.df.geometry[3].equals(Point(130, 0))
