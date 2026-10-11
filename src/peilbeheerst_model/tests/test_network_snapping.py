import geopandas as gpd
import shapely
from peilbeheerst_model.network_snapping import route_basin_links, snap_to_network
from ribasim import Node
from ribasim.nodes import basin, outlet
from shapely.geometry import LineString, Point, box

from ribasim_nl import Model

AREA = box(-10, -10, 110, 110)


def test_route_basin_links():
    lines = [LineString([(0, 0), (100, 0)]), LineString([(100, 0), (100, 100)])]
    connectors = {1: Point(100, 100), 2: Point(0, -5)}

    # the Basin node is moved onto the network, and the connector nodes are attached to it
    location, routes = route_basin_links(Point(50, 10), connectors, lines, [True, True], basin_area=AREA)
    assert location.equals(Point(50, 0))
    assert routes == {1: [(50, 0), (100, 0), (100, 100)], 2: [(50, 0), (0, 0), (0, -5)]}

    # without an area the Basin node is not moved
    location, routes = route_basin_links(Point(50, 10), connectors, lines, [True, True])
    assert location.equals(Point(50, 10))
    assert routes[1] == [(50, 10), (50, 0), (100, 0), (100, 100)]


def test_route_basin_links_bridges_gaps():
    lines = [LineString([(0, 0), (40, 0)]), LineString([(70, 0), (100, 0)])]
    connectors = {1: Point(100, 0)}
    # gaps are bridged, either from a dangling end within `max_gap`, or otherwise by the shortest bridge
    for max_gap in (50, 5):
        location, routes = route_basin_links(
            Point(20, 5), connectors, lines, [True, True], basin_area=AREA, max_gap=max_gap
        )
        assert location.equals(Point(20, 0))
        assert routes == {1: [(20, 0), (40, 0), (70, 0), (100, 0)]}


def test_route_basin_links_detour():
    # the route along the network is much longer than the straight line, so the link is not routed
    lines = [LineString([(0, 0), (0, 100), (10, 100), (10, 0)])]
    location, routes = route_basin_links(Point(0, 1), {1: Point(10, 1)}, lines, [True], basin_area=AREA)
    assert location.equals(Point(0, 1))
    assert routes == {}


def test_snap_to_network():
    """Basin 1 -> Outlet 3 -> Basin 2, with the Outlet slightly off the network."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    for node_id, (x, y), area in ((1, (50, 20), box(0, -50, 100, 50)), (2, (150, -20), box(100, -50, 200, 50))):
        model.basin.add(
            Node(node_id, Point(x, y)),
            [
                basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]),
                basin.State(level=[0.5]),
                basin.Area(geometry=[shapely.MultiPolygon([area])]),
            ],
        )
    model.outlet.add(Node(3, Point(100, 5)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[1], model.outlet[3])
    model.link.add(model.outlet[3], model.basin[2])
    # a broken geometry that does not connect its nodes
    model.link.df.loc[model.link.df.index[1], "geometry"] = LineString([(0, 0), (1, 1)])
    hydro_objects = gpd.GeoDataFrame(
        {"main-route": [True, True]},
        geometry=[LineString([(10, 0), (100, 0)]), LineString([(100, 0), (190, 0)])],
        crs="EPSG:28992",
    )

    model = snap_to_network(model, hydro_objects)
    nodes = model.node.df
    assert nodes.at[3, "geometry"].equals(Point(100, 5)), "Connector nodes are not moved"
    assert nodes.at[1, "geometry"].equals(Point(50, 0))
    assert nodes.at[2, "geometry"].equals(Point(150, 0))
    links = model.link.df.geometry.tolist()
    assert list(links[0].coords) == [(50, 0), (100, 0), (100, 5)]
    assert list(links[1].coords) == [(100, 5), (100, 0), (150, 0)]
