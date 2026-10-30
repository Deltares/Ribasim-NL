import geopandas as gpd
import shapely
from ribasim import Node
from ribasim.nodes import basin, outlet
from ribasim_nl.network_snapping import route_basin_links, route_over_water, snap_to_network
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
    bridged = {1: [(20, 0), (40, 0), (70, 0), (100, 0)]}

    # without evidence of a connection the gap is not bridged, only the longest part of the network is used, and
    # the connector is attached to it by a straight line
    location, routes = route_basin_links(Point(20, 5), connectors, lines, [True, True], basin_area=AREA)
    assert routes == {1: [(20, 0), (40, 0), (100, 0)]}

    # a culvert, water or a tiny gap connects the hydro-objects
    culvert = LineString([(41, 0), (69, 0)])
    location, routes = route_basin_links(Point(20, 5), connectors, lines, [True, True], culverts=[culvert])
    assert routes == {1: [(20, 5), (20, 0), (40, 0), (41, 0), (69, 0), (70, 0), (100, 0)]}
    water = box(30, -10, 80, 10)
    location, routes = route_basin_links(Point(20, 5), connectors, lines, [True, True], water=water, basin_area=AREA)
    assert location.equals(Point(20, 0))
    assert routes == bridged
    location, routes = route_basin_links(Point(20, 5), connectors, lines, [True, True], max_gap=40, basin_area=AREA)
    assert routes == bridged


def test_route_over_water():
    # a lake without hydro-objects, with a dike of 20 m between the lake and the connector
    water = box(0, 0, 1000, 100)
    area = box(-10, -10, 1010, 160)
    routes = route_over_water(Point(10, 50), {1: Point(990, 140)}, water, area)
    coords = routes[1]
    assert coords[0] == (10, 50) and coords[-1] == (990, 140)
    assert shapely.LineString(coords).difference(water.buffer(10)).length <= 50
    # crossing much land is not accepted
    assert route_over_water(Point(10, 50), {1: Point(990, 150)}, box(0, 0, 500, 100), area) == {}


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
