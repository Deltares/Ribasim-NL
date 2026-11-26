import geopandas as gpd
import numpy as np
import shapely
from ribasim import Node
from ribasim.nodes import basin, outlet
from ribasim_nl.network_snapping import (
    _BasinNetwork,
    _WaterRaster,
    culverts_in,
    read_lake_waterways,
    read_lines_on_water,
    route_basin_links,
    route_over_water,
    snap_to_network,
)
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


def test_route_basin_links_center():
    """The Basin node goes to the point of the network from which the routes are shortest in total."""
    # a waterway from west to east, crossed halfway by one from south to north, with a connector at each end
    lines = [
        LineString([(0, 0), (500, 0)]),
        LineString([(500, 0), (1000, 0)]),
        LineString([(500, 0), (500, 400)]),
        LineString([(500, -400), (500, 0)]),
    ]
    connectors = {1: Point(0, 0), 2: Point(1000, 0), 3: Point(500, 400), 4: Point(500, -400)}
    area = box(-10, -410, 1010, 410)
    location, routes = route_basin_links(Point(900, 10), connectors, lines, [True] * 4, basin_area=area)
    assert location.equals(Point(500, 0)), "The Basin node is where the routes split, not on the line nearest to it"
    assert routes[1] == [(500, 0), (0, 0)] and routes[3] == [(500, 0), (500, 400)]

    # with one connector all points on the route to it are as good, and the Basin node stays where it was
    location, routes = route_basin_links(Point(900, 10), {1: Point(0, 0)}, lines, [True] * 4, basin_area=area)
    assert location.equals(Point(900, 0))
    assert routes == {1: [(900, 0), (500, 0), (0, 0)]}

    # the center is within the area of the Basin
    location, _ = route_basin_links(Point(900, 10), connectors, lines, [True] * 4, basin_area=box(600, -10, 1010, 10))
    assert location.equals(Point(900, 0))
    # and not on top of a connector if it can be elsewhere
    network = _BasinNetwork(lines, [True] * 4)
    assert network.center([(0, 0)], (0, 0), area) == (0, 0)
    assert network.center([(0, 0)], (0, 0), area, [Point(0, 0)]) == (500, 0)
    assert network.center([(0, 0)], (1000, 0), box(2000, 0, 3000, 10)) is None


def test_route_basin_links_cost():
    """Routes follow the hydro-objects of the water authority, and use other lines where these leave a gap."""
    # a hydro-object around a parcel, and a ditch of another source that cuts the corner
    lines = [LineString([(0, 0), (100, 0), (100, 100)]), LineString([(0, 0), (100, 100)])]
    connectors = {1: Point(100, 100)}
    _, routes = route_basin_links(Point(0, 0), connectors, lines, [True, False])
    assert routes == {1: [(0, 0), (100, 100)]}
    _, routes = route_basin_links(Point(0, 0), connectors, lines, [True, False], cost=[1.0, 3.0])
    assert routes == {1: [(0, 0), (100, 0), (100, 100)]}
    # without the hydro-object the ditch is used
    _, routes = route_basin_links(Point(0, 0), connectors, lines[1:], [False], cost=[3.0])
    assert routes == {1: [(0, 0), (100, 100)]}


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


def test_culverts_in():
    """A culvert under a canal between two parts of a basin area is taken whole, others are clipped to the area."""
    area = box(0, -10, 40, 10).union(box(70, -10, 110, 10))
    geometries = np.array(
        [
            shapely.MultiLineString([[(35, 0), (75, 0)]]),  # under the canal between x = 40 and 70
            shapely.MultiLineString([[(30, 5), (50, 5)]]),  # out of the area
            shapely.MultiLineString([[(200, 0), (210, 0)]]),  # far away
        ]
    )
    culverts = culverts_in(geometries, shapely.STRtree(geometries), area)
    assert [list(c.coords) for c in culverts] == [[(35, 0), (75, 0)], [(30, 5), (40, 5)]]

    # the culvert connects the hydro-objects on either side, so the connector beyond the canal is routed
    lines = [LineString([(0, 0), (35, 0)]), LineString([(75, 0), (110, 0)])]
    _, routes = route_basin_links(Point(0, 0), {1: Point(110, 0)}, lines, [True, True], culverts=[culverts[0]])
    assert routes == {1: [(0, 0), (35, 0), (75, 0), (110, 0)]}


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


def test_route_basin_links_harbour():
    """A harbour channel behind a pier is joined to the fairway of the lake through the mouth of the harbour."""
    lake, mouth, harbour = box(0, 0, 1000, 400), box(300, 400, 340, 450), box(0, 450, 400, 550)
    pier = box(0, 400, 300, 450)
    water = shapely.union_all([lake, mouth, harbour])
    area = box(-10, -10, 1010, 560)
    fairway, channel = LineString([(500, 0), (500, 400)]), LineString([(320, 470), (30, 500)])
    connectors = {1: Point(20, 520)}
    _, routes = route_basin_links(
        Point(500, 200), connectors, [fairway, channel], [True, False], water=water, area=area, open_water=True
    )
    route = LineString(routes[1])
    assert route.coords[0] == (500, 200) and route.coords[-1] == (20, 520)
    assert route.intersection(pier).length == 0.0
    assert route.distance(Point(30, 500)) < 1e-6
    # no detour around the start of the path over water
    assert all(x <= 500 for x, _ in route.coords)


def test_read_lines_on_water(tmp_path):
    """Only the parts of the hydro-objects on the water, such as a harbour channel, not the ditches on land."""
    path = tmp_path / "network.gpkg"
    lines = [LineString([(-50, 50), (50, 50)]), LineString([(-50, -50), (-10, -50)])]
    gpd.GeoDataFrame({"main-route": [False, False]}, geometry=lines, crs="EPSG:28992").to_file(
        path, layer="hydro_objects"
    )
    on_water = read_lines_on_water([path], box(0, 0, 100, 100))
    assert [list(line.coords) for line in on_water.geometry] == [[(0, 50), (50, 50)]]
    assert not on_water["main-route"].any()
    assert read_lines_on_water([], box(0, 0, 100, 100)).empty


def test_connect_over_water():
    # a fairway, and two short ones far from it, which are closer to each other
    lines = [LineString([(0, 0), (1000, 0)]), LineString([(0, 600), (100, 600)]), LineString([(0, 700), (100, 700)])]
    water = box(-10, -10, 1010, 710)
    network = _BasinNetwork(lines, [True, True, True])
    network.bridge_gaps(2.0, water, 500.0)
    assert len(set(network.component)) == 2
    network.connect_over_water(water)
    assert len(set(network.component)) == 1
    assert network.graph.has_edge((0.0, 600.0), (0.0, 0.0))
    # not over land
    network = _BasinNetwork(lines, [True, True, True])
    network.connect_over_water(box(-10, -10, 1010, 300).union(box(-10, 550, 1010, 710)))
    assert len(set(network.component)) == 3


def lake_with_island() -> tuple[shapely.Geometry, shapely.Geometry]:
    """A lake with an island of 80 by 100 m, and the area around it."""
    water = box(0, -100, 1000, 400).difference(box(440, 50, 520, 150))
    return water, box(-10, -110, 1010, 410)


def test_attach_straight():
    water, area = lake_with_island()
    raster = _WaterRaster(water, area, land_cost=20.0, max_cells=100_000)
    lines = np.array([LineString([(0, 0), (1000, 0)])])
    points = {1: Point(600, 300), 2: Point(480, 250), 3: Point(480, 100)}
    attached = raster.attach_straight(lines, points, max_land=10.0)
    # 2 is behind the island, so it branches off the line to 1, which is attached first, and 3 is on the island
    assert attached == {1: [(600, 0), (600, 250), (600, 300)], 2: [(600, 0), (600, 250), (480, 250)]}
    # a line does not branch off at the end of another one, where it would pass through that point
    attached = raster.attach_straight(lines, {1: Point(600, 300), 2: Point(480, 350)}, max_land=10.0)
    assert attached == {1: [(600, 0), (600, 300)]}


def test_route_basin_links_open_water():
    water, area = lake_with_island()
    lines = [LineString([(0, 0), (1000, 0)])]
    connectors = {1: Point(600, 300), 2: Point(480, 250)}
    kwargs = {"water": water, "area": area, "max_land": 10.0}
    _, routes = route_basin_links(Point(100, 50), connectors, lines, [True], open_water=True, **kwargs)
    assert routes == {
        1: [(100, 50), (100, 0), (600, 0), (600, 250), (600, 300)],
        2: [(100, 50), (100, 0), (600, 0), (600, 250), (480, 250)],
    }
    # else they are routed along the least-cost paths over water, around the island
    _, routes = route_basin_links(Point(100, 50), connectors, lines, [True], **kwargs)
    assert set(routes) == {1, 2}
    assert shapely.LineString(routes[2]).intersection(box(450, 60, 510, 140)).is_empty


def test_read_lake_waterways(tmp_path):
    """Fairways that cross each other and the main route are split there, to connect them."""
    path = tmp_path / "fairway.gpkg"
    fairways = [LineString([(0, 0), (100, 0)]), LineString([(50, -50), (50, 50)])]
    gpd.GeoDataFrame(geometry=fairways, crs="EPSG:28992").to_file(path)
    main_route = gpd.GeoDataFrame(geometry=[LineString([(0, 20), (100, 20)])], crs="EPSG:28992")
    waterways = read_lake_waterways(path, box(-10, -60, 110, 60), main_route)
    assert waterways["main-route"].all()
    assert sorted(waterways.length.round(6).tolist()) == [20, 30, 50, 50, 50, 50, 50]


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


def test_snap_to_network_cost():
    """Links follow the hydro-objects with the lowest cost per metre."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.basin.add(
        Node(1, Point(0, 0)),
        [
            basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]),
            basin.State(level=[0.5]),
            basin.Area(geometry=[shapely.MultiPolygon([box(-10, -10, 110, 110)])]),
        ],
    )
    model.outlet.add(Node(2, Point(100, 100)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[1], model.outlet[2])
    hydro_objects = gpd.GeoDataFrame(
        {"main-route": [True, False], "cost": [1.0, 3.0]},
        geometry=[LineString([(0, 0), (100, 0), (100, 100)]), LineString([(0, 0), (100, 100)])],
        crs="EPSG:28992",
    )
    model = snap_to_network(model, hydro_objects, move_basins=False)
    assert list(model.link.df.geometry.iloc[0].coords) == [(0, 0), (100, 0), (100, 100)]


def test_snap_to_network_routing_areas():
    """Outlets 3 and 4 drain into Basin 1 over waterways far outside its basin area, which come together at (500, 0)."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    area = shapely.MultiPolygon([box(0, -50, 100, 50)])
    model.basin.add(
        Node(1, Point(50, 0)),
        [
            basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]),
            basin.State(level=[0.5]),
            basin.Area(geometry=[area]),
        ],
    )
    for node_id, point in ((3, Point(900, 300)), (4, Point(900, -300))):
        model.outlet.add(Node(node_id, point), [outlet.Static(flow_rate=[1.0])])
        model.link.add(model.outlet[node_id], model.basin[1])
    hydro_objects = gpd.GeoDataFrame(
        {"main-route": [True, False, False, False]},
        geometry=[
            LineString([(0, 0), (100, 0)]),
            LineString([(100, 0), (500, 0)]),
            LineString([(500, 0), (900, 300)]),
            LineString([(500, 0), (900, -300)]),
        ],
        crs=model.crs,
    )

    unrouted = snap_to_network(model.model_copy(deep=True), hydro_objects, route_water=False)
    assert all(len(line.coords) == 2 for line in unrouted.link.df.geometry), (
        "Outside the basin area without routing area"
    )

    routed = snap_to_network(model, hydro_objects, route_water=False, routing_areas={1: box(0, -400, 1000, 400)})
    from_3, from_4 = routed.link.df.geometry.tolist()
    assert list(from_3.coords) == [(900, 300), (500, 0), (100, 0), (50, 0)]
    assert list(from_4.coords) == [(900, -300), (500, 0), (100, 0), (50, 0)]
