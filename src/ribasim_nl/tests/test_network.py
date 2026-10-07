# %%
from pathlib import Path

import geopandas as gpd
import pytest
from ribasim_nl.link_geometries import get_link_geometry
from shapely.geometry import LineString, Point

from ribasim_nl import Network


@pytest.fixture
def osm_lines_gpkg():
    return Path(__file__).parent.joinpath("data", "osm_lines.gpkg")


@pytest.fixture
def link_columns():
    return ["node_from", "node_to", "name", "id", "length", "geometry"]


def test_network(tmp_path, link_columns):
    lines_gdf = gpd.GeoDataFrame(
        geometry=gpd.GeoSeries(
            [
                LineString(((0, 0), (10, 0))),
                LineString(((10, 0), (20, 0))),
                LineString(((10, 0), (10, 10))),
            ]
        )
    )

    network = Network(lines_gdf)
    assert len(network.graph.nodes) == 4
    assert len(network.graph.edges) == 3
    assert all(i in link_columns for i in network.links.columns)
    output_file = tmp_path / "network.gpkg"
    network.to_file(output_file)
    assert output_file.exists()


def test_gap_in_network(link_columns):
    lines_gdf = gpd.GeoDataFrame(
        geometry=gpd.GeoSeries(
            [
                LineString(((0, 0), (10, 0))),
                LineString(((10.5, 0), (20, 0))),
                LineString(((10, 0), (10, 10))),
            ]
        )
    )

    network = Network(lines_gdf)
    # we'll find 5 nodes now, as there is a gap
    assert len(network.graph.nodes) == 5

    # throw network away and regenerate it with tolerance
    network.reset()
    network.tolerance = 1

    # we'll find 4 nodes now, as the gap is repaired
    assert len(network.graph.nodes) == 4
    assert len(network.graph.edges) == 3

    assert all(i in link_columns for i in network.links.columns)


def test_link_within_tolerance():
    lines_gdf = gpd.GeoDataFrame(
        geometry=gpd.GeoSeries(
            [
                LineString(((0, 0), (4, 0))),
                LineString(((5, 0), (10, 0))),
                LineString(((10, 0), (20, 0))),
                LineString(((10, 0), (10, 10))),
                LineString(((4, 0), (5, 0))),  # .length == 1m
            ]
        ),
        crs=28992,
    )

    # not snapping within tolerance should produce all links and nodes
    network = Network(lines_gdf)
    assert len(network.graph.edges) == 5
    assert len(network.graph.nodes) == 6

    network.reset()
    # tolerance >1m should remove link
    network.tolerance = 1.1
    assert len(network.graph.edges) == 4
    assert len(network.graph.nodes) == 5


def test_split_intersecting_links():
    lines_gdf = gpd.GeoDataFrame(
        geometry=gpd.GeoSeries(
            [
                LineString(((0, 0), (20, 0))),
                LineString(((10, 0), (10, 10))),
            ]
        ),
        crs=28992,
    )

    network = Network(lines_gdf)

    assert len(network.graph.edges) == 3
    assert len(network.graph.nodes) == 4


def test_osm_lines(osm_lines_gpkg):
    network = Network.from_lines_gpkg(osm_lines_gpkg)

    assert len(network.graph.edges) == 42
    assert len(network.graph.nodes) == 35


@pytest.fixture
def t_network():
    """Network shaped as a T: (0, 0) - (10, 0) - (20, 0) with a branch (10, 0) - (10, 10)."""
    lines_gdf = gpd.GeoDataFrame(
        geometry=gpd.GeoSeries(
            [
                LineString(((0, 0), (10, 0))),
                LineString(((10, 0), (20, 0))),
                LineString(((10, 0), (10, 10))),
            ]
        ),
        crs=28992,
    )
    return Network(lines_gdf)


def node_at(network: Network, xy: tuple[float, float]) -> int:
    return next(n for n, geometry in network.graph.nodes(data="geometry") if geometry.equals(Point(xy)))


def assert_node_locations_in_sync(network: Network) -> None:
    expected = network.nodes[["type", "geometry"]]
    assert network.node_locations.index.equals(expected.index)
    assert network.node_locations["type"].tolist() == expected["type"].tolist()
    assert network.node_locations.geom_equals(expected.geometry).all()


def test_move_node(t_network):
    network = t_network
    node_id = network.move_node(Point(10.2, 0.3), max_distance=1, align_distance=1)
    assert node_id == node_at(network, (10.2, 0.3))
    # all links connected to the moved node start or end at the new location
    for u, v, geometry in network.graph.edges(data="geometry"):
        if u == node_id:
            assert geometry.coords[0] == (10.2, 0.3)
        if v == node_id:
            assert geometry.coords[-1] == (10.2, 0.3)
    assert_node_locations_in_sync(network)

    # nothing moves beyond max_distance
    assert network.move_node(Point(50, 50), max_distance=1, align_distance=1) is None


def test_add_node(t_network):
    network = t_network
    assert_node_locations_in_sync(network)
    node_id = network.add_node(Point(5, 0.2), max_distance=1, align_distance=1)
    assert node_id is not None
    assert len(network.graph.edges) == 4
    assert network.graph.nodes[node_id]["type"] == "connection"
    assert_node_locations_in_sync(network)


def test_path_to_line(t_network):
    network = t_network
    path = network.get_path(node_at(network, (0, 0)), node_at(network, (10, 10)), directed=False)
    line = network.path_to_line(path)
    assert line.coords[0] == (0, 0)
    assert line.coords[-1] == (10, 10)
    assert line.length == 20

    # travelling against the link direction reverses the link geometries
    reversed_line = network.path_to_line(path[::-1])
    assert reversed_line.coords[0] == (10, 10)
    assert reversed_line.coords[-1] == (0, 0)

    with pytest.raises(ValueError):
        network.path_to_line([path[0], path[-1]])


def test_get_link_geometry_forbidden_nodes(t_network):
    network = t_network
    source, junction, target = (node_at(network, xy) for xy in ((0, 0), (10, 0), (10, 10)))
    straight_line = LineString(((0, 0), (10, 10)))

    geometry = get_link_geometry(network, source, target, forbidden_nodes=[])
    assert geometry.length == 20

    # the only route passes the forbidden junction, so we get a straight line
    assert get_link_geometry(network, source, target, forbidden_nodes=[junction]).equals(straight_line)
    # a forbidden target also gives a straight line
    assert get_link_geometry(network, source, target, forbidden_nodes=[target]).equals(straight_line)


# %%
