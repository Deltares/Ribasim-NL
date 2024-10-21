# %%
from pathlib import Path

import geopandas as gpd
import pytest
from shapely.geometry import LineString

from ribasim_nl import Network


@pytest.fixture
def osm_lines_gpkg():
    return Path(__file__).parent.joinpath("data", "osm_lines.gpkg")


@pytest.fixture
def edge_columns():
    return ["node_from", "node_to", "name", "id", "length", "geometry"]


def test_network(tmp_path, edge_columns):
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
    assert all(i in edge_columns for i in network.links.columns)
    output_file = tmp_path / "network.gpkg"
    network.to_file(output_file)
    assert output_file.exists()


def test_gap_in_network(edge_columns):
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

    assert all(i in edge_columns for i in network.links.columns)


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


# %%
