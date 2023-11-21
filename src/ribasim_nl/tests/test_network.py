# %%
import geopandas as gpd
from ribasim_nl import Network
from shapely.geometry import LineString


def test_network(tmp_path):
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
    assert len(network.nodes) == len(network.graph.nodes) == 4
    assert len(network.links) == len(network.graph.edges) == 3
    assert all(i in ["geometry", "node_from", "node_to"] for i in network.links.columns)
    output_file = tmp_path / "network.gpkg"
    network.to_file(output_file)
    assert output_file.exists()


def test_gap_in_network(tmp_path):
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
    assert len(network.nodes) == len(network.graph.nodes) == 5

    # throw network away and regenerate it with tolerance
    network.reset()
    network.tolerance = 1

    # we'll find 4 nodes now, as the gap is repaired
    assert len(network.nodes) == len(network.graph.nodes) == 4
    assert len(network.links) == len(network.graph.edges) == 3

    assert all(i in ["geometry", "node_from", "node_to"] for i in network.links.columns)
