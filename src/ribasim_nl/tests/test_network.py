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
    assert len(network.graph.nodes) == 4
    assert len(network.graph.edges) == 3
    output_file = tmp_path / "network.gpkg"
    network.to_file(output_file)
    assert output_file.exists()
