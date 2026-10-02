import geopandas as gpd
from ribasim_nl.styles import add_styles_to_geopackage
from shapely.geometry import Point


def test_add_styles_reproducible(tmp_path):
    """Adding styles to identical GeoPackages gives identical bytes, so DVC hashes are stable."""
    contents = []
    for name in ("a.gpkg", "b.gpkg"):
        path = tmp_path / name
        gpd.GeoDataFrame(geometry=[Point(0, 0)], crs=28992).to_file(path, layer="nodes")
        add_styles_to_geopackage(path)
        contents.append(path.read_bytes())

    assert contents[0] == contents[1]
