import geopandas as gpd
from peilbeheerst_model.general_functions import read_gpkg_layers
from shapely.geometry import Point


def test_read_gpkg_layers(tmp_path, capsys):
    path = tmp_path / "layers.gpkg"
    first = gpd.GeoDataFrame({"value": [1]}, geometry=[Point(0, 0)], crs="EPSG:28992")
    second = gpd.GeoDataFrame({"value": [2]}, geometry=[Point(1, 1)], crs="EPSG:28992")
    first.to_file(path, layer="first")
    second.to_file(path, layer="second")

    result = read_gpkg_layers(path, print_var=True)

    assert list(result) == ["first", "second"]
    assert result["first"].equals(first)
    assert result["second"].equals(second)
    assert capsys.readouterr().out.splitlines() == ["first", "second"]
