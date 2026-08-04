import sqlite3
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

import hydamo


def test_initialize():
    damo = hydamo.HyDAMO(version="2.2.1")
    assert damo.version == "2.2.1"
    assert "stuw" in damo.layers
    assert isinstance(damo.stuw, hydamo.ExtendedGeoDataFrame)
    assert "kruinbreedte" in damo.stuw.columns
    # no data is loaded
    assert len(damo.stuw) == 0
    # no file is written
    path = Path("hydamo.gpkg")
    damo.to_geopackage(path)
    assert not path.is_file()


def test_geopackage_roundtrip(tmp_path):
    damo = hydamo.HyDAMO(version="2.2")
    source = gpd.GeoDataFrame(
        {
            "afvoercoefficient": [1.25, None],
            "globalid": ["id-1", "id-2"],
            "hoogteconstructie": [2.5, 3.5],
            "kruinbreedte": [4.5, 5.5],
            "nen3610id": ["nen-1", "nen-2"],
            "objectid": pd.Series([1, None], dtype="object"),
            "soortregelbaarheid": ["regelbaar", None],
            "created_date": ["2024-01-02T03:04:05", None],
            "extra_column": ["drop", "drop"],
            "geometry": [Point(0, 0), Point(1, 1)],
        },
        crs="EPSG:28992",
    )
    damo.stuw.set_data(source)

    path = tmp_path / "hydamo.gpkg"
    damo.to_geopackage(path)

    result = gpd.read_file(path, layer="stuw")
    assert "extra_column" not in result.columns
    assert result["globalid"].tolist() == ["id-1", "id-2"]
    assert result["objectid"].iloc[0] == 1
    assert pd.isna(result["objectid"].iloc[1])
    assert result["created_date"].iloc[0] == pd.Timestamp("2024-01-02T03:04:05")
    assert result.geometry.equals(source.geometry)

    with sqlite3.connect(path) as connection:
        column_types = {row[1]: row[2] for row in connection.execute("PRAGMA table_info(stuw)")}
    assert column_types["globalid"] == "TEXT"
    assert column_types["objectid"] == "INTEGER"
    assert column_types["afvoercoefficient"] == "REAL"
    assert column_types["created_date"] == "DATETIME"

    loaded = hydamo.HyDAMO.from_geopackage(path)
    assert loaded.stuw["globalid"].tolist() == ["id-1", "id-2"]
