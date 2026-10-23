import geopandas as gpd
import pandas as pd
import pytest
from ribasim_nl.bgt_water import COLUMNS, current_objects, read_bgt_water, write_bgt_water
from shapely.geometry import box


def download(rows: list[dict]) -> gpd.GeoDataFrame:
    """A GML-light download with the given rows, and defaults for the other columns."""
    defaults = dict.fromkeys(COLUMNS) | {"bgt-status": "bestaand", "eindRegistratie": pd.NaT}
    return gpd.GeoDataFrame([defaults | row for row in rows], crs=28992)


def test_current_objects():
    gdf = download(
        [
            # an earlier and the current version of object a
            {"identificatie.lokaalID": "a", "eindRegistratie": pd.Timestamp("2020-01-01"), "geometry": box(0, 0, 1, 1)},
            {"identificatie.lokaalID": "a", "bronhouder": "W0652", "geometry": box(0, 0, 2, 1)},
            # object b, twice as it lies in two downloaded tiles
            {"identificatie.lokaalID": "b", "geometry": box(5, 0, 6, 1)},
            {"identificatie.lokaalID": "b", "geometry": box(5, 0, 6, 1)},
            # a planned object
            {"identificatie.lokaalID": "c", "bgt-status": "plan", "geometry": box(8, 0, 9, 1)},
        ]
    )
    current = current_objects(gdf)
    assert current["lokaalID"].to_list() == ["a", "b"]
    assert list(current.columns) == [*COLUMNS.values(), "geometry"]
    assert current.loc[0, "bronhouder"] == "W0652"
    assert current.loc[0, "geometry"].area == 2


def test_current_objects_missing_columns():
    with pytest.raises(AssertionError, match="misses columns"):
        current_objects(gpd.GeoDataFrame({"identificatie.lokaalID": ["a"]}, geometry=[box(0, 0, 1, 1)], crs=28992))
    # a download without history or optional attributes
    gdf = gpd.GeoDataFrame(
        {"identificatie.lokaalID": ["a"], "bgt-status": ["bestaand"]}, geometry=[box(0, 0, 1, 1)], crs=28992
    )
    current = current_objects(gdf)
    assert list(current.columns) == [*COLUMNS.values(), "geometry"]
    assert current["lokaalID"].to_list() == ["a"]
    assert current["plus-type"].isna().all()


def test_current_objects_of_empty_download():
    empty = gpd.GeoDataFrame({"identificatie.lokaalID": [], "bgt-status": []}, geometry=[], crs=28992)
    current = current_objects(empty)
    assert current.empty
    assert list(current.columns) == [*COLUMNS.values(), "geometry"]


def test_write_and_read_bgt_water(tmp_path):
    gdf = current_objects(
        download([{"identificatie.lokaalID": str(i), "geometry": box(i * 100, 0, i * 100 + 10, 10)} for i in range(50)])
    )
    path = tmp_path / "bgt_water.parquet"
    write_bgt_water(gdf, path, row_group_size=10)
    assert len(read_bgt_water(path=path)) == 50
    # objects 3 and 4 intersect the mask
    assert sorted(read_bgt_water(box(305, 5, 405, 6), path=path)["lokaalID"]) == ["3", "4"]
    # each object occurs once
    with pytest.raises(AssertionError, match="identify each object"):
        write_bgt_water(pd.concat([gdf, gdf]), path)
