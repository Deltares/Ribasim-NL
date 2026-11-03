import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pytest
from ribasim_nl.profiles.depth import depth_from_hydrotopes, depth_from_measurements, normalise_measured_cross_sections
from ribasim_nl.profiles.gen_profiles import NO_DATA_LEVELS, _read_target_levels
from ribasim_nl.profiles.hydrotopes import Hydrotope, HydrotopeTable
from shapely.geometry import LineString, box


def test_depth_from_hydrotopes():
    table = HydrotopeTable(
        Hydrotope(1, "a", (1.0, 1.5, 2.0, 2.5)),
        Hydrotope(2, "b", (2.0, 2.5, 3.0, 3.5)),
        Hydrotope(3, "no depth", (np.nan, np.nan, np.nan, np.nan)),
    )
    # the hydrotope without depth comes first, so it would win a tie on row order
    hydrotope_map = gpd.GeoDataFrame(
        {"HYDROTYPE2": [3, 1, 2]}, geometry=[box(0, 10, 10, 20), box(0, 0, 10, 10), box(10, 0, 20, 10)]
    )
    hydro_objects = gpd.GeoDataFrame(
        {"width": [0.5, 0.5, 0.5, 2.0]},
        geometry=[
            LineString([(1, 1), (9, 1)]),  # within hydrotope 1
            LineString([(11, 1), (19, 1)]),  # within hydrotope 2
            LineString([(1, 10), (9, 10)]),  # on the boundary of hydrotopes 1 and 3
            LineString([(8, 5), (18, 5)]),  # mostly in hydrotope 2
        ],
        index=[40, 30, 20, 10],
    )
    result = depth_from_hydrotopes(hydro_objects, hydrotope_map, table, drop_na=False)

    # each hydro-object gets the hydrotope it overlaps most, by its own label
    assert result.index.tolist() == [40, 30, 20, 10]
    assert result["ht_code"].tolist() == [1, 2, 1, 2]
    assert result["depth"].tolist() == [1.0, 2.0, 1.0, 2.5]


def _write_target_levels(path: Path, levels: list[float]) -> Path:
    """Write the peilgebied and streefpeil layers of a crossings file, with a peilgebied per level."""
    ids = [f"id_{i}" for i in range(len(levels))]
    polygons = gpd.GeoDataFrame({"globalid": ids}, geometry=[box(i, 0, i + 1, 1) for i in range(len(levels))])
    polygons.set_crs("EPSG:28992").to_file(path, layer="peilgebied")
    table = gpd.GeoDataFrame({"waterhoogte": levels, "globalid": ids}, geometry=[None] * len(levels), crs="EPSG:28992")
    table.to_file(path, layer="streefpeil")
    return path


def test_no_data_target_levels_give_no_depth(tmp_path: Path):
    """A no-data level gets the depth of the hydrotope, rather than the level subtracted from a measured profile."""
    fn = _write_target_levels(tmp_path / "crossings.gpkg", [999.0, 0.0, -4.0])
    target_levels = _read_target_levels(fn, NO_DATA_LEVELS["Zuiderzeeland"])
    assert target_levels["meta_streefpeil"].isna().tolist() == [True, False, False]
    assert target_levels["meta_streefpeil"].iloc[1] == 0.0, "0 m NAP is a real level"

    cross_sections = gpd.GeoDataFrame(
        geometry=[
            LineString([(0.2, 0.5, -5), (0.8, 0.5, -5)]),
            LineString([(1.2, 0.5, -2), (1.8, 0.5, -2)]),
            LineString([(2.2, 0.5, -5), (2.8, 0.5, -5)]),
        ],
        crs="EPSG:28992",
    )
    normalised = normalise_measured_cross_sections(cross_sections, target_levels)
    hydro_objects = gpd.GeoDataFrame(
        {"main-route": [True] * 3, "depth": [1.5] * 3},
        geometry=[LineString([(i + 0.5, 0), (i + 0.5, 1)]) for i in range(3)],
        crs="EPSG:28992",
    )
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", RuntimeWarning)  # the mean of a missing depth
        depth = depth_from_measurements(hydro_objects, normalised)["depth"]
    assert depth.tolist() == [1.5, 2.0, 1.0]


def test_unknown_no_data_target_level_fails(tmp_path: Path):
    """A no-data level that is not known yet should not give a depth, also not without a list of them."""
    fn = _write_target_levels(tmp_path / "crossings.gpkg", [99.0, -1.0])
    with pytest.raises(AssertionError, match="out of range"):
        _read_target_levels(fn)
    assert _read_target_levels(fn, NO_DATA_LEVELS["Scheldestromen"])["meta_streefpeil"].isna().sum() == 1
