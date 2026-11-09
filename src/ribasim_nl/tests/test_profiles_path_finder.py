import itertools

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from ribasim_nl.profiles import path_finder
from ribasim_nl.profiles.cross_section import assign_basin_profiles
from ribasim_nl.profiles.run import _label_main_routing_from_network
from shapely.geometry import LineString, Point, box


def test_canonical():
    gdf = gpd.GeoDataFrame(
        {"code": ["c", "b", "a", "d"]},
        geometry=[Point(1, 1), Point(0.0004, 0), None, Point(1, 1)],
        crs="EPSG:28992",
    )
    out = path_finder.canonical(gdf)
    # sorted by geometry and then code, without geometry last, keeping the coordinates
    assert out["code"].tolist() == ["b", "c", "d", "a"]
    assert out.geometry.iloc[0].equals(Point(0.0004, 0))
    assert out.index.tolist() == [0, 1, 2, 3]
    # the order of the rows does not matter, nor does a duplicate index
    pd.testing.assert_frame_equal(path_finder.canonical(gdf.iloc[[3, 1, 0, 2]]), out)
    pd.testing.assert_frame_equal(path_finder.canonical(gdf.set_axis([0, 0, 1, 1])), out)


@pytest.mark.parametrize("noisy", ["y", "z"])
def test_canonical_noise(noisy):
    """Geometries that differ by noise are sorted by their other columns."""
    codes = ["z", "y"]
    points = [Point(1 + 1e-9, 1) if code == noisy else Point(1, 1) for code in codes]
    gdf = gpd.GeoDataFrame({"code": codes}, geometry=points, crs="EPSG:28992")
    assert path_finder.canonical(gdf)["code"].tolist() == ["y", "z"]


def test_main_route_independent_of_order():
    """Of the two routes of equal length around a square, the main route does not depend on the order of the lines."""
    corners = [(0, 0), (0, 100), (100, 100), (100, 0)]
    lines = [LineString([a, b]) for a, b in itertools.pairwise([*corners, corners[0]])]
    basins = gpd.GeoDataFrame({"node_id": [1]}, geometry=[box(-10, -10, 110, 110)], crs="EPSG:28992")
    crossings = gpd.GeoDataFrame(geometry=[Point(0, 0), Point(100, 100)], crs="EPSG:28992")

    routes = set()
    for order in itertools.permutations(lines):
        hydro_objects = path_finder.canonical(gpd.GeoDataFrame(geometry=list(order), crs="EPSG:28992"))
        main_route = _label_main_routing_from_network(basins, hydro_objects, crossings, 1.0, True, None)
        routes.add(frozenset(hydro_objects.geometry.iloc[list(main_route)].to_wkt()))
    assert len(routes) == 1
    # one of the two routes of two sides
    assert len(next(iter(routes))) == 2


def test_overlaps():
    lines = np.array(
        [
            LineString([(5, 5), (5, 20)]),  # through the boundary
            LineString([(10, 5), (20, 5)]),  # ends at the boundary
            LineString([(10 - 1e-9, 5), (20, 5)]),  # reaches in by noise
            LineString([(11, 5), (20, 5)]),  # outside
        ]
    )
    assert path_finder.overlaps(lines, box(0, 0, 10, 10)).tolist() == [True, False, False, False]


@pytest.mark.parametrize("noise", [0.0, 1e-9, -1e-9])
def test_basin_profiles_ignore_touching_lines(noise):
    """A line in one basin that ends at the boundary with another basin only counts in the first, whatever the noise."""
    basins = gpd.GeoDataFrame(
        {"node_id": [1, 2], "meta_streefpeil": [0.0, 0.0]},
        geometry=[box(0, 0, 10, 10), box(10, 0, 20, 10)],
        crs="EPSG:28992",
    )
    hydro_objects = gpd.GeoDataFrame(
        {"main-route": [True], "width": [2.0], "depth": [1.0], "ht_code": [1]},
        geometry=[LineString([(1, 5), (10 + noise, 5)])],
        crs="EPSG:28992",
    )
    profiles = assign_basin_profiles(basins, hydro_objects)
    assert set(profiles["node_id"]) == {1}
    assert profiles["area"].max() == pytest.approx(2.0 * (9 + noise))
