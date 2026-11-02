import itertools

import geopandas as gpd
import pandas as pd
import shapely
from ribasim_nl.profiles import path_finder
from ribasim_nl.profiles.run import _label_main_routing_from_network
from shapely.geometry import LineString, Point, box


def test_canonical():
    gdf = gpd.GeoDataFrame(
        {"code": ["c", "b", "a", "d"]},
        geometry=[Point(1, 1), Point(0.0004, 0), None, Point(1, 1)],
        crs="EPSG:28992",
    )
    out = path_finder.canonical(gdf)
    # rounded to millimetres, sorted by geometry and then code, without geometry last
    assert out["code"].tolist() == ["b", "c", "d", "a"]
    assert out.geometry.iloc[0].equals(Point(0, 0))
    assert out.index.tolist() == [0, 1, 2, 3]
    # the order of the rows does not matter
    shuffled = path_finder.canonical(gdf.iloc[[3, 1, 0, 2]])
    pd.testing.assert_frame_equal(shuffled, out)
    # nor does noise below the rounding, or a duplicate index
    noisy = gdf.set_geometry(shapely.transform(gdf.geometry, lambda coords: coords + 1e-8))
    pd.testing.assert_frame_equal(path_finder.canonical(noisy), out)
    pd.testing.assert_frame_equal(path_finder.canonical(gdf.set_axis([0, 0, 1, 1])), out)


def test_main_route_independent_of_order():
    """Of the two routes of equal length around a square, the main route does not depend on the order or noise."""
    corners = [(0, 0), (0, 100), (100, 100), (100, 0)]
    lines = [LineString([a, b]) for a, b in itertools.pairwise([*corners, corners[0]])]
    basins = gpd.GeoDataFrame({"node_id": [1]}, geometry=[box(-10, -10, 110, 110)], crs="EPSG:28992")
    crossings = gpd.GeoDataFrame(geometry=[Point(0, 0), Point(100, 100)], crs="EPSG:28992")

    routes = set()
    for order in itertools.permutations(range(len(lines))):
        for noise in (0.0, 1e-8):
            hydro_objects = gpd.GeoDataFrame(
                geometry=[shapely.transform(lines[i], lambda coords, noise=noise: coords + noise) for i in order],
                crs="EPSG:28992",
            )
            hydro_objects = path_finder.canonical(hydro_objects)
            main_route = _label_main_routing_from_network(
                basins, hydro_objects, path_finder.canonical(crossings), 1.0, True, None
            )
            routes.add(frozenset(hydro_objects.geometry.iloc[list(main_route)].to_wkt()))
    assert len(routes) == 1
    # one of the two routes of two sides
    assert len(next(iter(routes))) == 2
