import geopandas as gpd
from geopandas.testing import assert_geodataframe_equal
from ribasim_nl.geodataframe import gpd_overlay, snap_line_boundaries, sorted_sjoin, threaded_overlay
from shapely.geometry import LineString, Point, box


def test_snap_line_boundaries():
    lines_gdf = gpd.GeoDataFrame(
        geometry=[
            LineString([(0, 0), (10, 0)]),
            LineString([(10.1, 0), (20, 0)]),  # starts within tolerance of the end of the first line
            LineString([(25, 0), (30, 0)]),  # too far away from the others
        ],
        crs=28992,
    )
    snapped_gdf = snap_line_boundaries(lines_gdf, tolerance=0.25)

    # the input is not modified
    assert lines_gdf.geometry.iloc[1].coords[0] == (10.1, 0)
    assert snapped_gdf.geometry.iloc[0].equals(lines_gdf.geometry.iloc[0])
    assert snapped_gdf.geometry.iloc[1].coords[0] == (10, 0)
    assert snapped_gdf.geometry.iloc[2].equals(lines_gdf.geometry.iloc[2])


def test_sorted_sjoin():
    points = gpd.GeoDataFrame(
        {"name": ["a", "b", "c"]}, geometry=[Point(1, 1), Point(50, 50), Point(1.5, 1.5)], index=[30, 10, 20]
    )
    # many overlapping polygons, in reverse order, so the spatial index order is unlikely to be the row order
    polygons = gpd.GeoDataFrame(
        {"code": [f"p{i}" for i in range(20)]},
        geometry=[box(0, 0, 2 + i * 0.01, 2) for i in range(20)],
        index=range(100, 80, -1),
    )
    joined = sorted_sjoin(points, polygons, how="left", predicate="intersects")

    assert joined.columns.tolist() == ["name", "geometry", "index_right", "code"]
    # the left order is kept, with the matches of each row in the row order of the right frame
    assert joined.index.tolist() == [30] * 20 + [10] + [20] * 20
    assert joined["code"].tolist()[:20] == polygons["code"].tolist()
    assert joined["code"].tolist()[21:] == polygons["code"].tolist()
    assert joined.loc[10, "index_right"] != joined.loc[10, "index_right"]  # NaN for no match


def test_threaded_overlay():
    # grids of overlapping squares, so rows intersect several geometries of the other frame
    df1 = gpd.GeoDataFrame(
        {"code": range(25)}, geometry=[box(x, y, x + 1.3, y + 1.3) for x in range(5) for y in range(5)]
    )
    df2 = gpd.GeoDataFrame(
        {"node_id": range(9)},
        geometry=[box(x * 1.7 + 0.2, y * 1.7 + 0.2, x * 1.7 + 2.1, y * 1.7 + 2.1) for x in range(3) for y in range(3)],
    )
    original = gpd_overlay._overlay_difference
    for how in ("union", "difference", "symmetric_difference", "identity"):
        expected = gpd.overlay(df1, df2, how=how, keep_geom_type=True)
        result = threaded_overlay(df1, df2, how=how, keep_geom_type=True)
        assert_geodataframe_equal(result, expected)
        assert result.geometry.geom_equals_exact(expected.geometry, tolerance=0).all()
    assert gpd_overlay._overlay_difference is original
