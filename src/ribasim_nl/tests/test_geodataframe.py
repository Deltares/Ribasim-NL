import geopandas as gpd
from ribasim_nl.geodataframe import snap_line_boundaries, sorted_sjoin
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
