import geopandas as gpd
from ribasim_nl.drainage_units import (
    assign_drainage_units_to_basins,
    assign_segments_to_basins,
    basin_areas_from_drainage_units,
)
from shapely.geometry import LineString, box

CRS = 28992


def _inputs():
    # two Basins side by side, split at x = 10
    basin_areas = gpd.GeoDataFrame({"node_id": [1, 2]}, geometry=[box(0, 0, 10, 10), box(10, 0, 20, 10)], crs=CRS)
    # segment 1 lies mostly in Basin 1, segment 2 in Basin 2, segment 3 outside both
    segments = gpd.GeoDataFrame(
        {"segment_id": [1, 2, 3]},
        geometry=[LineString([(2, 5), (11, 5)]), LineString([(15, 1), (15, 9)]), LineString([(30, 0), (30, 10)])],
        crs=CRS,
    )
    # the drainage unit of segment 1 reaches into Basin 2, that of segment 3 overlaps Basin 2 only
    drainage_units = gpd.GeoDataFrame(
        {"segment_id": [1, 2, 3]},
        geometry=[box(0, 0, 14, 10), box(14, 0, 18, 10), box(18, 0, 25, 10)],
        crs=CRS,
    )
    return basin_areas, segments, drainage_units


def test_assign_segments_to_basins():
    basin_areas, segments, _ = _inputs()
    assert assign_segments_to_basins(segments, basin_areas).to_dict() == {1: 1, 2: 2}


def test_assign_drainage_units_to_basins():
    basin_areas, segments, drainage_units = _inputs()
    units = assign_drainage_units_to_basins(drainage_units, segments, basin_areas)
    assert units.node_id.tolist() == [1, 2, 2]
    assert units.assigned_by.tolist() == ["segment", "segment", "overlap"]


def test_basin_areas_from_drainage_units():
    basin_areas, segments, drainage_units = _inputs()
    areas = basin_areas_from_drainage_units(basin_areas, drainage_units, segments, boundary=box(0, 0, 22, 10))
    areas = areas.set_index("node_id").area
    # Basin 1 gets the drainage unit of segment 1, up to x = 14
    assert areas[1] == 140
    # Basin 2 gets the rest, up to the boundary at x = 22
    assert areas[2] == 80


def test_basin_areas_drop_slivers():
    basin_areas, segments, drainage_units = _inputs()
    # a detached sliver of Basin 1 that no drainage unit covers
    basin_areas.loc[0, "geometry"] = box(0, 0, 10, 10).union(box(0, 11, 1, 13))
    areas = basin_areas_from_drainage_units(basin_areas, drainage_units, segments, box(0, 0, 22, 10), min_part_area=10)
    assert areas.set_index("node_id").area[1] == 140
