from pathlib import Path

import numpy as np
import pytest
import shapely
from ribasim_nl.geometry import connected_parts, line_between, polygon_parts, union_polygons, without_slivers
from shapely.geometry import GeometryCollection, LineString, MultiPolygon, Point, Polygon, box


def test_polygon_parts():
    sliver = box(10, 0, 10 + 1e-6, 10)
    geometry = GeometryCollection([MultiPolygon([box(0, 0, 10, 10), sliver]), LineString([(0, 0), (1, 1)])])
    assert polygon_parts(geometry) == [box(0, 0, 10, 10), sliver]
    assert polygon_parts(geometry, tolerance=0.001) == [box(0, 0, 10, 10)]


def test_without_slivers():
    hole, sliver_hole = box(2, 2, 4, 4), box(6, 2, 6 + 1e-6, 8)
    polygon = Polygon(box(0, 0, 10, 10).exterior, [hole.exterior, sliver_hole.exterior])
    sliver = box(20, 0, 20 + 1e-6, 10)
    expected = MultiPolygon([Polygon(box(0, 0, 10, 10).exterior, [hole.exterior])])
    assert without_slivers(MultiPolygon([polygon, sliver])).equals(expected)
    # a bow tie, which crosses itself, becomes two triangles
    bow_tie = Polygon([(0, 0), (2, 2), (2, 0), (0, 2)])
    assert not bow_tie.is_valid
    result = without_slivers(bow_tie)
    assert result.is_valid and len(result.geoms) == 2 and result.area == pytest.approx(2.0)


def test_connected_parts():
    # the second part touches the first, the third is a sliver apart from the second, the fourth is far away
    parts = [box(0, 0, 10, 10), box(10, 10, 20, 20), box(20 + 1e-6, 10, 30, 20), box(100, 0, 110, 10)]
    geometry, detached = connected_parts(MultiPolygon(parts), Point(5, 5))
    assert geometry.equals(MultiPolygon(parts[:3]))
    assert detached == parts[3:]
    geometry, detached = connected_parts(MultiPolygon(parts), Point(105, 5))
    assert geometry == parts[3]
    assert detached == parts[:3]


def test_union_polygons():
    assert union_polygons(np.array([box(0, 0, 1, 1), box(1, 0, 2, 1)])).equals(box(0, 0, 2, 1))
    # a basin area and a sliver across its boundary, whose union GEOS 3.12 and 3.14 make 66 000 m2 too large
    path = Path(__file__).parent / "data" / "union_with_sliver.wkt"
    polygon, sliver = shapely.from_wkt(path.read_text().splitlines())
    assert union_polygons(np.array([polygon, sliver])).area == pytest.approx(polygon.area + sliver.area)


def test_line_between():
    line = LineString([(0, 0), (10, 0), (10, 10), (20, 10)])
    # the part of the line between the points nearest to them, ending on the points themselves
    assert list(line_between(line, Point(5, 1), Point(10, 5)).coords) == [(5, 1), (10, 0), (10, 5)]
    assert list(line_between(line, Point(0, 0), Point(20, 10)).coords) == list(line.coords)
    # without vertices in between
    assert list(line_between(line, Point(2, 0), Point(4, 0)).coords) == [(2, 0), (4, 0)]
    with pytest.raises(AssertionError, match="must lie before"):
        line_between(line, Point(10, 5), Point(5, 1))
