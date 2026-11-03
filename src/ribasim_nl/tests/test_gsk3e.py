import pytest
from ribasim_nl.gsk3e import line_between
from shapely.geometry import LineString, Point


def test_line_between():
    line = LineString([(0, 0), (10, 0), (10, 10), (20, 10)])
    # the part of the line between the points nearest to them, ending on the points themselves
    assert list(line_between(line, Point(5, 1), Point(10, 5)).coords) == [(5, 1), (10, 0), (10, 5)]
    assert list(line_between(line, Point(0, 0), Point(20, 10)).coords) == list(line.coords)
    # without vertices in between
    assert list(line_between(line, Point(2, 0), Point(4, 0)).coords) == [(2, 0), (4, 0)]
    with pytest.raises(AssertionError, match="must lie before"):
        line_between(line, Point(10, 5), Point(5, 1))
