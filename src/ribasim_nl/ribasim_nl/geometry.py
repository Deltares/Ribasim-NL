# %%
"""Functions to apply on a shapely.geometry"""

import typing
import warnings
from typing import get_type_hints

import numpy as np
from shapely.geometry import LineString, MultiLineString, MultiPolygon, Point, Polygon
from shapely.ops import polygonize, polylabel

from ribasim_nl.generic import _validate_inputs


def basin_to_point(basin_polygon: Polygon | MultiPolygon, tolerance: None | float = None) -> Point:
    """Return a representative point for the basin; centroid if it is within (Multi)Polygon or polylabel if not.

    Parameters
    ----------
    basin_polygon : Polygon | MultiPolygon
        (Multi)Polygon to get representative point for
    tolerance: None | float
        Enforce a tolerance by which the point is to be within the polygon

    Returns
    -------
    Point
        Representative point for the basin
    """
    if tolerance is not None:
        basin_polygon = basin_polygon.buffer(-tolerance)

    point = basin_polygon.centroid

    # if point not within basin, we return polylabel
    if not point.within(basin_polygon):
        # polylabel only works on polygons; we use largest polygon if input is MultiPolygon
        if isinstance(basin_polygon, MultiPolygon):
            basin_polygon = sort_basins(list(basin_polygon.geoms))[-1]
        point = polylabel(basin_polygon)

    return point


def sort_basins(basin_polygons: MultiPolygon | list[Polygon]) -> MultiPolygon | list:
    """Sort basins in a MultiPolygon or list of Polygons on .area in ascending order (small to large).

    Parameters
    ----------
    basin_polygons : MultiPolygon | list[Polygon]
        MultiPolygon or list of polygons to be sorted

    Returns
    -------
    MultiPolygon | list
        MultiPolygon with sorted polygons
    """
    is_multipolygon = isinstance(basin_polygons, MultiPolygon)

    # sorting function
    def basin_sorter(polygon):
        return polygon.area

    # make list from basin_polygons
    if is_multipolygon:
        basin_polygons = list(basin_polygons.geoms)

    if is_multipolygon:
        return MultiPolygon(sorted(basin_polygons, key=basin_sorter))
    else:
        return sorted(basin_polygons, key=basin_sorter)


def split_basin(basin_polygon: Polygon, line: LineString) -> MultiPolygon:
    """Split a polygon with a line into two polygons.

    Credits to https://kuanbutts.com/2020/07/07/subdivide-polygon-with-linestring/

    Parameters
    ----------
    basin_polygon : Polygon
        Polygon to cut into two
    line : LineString
        Cutline fro the Polygon

    Returns
    -------
    MultiPolygon
        Multipolygon with two polygons
    """
    _validate_inputs(split_basin, polygon=basin_polygon, line=line)

    unioned = basin_polygon.boundary.union(line)

    # use polygonize geos operator and filter out poygons outside of origina input polygon
    keep_polys = [poly for poly in polygonize(unioned) if poly.representative_point().within(basin_polygon)]

    # remaining polygons are the split polys of original shape
    if len(keep_polys) != 2:
        raise ValueError(
            f"""
                         Cut should always result in a MultiPolygon with 2 polygons, not {len(keep_polys)}.
                         Make sure you draw a correct cutline trough the polygon
                         """
        )

    # return sorted basins; smallest go first
    return MultiPolygon(sort_basins(keep_polys))


def split_basin_multi_polygon(basin_polygon: MultiPolygon, line: LineString):
    line_centre = line.interpolate(0.5, normalized=True)

    # get the polygon to cut
    basin_geoms = list(basin_polygon.geoms)
    if len(basin_geoms) == 0:
        cut_idx = 0
    else:
        try:
            cut_idx = next(idx for idx, i in enumerate(basin_geoms) if i.contains(line_centre))
        except StopIteration:
            cut_idx = next(idx for idx, i in enumerate(basin_geoms) if line.intersects(i))

    # split it
    right_basin_poly, left_basin_poly = split_basin(basin_geoms[cut_idx], line).geoms

    # concat left-over polygons to the right-side
    right_basin_poly = [right_basin_poly]
    left_basin_poly = [left_basin_poly]

    for idx, geom in enumerate(basin_geoms):
        if idx != cut_idx:
            if geom.distance(right_basin_poly[0]) < geom.distance(left_basin_poly[0]):
                right_basin_poly += [geom]
            else:
                left_basin_poly += [geom]

    return MultiPolygon(right_basin_poly), MultiPolygon(left_basin_poly)


def drop_z(geometry: LineString | MultiPolygon | Point | Polygon) -> Point | Polygon | MultiPolygon:
    """Drop the z-coordinate of a geometry if it has.

    Parameters
    ----------
    geometry : LineString | MultiPolygon | Point | Polygon
        Input geometry

    Returns
    -------
    Point | Polygon | MultiPolygon
        Output geometry
    """
    # MultiPolygon
    if isinstance(geometry, MultiPolygon):
        geometry = MultiPolygon([drop_z(poly) for poly in geometry.geoms])

    elif geometry.has_z:
        # LineString
        if isinstance(geometry, LineString):
            geometry = LineString([(x, y) for x, y, _ in geometry.coords])

        # Point
        elif isinstance(geometry, Point):
            geometry = Point(geometry.xy[0][0], geometry.xy[1][0])

        # Polygon
        elif isinstance(geometry, Polygon):
            exterior = [(x, y) for x, y, _ in geometry.exterior.coords]
            interiors = [[(x, y) for x, y, _ in ring.coords] for ring in geometry.interiors]
            geometry = Polygon(exterior, interiors)

        else:
            raise ValueError(
                f"""
                             geometry.geom_type = {geometry.geom_type} not supported.
                              supported geometry.types are: {get_type_hints(drop_z)["geometry"]}
                              """
            )

    return geometry


def link(point_from: Point, point_to: Point) -> LineString:
    """Create a LineString geometry between two Point geometries, dropping z-coordinate if any

    Args:
        point_from (Point): _description_
        point_to (Point): _description_

    Returns
    -------
        LineString: LineString without z-coordinate
    """
    if point_from.has_z:
        point_from = Point(point_from.x, point_from.y)
    if point_to.has_z:
        point_to = Point(point_to.x, point_to.y)
    return LineString((point_from, point_to))


def project_point(line: LineString, point: Point, tolerance: float = 1) -> Point:
    """Projects a point to a LineString

    Args:
        line (LineString): LineString to project point on
        point (Point): Point to project on LineString
        tolerance (float): Tolerance of point to line

    Returns
    -------
        Point: Projected Point
    """
    if point.distance(line) > tolerance:
        raise ValueError(f"point > {tolerance} from line ({point.distance(line)})")
    return line.interpolate(line.project(point))


@typing.overload
def split_line(
    line: LineString, point: Point, *deprecated: typing.Any, tolerance: float = 0.1, as_multilinestring: None
) -> MultiLineString | LineString: ...


# TODO: Next transition step: Default `as_multilinestring=None` with `=True`-behaviour and a UserWarning


@typing.overload
def split_line(
    line: LineString,
    point: Point,
    *deprecated: typing.Any,
    tolerance: float = 0.1,
    as_multilinestring: typing.Literal[True] = True,
) -> MultiLineString | LineString: ...


@typing.overload
def split_line(
    line: LineString,
    point: Point,
    *deprecated: typing.Any,
    tolerance: float = 0.1,
    as_multilinestring: typing.Literal[False],
) -> tuple[LineString, ...]: ...


def split_line(
    line: LineString,
    point: Point,
    *deprecated: typing.Any,
    tolerance: float = 0.1,
    as_multilinestring: bool | None = True,
) -> MultiLineString | LineString | tuple[LineString, ...]:
    """Split a line into two lines based on a point.

    As the built-in `shapely.ops.split` is not designed to do this splitting, its use should be avoided.

    This line-splitting function uses the splitting point as part of the newly generated lines.

    Args:
        line (LineString): line to be split
        point (Point): point to mark split-location
        deprecated (any): the `tolerance`-argument will eventually be keyword-only
        tolerance (float, optional): tolerance of distance from point to line, defaults to 0.1.
        as_multilinestring (bool, optional): return split line as MultiLineString-object, defaults to True
            raises deprecation warning as the future implementation will always return a tuple of LineString-objects

    Returns
    -------
        MultiLineString | LineString: in case `as_multilinestring=True`, returns a MultiLineString with two lines if
            being split otherwise a single LineString (will be deprecated)
        tuple[LineString, ...]: in case `as_multilinestring=False`, returns a tuple of one or two LineString-objects,
            and is the intended approach for the future
    """
    # deprecation warnings
    if deprecated:
        warnings.warn(
            "Passing 'tolerance' positionally is deprecated. Use keyword argument 'tolerance='.",
            FutureWarning,
            stacklevel=2,
        )
        tolerance = deprecated[0]
    if as_multilinestring is None:
        warnings.warn(
            "Calling 'split_line' without specifying 'as_multilinestring' is deprecated. "
            "In a future version, this will return 'tuple[LineString, ...]'. "
            "Set 'as_multilinestring' explicitly (and set to False for future-proofing the code).",
            UserWarning,
            stacklevel=2,
        )
    elif as_multilinestring:
        warnings.warn(
            "The 'split_line'-function will return tuple with LineString-objects in the future. "
            "Change to the future behaviour by setting 'as_multilinestring=False'.",
            FutureWarning,
            stacklevel=2,
        )

    # point is too far from the line: no splitting
    if point.distance(line) > tolerance:
        return line if as_multilinestring else (line,)

    # point is too close to the line's end: no splitting
    distance = line.project(point)
    if distance <= tolerance or distance >= line.length - tolerance:
        return line if as_multilinestring else (line,)

    # find splitting index
    coordinates = np.array(line.coords)
    segments = coordinates[1:] - coordinates[:-1]
    seg_lengths = np.linalg.norm(segments, axis=1)
    cum_lengths = np.cumsum(seg_lengths)
    i = np.searchsorted(cum_lengths, distance) + 1

    # create split lines
    line1 = LineString([*coordinates[:i], point])
    line2 = LineString([point, *coordinates[i:]])
    if as_multilinestring:
        return MultiLineString([line1, line2])
    return line1, line2


# def split_line(line: LineString, point: Point, tolerance: float = 0.1) -> MultiLineString | LineString:
#     """Split a line into a 2 geometry multiline
#
#     Args:
#         line (LineString): input line
#         point (Point): point to split on
#         tolerance (float, optional): tolerance of point to line. Defaults to 0.1.
#
#     Returns
#     -------
#         MultiLineString | LineString: in case LineString is split, return MultiLineString with 2 LineStrings
#     """
#     # if point is within tolerance of line.boundary we don't split
#     if line.boundary.distance(point) < tolerance:
#         return line
#
#     # try if a normal split works
#     result = split(line, point)
#     if len(list(result.geoms)) == 2:
#         return MultiLineString(result)
#
#     # if not, we try it again
#     else:
#         # project the point on the line, checking if it's not too far of first
#         point = project_point(line, point, tolerance)
#
#         # we snap the line to the point so it will have the point coordinate
#         line = snap(line, point, 1e-8)
#
#         # now we should be able to split
#         result = split(line, point)
#         if len(list(result.geoms)) == 2:
#             return MultiLineString(result)
#         else:
#             return line


def snap_boundaries_to_other_line(line: LineString, other_line: LineString, tolerance: float = 0.1) -> LineString:
    """Snap the boundaries of line to the boundary or link/vertice of other_line if boundaries are within tolerance"""
    coords = list(line.coords)
    start_pt, end_pt = line.boundary.geoms

    # modify other_start_pt if within tolerance
    if start_pt.distance(other_line) < tolerance:
        if start_pt.distance(other_line.boundary) < tolerance:
            pt = next(i for i in other_line.boundary.geoms if i.distance(start_pt) < tolerance)
        else:
            pt = line.interpolate(line.project(start_pt))
        coords[0] = (pt.x, pt.y)

    # modify end_start_pt if within tolerance
    if end_pt.distance(other_line) < tolerance:
        if end_pt.distance(other_line.boundary) < tolerance:
            pt = next(i for i in other_line.boundary.geoms if i.distance(end_pt) < tolerance)
        else:
            pt = line.interpolate(line.project(end_pt))
        coords[-1] = (pt.x, pt.y)

    return LineString(coords)
