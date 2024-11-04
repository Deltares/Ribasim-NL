"""Functions to apply on a shapely.geometry"""

from typing import get_type_hints

from shapely.geometry import LineString, MultiPolygon, Point, Polygon
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
                              supported geometry.types are: {get_type_hints(drop_z)['geometry']}
                              """
            )

    return geometry


def edge(point_from: Point, point_to: Point) -> LineString:
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
