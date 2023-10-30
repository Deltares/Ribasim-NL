"""Some misc geometry editing utilities you usually find in QGIS"""
from typing import Union, get_type_hints

from shapely.geometry import LineString, MultiPolygon, Polygon
from shapely.ops import polygonize


def _validate_inputs(function, **kwargs):
    """Check if all inputs are of the correct type"""
    hints = get_type_hints(function)

    for k, v in kwargs.items():
        if k in hints.keys():
            if not isinstance(v, hints[k]):
                raise TypeError(
                    f"'{k}' must be of type '{hints[k].__name__}', not {type(v).__name__}"
                )


def sort_basins(basin_polygons: Union[MultiPolygon, list]) -> Union[MultiPolygon, list]:
    is_multipolygon = isinstance(basin_polygons, MultiPolygon)

    # sorting function
    def basin_sorter(polygon):
        return polygon.area

    # make list from basin_polygons
    if is_multipolygon:
        basin_polygons = list(basin_polygons.geoms)

    if is_multipolygon:
        return sorted(basin_polygons, key=basin_sorter)
    else:
        return MultiPolygon(sorted(basin_polygons, key=basin_sorter))


def cut_basin(basin_polygon: Polygon, line: LineString) -> MultiPolygon:
    """Cut a polygon with a line into two polygons.

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

    _validate_inputs(cut_basin, polygon=basin_polygon, line=line)

    unioned = basin_polygon.boundary.union(line)

    # use polygonize geos operator and filter out poygons outside of origina input polygon
    keep_polys = [
        poly
        for poly in polygonize(unioned)
        if poly.representative_point().within(basin_polygon)
    ]

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
