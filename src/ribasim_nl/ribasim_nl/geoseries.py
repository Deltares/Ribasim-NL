# %%
import numpy as np
from geopandas import GeoSeries
from scipy.spatial import cKDTree
from shapely.geometry import Point

from ribasim_nl.geometry import basin_to_point


def basins_to_points(basin_series: GeoSeries) -> GeoSeries:
    """Get the representative points of a GeoSeries with basins

    Parameters
    ----------
    basin_series : GeoSeries
        Basin (Multi)polygons

    Returns
    -------
    GeoSeries
        Basin points
    """
    return basin_series.apply(basin_to_point)


def get_line_ends(line):
    return [Point(line.coords[0]), Point(line.coords[-1])]


def get_all_vertices(line):
    return [Point(coord) for coord in line.coords]


def snap_point_to_target(point, targets, tolerance):
    if not targets:
        return point

    coords = np.array([[p.x, p.y] for p in targets])
    tree = cKDTree(coords)
    dist, idx = tree.query([point.x, point.y])
    if dist <= tolerance:
        return targets[idx]
    return point
