from itertools import product
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely import affinity
from shapely.geometry import (
    MultiLineString,
    LineString,
    MultiPolygon,
    Polygon,
    MultiPoint,
    Point,
)


def possibly_intersecting(dataframebounds, geometry, buffer=0):
    """
    Finding intersecting profiles for each branch is a slow process in case of large datasets
    To speed this up, we first determine which profile intersect a square box around the branch
    With the selection, the interseting profiles can be determines much faster.

    Parameters
    ----------
    dataframebounds : numpy.array
    geometry : shapely.geometry.Polygon
    """

    geobounds = geometry.bounds
    idx = (
        (dataframebounds[0] - buffer < geobounds[2])
        & (dataframebounds[2] + buffer > geobounds[0])
        & (dataframebounds[1] - buffer < geobounds[3])
        & (dataframebounds[3] + buffer > geobounds[1])
    )
    # Get intersecting profiles
    return idx


def find_nearest_branch(branches, geometries, method="overall", maxdist=5):
    """
    Method to determine nearest branch for each geometry.
    The nearest branch can be found by finding t from both ends (ends) or the nearest branch from the geometry
    as a whole (overall), the centroid (centroid), or intersecting (intersect).

    Parameters
    ----------
    branches : geopandas.GeoDataFrame
        Geodataframe with branches
    geometries : geopandas.GeoDataFrame
        Geodataframe with geometries to snap
    method='overall' : str
        Method for determine branch
    maxdist=5 : int or float
        Maximum distance for finding nearest geometry
    minoffset : int or float
        Minimum offset from the end of the corresponding branch in case of method=equal
    """
    # Check if method is in allowed methods
    allowed_methods = ["intersecting", "overall", "centroid", "ends"]
    if method not in allowed_methods:
        raise NotImplementedError(f'Method "{method}" not implemented.')

    # Add columns if not present
    if "branch_id" not in geometries.columns:
        geometries["branch_id"] = ""
    if "branch_offset" not in geometries.columns:
        geometries["branch_offset"] = np.nan

    if method == "intersecting":
        # Determine intersection geometries per branch
        geobounds = geometries.bounds.values.T
        for branch in branches.itertuples():
            selectie = geometries.loc[
                possibly_intersecting(geobounds, branch.geometry)
            ].copy()
            intersecting = selectie.loc[selectie.intersects(branch.geometry).values]

            # For each geometrie, determine offset along branch
            for geometry in intersecting.itertuples():
                # Determine distance of profile line along branch
                geometries.at[geometry.Index, "branch_id"] = branch.Index

                # Calculate offset
                branchgeo = branch.geometry
                mindist = min(0.1, branchgeo.length / 2.0)
                offset = round(
                    branchgeo.project(
                        branchgeo.intersection(geometry.geometry).centroid
                    ),
                    3,
                )
                offset = max(mindist, min(branchgeo.length - mindist, offset))
                geometries.at[geometry.Index, "branch_offset"] = offset

    else:
        branch_bounds = branches.bounds.values.T
        # In case of looking for the nearest, it is easier to iteratie over the geometries instead of the branches
        for geometry in geometries.itertuples():
            # Find near branches
            nearidx = possibly_intersecting(
                branch_bounds, geometry.geometry, buffer=maxdist
            )
            selectie = branches.loc[nearidx]

            if method == "overall":
                # Determine distances to branches
                dist = selectie.distance(geometry.geometry)
            elif method == "centroid":
                # Determine distances to branches
                dist = selectie.distance(geometry.geometry.centroid)
            elif method == "ends":
                # Since a culvert can cross a channel, it is
                crds = geometry.geometry.coords[:]
                dist = selectie["geometry"].apply(lambda x: max(x.distance(Point(*crds[0])),
                                                                x.distance(Point(*crds[-1])))).astype(float)
                # dist = (
                #     selectie.distance(Point(*crds[0]))
                #     + selectie.distance(Point(*crds[-1]))
                # ) * 0.5

            # Determine nearest
            if dist.min() < maxdist:
                branchidxmin = dist.idxmin()
                geometries.at[geometry.Index, "branch_id"] = dist.idxmin()
                if isinstance(geometry.geometry, Point):
                    geo = geometry.geometry
                else:
                    geo = geometry.geometry.centroid

                # Calculate offset
                branchgeo = branches.at[branchidxmin, "geometry"]
                mindist = min(0.1, branchgeo.length / 2.0)
                offset = max(
                    mindist,
                    min(branchgeo.length - mindist, round(branchgeo.project(geo), 3)),
                )
                geometries.at[geometry.Index, "branch_offset"] = offset
