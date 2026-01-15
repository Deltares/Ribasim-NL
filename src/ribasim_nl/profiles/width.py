"""
Generate representative profiles for the basins.

Representative profiles are split between 'doorgaand' and 'bergend', resulting in two different A(h)-relations per
basin, or only a 'bergend' A(h)-relation in case there are no 'doorgaand' routes within the basin.
"""

import itertools
import logging

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

LOG = logging.getLogger(__name__)


def couple_bgt_to_hydro_objects(
    hydro_objects: gpd.GeoDataFrame, bgt_data: gpd.GeoDataFrame, **kwargs
) -> gpd.GeoDataFrame:
    """Couple BGT-data to hydro-objects.

    Coupling of BGT-data to hydro-objects is based on the intersection between the polygons of the BGT-data and the
    lines of the hydro-objects data. Per hydro-object, all intersecting polygons are listed (optionally with a minimum
    overlap).

    # TODO: Add documentation on how to deal with non-overlapping hydro-objects

    :param hydro_objects: geospatial data of hydro-objects
    :param bgt_data: geospatial data with BGT-polygons
    :param kwargs: optional arguments

    :key bgt_id: column-name with ID-information of the BGT-data, defaults to 'gml_id'
    :key min_overlap: minimum required overlap (fraction of line-length) to couple BGT-polygon to hydro-object,
        defaults to None

    :type hydro_objects: geopandas.GeoDataFrame
    :type bgt_data: geopandas.GeoDataFrame

    :return: BGT-coupled hydro-objects
    :rtype: geopandas.GeoDataFrame

    :raises ValueError: if the main routing of the hydro-objects is not yet determined
    """
    # optional arguments
    bgt_id: str = kwargs.get("bgt_id", "gml_id")
    min_overlap: float = kwargs.get("min_overlap")

    # verify preprocessing
    if "main-route" not in hydro_objects.columns:
        msg = "Routing of main flowpaths not yet determined"
        raise ValueError(msg)

    # align CRSs
    if hydro_objects.crs != bgt_data.crs:
        bgt_data.to_crs(hydro_objects.crs)
        LOG.warning(f"BGT-data' CRS changed to {bgt_data.crs=}")

    def couple_bgt(network_selection: gpd.GeoDataFrame, bgt_selection: gpd.GeoDataFrame) -> pd.Series:
        """Couple BGT-data (selection) to (selection of) hydro-objects."""

        def overlap_with_bgt(line: shapely.LineString, polygon_index: int) -> bool:
            """Verify minimum overlap of hydro-object with BGT-polygon to be coupled."""
            polygon = bgt_data.loc[polygon_index, "geometry"]
            return (line.intersection(polygon).length / line.length) > min_overlap

        # TODO: Decide on method: No intersection results in discarding the hydro-object (how='inner'); or keep it (how='left')
        out = network_selection.sjoin(bgt_selection[[bgt_id, "geometry"]], how="inner", predicate="intersects")
        if min_overlap is not None:
            out = out[out.apply(lambda row: overlap_with_bgt(row["geometry"], row["index_right"]), axis=1)]
        out = out.groupby(level=0)["index_right"].apply(list)
        return out

    # couple BGT-data to hydro-objects
    main_route = hydro_objects["main-route"]
    ct1 = couple_bgt(hydro_objects[main_route], bgt_data)
    ct2 = couple_bgt(hydro_objects[~main_route], bgt_data[~bgt_data.index.isin(itertools.chain(*ct1.values))])

    # write BGT-coupling to hydro-objects
    couple_table = pd.concat([ct1, ct2], axis=0, ignore_index=False, sort=True)
    results = pd.concat([hydro_objects, couple_table], axis=1, ignore_index=False)

    # dealing with uncoupled hydro-objects
    hydro_objects["index_right"] = results["index_right"].apply(lambda i: i if isinstance(i, list) else [])

    # return BGT-coupled hydro-objects
    return hydro_objects


def estimate_width(hydro_objects: gpd.GeoDataFrame, bgt_data: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Estimate representative width of hydro-objects based on BGT-data.

    :param hydro_objects: geospatial data of hydro-objects
    :param bgt_data: geospatial data with BGT-polygons

    :type hydro_objects: geopandas.GeoDataFrame
    :type bgt_data: geopandas.GeoDataFrame

    :return: hydro-objects with width-estimates
    :rtype: geopandas.GeoDataFrame

    :raises ValueError: if hydro-objects are not coupled to BGT-data
    """
    if "index_right" not in hydro_objects.columns:
        msg = "Hydro-objects not yet coupled to BGT-data"
        raise ValueError(msg)

    def width_calculator(polygon: shapely.Polygon) -> float:
        """Estimation of a polygon's width based on its circumference (polygon.length) and surface area (polygon.area).

        The width of the polygon is calculated by assuming the polygon to be representing a rectangular shape. In doing
        so, its circumference (`polygon.length`) and area (`polygon.area`) can be used to calculate the two sides of
        such a rectangle by using the following two definitions:
         1. circumference: C = 2a + 2b;
         2. area: A = ab.
        Solving for a and b gives:

            a, b = 0.25 * (C +/- sqrt(C^2 - 16A))

        As the width is smaller than the length, the width (W) is estimated as:

            W = 0.25 * (C - sqrt(C^2 - 16A))

        :param polygon: BGT-water polygon
        :type polygon: shapely.Polygon

        :return: width estimation
        :rtype: float
        """
        return 0.25 * (polygon.length - np.sqrt(polygon.length**2 - 16 * polygon.area))

    def representative_width(indices: list[int]) -> float | None:
        """Take the mean of estimated widths of all connected polygons."""
        if indices:
            return float(np.mean([width_calculator(bgt_data.loc[i, "geometry"]) for i in indices]))
        return None

    # assign representative width estimates to the hydro-objects
    hydro_objects["width"] = hydro_objects["index_right"].apply(representative_width)
    return hydro_objects
