import logging
import time
import warnings

import geopandas as gpd
import pandas as pd

from .general import (
    connect_endpoints_by_buffer,
    get_most_overlapping_polygon,
    report_time_interval,
)

# %% wfd


def add_wfd_id_to_hydroobjects(
    hydroobjects: gpd.GeoDataFrame,
    wfd_lines: gpd.GeoDataFrame = None,
    wfd_polygon: gpd.GeoDataFrame = None,
    wfd_id_column: str = "owmident",
    buffer_distance: float = 10,
    overlap_ratio: float = 0.9,
) -> gpd.GeoDataFrame:
    """


    Parameters
    ----------
    hydroobjects : gpd.GeoDataFrame
        GeoDataFrame that contains (preprocessed) hydroobjects
    wfd_lines : gpd.GeoDataFrame
        GeoDataFrame that contains wfd lines.
    wfd_polygon : gpd.GeoDataFrame
        GeoDataFrame that contains wfd polygons.
    buffer_distance : float, optional
        Buffer distance for linefeatures. The default is 0.5.
    overlap_ratio : float, optional
        Minimum ratio for surface area overlap between
        buffer polygons hydroobjects and buffered wfd lines / wfd polygons.
        The default is 0.9.

    Returns
    -------
    GeoDataFrame that contains hydroobjects with their assigned wfd body id

    """  # noqa: D205
    warnings.filterwarnings("ignore")
    start_time = time.time()

    logging.info("Assigning wfd ids to corresponding hydroobjects...")

    # 1. Buffer lines and merge with polygons
    hydroobjects["left_polygon_geometry"] = hydroobjects.geometry.buffer(buffer_distance, join_style="round")

    wfd_total = None
    if wfd_lines is not None:
        wfd_lines["geometry"] = wfd_lines.geometry.buffer(buffer_distance * 1.5, join_style="round")
        wfd_lines = wfd_lines.explode().reset_index(drop=True)
        if wfd_total is None:
            wfd_total = wfd_lines[[wfd_id_column, "geometry"]]

    if wfd_polygon is not None:
        if wfd_total is None:
            wfd_total = wfd_polygon[[wfd_id_column, "geometry"]]
        else:
            wfd_total = pd.concat([wfd_total, wfd_polygon[[wfd_id_column, "geometry"]]], axis=0)

    wfd_total = wfd_total.dissolve(by=wfd_id_column, aggfunc="sum", as_index=False)

    # 3. Join wfd & hydroobjects
    hydroobjects["line_geometry"] = hydroobjects.geometry
    hydroobjects.geometry = hydroobjects.left_polygon_geometry

    # 4. Get most overlapping wfd polygons for each hydroobject
    hydroobjects = get_most_overlapping_polygon(hydroobjects, wfd_total, "code", wfd_id_column)

    # 5. Remove overlapping polygons with less than 90 % overlap
    hydroobjects[wfd_id_column] = hydroobjects.apply(
        lambda x: x["most_overlapping_polygon_id"]
        if x["most_overlapping_polygon_area"] > overlap_ratio * x["left_area"]
        else None
        if type(x["most_overlapping_polygon_area"]) == float  # noqa: E721: FIXME: isinstance(x["most_overlapping_polygon_area"], float)
        else None,
        axis=1,
    )

    # 6. Postprocessing
    hydroobjects["geometry"] = hydroobjects["line_geometry"]

    end_time = time.time()
    passed_time = report_time_interval(start_time, end_time)

    nr_hydroobjects_wfd = len(hydroobjects[hydroobjects[wfd_id_column].isna() == False][wfd_id_column])  # noqa: E712 not hydroobjects[wfd_id_column].isna()
    nr_unique_wfd_ids = len(hydroobjects[hydroobjects[wfd_id_column].isna() == False][wfd_id_column].unique())  # noqa: E712 not hydroobjects[wfd_id_column].isna()

    hydroobjects = hydroobjects.drop(
        columns=[
            "left_area",
            "left_polygon_geometry",
            "overlapping_areas",
            "line_geometry",
            "most_overlapping_polygon_id",
            "most_overlapping_polygon_area",
        ]
    )  #'overlapping_area' 'left_area'

    logging.info(
        f"Summary:\n\n\
          Number of hydroobjects that received a wfd id: {nr_hydroobjects_wfd} \n\
          Number of unique wfd ids within hydroobjects: {nr_unique_wfd_ids}\n"
    )

    logging.info(f"Finished within {passed_time}")

    return hydroobjects


# %% Preprocess Hydamo Hydroobjects


def preprocess_hydamo_hydroobjects(
    hydroobjects: gpd.GeoDataFrame,
    wfd_lines: gpd.GeoDataFrame = None,
    wfd_polygons: gpd.GeoDataFrame = None,
    buffer_distance_endpoints: float = 0.5,
    wfd_id_column: str = "owmident",
    buffer_distance_wfd: float = 10,
    overlap_ratio_wfd: float = 0.9,
) -> gpd.GeoDataFrame:
    """

    Parameters
    ----------
    hydroobjects : gpd.GeoDataFrame
        GeoDataFrame that contains (preprocessed) hydroobjects
    wfd_lines : gpd.GeoDataFrame
        GeoDataFrame that contains wfd lines.
    wfd_polygon : gpd.GeoDataFrame
        GeoDataFrame that contains wfd polygons.
    buffer_distance_endpoints (float): Buffer distance for connecting line boundary endpoints, expressed
        in the distance unit of vector line dataset
    wfd_id_column : str
        wfd id column
    buffer_distance_wfd : float, optional
        Buffer distance for linefeatures. The default is 10.
    overlap_ratio_wfd : float, optional
        Minimum ratio for surface area overlap between
        buffer polygons hydroobjects and buffered wfd lines / wfd polygons for assigning a wfd body to
        hydroobject.
        The default is 0.9.

    Returns
    -------
    GeoDataFrame that contains preprocessed hydroobjects

    """  # noqa: D205
    # Connect unconnected endpoints within buffer
    preprocessed_hydroobjects = connect_endpoints_by_buffer(hydroobjects, buffer_distance_endpoints)

    # Assign wfd waterlichaam to hydroobjects
    if wfd_lines is not None or wfd_polygons is not None:
        preprocessed_hydroobjects = add_wfd_id_to_hydroobjects(
            preprocessed_hydroobjects, wfd_lines, wfd_polygons, wfd_id_column, buffer_distance_wfd, overlap_ratio_wfd
        )

    return preprocessed_hydroobjects
