"""
Definition of the 'Basin / profile'-table of Ribasim.

The definition of the basin profiles consists of a tabulated A(h)-relation. Every basin is split in 'doorgaand' and
'bergend' for which two different approaches apply regarding the definition of the A(h)-relation:
 'doorgaand':   The A(h)-relation is based on measured cross-sectional profiles of the hydro-objects representing this
                type (i.e., `main-route = True`).
 'bergend':     The A(h)-relation is based on 'Van der Gaast'-profiling in which cross-sectional profiles are assumed to
                be trapezoidal. The width (at the surface) of every hydro-object is based on a coupling to the BGT-data
                (see `./width.py`), and the depth is based on the so-called hydrotopes in the Netherlands (see
                `./hydrotopes.py`) in combination with a conversion table by Van der Gaast (see `./depth.py`), hence the
                name.
"""

import logging

import geopandas as gpd
import numpy as np
import pandas as pd

LOG = logging.getLogger(__name__)


def weighted_average(values: np.ndarray[float], weights: np.ndarray[float]) -> float:
    """Calculation of the weighted average.

    :param values: values to take the weighted average of
    :param weights: weights of the values

    :type values: numpy.array[float]
    :type weights: numpy.array[float]

    :return: weighted average
    :rtype: float
    """
    return sum(weights * values) / sum(weights)


def trapezoidal_profile(
    depth: float, width: float, z_ref: float = 0, slope: float = 1 / 3, margin: float | tuple[float, float] = 1e-4
) -> list[tuple]:
    """Trapezoidal profile based on (maximum) depth, width (at surface), and slope.

    :param depth: (maximum) water depth
    :param width: width (at the surface)
    :param z_ref: reference (water) level, defaults to 0
    :param slope: slope (v/h) of the banks of the profile, defaults to 1/3
    :param margin: spatial step for defining a horizontal bottom, defaults to 1e-4
        When two values are given, the first is considered as the horizontal margin, and the second as vertical margin.

    :type depth: float
    :type width: float
    :type z_ref: float, optional
    :type slope: float, optional
    :type margin: float | tuple[float, float], optional

    :return: A(h)-relation description as a list of (h, W)-coordinates
    :rtype: list[tuple]
    """
    if hasattr(margin, "__len__"):
        assert len(margin) == 2
        h_margin, v_margin = margin
    else:
        h_margin = v_margin = margin

    bottom_width = width - 2 * slope * depth
    return [(z_ref, width), (z_ref - depth + v_margin, max(bottom_width, h_margin)), (z_ref - depth, h_margin)]


def assign_basin_profiles(
    basins: gpd.GeoDataFrame, hydro_objects: gpd.GeoDataFrame, **kwargs
) -> pd.DataFrame | gpd.GeoDataFrame:
    """Assign trapezoidal cross-sectional profiles to basins: 'bergend'.

    :param basins: geospatial data of basins
    :param hydro_objects: geospatial data of hydro-objects
    :param kwargs: optional arguments

    :key as_geo_dataframe: return profile table as a `geopandas.GeoDataFrame` (without geometry), defaults to False
    :key margin: spatial step for defining a horizontal bottom, defaults to 1e-4
    :key slope: slope (v/h) of the banks of the profile, defaults to 1/3

    :type basins: geopandas.GeoDataFrame
    :type hydro_objects: geopandas.GeoDataFrame

    :return: profile table
    :rtype: pandas.DataFrame | geopandas.GeoDataFrame (optional)
    """
    # optional arguments
    as_geo_dataframe: bool = kwargs.get("as_geo_dataframe", False)
    margin: float = kwargs.get("margin", 1e-4)
    slope: float = kwargs.get("slope", 1 / 3)

    # validate required data
    _ho_cols = hydro_objects.columns
    missing = [col for col in ("main-route", "width", "depth", "ht_code") if col not in _ho_cols]
    if any(missing):
        msg = f"Column-name(s) with required data missing: {missing=}"
        raise ValueError(msg)

    # warn if hydro-objects are a mix of 'doorgaand' and 'bergend'
    if len(hydro_objects["main-route"].unique()) > 1:
        LOG.critical("Basin profiles assigned without distinction in profile-type")

    # couple hydro-objects to basins
    gdf_joined = gpd.sjoin(basins, hydro_objects, how="left", predicate="intersects", lsuffix="basin", rsuffix="ho")
    gdf_joined.dropna(subset=["index_ho"], inplace=True)

    # weighted average of profile dimensions
    gdf_joined["length"] = hydro_objects.loc[gdf_joined["index_ho"], "geometry"].length.values
    grouped = gdf_joined.groupby("node_id")
    width = grouped.apply(lambda row: weighted_average(row["width"], row["length"]), include_groups=False)
    depth = grouped.apply(lambda row: weighted_average(row["depth"], row["length"]), include_groups=False)
    length = grouped["length"].agg("sum")

    # concatenate profile-data
    depth.name = "depth"
    width.name = "width"
    dimensions = pd.concat([depth, width, length], axis=1, ignore_index=False)
    dimensions = pd.concat(
        [basins.set_index("node_id"), dimensions], axis=1, join="inner", ignore_index=False
    ).reset_index(drop=False)

    # clean up dataframe
    dimensions["meta_streefpeil"] = pd.to_numeric(dimensions["meta_streefpeil"], errors="coerce").fillna(0)

    # define basin-profiles
    df_profiles = dimensions.apply(
        lambda row: trapezoidal_profile(
            row["depth"], row["width"], float(row["meta_streefpeil"]), slope=slope, margin=(0, margin)
        ),
        axis=1,
    ).explode()
    df_profiles = pd.DataFrame(df_profiles.tolist(), columns=["level", "area"], index=df_profiles.index)
    df_profiles["node_id"] = dimensions["node_id"]
    df_profiles["area"] *= dimensions["length"]
    df_profiles.replace({"area": (0, margin)}, inplace=True)

    # define 'Basin / profile'-table
    table = df_profiles[["node_id", "level", "area"]]

    # return table (optionally as GeoDataFrame)
    if as_geo_dataframe:
        return gpd.GeoDataFrame(table)
    return table
