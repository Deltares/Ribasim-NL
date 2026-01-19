import logging

import geopandas as gpd
import numpy as np
import pandas as pd

LOG = logging.getLogger(__name__)


def weighted_average(values: np.ndarray[float], weights: np.ndarray[float]) -> float:
    return sum(weights * values) / sum(weights)


def trapezoidal_profile(
    depth: float, width: float, z_ref: float = 0, slope: float = 1 / 3, margin: float | tuple[float, float] = 1e-4
) -> list[tuple]:
    """Trapezoidal profile based on (maximum) depth, width (at surface), and slope.

    :param depth: (maximum) water depth
    :param width: width (at the surface)
    :param z_ref: reference (water) level, defaults to 0
    :param slope: slope (v/h) of the banks of the profile, defaults to 1/3
    :param margin: spatial step for defining an horizontal bottom, defaults to 1e-4
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


def measured_profile(*args, **kwargs) -> list[tuple]:
    LOG.warning("`measured_profile()` not yet implemented, `trapezoidal_profile()` used instead")
    return trapezoidal_profile(*args, **kwargs)


def assign_basin_profiles_trapezoidal(
    basins: gpd.GeoDataFrame, hydro_objects: gpd.GeoDataFrame, **kwargs
) -> pd.DataFrame:
    # optional arguments
    margin: float | tuple[float, float] = kwargs.get("margin", 1e-4)
    slope: float = kwargs.get("slope", 1 / 3)

    # validate required data
    _ho_cols = hydro_objects.columns
    for col in ("main-route", "width", "depth", "ht_code"):
        assert col in _ho_cols, f'Column-name "{col}" missing, which contains required data.'

    # warn if hydro-objects are a mix of 'doorgaand' and 'bergend'
    if len(hydro_objects["main-route"].unique()) > 1:
        LOG.critical("Basin profiles assigned without distinction in profile-type")

    # couple hydro-objects to basins
    temp = gpd.sjoin(basins, hydro_objects, how="left", predicate="intersects", lsuffix="basin", rsuffix="ho")

    # weighted average of profile dimensions
    temp["length"] = hydro_objects.loc[temp["index_ho"], "geometry"].length.values
    grouped = temp.groupby("node_id")
    width = grouped.apply(lambda row: weighted_average(row["width"], row["length"]), include_groups=False)
    depth = grouped.apply(lambda row: weighted_average(row["depth"], row["length"]), include_groups=False)
    length = grouped["length"].agg("sum")

    # concatenate profile-data
    depth.name = "depth"
    width.name = "width"
    dimensions = pd.concat([depth, width, length], axis=1, ignore_index=False)
    dimensions = pd.concat([basins.set_index("node_id"), dimensions], axis=1, ignore_index=False).reset_index(
        drop=False
    )

    # define basin-profiles
    temp = dimensions.apply(
        lambda row: trapezoidal_profile(
            row["depth"], row["width"], float(row["meta_streefpeil"]), slope=slope, margin=(0, margin)
        ),
        axis=1,
    ).explode()
    temp = pd.DataFrame(temp.tolist(), columns=["level", "area"], index=temp.index)
    temp["area"] *= dimensions["length"]
    temp.replace({"area": (0, margin)}, inplace=True)

    # define 'Basin / profile'-table
    table = pd.concat([temp, basins[["node_id"]]], axis=1)
    table = table[["node_id", "level", "area"]]
    return table
