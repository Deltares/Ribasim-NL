from geopandas import GeoSeries

from ribasim_nl.utils.geometry import basin_to_point


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
