import math
from pathlib import Path

import geopandas as gpd
import numpy as np
import rasterio
from osgeo import gdal
from pandas import DataFrame
from rasterio import features  # noqa:F401
from rasterio.windows import from_bounds
from shapely.geometry import LineString, Polygon

DEFAULT_PERCENTILES = [
    0.01,
    0.1,
    1,
    5,
    10,
    20,
    30,
    40,
    50,
    60,
    70,
    80,
    90,
    95,
    99,
    99.9,
    99.99,
    100,
]


def build_vrt(raster_dir: Path):
    """Build a vrt-file inside a directory of rasters.

    Important notes!
    1. Only tif-files will be included
    2. All rasters should be equal in coordinate reference system, dtype (probably also nodata)

    Parameters
    ----------
    raster_dir : Path
        _description_
    """

    rasters_vrt = raster_dir / f"{raster_dir.name}.vrt"
    if rasters_vrt.exists():
        rasters_vrt.unlink()
    raster_files = [f"{i.as_posix()}" for i in raster_dir.glob("*.tif")]

    ds = gdal.VSIFOpenL(rasters_vrt.as_posix(), "w")
    gdal.VSIFCloseL(ds)

    vrt_ds = gdal.BuildVRT(rasters_vrt.as_posix(), raster_files)

    for idx, raster_file in enumerate(raster_files):
        file_name = Path(raster_file).name
        subdataset_name = f"SUBDATASET_{idx}_NAME"
        subdataset_description = f"SUBDATASET_{idx}_DESC"
        vrt_ds.GetRasterBand(1).SetMetadataItem(subdataset_name, file_name)
        vrt_ds.GetRasterBand(1).SetMetadataItem(
            subdataset_description, f"File: {file_name}"
        )

    # Save the changes and close the VRT file
    vrt_ds = None


def sample_level_area(
    raster_path: Path, polygon: Polygon, ident=None, percentiles=DEFAULT_PERCENTILES
) -> DataFrame:
    # Define the window coordinates (left, right, top, bottom)

    # Open raster and read window from polygon.bounds
    with rasterio.open(raster_path) as src:
        # Read the raster data within the specified window
        window = from_bounds(*polygon.bounds, transform=src.transform)
        profile = src.profile
        window_data = src.read(1, window=window)
        scales = src.scales
        dx, dy = src.res
        cell_area = dx * dy

        # get actual value if data is scaled
        if scales[0] != 1:
            window_data = np.where(
                window_data == profile["nodata"],
                profile["nodata"],
                window_data * scales[0],
            )

        # Get the affine transformation associated with the window
        window_transform = src.window_transform(window)

    # create a mask-array from polygon
    mask = rasterio.features.geometry_mask(
        [polygon], window_data.shape, window_transform, all_touched=False, invert=True
    )

    # include nodata as False in mask
    mask[window_data == profile["nodata"]] = False

    # compute levels by percentiles
    level = np.percentile(window_data[mask], percentiles)

    # compute areas by level and cell-area
    area = [np.sum(mask & (window_data <= value)) * cell_area for value in level]

    df = DataFrame({"percentiles": percentiles, "level": level, "area": area})
    df = df[~df[["level", "area"]].duplicated()]

    if ident is not None:
        print(f"sampled polygon {ident}")
        df["id"] = ident
    return df


def line_to_samples(
    line: LineString, sample_dist: float, crs=28992
) -> gpd.GeoDataFrame:
    """Convert line to samples

    Parameters
    ----------
    line : LineString
        Input line
    sample_dist : float
        minimal distance along line
    crs : int, optional
        output projection, by default 28992

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with Point geometry and distance along the line
    """
    nbr_points = math.ceil(line.length / sample_dist)
    gdf = gpd.GeoDataFrame(
        geometry=[
            line.interpolate(i / float(nbr_points - 1), normalized=True)
            for i in range(nbr_points)
        ],
        crs=crs,
    )
    gdf["distance"] = gdf.geometry.apply(lambda x: line.project(x))
    return gdf


def sample_elevation_distance(raster_path: Path, line: LineString) -> gpd.GeoDataFrame:
    """Sample values over an elevation raster using a line

    Parameters
    ----------
    raster_path : Path
        Path to raster
    line : LineString
        LineString to sample at raster-resolution

    Returns
    -------
    gpd.GeoDataFrame
        GeoDataFrame with Point-geometry, distance along the line and elevation value
    """

    with rasterio.open(raster_path) as src:
        sample_dist = abs(src.res[0])
        gdf = line_to_samples(line, sample_dist)
        coords = zip(gdf["geometry"].x, gdf["geometry"].y)
        gdf["elevation"] = [i[0] for i in src.sample(coords)]

        gdf = gdf[gdf.elevation != src.nodata]
        # get actual value if data is scaled
        if src.scales[0] != 1:
            gdf["elevation"] = gdf["elevation"] * src.scales[0]

    return gdf
