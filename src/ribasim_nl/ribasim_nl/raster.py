# %%
from pathlib import Path

import numpy as np
import rasterio
from osgeo import gdal
from pandas import DataFrame
from rasterio import features  # noqa:F401
from rasterio.windows import from_bounds
from shapely.geometry import Polygon

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


def level_area(
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

    if ident is not None:
        print(f"sampled polygon {ident}")
        df["id"] = ident
    return df


# %%
