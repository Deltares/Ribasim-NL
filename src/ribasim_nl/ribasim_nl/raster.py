from collections.abc import Callable
from functools import wraps
from pathlib import Path

import numpy as np
import rasterio
from pandas import DataFrame
from rasterio import features  # noqa:F401
from rasterio.windows import from_bounds
from shapely.geometry import Polygon


def with_rasterio_env[**P, R](func: Callable[P, R]) -> Callable[P, R]:
    """Run `func` within a single rasterio environment.

    Without an active environment, rasterio creates one for every call. With AWS credentials in the
    environment variables, each of those also creates a boto3 session, which is slow when repeated thousands
    of times. `Env.from_defaults()` is what rasterio itself uses when no environment is active.
    """

    @wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        with rasterio.Env.from_defaults():
            return func(*args, **kwargs)

    return wrapper


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


def sample_level_area(raster_path: Path, polygon: Polygon, ident=None, percentiles=DEFAULT_PERCENTILES) -> DataFrame:
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
