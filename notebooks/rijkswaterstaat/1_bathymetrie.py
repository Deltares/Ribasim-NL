# %%
import itertools
import math
from functools import partial

import fiona
import geopandas as gpd
import numpy as np
import rasterio
from geocube.api.core import make_geocube
from geocube.rasterize import rasterize_points_griddata
from rasterio.enums import Resampling
from rasterio.features import rasterize
from rasterio.merge import merge
from rasterio.transform import from_origin
from rasterio.windows import from_bounds
from shapely.geometry import MultiPolygon, box

from ribasim_nl import CloudStorage

cloud = CloudStorage()


out_dir = cloud.joinpath("Rijkswaterstaat/verwerkt/bathymetrie")
out_dir.mkdir(exist_ok=True)
baseline_file = cloud.joinpath(
    "baseline-nl_land-j23_6-v1/baseline.gdb"
)  # dit bestand is read-only voor D2HYDRO ivm verwerkersovereenkomst
layer = "bedlevel_points"

krw_poly_gpkg = cloud.joinpath("Basisgegevens/KRW/krw_oppervlaktewaterlichamen_nederland_vlakken.gpkg")

bathymetrie_nl = cloud.joinpath("Rijkswaterstaat/aangeleverd/bathymetrie")

water_mask_path = out_dir / "water-mask.gpkg"


cloud.synchronize(filepaths=[bathymetrie_nl, krw_poly_gpkg, baseline_file, water_mask_path])

res = 5
tile_size = 10000
bounds = (0, 300000, 320000, 660000)
nodata = -9999

# %%

datasets = [rasterio.open(i) for i in bathymetrie_nl.glob("*NAP.tif")]

data, transform = merge(datasets, bounds=bounds, res=(res, res), nodata=nodata)

data = np.where(data != nodata, data * 100, nodata).astype("int16")

profile = {
    "driver": "GTiff",
    "dtype": "int16",
    "nodata": nodata,
    "width": data.shape[2],
    "height": data.shape[1],
    "count": 1,
    "crs": 28992,
    "transform": transform,
    "blockxsize": 256,
    "blockysize": 256,
    "compress": "deflate",
}

with rasterio.open(out_dir / "bathymetrie-nl.tif", mode="w", **profile) as dst:
    dst.write(data)
    dst.build_overviews([5, 20], Resampling.average)
    dst.update_tags(ns="rio_overview", resampling="average")
    dst.scales = (0.01,)


# %%
print("read mask")
water_geometries = gpd.read_file(water_mask_path)
with fiona.open(baseline_file, layer=layer) as src:
    xmin, ymin, xmax, ymax = src.bounds
    xmin = math.floor(xmin / tile_size) * tile_size
    ymin = math.floor(ymin / tile_size) * tile_size
    xmax = math.ceil(xmax / tile_size) * tile_size
    ymax = math.ceil(ymax / tile_size) * tile_size
    xmins = list(range(xmin, xmax, tile_size))
    ymins = list(range(ymin, ymax, tile_size))
    xymins = itertools.product(xmins, ymins)
    transform = from_origin(xmin, ymax, res, res)

# %%
with rasterio.open(
    out_dir / f"{layer}_{xmin}_{ymin}_{xmax}_{ymax}.tif",
    mode="w",
    driver="GTiff",
    dtype="int16",
    nodata=-32768.0,
    count=1,
    crs=28992,
    compress="lzw",
    tiled=True,
    width=int((xmax - xmin) / res),
    height=int((ymax - ymin) / res),
    transform=transform,
) as dst:
    profile = dst.profile
    dst_bounds = dst.bounds
    for xmin, ymin in xymins:
        xmax = xmin + tile_size
        ymax = ymin + tile_size

        print(f"doing {xmin}, {ymin}, {xmax}, {ymax}")

        bounds = (xmin, ymin, xmax, ymax)
        area_poly = box(*bounds)
        print("select water-mask geometries")
        water_geometries_select = water_geometries[water_geometries.intersects(area_poly)]
        if not water_geometries_select.empty:
            area_poly_buffer = area_poly.buffer(res * 4)
            print("read points")
            gdf = gpd.read_file(
                baseline_file,
                layer=layer,
                bbox=area_poly_buffer.bounds,
            )
            gdf = gdf.loc[(gdf.ELEVATION > -60) & (gdf.ELEVATION < 50)]

            if not gdf.empty:
                # we drop duplicated points within the same meter
                print("drop duplicates")
                gdf["x"] = gdf.geometry.x.astype(int)
                gdf["y"] = gdf.geometry.y.astype(int)
                gdf.drop_duplicates(["x", "y"], inplace=True)

                print("make cube")
                cube = make_geocube(
                    gdf,
                    measurements=["ELEVATION"],
                    resolution=(res, -res),
                    rasterize_function=partial(rasterize_points_griddata, method="linear"),
                    interpolate_na_method="nearest",
                )

                print("scale cube")
                cube["ELEVATION"] = (cube.ELEVATION * 100).astype("int16")
                cube.ELEVATION.attrs["_FillValue"] = -32768
                cube.ELEVATION.attrs["scale_factor"] = 0.01

                print("clip cube")
                mask = water_geometries_select.geometry.union_all().intersection(area_poly)
                convex_hull = gdf.union_all().convex_hull
                if isinstance(mask, MultiPolygon):
                    mask = [i.intersection(convex_hull) for i in mask.geoms if i.intersects(convex_hull)]

                else:  # is one polygon
                    mask = [mask.intersection(convex_hull)]

                cube = cube.rio.clip(mask)

                if cube.ELEVATION.size > 0:
                    print("add to tiff")
                    window = from_bounds(*cube.rio.bounds(), transform)
                    dst.write(np.fliplr(np.flipud(cube.ELEVATION)), window=window, indexes=1)
                else:
                    print("no cube.ELEVATION points within water-mask")
            else:
                print("no samples within boundary")
        else:
            print("no water-mask within bounds")

    dst.build_overviews([5, 20], Resampling.average)
    dst.update_tags(ns="rio_overview", resampling="average")
    dst.scales = (0.01,)

# %%

# read bathymetrie-nl and its spatial characteristics
with rasterio.open(out_dir / "bathymetrie-nl.tif") as src:
    bathymetry_nl_data = src.read(1)
    bathymetry_nl_data = np.where(bathymetry_nl_data == src.nodata, nodata, bathymetry_nl_data)
    bounds = src.bounds
    transform = src.transform
    profile = src.profile
    scales = src.scales


with rasterio.open(out_dir / "bedlevel_points_-20000_300000_320000_660000.tif") as src:
    window = from_bounds(*bounds, src.transform)
    baseline_data = src.read(1, window=window)
    baseline_data = np.where(baseline_data == src.nodata, nodata, baseline_data)


# data = baseline_data

shapes = (i.geometry for i in water_geometries.itertuples() if i.baseline)

baseline_mask = rasterize(
    shapes,
    out_shape=bathymetry_nl_data.shape,
    transform=transform,
    fill=0,
    default_value=1,
    all_touched=True,
    dtype="int8",
).astype(bool)

data = np.where(baseline_mask, baseline_data, bathymetry_nl_data)

profile = {
    "driver": "GTiff",
    "dtype": "int16",
    "nodata": nodata,
    "width": data.shape[1],
    "height": data.shape[0],
    "count": 1,
    "crs": 28992,
    "transform": transform,
    "blockxsize": 256,
    "blockysize": 256,
    "compress": "deflate",
}

with rasterio.open(out_dir / "bathymetrie-merged.tif", mode="w", **profile) as dst:
    dst.write(data, 1)
    dst.build_overviews([5, 20], Resampling.average)
    dst.update_tags(ns="rio_overview", resampling="average")
    dst.scales = (0.01,)
