# %%
import itertools
import math
from functools import partial
from pathlib import Path

import fiona
import geopandas as gpd
import numpy as np
import rasterio
from geocube.api.core import make_geocube
from geocube.rasterize import rasterize_points_griddata
from rasterio.enums import Resampling
from rasterio.transform import from_origin
from rasterio.windows import from_bounds
from shapely.geometry import box

out_dir = Path(
    r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Rijkswaterstaat\verwerkt\bathymetrie"
)
out_dir.mkdir(exist_ok=True)
baseline_file = Path(
    r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\baseline-nl_land-j23_6-v1\bathymetrie.gpkg"
)
layer = "bedlevel_points"
baseline_file = Path(
    r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\baseline-nl_land-j23_6-v1\baseline.gdb"
)

krw_poly_gpkg = Path(
    r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Basisgegevens\KRW\krw_oppervlaktewaterlichamen_nederland_vlakken.gpkg"
)
res = 5
tile_size = 10000

# %%
# print("read krw waterlichamen")
# krw_poly = gpd.read_file(krw_poly_gpkg, engine="pyogrio")

# print("read TOP10NL-waterdelen")
# water_poly_gdf = gpd.read_file(
#     r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Basisgegevens\Top10NL\top10nl_Compleet.gpkg",
#     engine="pyogrio",
#     layer="top10nl_waterdeel_vlak",
# )

# print("create overlay")
# water_geometries = gpd.overlay(water_poly_gdf, krw_poly, how="intersection")

# water_geometries.to_file(out_dir / "water-mask.gpkg", engine="pyogrio")


# %%
print("read mask")
water_geometries = gpd.read_file(out_dir / "water-mask.gpkg", engine="pyogrio")
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
        print("clip mask")
        water_geometries_select = water_geometries[
            water_geometries.intersects(area_poly)
        ]
        if not water_geometries_select.empty:
            area_poly_buffer = area_poly.buffer(res * 4)
            print("read points")
            gdf = gpd.read_file(
                baseline_file,
                layer=layer,
                engine="pyogrio",
                bbox=area_poly_buffer.bounds,
            )
            gdf = gdf.loc[(gdf.ELEVATION > -60) & (gdf.ELEVATION < 5)]

            if not gdf.empty:
                print("drop duplicates")
                gdf.drop_duplicates(["geometry"], inplace=True)

                print("make cube")
                cube = make_geocube(
                    gdf,
                    measurements=["ELEVATION"],
                    resolution=(res, -res),
                    rasterize_function=partial(
                        rasterize_points_griddata, method="cubic"
                    ),
                    interpolate_na_method="nearest",
                )

                print("scale cube")
                cube["ELEVATION"] = (cube.ELEVATION * 100).astype("int16")
                cube.ELEVATION.attrs["_FillValue"] = -32768
                cube.ELEVATION.attrs["scale_factor"] = 0.01

                print("clip cube")
                mask = water_geometries_select.geometry.unary_union.intersection(
                    area_poly
                )
                cube = cube.rio.clip([mask])

                if cube.ELEVATION.size > 0:
                    print("add to tiff")
                    window = from_bounds(*cube.rio.bounds(), transform)
                    dst.write(
                        np.fliplr(np.flipud(cube.ELEVATION)), window=window, indexes=1
                    )
                else:
                    print("no cube.ELEVATION points within water-mask")
            else:
                print("no samples within boundary")
        else:
            print("no water within bounds")

    dst.build_overviews([5, 20], Resampling.average)
    dst.update_tags(ns="rio_overview", resampling="average")
    dst.scales = (0.01,)

# %%
