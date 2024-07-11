# %%
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio import features
from ribasim_nl import CloudStorage

cloud = CloudStorage()

path = cloud.joinpath("Basisgegevens", "VanDerGaast_QH")


GHG_ASC = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "ghg-kk2010.asc"
)

KWEL_TIF = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "LSW_kwel_mmd.tif"
)

MA_TIF = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "LSW_MA_mmd.tif"
)

GLG_TIF = cloud.joinpath("Rijkswaterstaat", "verwerkt", "bergingsknopen", "GLG_cm.tif")


LSW_SHP = cloud.joinpath("Rijkswaterstaat", "verwerkt", "bergingsknopen", "lsws.shp")

XLSX_IN = cloud.joinpath("Basisgegevens", "VanDerGaast_QH", "lsws_cor_QhV5a.xlsx")

df = pd.read_excel(XLSX_IN, sheet_name="Data", skiprows=[0, 2]).set_index("LSWFINAL")

lsws_gdf = gpd.read_file(LSW_SHP)
lsws_gdf = lsws_gdf.dissolve(by="LSWFINAL").reset_index()

# make rasters
with rasterio.open(GHG_ASC) as raster_src:
    profile = raster_src.profile
    profile["driver"] = "GTiff"
    profile["dtype"] = "float32"
    profile["compress"] = "deflate"
    profile["tiled"] = "YES"
    profile["predictor"] = 2
    transform = profile["transform"]
    profile["crs"] = 28992
    out_shape = raster_src.shape
    fill = profile["nodata"]

    with rasterio.open(KWEL_TIF, "w+", **profile) as dst:
        shapes = (
            (i.geometry, df.at[i.LSWFINAL, "Kwel"]) for i in lsws_gdf.itertuples()
        )
        data = features.rasterize(
            shapes, out_shape=out_shape, fill=fill, transform=transform
        )
        dst.write(data, 1)

    with rasterio.open(MA_TIF, "w+", **profile) as dst:
        shapes = ((i.geometry, df.at[i.LSWFINAL, "MA"]) for i in lsws_gdf.itertuples())
        data = features.rasterize(
            shapes, out_shape=out_shape, fill=fill, transform=transform
        )
        dst.write(data, 1)

    with rasterio.open(GLG_TIF, "w+", **profile) as dst:
        shapes = ((i.geometry, df.at[i.LSWFINAL, "GLG"]) for i in lsws_gdf.itertuples())
        data = features.rasterize(
            shapes, out_shape=out_shape, fill=fill, transform=transform
        )
        dst.write(data, 1)
