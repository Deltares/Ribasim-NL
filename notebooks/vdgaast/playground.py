# %%
import geopandas as gpd
import pandas as pd
import rasterio
from rasterstats import zonal_stats
from ribasim_nl import CloudStorage

cloud = CloudStorage()

# url = cloud.joinurl("Basisgegevens", "VanDerGaast_QH")

# cloud.download_content(url)
# %%

GHG_ASC = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "ghg-kk2010.asc"
)

GLG_ASC = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "glg-kk2010.asc"
)
KWEL_TIF = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "LHM_kwel_mmd.tif"
)

LSW_SHP = cloud.joinpath("Rijkswaterstaat", "verwerkt", "bergingsknopen", "lsws.shp")

XLSX = cloud.joinpath("Rijkswaterstaat", "verwerkt", "bergingsknopen", "resultaat.xlsx")

lsws_gdf = gpd.read_file(LSW_SHP)
lsws_gdf = lsws_gdf.dissolve(by="LSWFINAL").reset_index()

with rasterio.open(GHG_ASC) as raster_src:
    data = raster_src.read(1)
    affine = raster_src.transform
    ghg_mean = zonal_stats(
        lsws_gdf, data, affine=affine, stats="mean", nodata=raster_src.nodata
    )

with rasterio.open(GLG_ASC) as raster_src:
    data = raster_src.read(1)
    affine = raster_src.transform
    glg_mean = zonal_stats(
        lsws_gdf, data, affine=affine, stats="mean", nodata=raster_src.nodata
    )

with rasterio.open(KWEL_TIF) as raster_src:
    data = raster_src.read(1)
    affine = raster_src.transform
    kwel_mean = zonal_stats(
        lsws_gdf, data, affine=affine, stats="mean", nodata=raster_src.nodata
    )

df = pd.DataFrame(
    data={
        "LSWFINAL": lsws_gdf["LSWFINAL"].to_list(),
        "ghg": [i["mean"] for i in ghg_mean],
        "glg": [i["mean"] for i in glg_mean],
        "kwel": [i["mean"] for i in kwel_mean],
    }
)

df.to_excel(XLSX, index=False)
