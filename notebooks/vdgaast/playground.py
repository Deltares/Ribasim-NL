# %%
import geopandas as gpd
import pandas as pd
import rasterio
from rasterio import features
from rasterstats import zonal_stats
from ribasim_nl import CloudStorage

cloud = CloudStorage()

path = cloud.joinpath("Basisgegevens", "VanDerGaast_QH")

if path.exists():
    url = cloud.joinurl("Basisgegevens", "VanDerGaast_QH")
    cloud.download_content(url)
# %%

GHG_ASC = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "ghg-kk2010.asc"
)

GLG_ASC = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "glg-kk2010.asc"
)
KWEL_TIF = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "LSW_kwel_mmd.tif"
)

MA_TIF = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "LSW_MA_mmd.tif"
)


LSW_SHP = cloud.joinpath("Rijkswaterstaat", "verwerkt", "bergingsknopen", "lsws.shp")

XLSX_IN = cloud.joinpath("Basisgegevens", "VanDerGaast_QH", "lsws_cor_QhV5a.xlsx")


XLSX_OUT = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "resultaat.xlsx"
)

df = pd.read_excel(XLSX_IN, sheet_name="Data", skiprows=[0, 2]).set_index("LSWFINAL")

# %%
lsws_gdf = gpd.read_file(LSW_SHP)
lsws_gdf = lsws_gdf.dissolve(by="LSWFINAL").reset_index()


# make rasters
with rasterio.open(GHG_ASC) as raster_src:
    profile = raster_src.profile
    transform = profile["transform"]
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


# %%


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

df = pd.DataFrame(
    data={
        "LSWFINAL": lsws_gdf["LSWFINAL"].to_list(),
        "ghg": [i["mean"] for i in ghg_mean],
        "glg": [i["mean"] for i in glg_mean],
    }
)

df.to_excel(XLSX_OUT, index=False)