# %%
import geopandas as gpd
import pandas as pd
import rasterio
from rasterstats import zonal_stats
from ribasim_nl import CloudStorage

cloud = CloudStorage()


LSW_SHP = cloud.joinpath("Rijkswaterstaat", "verwerkt", "bergingsknopen", "lsws.shp")


XLSX_OUT = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "resultaat.xlsx"
)

GHG_ASC = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "ghg-kk2010.asc"
)

GLG_TIF = cloud.joinpath("Rijkswaterstaat", "verwerkt", "bergingsknopen", "GLG_cm.tif")
KWEL_TIF = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "LSW_kwel_mmd.tif"
)

MA_TIF = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "bergingsknopen", "LSW_MA_mmd.tif"
)


# %%
lsws_gdf = gpd.read_file(LSW_SHP)
lsws_gdf = lsws_gdf.dissolve(by="LSWFINAL").reset_index()

with rasterio.open(GHG_ASC) as raster_src:
    data = raster_src.read(1)
    affine = raster_src.transform

    ghg_mean = zonal_stats(
        lsws_gdf, data, affine=affine, stats="mean", nodata=raster_src.nodata
    )

with rasterio.open(GLG_TIF) as raster_src:
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

with rasterio.open(MA_TIF) as raster_src:
    data = raster_src.read(1)
    affine = raster_src.transform
    ma_mean = zonal_stats(
        lsws_gdf, data, affine=affine, stats="mean", nodata=raster_src.nodata
    )

data_df = pd.DataFrame(
    data={
        "LSWFINAL": lsws_gdf["LSWFINAL"].to_list(),
        "GHG": [i["mean"] for i in ghg_mean],
        "GLG": [i["mean"] for i in glg_mean],
        "MA": [i["mean"] for i in ma_mean],
        "Kwel": [i["mean"] for i in kwel_mean],
    }
)


lsws_gdf.set_index("LSWFINAL", inplace=True)

calc_df = data_df[["LSWFINAL"]]
calc_df.loc[:, "LSW_area"] = calc_df["LSWFINAL"].apply(
    lambda x: lsws_gdf.at[x, "geometry"].area
)
calc_df.loc[:, "D1"] = 0
calc_df.loc[:, "2Q"] = data_df["MA"] * 2
calc_df.loc[:, "D2"] = data_df["GHG"] / 2
calc_df.loc[:, "Q15"] = data_df["MA"] / 2
calc_df.loc[:, "D3"] = data_df["GHG"]
calc_df.loc[:, "Q50"] = (data_df["MA"] + data_df["Kwel"]) * 0.330
calc_df.loc[:, "D4"] = data_df["GLG"]
calc_df.loc[:, "Q0_2"] = (data_df["MA"] + data_df["Kwel"]) * 0.2
calc_df.loc[:, "D6"] = calc_df[["D4"]] + 100
calc_df.loc[:, "Q365"] = data_df["Kwel"]

with pd.ExcelWriter(XLSX_OUT, engine="openpyxl") as writer:
    data_df.to_excel(writer, index=False, sheet_name="data")
    calc_df.to_excel(writer, index=False, sheet_name="Qh_mmd")
