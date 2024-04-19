# %%
import geopandas as gpd
import pandas as pd
from ribasim_nl import CloudStorage

cloud = CloudStorage()

# mapping van Schaallabe naar categorie
MAP_SCHALE = {
    "nationaal hoofdwater": {"lower_limit": 1, "upper_limit": 2},
    "regionaal hoofdwater": {"lower_limit": 2, "upper_limit": 5},
    "regionaal stromend water": {"lower_limit": 5, "upper_limit": 8},
}

# Bestanden op de cloud-storage
waterlijnen_rws = cloud.joinpath(
    "Rijkswaterstaat", "aangeleverd", "BRT_waterlijnen_RWS", "WaterenNL.shp"
)
recategorize_xlsx = cloud.joinpath("Rijkswaterstaat", "verwerkt", "recategorize.xlsx")
result_gpkg = cloud.joinpath(
    "Rijkswaterstaat", "Verwerkt", "categorie_oppervlaktewater.gpkg"
)

# %% Fix RWS-lijnen
rws_waterlijn_gdf = gpd.read_file(waterlijnen_rws, engine="pyogrio", fid_as_index=True)
rws_waterlijn_gdf.loc[:, "categorie"] = None

# drop all FIDs we don't need. Mainly offshore.
drop_fid_df = pd.read_excel(recategorize_xlsx, sheet_name="RWS_EXCLUDE_FID")
rws_waterlijn_gdf = rws_waterlijn_gdf.loc[
    ~rws_waterlijn_gdf.index.isin(drop_fid_df["FID"])
]

# first map scales to water systems
for k, v in MAP_SCHALE.items():
    mask = rws_waterlijn_gdf["Schaallabe"] >= v["lower_limit"]
    mask = mask & (rws_waterlijn_gdf["Schaallabe"] < v["upper_limit"])
    rws_waterlijn_gdf.loc[mask, ["categorie"]] = k

# drop all that is na
rws_waterlijn_gdf = rws_waterlijn_gdf[rws_waterlijn_gdf["categorie"].notna()]

# remap to national_hw
label_to_national_hw_df = pd.read_excel(
    recategorize_xlsx, sheet_name="RWS_LABEL_TO_NATIONAL_HW"
)
rws_waterlijn_gdf.loc[
    rws_waterlijn_gdf.Label.isin(label_to_national_hw_df["Label"]), ["categorie"]
] = "nationaal hoofdwater"

# remap to regional_hw
label_to_regional_hw_df = pd.read_excel(
    recategorize_xlsx, sheet_name="RWS_LABEL_TO_REGIONAL_HW"
)
rws_waterlijn_gdf.loc[
    rws_waterlijn_gdf.Label.isin(label_to_regional_hw_df["Label"]), ["categorie"]
] = "regionaal hoofdwater"

fid_to_regional_hw_df = pd.read_excel(
    recategorize_xlsx, sheet_name="RWS_FID_TO_REGIONAL_HW"
)

rws_waterlijn_gdf.loc[
    rws_waterlijn_gdf.index.isin(fid_to_regional_hw_df["FID"]), ["categorie"]
] = "regionaal hoofdwater"

fid_to_national_hw_df = pd.read_excel(
    recategorize_xlsx, sheet_name="RWS_FID_TO_NATIONAL_HW"
)

rws_waterlijn_gdf.loc[
    rws_waterlijn_gdf.index.isin(fid_to_national_hw_df["FID"]), ["categorie"]
] = "nationaal hoofdwater"


rws_waterlijn_gdf.to_file(result_gpkg, layer="waterlijnen_rws", engine="pyogrio")
