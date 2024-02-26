# %%
import geopandas as gpd
import pandas as pd
from ribasim_nl import CloudStorage
from ribasim_nl.geodataframe import join_by_poly_overlay

cloud = CloudStorage()

top10_nl_water = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")
waterlijnen_rws = cloud.joinpath(
    "Rijkswaterstaat", "aangeleverd", "Waterlijnen_RWS", "WaterenNL.shp"
)

waterschapsgrenzen = cloud.joinpath(
    "Basisgegevens", "waterschapsgrenzen", "waterschapsgrenzen.gpkg"
)

result_gpkg = cloud.joinpath(
    "Rijkswaterstaat", "Verwerkt", "categorie_oppervlaktewater.gpkg"
)

print("read")
watervlak_gdf = gpd.read_file(
    top10_nl_water, layer="top10nl_waterdeel_vlak", engine="pyogrio"
)

rws_waterlijn_gdf = gpd.read_file(waterlijnen_rws, engine="pyogrio")

waterschapsgrenzen_gdf = gpd.read_file(waterschapsgrenzen, engine="pyogrio")
waterschapsgrenzen_gdf.rename(columns={"name": "waterschap"}, inplace=True)

# %% dissolve naar naam
# bepaal officiele naam, haalop uit naamnl of naamfries
watervlak_gdf.loc[
    watervlak_gdf.naamofficieel.isna(), ["naamofficieel"]
] = watervlak_gdf[watervlak_gdf.naamofficieel.isna()]["naamnl"]
watervlak_gdf.loc[
    watervlak_gdf.naamofficieel.isna(), ["naamofficieel"]
] = watervlak_gdf[watervlak_gdf.naamofficieel.isna()]["naamfries"]


# %%dissolve naar naamofficieel, we voegen all polygonen zonder naam weer toe
print("dissolve")
watervlak_name_gdf = watervlak_gdf[watervlak_gdf.naamofficieel.notna()]

print("join waterschap")
watervlak_name_gdf = join_by_poly_overlay(
    watervlak_name_gdf,
    waterschapsgrenzen_gdf[["waterschap", "geometry"]],
    select_by="poly_area",
)
# make sure we don't lose polygons by missing waterschap attribute
watervlak_name_gdf.loc[watervlak_name_gdf.waterschap.isna(), "waterschap"] = "overig"
watervlak_name_gdf = watervlak_name_gdf.dissolve(
    by=["naamofficieel", "waterschap"], as_index=False
)

watervlak_diss_gdf = pd.concat(
    [watervlak_name_gdf, watervlak_gdf[watervlak_gdf.naamofficieel.isna()]]
)
watervlak_diss_gdf.reset_index(drop=True, inplace=True)

print("write")
watervlak_diss_gdf.to_file(result_gpkg, layer="watervlak_dissolved", engine="pyogrio")
# %% read dissolved layer
watervlak_diss_gdf = gpd.read_file(
    result_gpkg, layer="watervlak_dissolved", engine="pyogrio"
)

# %% Nationaal hoofdwater
print("nationaal hoofdwater")
scale_limit = 2
overlap = 0.1
category = "nationaal hoofdwater"
waterlijn_select_gdf = rws_waterlijn_gdf[rws_waterlijn_gdf["Schaallabe"] <= scale_limit]

for row in waterlijn_select_gdf.itertuples():
    idx = watervlak_diss_gdf.sindex.intersection(row.geometry.bounds)
    idx = watervlak_diss_gdf.iloc[idx][
        watervlak_diss_gdf.iloc[idx].intersects(row.geometry)
    ].index
    watervlak_diss_gdf.loc[idx, ["categorie"]] = category
    mask = watervlak_diss_gdf.loc[idx].intersection(row.geometry).length > (
        row.geometry.length * overlap
    )
    idx = mask[mask].index
    watervlak_diss_gdf.loc[idx, ["categorie"]] = category
print("write")
watervlak_diss_gdf.to_file(result_gpkg, layer="watervlak_dissolved", engine="pyogrio")

# %%
