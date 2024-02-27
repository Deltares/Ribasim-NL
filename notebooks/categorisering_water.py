# %%
import geopandas as gpd
import pandas as pd
from ribasim_nl import CloudStorage
from shapely.geometry import MultiPolygon

"""
General method used by Kadaster:
- dissolve top10NL layer to naam to an internal (not shared) layer
- read rws waterlines schaallabe per scale
- find intersecting polygons within scale lines
- add other polygons that are large of droogvallend

# Names
standard dissolving doesn't work, e.g.:
- Noordervaart exists in Brabant and Noord Holland.
- Names in Fryslan are scattered

- We make a custom dissolve using this trick: https://stackoverflow.com/questions/67280722/how-to-merge-touching-polygons-with-geopandas
- We keep "fysiek voorkomen" == "in sluis" separated to avoid problems at Hollandse IJssel (seperate at Gouda) and Overijsselskanaal (seperate at Vecht)

"""


cloud = CloudStorage()

EXCLUDE_NAMES = ["Noordzee"]
TO_REGIONAL = [
    "Eemskanaal",
    "Van Starkenborghkanaal",
    "Prinses Margrietkanaal",
    "Slotermeer",
    "Tjeukemeer",
    "Follegasloot",
    "Heegermeer",
    "Fluessen",
    "Westeinderplassen",
    "Braassemermeer",
    "Dokkumerdiep",
    "Slenk",
    "Jaap Deensgat",
    "Oude Robbengat",
    "Lmeer",
    "Toeloop naar Voormalige Veerhaven te Oostmahorn",
    "Voormalige Veerhaven te Oostmahorn",
    "Toeloop naar Bantshaven",
    "Vaarwater naar Oostmahorn",
    "Nieuwe Robbengat",
    "Noordergat",
]

top10_nl_water = cloud.joinpath("Basisgegevens", "Top10NL", "top10nl_Compleet.gpkg")
waterlijnen_rws = cloud.joinpath(
    "Rijkswaterstaat", "aangeleverd", "Waterlijnen_RWS", "WaterenNL.shp"
)

result_gpkg = cloud.joinpath(
    "Rijkswaterstaat", "Verwerkt", "categorie_oppervlaktewater.gpkg"
)

print("read")
watervlak_gdf = gpd.read_file(
    top10_nl_water, layer="top10nl_waterdeel_vlak", engine="pyogrio"
)
rws_waterlijn_gdf = gpd.read_file(waterlijnen_rws, engine="pyogrio")
rws_waterlijn_gdf.loc[rws_waterlijn_gdf.Label.isin(TO_REGIONAL), ["Schaallabe"]] = 3

rws_waterlijn_gdf.to_file(result_gpkg, layer="waterlijnen_rws")


# %% dissolve naar naam
# bepaal officiele naam, haalop uit naamnl of naamfries
watervlak_gdf.loc[:, ["naam"]] = watervlak_gdf["naamofficieel"]
watervlak_gdf.loc[watervlak_gdf.naam.isna(), ["naam"]] = watervlak_gdf[
    watervlak_gdf.naam.isna()
]["naamnl"]
watervlak_gdf.loc[watervlak_gdf.naam.isna(), ["naam"]] = watervlak_gdf[
    watervlak_gdf.naam.isna()
]["naamfries"]

watervlak_gdf = watervlak_gdf[~watervlak_gdf.naam.isin(EXCLUDE_NAMES)]

# %% dissolve naar naamofficieel, we voegen all polygonen zonder naam weer toe
print("dissolve")
data = {"naam": [], "geometry": []}

for name, df in watervlak_gdf.groupby("naam"):
    # dissolve touching polygons
    geometry = df.geometry.buffer(0.1).unary_union.buffer(-0.1)
    if isinstance(geometry, MultiPolygon):
        geometries = list(geometry.geoms)
    else:
        geometries = [geometry]  # that is 1 Polygon

    data["geometry"] += geometries
    data["naam"] += [name] * len(geometries)

# we add all unnamed polygons un-dissolved
watervlak_diss_gdf = pd.concat(
    [
        gpd.GeoDataFrame(data, crs=28992),
        watervlak_gdf[watervlak_gdf.naam.isna()][["naam", "geometry"]],
    ]
).reset_index(drop=True)

print("write")
watervlak_diss_gdf.to_file(result_gpkg, layer="watervlak_dissolved", engine="pyogrio")
# %% read dissolved layer
# watervlak_diss_gdf = gpd.read_file(
#     result_gpkg, layer="watervlak_dissolved", engine="pyogrio"
# )

# %% Nationaal hoofdwater
print("nationaal hoofdwater")
lower_limit = 0
upper_limit = 2
overlap = 0.1
category = "nationaal hoofdwater"
waterlijn_select_gdf = rws_waterlijn_gdf[
    (rws_waterlijn_gdf["Schaallabe"] > lower_limit)
    & (rws_waterlijn_gdf["Schaallabe"] <= upper_limit)
]

for row in waterlijn_select_gdf.itertuples():
    idx = watervlak_diss_gdf.sindex.intersection(row.geometry.bounds)
    idx = watervlak_diss_gdf.iloc[idx][
        watervlak_diss_gdf.iloc[idx].intersects(row.geometry)
    ].index
    mask = watervlak_diss_gdf.loc[idx].intersection(row.geometry).length > (
        row.geometry.length * overlap
    )
    idx = mask[mask].index
    watervlak_diss_gdf.loc[idx, ["categorie"]] = category
print("write")
watervlak_diss_gdf.to_file(result_gpkg, layer="watervlak_dissolved", engine="pyogrio")

# %%
