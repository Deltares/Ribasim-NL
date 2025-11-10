# %%
import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage

cloud = CloudStorage()
dir = cloud.joinpath("Basisgegevens/BGT")

waterdeel_df = gpd.read_file(dir / "bgt_waterdeel_current.gpkg")

# %% [markdown]

# ## Verdeelpunten
# We lezen de verdeelpunten in voor heel Nederland

# %% [markdown]

# verdeelpunten
# * lezen excel
# * eenheden omschrijven naar m3/s

verdeelpunten_df = pd.read_excel(
    cloud.joinpath("Landelijk/waterverdeling/verdeelpunten.xlsx"), sheet_name="verdeelpunten"
)
verdeelpunten_df = gpd.GeoDataFrame(verdeelpunten_df, geometry=gpd.GeoSeries(None, crs=28992))

# l/s -> m3/s
mask = verdeelpunten_df["aanvoer_eenheid"] == "l/s"
verdeelpunten_df.loc[mask, ["aanvoer_winter", "aanvoer_zomer", "aanvoer_min", "aanvoer_max"]] = (
    verdeelpunten_df.loc[mask, ["aanvoer_winter", "aanvoer_zomer", "aanvoer_min", "aanvoer_max"]] / 1000
)
verdeelpunten_df.loc[mask, "aanvoer_eenheid"] = "m3/s"


# %%

all_aanvoergebieden = []
all_hydroobject = []

# %% [markdown]
# Limburg
# * Toevoegen geometrie aan verdeelpunten
# * Hydroobject filteren op supply areas
# * Aanvoergebieden maken op basis van hydroobjecten + BGT

bronhouder = "W0665"
hydroobject_buffer = 0.5
mask = verdeelpunten_df["waterbeheerder"] == "Limburg"
hydamo_gpkg = cloud.joinpath("Limburg/verwerkt/4_ribasim/hydamo.gpkg")

# Toevoegen geometrie aan verdeelpunten
for objecttype in verdeelpunten_df[mask].objecttype.unique():
    geoseries = gpd.read_file(hydamo_gpkg, layer=objecttype).set_index("code")["geometry"].force_2d()
    objecttype_mask = mask & (verdeelpunten_df["objecttype"] == objecttype)
    verdeelpunten_df.loc[objecttype_mask, "geometry"] = geoseries.loc[
        verdeelpunten_df[objecttype_mask]["code_waterbeheerder"].to_numpy()
    ].to_numpy()


# hydroobject filteren
areas_gpkg = cloud.joinpath("Limburg/verwerkt/4_ribasim/areas.gpkg")
hydroobject_df = gpd.read_file(hydamo_gpkg, layer="hydroobject")
supply_df = gpd.read_file(areas_gpkg, layer="supply_areas")

names = ["Kaweische Loop", "Witte Dijk"]

hydroobject_select_df = hydroobject_df[
    hydroobject_df.within(supply_df.union_all().buffer(-90)) | hydroobject_df["naam"].isin(names)
]

# bgt filteren
waterdeel_select_df = waterdeel_df[waterdeel_df["bronhouder"] == bronhouder]
waterdeel_select_df = waterdeel_select_df[waterdeel_select_df.intersects(hydroobject_select_df.union_all())]


hydroobject_poly_df = hydroobject_select_df.copy()
hydroobject_poly_df["geometry"] = hydroobject_poly_df.buffer(hydroobject_buffer, cap_style="flat", join_style="mitre")
aanvoergebied_combined_df = pd.concat(
    [
        hydroobject_poly_df,
        waterdeel_select_df[["geometry"]],
    ]
)

aanvoergebied_combined_df = aanvoergebied_combined_df.dissolve().explode()[["geometry"]]
aanvoergebied_combined_df["waterbeheerder"] = "Limburg"


all_aanvoergebieden = [aanvoergebied_combined_df]
all_hydroobject = [hydroobject_select_df]

# %% [markdown]
# Aa en Maas
# * Hydroobject filteren op supply areas
# * Aanvoergebieden maken op basis van hydroobjecten + BGT

bronhouder = "W0654"
hydamo_gpkg = cloud.joinpath("AaenMaas/verwerkt/4_ribasim/hydamo.gpkg")
stuwen_shp = cloud.joinpath("AaenMaas/verwerkt/1_ontvangen_data/Na_levering_20240418/stuw.gpkg")
mask = verdeelpunten_df["waterbeheerder"] == "AaenMaas"
# Toevoegen geometrie aan verdeelpunten
for objecttype in verdeelpunten_df[mask].objecttype.unique():
    if objecttype == "stuw":
        geoseries = gpd.read_file(stuwen_shp).set_index("CODE")["geometry"].force_2d()
    else:
        geoseries = gpd.read_file(hydamo_gpkg, layer=objecttype).set_index("code")["geometry"].force_2d()

    objecttype_mask = mask & (verdeelpunten_df["objecttype"] == objecttype)
    assign_geometry_mask = mask & verdeelpunten_df["code_waterbeheerder"].isin(geoseries.index)

    verdeelpunten_df.loc[assign_geometry_mask, "geometry"] = geoseries.loc[
        verdeelpunten_df[assign_geometry_mask]["code_waterbeheerder"].to_numpy()
    ].to_numpy()


osm_waterway_gpkg = cloud.joinpath("Basisgegevens/OSM/Nederland/waterway_canal_the_netherlands.gpkg")
osm_stream_gpkg = cloud.joinpath("Basisgegevens/OSM/Nederland/waterway_stream_the_netherlands.gpkg")
osm_river_gpkg = cloud.joinpath("Basisgegevens/OSM/Nederland/waterway_stream_the_netherlands.gpkg")
waterschappen_gpkg = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")
waterschappen_df = gpd.read_file(waterschappen_gpkg)


bbox = waterschappen_df.set_index("code").at["38", "geometry"].bounds
osm_lines_df = pd.concat(
    [
        gpd.read_file(osm_waterway_gpkg, bbox=bbox),
        gpd.read_file(osm_stream_gpkg, bbox=bbox),
        gpd.read_file(osm_river_gpkg, bbox=bbox),
    ]
)

watergangen = [
    "Astense Aa",
    "Nieuwe AaSoeloop",
    "Oude Aa",
    "De Vlier",
    "Deurne-Noord",
    "Witte Dijk",
    "Kawijse Loop",
    "Snelle Loop",
    "Peelse Loop",
    "Vredepaal",
    "Sambeekse Uitwatering",
    "Sambeeksche Uitwatering",
    "Grave",
    "Biezenloop",
    "Schotense Loop",
    "Meanderende Aa",
    "Goorloop",
    "Donkervoortse Loop",
    "Kievitsloop",
    "Gulden Aa",
    "Lactariabeek",
    "Vierlingsbeekse Molenbeek",
    "Aanvoergemaal Loonse Vaart",
    "Aanvoergemaal Bossche Sloot",
    "Aanvoergemaal Herpt",
    "Teefelense Wetering",
    "Kanaal van Deurne",
    "Helenavaart",
    "Defensiekanaal",
    "Vlier",
    "Kaweiseloop",
]


supply_df = osm_lines_df[osm_lines_df["name"].isin(watergangen)].buffer(10)

supply_df.to_file(r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\Basisgegevens\waterverdeling\supply.gpkg")

hydroobject_df = gpd.read_file(hydamo_gpkg, layer="hydroobject")


globalids = [
    "{9FB44ABD-F1B8-4293-A85E-E829B6AB1243}",
    "{3E597E7B-1FC2-471D-A0F9-560722A69AFD}",
    "{595BABC5-9BF3-4E37-ABE0-297D4BD3FC2F}",
    "{A52A84BA-EA5E-414D-BB7D-7656AFCB3160}",
    "{386A3665-2600-44B9-B941-038775F9E20A}",
    "{F071A167-77F8-48E3-992C-AE6F2713B427}",
    "{9FAEFD70-84D8-4E1C-92CB-928D223483B6}",
    "{2DD8566B-4B0D-44A7-9B0E-CACD0C0E39F2}",
    "{1D1EA7C0-0DA2-440D-9C8F-C08B2F5795EF}",
    "{1DC1ABE9-CCB1-4413-B6A0-F62C45D472E5}",
    "{44AFB93D-F34A-4BBB-864E-23C547E829B8}",
    "{B8CC394C-D44E-4EF8-B9D0-8C34B133F091}",
    "{124D9897-6B4B-4CD0-B80E-C0B549508959}",
    "{1DC1ABE9-CCB1-4413-B6A0-F62C45D472E5}",
    "{02854C69-AC5C-4164-B8FA-507B70C1D00F}",
    "{F48E89B3-4ED9-4DA2-A687-22DACF259C81}",
    "{B51D5647-FCBC-4C13-864D-A46260473108}",
    "{2EB11D22-4817-4393-A15C-DCEE2080E70C}",
    "{DC5794BD-10AF-4DB4-971A-1A7B9BACDC34}",
    "{1A65A5BA-E81F-46DD-8EEA-F51EF36EEAC5}",
    "{BEC294CB-E691-417E-A660-39E93CD20560}",
    "{B292FAE7-D18E-420A-BF5D-1D52FA47BFEC}",
    "{F7B16C45-CD15-4852-B627-FE446F236687}",
    "{2E816594-AA22-49D7-9629-61D299B0903E}",
    "{B5E80480-B59B-47BC-96BF-05E7D61DA5C3}",
    "{27B903D6-89CD-4F9C-8463-C761A24E48C9}",
    "{76E2BAF4-96A7-4DE0-9A9A-1A23EB611C41}",
    "{D6EA2793-EB3A-445E-B150-9DC60E868A6A}",
    "{27EEB946-6908-4F16-AE3C-08502DEC922E}",
    "{B4CC7868-75F7-420D-B6A9-D7948DADE3DE}",
    "{31047362-8C69-436B-995F-027417B72ACD}",
    "{A7EB559D-4F9F-40AA-A1D0-EF54A309BA73}",
    "{11908D65-30BE-4783-816D-229357408CDF}",
    "{B695A987-AC51-43F6-930D-919221D2CC39}",
    "{97E2A845-B26D-41A6-B9A9-A05B2C647115}",
    "{6C275D94-013E-48EA-B7B3-5EE3140E959F}",
    "{284D1C10-870C-4F3F-9286-B5C271B637E8}",
    "{E1B795AF-2B1A-4CA1-A6AA-7BE56C62C1DD}",
    "{70900B77-3BC9-4E1B-A366-5BD4BE73E722}",
    "{7D60FF8A-E96D-47C7-8EC8-797AD2B6345E}",
    "{0E6D7FFD-8868-40FC-93D7-426A8A206B85}",
    "{B1FDBAA4-7549-4933-99F8-A35DC77AEC88}",
    "{B31C749D-0FB2-4856-92DF-7716F413100D}",
    "{0822E884-4043-4E8A-9B0D-AA720AEC2329}",
    "{05C99042-A3C7-4849-93C1-0DF4F891A5C2}",
]

hydroobject_select_df = hydroobject_df[
    hydroobject_df.within(supply_df.union_all()) | hydroobject_df["globalid"].isin(globalids)
]

waterdeel_select_df = waterdeel_df[waterdeel_df["bronhouder"] == bronhouder]
waterdeel_select_df = waterdeel_select_df[
    ~waterdeel_select_df["gml_id"].isin(["bc08b044c-7aa7-5392-5cbe-3d9ccc37dc5a"])
]
waterdeel_select_df = waterdeel_select_df[waterdeel_select_df.intersects(hydroobject_select_df.union_all())]


hydroobject_poly_df = hydroobject_select_df.copy()
hydroobject_poly_df["geometry"] = hydroobject_poly_df.buffer(hydroobject_buffer, cap_style="flat", join_style="mitre")
aanvoergebied_combined_df = pd.concat(
    [
        hydroobject_poly_df,
        waterdeel_select_df[["geometry"]],
    ]
)

aanvoergebied_combined_df = aanvoergebied_combined_df.dissolve().explode()[["geometry"]]
aanvoergebied_combined_df["waterbeheerder"] = "AaenMaas"

all_aanvoergebieden += [aanvoergebied_combined_df]
all_hydroobject += [hydroobject_select_df]

# %% [markdown]
# * wegschrijven laag verdeelpunten


aanvoergebieden_gpkg = cloud.joinpath("Basisgegevens/waterverdeling/aanvoer.gpkg")
verdeelpunten_df.to_file(aanvoergebieden_gpkg, layer="verdeelpunten")
pd.concat(all_aanvoergebieden).to_file(aanvoergebieden_gpkg, layer="aanvoergebieden")
pd.concat(all_hydroobject).to_file(aanvoergebieden_gpkg, layer="hydroobject")

# %%
