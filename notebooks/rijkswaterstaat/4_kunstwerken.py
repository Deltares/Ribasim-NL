# %%
#  We extraheren kunstwerken voor het hoofdwatersysteem uit RWS data. Methode:
# 1. We pakken de kunstwerken uit Netwerk Informatie Systeem ( NIS ) van Rijkswaterstaat (nis_all_kunstwerken_hws_2019.gpkg)
# 2. We selecteren de kunstwerken die binnen het KRW lichamen vallen
# 3. Daarnaast voegen we extra kunstwerken  die niet in de nis voorkomen maar wel noodzakelijk zijn voor modellering hoofdwatersysteem.
# Deze extra kunstwereken komen uit: baseline.gdb ; layer = "stuctures" en uit de lrws-legger:kunstwerken_primaire_waterkeringen)
# Ook voegen we drinkwateronttrekkingen toe

import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage

cloud = CloudStorage()

COLUMNS = [
    "code",
    "naam",
    "complex_code",
    "complex_naam",
    "beheerder",
    "soort",
    "sector",
    "bron",
    "capaciteit",
    "productie",
    "referentie",
    "photo_url",
    "geometry",
]

ADD_BASELINE = [
    "DM_68.30_R_US_Reevespuisluis",
    "OS_SK_Oosterscheldekering39_Geul-van-Roggenplaat",
    "KR_C_HK_Kadoelen",
    "CK_1017.96_L_SS_Rozenburgsesluis",
]

REMOVE_NIS = ["40D-350", "42D-001"]

ADD_NIS = [
    "21G-350",
    "33F-001",
    "34E-001",
    "34F-001",
    "44D-002",
    "44G-350",
    "45B-352",
    "45D-358",
    "45G-350",
    "45G-352",
    "49D-400",
    "50F-350",
    "51A-002",
    "51E-001",
    "51F-001",
    "51H-003",
    "51H-004",
    "51H-357",
    "57F-001",
    "57H-001",
    "58A-001",
    "58A-002",
    "58A-351",
    "58C-001",
    "58C-002",
    "58D-001",
    "60A-001",
    "61F-002",
    "61F-313",
]


ADD_PRIMAIRE_KERINGEN = ["KD.32.gm.015", "KD.44.gm.001"]

# %% functies


# functies
def photo_url(code):
    """Add photo url from Excel"""
    if code in kwk_media.index:
        return kwk_media.at[code, "photo_url"]
    else:
        return r"https://www.hydrobase.nl/static/icons/photo_placeholder.png"


def name_from_baseline(string):
    """Strip name from baseline"""
    last_underscore_index = string.rfind("_")
    if last_underscore_index != -1:
        last_underscore_index += 1
        return string[last_underscore_index:]
    else:
        return string


def name_from_intake_tag(string):
    """Strip name from an OSM intake tag"""
    return string[string.find("(") + 1 : -1]


# %% Inlezen bestanden

# input
krw_lichaam = cloud.joinpath("Basisgegevens/KRW/krw_oppervlaktewaterlichamen_nederland_vlakken.gpkg")

baseline = cloud.joinpath(
    "Basisgegevens/Baseline/baseline-nl_land-j23_6-v1/baseline.gdb"
)  # dit bestand is read-only voor D2HYDRO ivm verwerkersovereenkomst

nis_hws = cloud.joinpath("Rijkswaterstaat/aangeleverd/NIS/nis_all_kunstwerken_hws_2019.gpkg")
nis_hwvn = cloud.joinpath("Rijkswaterstaat/aangeleverd/NIS/nis_alle_kunstwerken_hwvn_2019.gpkg")

osm_scheeresluis = cloud.joinpath("Basisgegevens/OSM/osm_scheeresluis.gpkg")
osm_sluizen_belgie = cloud.joinpath("Basisgegevens/OSM/lock_belgium.gpkg")
osm_stuwen_belgie = cloud.joinpath("Basisgegevens/OSM/waterway_weir_belgium.gpkg")

primaire_kunstwerken = cloud.joinpath("Rijkswaterstaat/aangeleverd/kunstwerken_primaire_waterkeringen.gpkg")

onttrekkingen = cloud.joinpath("Basisgegevens/Onttrekkingen/onttrekkingen.gpkg")

kwk_media_path = cloud.joinpath("Rijkswaterstaat/verwerkt/kwk_media.csv")

cloud.synchronize(
    filepaths=[
        krw_lichaam,
        kwk_media_path,
        nis_hws,
        nis_hwvn,
        onttrekkingen,
        osm_scheeresluis,
        osm_sluizen_belgie,
        osm_stuwen_belgie,
        primaire_kunstwerken,
    ]
)
cloud.synchronize(filepaths=[baseline], overwrite=False)

# output
hydamo_path = cloud.joinpath("Rijkswaterstaat/verwerkt/hydamo.gpkg")

# media Excel
kwk_media = pd.read_csv(kwk_media_path)
kwk_media.set_index("code", inplace=True)

# geoDataFrames
nis_hws_gdf = gpd.read_file(nis_hws)
nis_hwvn_gdf = gpd.read_file(nis_hwvn)
krw_lichaam_gdf = gpd.read_file(krw_lichaam)
primaire_kunstwerken_gdf = gpd.read_file(primaire_kunstwerken)
drinkwater_gdf = gpd.read_file(onttrekkingen, layer="Drinkwater_oppervlaktewater")
energie_gdf = gpd.read_file(onttrekkingen, layer="Energiecentrales-inlaat")
baseline_kunstwerken_gdf = gpd.read_file(baseline, layer="structure_lines")
osm_scheeresluis_gdf = gpd.read_file(osm_scheeresluis)
osm_sluizen_belgie_gdf = gpd.read_file(osm_sluizen_belgie)
osm_stuwen_belgie_gdf = gpd.read_file(osm_stuwen_belgie)

osm_kunstwerken_gdf = pd.concat(
    [osm_scheeresluis_gdf, osm_sluizen_belgie_gdf, osm_stuwen_belgie_gdf],
    ignore_index=True,
)

# %% Locaties uit NIS

# Samenstellen locaties uit NIS
baseline_in_model = gpd.sjoin_nearest(baseline_kunstwerken_gdf, krw_lichaam_gdf, how="inner", max_distance=50)
selected_hws_points = gpd.sjoin_nearest(nis_hws_gdf, krw_lichaam_gdf, how="inner", max_distance=100)
selected_hwvn_points = gpd.sjoin_nearest(nis_hwvn_gdf, krw_lichaam_gdf, how="inner", max_distance=20)

# combineren nis_kunstwerken en filteren op soort
nis_points_gdf = pd.concat([selected_hws_points, selected_hwvn_points])

# Filter points
desired_kw_soort = [
    "Stuwen",
    "Stormvloedkeringen",
    "Spuisluizen",
    "Gemalen",
    "keersluizen",
    "Waterreguleringswerken",
    "Schutsluizen",
]
nis_points_gdf = nis_points_gdf[nis_points_gdf["kw_soort"].isin(desired_kw_soort)]

# Remove all nis_points that are far from baseline
nis_points_gdf.loc[:, ["nearest_distance"]] = [
    filtered_point["geometry"].distance(baseline_in_model.union_all())
    for _, filtered_point in nis_points_gdf.iterrows()
]
nis_points_gdf = nis_points_gdf[nis_points_gdf["nearest_distance"] < 500]

# Remove specific values
filtered_nearest_points = nis_points_gdf[~nis_points_gdf["complex_code"].isin(REMOVE_NIS)]
nis_points_gdf = nis_points_gdf.drop(["index_right"], axis=1)

nis_points_gdf = pd.concat(
    [
        nis_points_gdf,
        nis_hws_gdf[nis_hws_gdf["complex_code"].isin(ADD_NIS)],
        nis_hwvn_gdf[nis_hwvn_gdf["complex_code"].isin(ADD_NIS)],
    ]
)
nis_points_gdf.rename(columns={"kw_naam": "naam", "kw_code": "code", "kw_soort": "soort"}, inplace=True)
nis_points_gdf.loc[:, ["beheerder", "sector", "bron"]] = (
    "Rijkswaterstaat",
    "water",
    "NIS",
)
nis_points_gdf = nis_points_gdf[[i for i in COLUMNS if i in nis_points_gdf.columns]]

# %% Locaties uit Baseline

# kunstwerken uit BaseLine
baseline_kwk_add_gdf = baseline_kunstwerken_gdf[baseline_kunstwerken_gdf["NAME"].isin(ADD_BASELINE)]
baseline_kwk_add_gdf.loc[:, ["geometry"]] = baseline_kwk_add_gdf.centroid
baseline_kwk_add_gdf.loc[:, ["naam"]] = baseline_kwk_add_gdf["NAME"].apply(name_from_baseline)
baseline_kwk_add_gdf.loc[:, ["complex_naam"]] = baseline_kwk_add_gdf["naam"]
baseline_kwk_add_gdf.loc[:, ["code"]] = baseline_kwk_add_gdf["NAME"]

baseline_kwk_add_gdf.loc[:, ["beheerder", "sector", "bron"]] = (
    "Rijkswaterstaat",
    "water",
    "baseline",
)

baseline_kwk_add_gdf = baseline_kwk_add_gdf[[i for i in COLUMNS if i in baseline_kwk_add_gdf.columns]]

# %% Kunstwerken uit OSM

# kunstwerken uit OSM
osm_kunstwerken_gdf.loc[:, ["geometry"]] = osm_kunstwerken_gdf.centroid
osm_kunstwerken_gdf.rename(columns={"name": "naam", "osm_id": "code"}, inplace=True)

osm_kunstwerken_gdf.loc[:, ["complex_naam"]] = osm_kunstwerken_gdf["naam"]

osm_kunstwerken_gdf.loc[:, ["beheerder", "sector", "bron"]] = (
    "Rijkswaterstaat",
    "water",
    "OSM",
)

osm_kunstwerken_gdf = osm_kunstwerken_gdf[[i for i in COLUMNS if i in osm_kunstwerken_gdf.columns]]


# %% Primaire keringen

# RWS primaire keringen
primaire_kwk_add_gdf = primaire_kunstwerken_gdf[primaire_kunstwerken_gdf["kd_code1"].isin(ADD_PRIMAIRE_KERINGEN)]

# alles hernoemen naar juiste kolomnamen
primaire_kwk_add_gdf.rename(
    columns={"kd_code1": "code", "kd_naam": "naam", "naam_compl": "complex_naam"},
    inplace=True,
)

primaire_kwk_add_gdf.loc[:, ["beheerder", "sector", "bron"]] = (
    "Rijkswaterstaat",
    "water",
    "kunsterken primaire waterkeringen",
)

primaire_kwk_add_gdf = primaire_kwk_add_gdf[[i for i in COLUMNS if i in primaire_kwk_add_gdf.columns]]


# %% Toevoegen onttrekkingen
# Toevoegen onttrekkingen

# drinkwater

# drinkwater_gdf.loc[:, ["bron", "sector", "soort"]] = "OSM", "drinkwater", "Inlaat"
# drinkwater_gdf.loc[:, "naam"] = drinkwater_gdf.naam.apply(name_from_intake_tag)
# drinkwater_gdf.loc[:, "complex_naam"] = drinkwater_gdf["naam"]
drinkwater_gdf = drinkwater_gdf[[i for i in COLUMNS if i in drinkwater_gdf.columns]]
energie_gdf = energie_gdf[[i for i in COLUMNS if i in energie_gdf.columns]]
energie_gdf.loc[:, ["sector"]] = energie_gdf.sector.str.lower()


# Concatenate additional points
kunstwerken_gdf = pd.concat(
    [
        nis_points_gdf,
        baseline_kwk_add_gdf,
        osm_kunstwerken_gdf,
        primaire_kwk_add_gdf,
        drinkwater_gdf,
        energie_gdf,
    ]
)

# add photo_url
kunstwerken_gdf.loc[:, ["photo_url"]] = kunstwerken_gdf["code"].apply(photo_url)

# Save results to GeoPackage files
kunstwerken_gdf.to_file(hydamo_path, layer="kunstwerken", driver="GPKG")

# %%
