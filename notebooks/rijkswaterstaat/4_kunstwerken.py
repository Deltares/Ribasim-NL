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


# %%
def photo_url(code):
    if code in kwk_media.index:
        return kwk_media.at[code, "photo_url"]  # noqa: PD008
    else:
        return r"https://www.hydrobase.nl/static/icons/photo_placeholder.png"


krw_lichaam = cloud.joinpath(
    "Basisgegevens", "KRW", "krw_oppervlaktewaterlichamen_nederland_vlakken.gpkg"
)

nis_hws = cloud.joinpath(
    "Rijkswaterstaat", "aangeleverd", "NIS", "nis_all_kunstwerken_hws_2019.gpkg"
)
baseline = cloud.joinpath("baseline-nl_land-j23_6-v1", "baseline.gdb")
osm = cloud.joinpath("basisgegevens", "osm", "osm_scheeresluis.gpkg")

nis_hwvn = cloud.joinpath(
    "Rijkswaterstaat", "aangeleverd", "NIS", "nis_alle_kunstwerken_hwvn_2019.gpkg"
)
primaire_kunstwerken = cloud.joinpath(
    "Rijkswaterstaat", "aangeleverd", "kunstwerken_primaire_waterkeringen.gpkg"
)

drinkwater = cloud.joinpath("drinkwaterbedrijven", "drinkwater_inlets.gpkg")

# load media
kwk_media = pd.read_csv(cloud.joinpath("Rijkswaterstaat", "verwerkt", "kwk_media.csv"))
kwk_media.set_index("code", inplace=True)

# Load GeoDataFrames
(
    krw_lichaam_gdf,
    nis_hws_gdf,
    nis_hwvn_gdf,
    primaire_kunstwerken_gdf,
) = (
    gpd.read_file(file)
    for file in [krw_lichaam, nis_hws, nis_hwvn, primaire_kunstwerken]
)
drinkwater_gdf = gpd.read_file(drinkwater, layer="inlet__netherlands")
baseline_kunstwerken_gdf = gpd.read_file(baseline, layer="structure_lines")
osm_kunstwerken_gdf = gpd.read_file(osm)

# Spatial joins
baseline_in_model = gpd.sjoin_nearest(
    baseline_kunstwerken_gdf, krw_lichaam_gdf, how="inner", max_distance=50
)
selected_hws_points = gpd.sjoin_nearest(
    nis_hws_gdf, krw_lichaam_gdf, how="inner", max_distance=100
)
selected_hwvn_points = gpd.sjoin_nearest(
    nis_hwvn_gdf, krw_lichaam_gdf, how="inner", max_distance=20
)

# Combine selected points
selected_points = pd.concat([selected_hws_points, selected_hwvn_points])

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
filtered_points = selected_points[
    selected_points["kw_soort"].isin(desired_kw_soort)
].copy()

# Calculate nearest distance
filtered_points.loc[:, ["nearest_distance"]] = [
    filtered_point["geometry"].distance(baseline_in_model.unary_union)
    for _, filtered_point in filtered_points.iterrows()
]

# Filter points based on distance
filtered_nearest_points = filtered_points[
    filtered_points["nearest_distance"] < 500
].copy()

# Remove specific values
filtered_nearest_points = filtered_nearest_points[
    ~filtered_nearest_points["complex_code"].isin(["40D-350", "42D-001"])
]
filtered_nearest_points = filtered_nearest_points.drop(["index_right"], axis=1)


# Additional structures from Baseline, primaire waterkeringen and NIS and drinking water inlets
def name_from_baseline(string):
    last_underscore_index = string.rfind("_")
    if last_underscore_index != -1:
        last_underscore_index += 1
        return string[last_underscore_index:]
    else:
        return string


baseline_kwk_add_gdf = baseline_kunstwerken_gdf[
    baseline_kunstwerken_gdf["NAME"].isin(
        [
            "DM_68.30_R_US_Reevespuisluis",
            "OS_SK_Oosterscheldekering39_Geul-van-Roggenplaat",
            "KR_C_HK_Kadoelen",
            "CK_1017.96_L_SS_Rozenburgsesluis",
        ]
    )
]

osm_kunstwerken_gdf.loc[:, ["geometry"]] = osm_kunstwerken_gdf.centroid
osm_kunstwerken_gdf.rename(
    columns={"osm_id": "kw_code", "name": "kw_naam"}, inplace=True
)

baseline_kwk_add_gdf.loc[:, ["geometry"]] = baseline_kwk_add_gdf.centroid
baseline_kwk_add_gdf.loc[:, ["kw_naam"]] = baseline_kwk_add_gdf["NAME"].apply(
    name_from_baseline
)
baseline_kwk_add_gdf.loc[:, ["bron"]] = "baseline"
baseline_kwk_add_gdf.loc[:, ["complex_naam"]] = baseline_kwk_add_gdf["kw_naam"]
baseline_kwk_add_gdf.loc[:, ["kw_code"]] = baseline_kwk_add_gdf["NAME"]

primaire_kwk_add_gdf = primaire_kunstwerken_gdf[
    primaire_kunstwerken_gdf["kd_code1"].isin(["KD.32.gm.015", "KD.44.gm.001"])
]

primaire_kwk_add_gdf.rename(
    columns={"kd_code1": "kw_code", "kd_naam": "kw_naam", "naam_compl": "complex_naam"},
    inplace=True,
)
# %%
# Add drinking water inlets (OSM)
primaire_kwk_add_gdf.loc[:, ["bron"]] = "kunsterken primaire waterkeringen"
drinkwater_gdf.loc[:, ["bron"]] = "OSM"
drinkwater_gdf.rename(
    columns={
        "osm_id": "kw_code",
        "name": "kw_naam",
        "operator": "beheerder",
    },
    inplace=True,
)

additional_points = pd.concat(
    [
        primaire_kwk_add_gdf,
        nis_hws_gdf[
            nis_hws_gdf["complex_code"].isin(
                [
                    "49D-400",
                    "44D-002",
                    "58C-001",
                    "45B-352",
                    "33F-001",
                    "21G-350",
                    "58D-001",
                    "51F-001",
                ]
            )
        ],
        nis_hwvn_gdf[
            nis_hwvn_gdf["complex_code"].isin(
                [
                    "49D-400",
                    "44D-002",
                    "58C-001",
                    "45B-352",
                    "33F-001",
                    "21G-350",
                    "58D-001",
                    "51F-001",
                ]
            )
        ],
        baseline_kwk_add_gdf,
        drinkwater_gdf,
        osm_kunstwerken_gdf,
    ]
)

# add_baseline = gpd.sjoin_nearest(
#    baseline_kunstwerken_gdf, filtered_nearest_points, how="inner", max_distance=1000
# )

# Concatenate additional points
final_filtered_points = pd.concat([filtered_nearest_points, additional_points])

final_filtered_points.rename(
    columns={"kw_naam": "naam", "kw_code": "code"}, inplace=True
)

final_filtered_points.loc[final_filtered_points["bron"].isna(), ["bron"]] = "NIS"
# Add the lines from baseline_in_model to final_filtered_points

# add photo_url
final_filtered_points.loc[:, ["photo_url"]] = final_filtered_points["code"].apply(
    photo_url
)

# Save results to GeoPackage files
output_file = cloud.joinpath("Rijkswaterstaat", "verwerkt", "hydamo.gpkg")
final_filtered_points.to_file(
    output_file, layer="kunstwerken", driver="GPKG", engine="pyogrio"
)


# %%
