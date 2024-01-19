# %%
#  We extraheren kunstwerken voor het hoofdwatersysteem uit RWS data. Methode:
# 1. We pakken de kunstwerken uit Netwerk Informatie Systeem ( NIS ) van Rijkswaterstaat (nis_all_kunstwerken_hws_2019.gpkg)
# 2. We selecteren de kunstwerken die binnen het KRW lichamen vallen
# 3. Daarnaast voegen we extra kunstwerken  die niet in de nis voorkomen maar wel noodzakelijk zijn voor modellering hoofdwatersysteem.
# Deze extra kunstwereken komen uit: baseline.gdb ; layer = "stuctures" en uit de lrws-legger:kunstwerken_primaire_waterkeringen)

import geopandas as gpd
import pandas as pd
from ribasim_nl import CloudStorage

cloud = CloudStorage()

# File paths
krw_lichaam = cloud.joinpath(
    "Basisgegevens", "KRW", "krw_oppervlaktewaterlichamen_nederland_vlakken.gpkg"
)

nis_hws = cloud.joinpath(
    "Rijkswaterstaat", "aangeleverd", "NIS", "nis_all_kunstwerken_hws_2019.gpkg"
)
baseline = cloud.joinpath("baseline-nl_land-j23_6-v1", "baseline.gdb")
nis_hwvn = cloud.joinpath(
    "Rijkswaterstaat", "aangeleverd", "NIS", "nis_alle_kunstwerken_hwvn_2019.gpkg"
)
primaire_kunstwerken = cloud.joinpath(
    "Rijkswaterstaat", "aangeleverd", "kunstwerken_primaire_waterkeringen.gpkg"
)

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

baseline_kunstwerken_gdf = gpd.read_file(baseline, layer="structure_lines")

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
filtered_points["nearest_distance"] = [
    filtered_point["geometry"].distance(baseline_in_model.unary_union)
    for _, filtered_point in filtered_points.iterrows()
]

# Filter points based on distance
filtered_nearest_points = filtered_points[
    filtered_points["nearest_distance"] < 500
].copy()

# Remove specific values
filtered_nearest_points = filtered_nearest_points[
    ~filtered_nearest_points["complex_code"].isin(["40D-350", "48H-353"])
]
filtered_nearest_points = filtered_nearest_points.drop(["index_right"], axis=1)

# Additional points
additional_points = pd.concat(
    [
        primaire_kunstwerken_gdf[
            primaire_kunstwerken_gdf["kd_code1"].isin(
                ["KD.32.gm.015", "KD.v2.ks.001", "KD.44.gm.001"]
            )
        ],
        nis_hws_gdf[
            nis_hws_gdf["complex_code"].isin(
                ["49D-400", "44D-002", "58C-001", "45B-352", "33F-001", "21G-350"]
            )
        ],
        nis_hwvn_gdf[
            nis_hwvn_gdf["complex_code"].isin(
                ["49D-400", "44D-002", "58C-001", "45B-352", "33F-001", "21G-350"]
            )
        ],
        baseline_kunstwerken_gdf[
            baseline_kunstwerken_gdf["NAME"].isin(
                [
                    "DM_68.30_R_US_Reevespuisluis",
                    "KR_C_HK_Kadoelen",
                ]
            )
        ],
    ]
)

add_baseline = gpd.sjoin_nearest(
    baseline_kunstwerken_gdf, filtered_nearest_points, how="inner", max_distance=1000
)


# Concatenate additional points
final_filtered_points = pd.concat(
    [filtered_nearest_points, additional_points, add_baseline]
)

# Add the lines from baseline_in_model to final_filtered_points


# Save results to GeoPackage files
output_file = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "kunstwerken_select_legger.gpkg"
)
final_filtered_points.to_file(output_file, driver="GPKG")


# %%
