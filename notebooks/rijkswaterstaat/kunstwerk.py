# %%
import geopandas as gpd
import pandas as pd
from ribasim_nl import CloudStorage

cloud = CloudStorage()

# %% We extraheren kunstwerken voor het hoofdwatersysteem uit RWS data. Methode:
# 1. We pakken de kunstwerken uit de legger
# 2. We koppelen deze op basis van een gedefinieerde maximale afstand aan de puntenfile: kunstwerken_primaire_waterkeringen.gpkg.
# 3. Daarnaast voegen we extra kunstwerken toe die niet in beide bestanden voorkomen, maar wel noodzakelijk zijn voor modellering hoofdwatersysteem


def calculate_centroids(polygons):
    return gpd.sjoin_nearest(
        gpd.GeoDataFrame(geometry=polygons.centroid, crs=polygons.crs),
        polygons,
        how="left",
    )


def add_kunstwerken(points, polygons, distance_threshold):
    points_filtered = points[points["kd_type_om"].isin(["gemaal", "spuisluis"])]

    if distance_threshold > 0:
        buffered_polygons = polygons.buffer(distance_threshold)
        result = points_filtered[points_filtered.within(buffered_polygons.unary_union)]

        if "beheerobjectcode" in polygons.columns:
            Eefde = polygons[polygons["beheerobjectcode"].isin(["34F-001-04"])]
            result = pd.concat([result, calculate_centroids(Eefde)], ignore_index=True)
    else:
        result = calculate_centroids(polygons)

        if "id_beheer" in points.columns:
            kunstwerk_points = points[
                points["id_beheer"].isin(["42D-001-01", "37B-350-01"])
            ].drop_duplicates()
            result = pd.concat([result, kunstwerk_points], ignore_index=True)
    return result


point_file = cloud.joinpath(
    "Rijkswaterstaat", "aangeleverd", "kunstwerken_primaire_waterkeringen.gpkg"
)
polygon_files = [
    cloud.joinpath("Rijkswaterstaat", "aangeleverd", f"{type}_legger.gpkg")
    for type in ["gemaal", "in_of_uitwateringssluis", "stuw"]
]
distance_thresholds = [10, 45, 0]
output_file = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "kunstwerken_select_legger.gpkg"
)

results = [
    add_kunstwerken(
        gpd.read_file(point_file), gpd.read_file(polygon_file), distance_threshold
    )
    for polygon_file, distance_threshold in zip(polygon_files, distance_thresholds)
]
result = pd.concat(results, ignore_index=True)

result["kunstwerk_naam"] = result.apply(
    lambda row: row["naam_compl"]
    if not pd.isna(row["naam_compl"]) and row["naam_compl"] != "nvt"
    else row["kd_naam"]
    if row["naam_compl"] == "nvt"
    else row["objectnaam"],
    axis=1,
)

if output_file:
    result.to_file(output_file, driver="GPKG")

# %%
