# %% Import Libraries and Initialize Variables
import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage, Model

# Initialize cloud storage and set authority/model parameters
cloud_storage = CloudStorage()
authority_name = "AaenMaas"
model_short_name = "aam"

# Define the path to the Ribasim model configuration file
ribasim_model_path = cloud_storage.joinpath(
    authority_name, "modellen", f"{authority_name}_fix_model_network", f"{model_short_name}.toml"
)
model = Model.read(ribasim_model_path)


# %% Load Input Geospatial Files
drainage_units_path = cloud_storage.joinpath(
    authority_name,
    "verwerkt",
    "1_ontvangen_data",
    "Na_levering_202404",
    "afwateringseenheden_WAM",
    "Afwateringseenheden.shp",
)
drainage_units_gdf = gpd.read_file(drainage_units_path, fid_as_index=True)

# Load alternative drainage data
drainage_units_path = cloud_storage.joinpath(
    authority_name, "aangeleverd", "Eerste_levering", "AfvoergebiedAanvoergebied.shp"
)
drainage_units_johnny_gdf = gpd.read_file(drainage_units_path, fid_as_index=True)

# Load Ribasim model basin areas
ribasim_areas_path = cloud_storage.joinpath(authority_name, "verwerkt", "4_ribasim", "areas.gpkg")
ribasim_areas_gdf = gpd.read_file(ribasim_areas_path, fid_as_index=True)

# Load node edit data
basin_node_edits_path = cloud_storage.joinpath(
    authority_name, "modellen", "AaenMaas_fix_model_network", "basin_node_edits.gpkg"
)
basin_node_edits_gdf = gpd.read_file(basin_node_edits_path, fid_as_index=True)

# %% Assign Ribasim model ID's (dissolved areas) to the model basin areas (original areas with code) by overlapping the Ribasim area file baed on largest overlap
# then assign Ribasim node-ID's to areas with the same area code. Many nodata areas disappear by this method
combined_basin_areas_gdf = gpd.overlay(ribasim_areas_gdf, model.basin.area.df, how="union").explode()
combined_basin_areas_gdf["geometry"] = combined_basin_areas_gdf["geometry"].apply(lambda x: x if x.has_z else x)
combined_basin_areas_gdf["area"] = combined_basin_areas_gdf.geometry.area
non_null_basin_areas_gdf = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].notna()]

largest_area_node_ids = non_null_basin_areas_gdf.loc[
    non_null_basin_areas_gdf.groupby("code")["area"].idxmax(), ["code", "node_id"]
].reset_index(drop=True)

combined_basin_areas_gdf = combined_basin_areas_gdf.merge(largest_area_node_ids, on="code", how="left")
combined_basin_areas_gdf["node_id"] = combined_basin_areas_gdf["node_id_y"]
combined_basin_areas_gdf.drop(columns=["node_id_x", "node_id_y"], inplace=True)
combined_basin_areas_gdf = combined_basin_areas_gdf.drop_duplicates(keep="first")
combined_basin_areas_gdf = combined_basin_areas_gdf.dissolve(by="code").reset_index()

# %% The Ribasim model basins that have still nodata are being checked if they have overlap with aftwareringseenheden.shp.
# If overlap, they get ther Ribasim node-id where they have the largest overlap with
filtered_drainage_units_gdf = drainage_units_johnny_gdf[drainage_units_johnny_gdf["SOORTAFVOE"] != "Deelstroomgebied"]
filtered_drainage_units_gdf["geometry"] = filtered_drainage_units_gdf["geometry"].apply(lambda x: x if x.has_z else x)
filtered_drainage_units_gdf = filtered_drainage_units_gdf.to_crs(combined_basin_areas_gdf.crs)

# Overlay filtered drainage units and updated basin areas
combined_basin_areas_johnny_gdf = gpd.overlay(
    filtered_drainage_units_gdf, combined_basin_areas_gdf, how="union"
).explode()
combined_basin_areas_johnny_gdf = combined_basin_areas_johnny_gdf.dissolve(by="CODE")

# Step 1: Separate unassigned from assigned units
unassigned_units_gdf = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].isnull()]
unassigned_units_gdf["geometry"] = unassigned_units_gdf["geometry"].apply(lambda x: x if x.has_z else x)
assigned_units_gdf = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].notna()]
assigned_units_gdf["geometry"] = assigned_units_gdf["geometry"].apply(lambda x: x if x.has_z else x)

# Step 2: Calculate intersection areas between unassigned units and Johnny basins
overlap_gdf = gpd.overlay(combined_basin_areas_johnny_gdf, unassigned_units_gdf, how="union")

# Step 3: Add overlap area for each polygon
overlap_gdf["overlap_area"] = overlap_gdf.geometry.area

# Step 4: Select the largest overlap per code to assign `node_id`
largest_area_node_ids = overlap_gdf.loc[
    overlap_gdf.groupby("OBJECTID_2")["overlap_area"].idxmax(), ["node_id_1", "OBJECTID_2"]
].reset_index(drop=True)

# Step 5: Merge largest node_id from overlaps back to unassigned units
unassigned_units_gdf = unassigned_units_gdf.merge(
    largest_area_node_ids, left_on=["OBJECTID"], right_on=["OBJECTID_2"], how="outer"
)
unassigned_units_gdf["node_id"] = unassigned_units_gdf["node_id_1"]
unassigned_units_gdf.drop(columns=["node_id_1"], inplace=True)

# Step 6: Merge unassigned node_ids back into the main dataset
basin_area_update = combined_basin_areas_gdf.merge(
    unassigned_units_gdf[["OBJECTID", "node_id"]],
    on="OBJECTID",
    how="left",
    suffixes=("", "_unassigned"),
)
# Step 7: Finalize missing `node_id` values from unassigned units
basin_area_update["node_id"] = basin_area_update["node_id"].fillna(basin_area_update["node_id_unassigned"])
basin_area_update.drop(columns=["node_id_unassigned"], inplace=True)

# %% If there are still nodata basins they are removed by assigning Nearest Basin ID
null_node_rows = basin_area_update[basin_area_update["node_id"].isnull()]

if not null_node_rows.empty:
    null_node_rows = null_node_rows.set_geometry(null_node_rows.geometry.centroid)
    basin_area_update_centroid = basin_area_update.set_geometry(basin_area_update.geometry.centroid)

    nearest_basin = gpd.sjoin_nearest(
        null_node_rows,
        basin_area_update_centroid[basin_area_update_centroid["node_id"].notna()][["geometry", "node_id"]],
        how="left",
        distance_col="distance",
    )
    basin_area_update.loc[basin_area_update["node_id"].isnull(), "node_id"] = nearest_basin["node_id_right"]

# %% Based on basin_node_edits.gpkg, areas are assigned the Ribasim node_id that is in the file
basin_node_edits_notnull_gdf = basin_node_edits_gdf[basin_node_edits_gdf["add_area_code"].notna()]
merged_gdf = basin_area_update.merge(
    basin_node_edits_notnull_gdf[["add_area_code", "node_id"]], how="left", left_on="code", right_on="add_area_code"
)
merged_gdf["node_id"] = merged_gdf["node_id_y"].combine_first(merged_gdf["node_id_x"])
merged_gdf.drop(columns=["node_id_x", "node_id_y", "add_area_code"], inplace=True)

# Dissolve geometries by `node_id` and save final GeoDataFrame
final_basins_gdf = merged_gdf.set_index("node_id").dissolve(by="node_id", aggfunc={"code": "first"}).reset_index()
final_basins_gdf.rename(columns={"code": "meta_code_waterbeheerder"}, inplace=True)
final_basins_gdf.to_file("basins_noholes.gpkg", layer="basins_noholes")

# %% Check Differences in Node_ID Between Initial and Final Models
final_node_ids = final_basins_gdf["node_id"]
model_node_ids = model.basin.area.df["node_id"]
missing_in_model = final_basins_gdf[~final_basins_gdf["node_id"].isin(model_node_ids)]
missing_in_final = model.basin.area.df[~model.basin.area.df["node_id"].isin(final_node_ids)]
missing_gdf = pd.concat([missing_in_model, missing_in_final])

if "fid" in missing_gdf.columns:
    missing_gdf = missing_gdf.rename(columns={"fid": "new_fid_name"})
# %%
