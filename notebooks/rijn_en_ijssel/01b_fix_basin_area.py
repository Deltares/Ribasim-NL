# %% Import Libraries and Initialize Variables

import inspect

import geopandas as gpd

from ribasim_nl import CloudStorage, Model, NetworkValidator

# Initialize cloud storage and set authority/model parameters
cloud_storage = CloudStorage()
authority_name = "RijnenIJssel"
model_short_name = "wrij"

# Define the path to the Ribasim model configuration file
ribasim_model_dir = cloud_storage.joinpath(authority_name, "modellen", f"{authority_name}_fix_model_network")
ribasim_model_path = ribasim_model_dir / f"{model_short_name}.toml"
model = Model.read(ribasim_model_path)
network_validator = NetworkValidator(model)

# Load Ribasim areas
ribasim_areas_path = cloud_storage.joinpath(authority_name, "verwerkt", "4_ribasim", "areas.gpkg")
ribasim_areas_gdf = gpd.read_file(ribasim_areas_path, fid_as_index=True, layer="areas")

# Load node edit data
model_edits_url = cloud_storage.joinurl(authority_name, "verwerkt", "model_edits.gpkg")
model_edits_path = cloud_storage.joinpath(authority_name, "verwerkt", "model_edits.gpkg")
if not model_edits_path.exists():
    cloud_storage.download_file(model_edits_url)

# %%
actions = [
    "remove_node",
    "remove_edge",
    "add_basin",
    "add_basin_area",
    "update_basin_area",
    "merge_basins",
    "reverse_edge",
    "update_node",
    "move_node",
    "connect_basins",
]
actions = [i for i in actions if i in gpd.list_layers(model_edits_path).name.to_list()]
for action in actions:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_path, layer=action, fid_as_index=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)

# remove unassigned basin area
model.remove_unassigned_basin_area()


# %% Assign Ribasim model ID's (dissolved areas) to the model basin areas (original areas with code) by overlapping the Ribasim area file baed on largest overlap
# then assign Ribasim node-ID's to areas with the same area code. Many nodata areas disappear by this method
# Create the overlay of areas
ribasim_areas_gdf = ribasim_areas_gdf.to_crs(model.basin.area.df.crs)
combined_basin_areas_gdf = gpd.overlay(ribasim_areas_gdf, model.basin.area.df, how="union").explode()
combined_basin_areas_gdf["geometry"] = combined_basin_areas_gdf["geometry"].apply(lambda x: x if x.has_z else x)

# Calculate area for each geometry
combined_basin_areas_gdf["area"] = combined_basin_areas_gdf.geometry.area

# Separate rows with and without node_id
non_null_basin_areas_gdf = combined_basin_areas_gdf[combined_basin_areas_gdf["node_id"].notna()]

# Find largest area node_ids for each code
largest_area_node_ids = non_null_basin_areas_gdf.loc[
    non_null_basin_areas_gdf.groupby("code")["area"].idxmax(), ["code", "node_id"]
]

# Merge largest area node_ids back into the combined DataFrame
combined_basin_areas_gdf = combined_basin_areas_gdf.merge(
    largest_area_node_ids, on="code", how="left", suffixes=("", "_largest")
)

# Fill missing node_id with the largest_area node_id
combined_basin_areas_gdf["node_id"] = combined_basin_areas_gdf["node_id"].fillna(
    combined_basin_areas_gdf["node_id_largest"]
)
combined_basin_areas_gdf.drop(columns=["node_id_largest"], inplace=True)
combined_basin_areas_gdf = combined_basin_areas_gdf.drop_duplicates()
combined_basin_areas_gdf = combined_basin_areas_gdf.dissolve(by="node_id").reset_index()
combined_basin_areas_gdf = combined_basin_areas_gdf[["node_id", "geometry"]]
combined_basin_areas_gdf.index.name = "fid"

# %%
model.basin.area.df = combined_basin_areas_gdf


# %%
model.use_validation = True
model.write(ribasim_model_dir.with_stem(f"{authority_name}_fix_model_area") / f"{model_short_name}.toml")
model.report_basin_area()
model.report_internal_basins()

# %%
