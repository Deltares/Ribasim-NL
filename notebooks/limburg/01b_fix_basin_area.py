# %% Import Libraries and Initialize Variables
import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage, Model, NetworkValidator

# Initialize cloud storage and set authority/model parameters
cloud_storage = CloudStorage()
authority_name = "Limburg"
model_short_name = "limburg"

# Define the path to the Ribasim model configuration file
ribasim_model_dir = cloud_storage.joinpath(authority_name, "modellen", f"{authority_name}_fix_model_network")
ribasim_model_path = ribasim_model_dir / f"{model_short_name}.toml"
model = Model.read(ribasim_model_path)
network_validator = NetworkValidator(model)

# Load node edit data
model_edits_url = cloud_storage.joinurl(authority_name, "verwerkt", "model_edits.gpkg")
model_edits_path = cloud_storage.joinpath(authority_name, "verwerkt", "model_edits.gpkg")
if not model_edits_path.exists():
    cloud_storage.download_file(model_edits_url)

basin_node_edits_gdf = gpd.read_file(model_edits_path, fid_as_index=True, layer="unassigned_basin_node")
rename_basin_area_gdf = gpd.read_file(model_edits_path, fid_as_index=True, layer="rename_basin_area")
add_basin_area_gdf = gpd.read_file(model_edits_path, layer="add_basin_area")
connect_basins_gdf = gpd.read_file(model_edits_path, fid_as_index=True, layer="connect_basins")
reverse_edge_gdf = gpd.read_file(model_edits_path, fid_as_index=True, layer="reverse_edge")
add_basin_outlet_gdf = gpd.read_file(model_edits_path, fid_as_index=True, layer="add_basin_outlet")

# %%
# rename node-ids
df = rename_basin_area_gdf.set_index("ribasim_fid")
model.basin.area.df.loc[df.index, ["node_id"]] = df["to_node_id"].astype("int32")

# %%add basin area
add_basin_area_gdf.index += model.basin.area.df.index.max() + 1
add_basin_area_gdf.index.name = "fid"
model.basin.area.df = pd.concat([model.basin.area.df, add_basin_area_gdf])

# %% merge_basins

# merge basins
selection_df = basin_node_edits_gdf[basin_node_edits_gdf["to_node_id"].notna()]
for row in selection_df.itertuples():
    if pd.isna(row.connected):
        are_connected = True
    else:
        are_connected = row.connected
    model.merge_basins(basin_id=row.node_id, to_basin_id=row.to_node_id, are_connected=are_connected)

# %% reverse edges

# reverse edges
for edge_id in reverse_edge_gdf.edge_id:
    model.reverse_edge(edge_id=edge_id)


# %% change node_type

# change node type
selection_df = basin_node_edits_gdf[basin_node_edits_gdf["change_node_type"].notna()]
for row in basin_node_edits_gdf[basin_node_edits_gdf["change_node_type"].notna()].itertuples():
    if row.change_node_type:
        model.update_node(node_id=row.node_id, node_type=row.change_node_type)

# %% remove nodes

# remove nodes
for node_id in basin_node_edits_gdf[basin_node_edits_gdf["remove_node_id"].notna()].node_id:
    model.remove_node(node_id=node_id, remove_edges=True)

# %% add and connect basins

# add and connect basins
for row in connect_basins_gdf.itertuples():
    from_basin_id = row.node_id
    to_basin_id = row.to_node_id
    if row.add_object == "duikersifonhevel":
        node_type = "TabulatedRatingCurve"
    model.add_and_connect_node(
        from_basin_id, int(to_basin_id), geometry=row.geometry, node_type=node_type, name=row.add_object_name
    )

# %% add basin outlet

# add basin outlet
for row in add_basin_outlet_gdf.itertuples():
    basin_id = row.node_id
    if row.add_object == "duikersifonhevel":
        node_type = "TabulatedRatingCurve"
    model.add_basin_outlet(basin_id, geometry=row.geometry, node_type=node_type, name=row.add_object_name)

# %% weggooien basin areas zonder node

df = model.basin.area.df[~model.basin.area.df.index.isin(model.unassigned_basin_area.index)]
df = df.dissolve(by="node_id").reset_index()
df.index.name = "fid"
model.basin.area.df = df


# %% corrigeren knoop-topologie
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.edge_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet")

# Inlaten van ManningResistance naar Outlet
for row in network_validator.edge_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet")

# %%
model.use_validation = True
ribasim_model_dir = ribasim_model_dir.with_stem(f"{authority_name}_fix_model_area")
model.report_basin_area()
model.report_internal_basins()
