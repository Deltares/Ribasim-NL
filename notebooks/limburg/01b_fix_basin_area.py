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
basin_node_edits_path = cloud_storage.joinpath(authority_name, "verwerkt", "model_edits.gpkg")
basin_node_edits_gdf = gpd.read_file(basin_node_edits_path, fid_as_index=True, layer="unassigned_basin_node")
basin_area_edits_gdf = gpd.read_file(basin_node_edits_path, fid_as_index=True, layer="unassigned_basin_area")
internal_basin_edits_gdf = gpd.read_file(basin_node_edits_path, fid_as_index=True, layer="internal_basin_node")


# %% merge_basins
for row in basin_node_edits_gdf[basin_node_edits_gdf["to_node_id"].notna()].itertuples():
    if pd.isna(row.connected):
        are_connected = True
    else:
        are_connected = row.connected
    model.merge_basins(basin_id=row.node_id, to_basin_id=row.to_node_id, are_connected=are_connected)

# mask = internal_basin_edits_gdf["to_node_id"].notna() & internal_basin_edits_gdf["add_object"].isna()
# for row in internal_basin_edits_gdf[mask].itertuples():
#     if pd.isna(row.connected):
#         are_connected = True
#     else:
#         are_connected = row.connected
#     model.merge_basins(basin_id=row.node_id, to_basin_id=row.to_node_id, are_connected=are_connected)

# # %% add and connect nodes
# for row in internal_basin_edits_gdf[internal_basin_edits_gdf.add_object.notna()].itertuples():
#     from_basin_id = row.node_id
#     to_basin_id = row.to_node_id
#     if row.add_object == "stuw":
#         node_type = "TabulatedRatingCurve"
#     model.add_and_connect_node(
#         from_basin_id, int(to_basin_id), geometry=row.geometry, node_type=node_type, name=row.add_object_name
#     )

# # %% reverse direction at node
# for row in internal_basin_edits_gdf[internal_basin_edits_gdf["reverse_direction"]].itertuples():
#     model.reverse_direction_at_node(node_id=row.node_id)

# # %% change node_type
# for row in basin_node_edits_gdf[basin_node_edits_gdf["change_to_node_type"].notna()].itertuples():
#     if row.change_to_node_type:
#         model.update_node(row.node_id, row.change_to_node_type, data=[level_boundary.Static(level=[0])])


# # %% corrigeren knoop-topologie
# outlet_data = outlet.Static(flow_rate=[100])
# # ManningResistance bovenstrooms LevelBoundary naar Outlet
# for row in network_validator.edge_incorrect_type_connectivity().itertuples():
#     model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# # Inlaten van ManningResistance naar Outlet
# for row in network_validator.edge_incorrect_type_connectivity(
#     from_node_type="LevelBoundary", to_node_type="ManningResistance"
# ).itertuples():
#     model.update_node(row.to_node_id, "Outlet", data=[outlet_data])


# # %%
# # basin-profielen/state updaten
# df = pd.DataFrame(
#     {
#         "node_id": np.repeat(model.basin.node.df.index.to_numpy(), 2),
#         "level": [0.0, 1.0] * len(model.basin.node.df),
#         "area": [0.01, 1000.0] * len(model.basin.node.df),
#     }
# )
# df.index.name = "fid"
# model.basin.profile.df = df

# df = model.basin.profile.df.groupby("node_id")[["level"]].max().reset_index()
# df.index.name = "fid"
# model.basin.state.df = df


# # tabulated_rating_curves updaten
# df = pd.DataFrame(
#     {
#         "node_id": np.repeat(model.tabulated_rating_curve.node.df.index.to_numpy(), 2),
#         "level": [0.0, 5] * len(model.tabulated_rating_curve.node.df),
#         "flow_rate": [0, 0.1] * len(model.tabulated_rating_curve.node.df),
#     }
# )
# df.index.name = "fid"
# model.tabulated_rating_curve.static.df = df


model.write(ribasim_model_dir.with_stem(f"{authority_name}_fix_model_area") / f"{model_short_name}.toml")
model.report_basin_area()
model.report_internal_basins()
# %%
