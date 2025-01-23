# %%
import inspect
from pathlib import Path

import geopandas as gpd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet, tabulated_rating_curve

from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.reset_static_tables import reset_static_tables

cloud = CloudStorage()

authority = "HunzeenAas"
name = "hea"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", f"{name}.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")

# %% read model
model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)

# Load node edit data
model_edits_url = cloud.joinurl(authority, "verwerkt", "model_edits.gpkg")
model_edits_path = cloud.joinpath(authority, "verwerkt", "model_edits.gpkg")
if not model_edits_path.exists():
    cloud.download_file(model_edits_url)

# Load area file to fill basin area holes
ribasim_areas_path = cloud.joinpath(authority, "verwerkt", "4_ribasim", "areas.gpkg")
ribasim_areas_gdf = gpd.read_file(ribasim_areas_path, fid_as_index=True, layer="areas")


# %% some stuff we'll need again
manning_data = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])
level_data = level_boundary.Static(level=[0])

basin_data = [
    basin.Profile(level=[0.0, 1.0], area=[0.01, 1000.0]),
    basin.Static(
        drainage=[0.0],
        potential_evaporation=[0.001 / 86400],
        infiltration=[0.0],
        precipitation=[0.005 / 86400],
    ),
    basin.State(level=[0]),
]
outlet_data = outlet.Static(flow_rate=[100])

tabulated_rating_curve_data = tabulated_rating_curve.Static(level=[0.0, 5], flow_rate=[0, 0.1])

# HIER KOMEN ISSUES

# %%
# Verwijderen duplicate edges
model.edge.df.drop_duplicates(inplace=True)

# %%
# toevoegen ontbrekende basins

basin_edges_df = network_validator.edge_incorrect_connectivity()
basin_nodes_df = network_validator.node_invalid_connectivity()

for row in basin_nodes_df.itertuples():
    # maak basin-node
    basin_node = model.basin.add(Node(geometry=row.geometry), tables=basin_data)

    # update edge_table
    mask = (basin_edges_df.from_node_id == row.node_id) & (basin_edges_df.distance(row.geometry) < 0.1)
    model.edge.df.loc[basin_edges_df[mask].index, ["from_node_id"]] = basin_node.node_id
    mask = (basin_edges_df.to_node_id == row.node_id) & (basin_edges_df.distance(row.geometry) < 0.1)
    model.edge.df.loc[basin_edges_df[mask].index, ["to_node_id"]] = basin_node.node_id


# EINDE ISSUES


# %%
# corrigeren knoop-topologie

# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.edge_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet", data=[outlet_data])

# Inlaten van ManningResistance naar Outlet
for row in network_validator.edge_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet", data=[outlet_data])


# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)
# fix unassigned basin area
model.fix_unassigned_basin_area()
model.explode_basin_area()

# %%

actions = [
    "remove_basin_area",
    "remove_node",
    "add_basin",
    "update_node",
    "add_basin_area",
    "update_basin_area",
    "reverse_edge",
    "redirect_edge",
    "merge_basins",
    "move_node",
    "connect_basins",
    "deactivate_node",
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


# %% Assign Ribasim model ID's (dissolved areas) to the model basin areas (original areas with code) by overlapping the Ribasim area file baed on largest overlap
# then assign Ribasim node-ID's to areas with the same area code. Many nodata areas disappear by this method
# Create the overlay of areas
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

model.basin.area.df = combined_basin_areas_gdf

model.remove_unassigned_basin_area()

#  %% write model
model.use_validation = True
ribasim_fix_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %%
# %% Test run model

model = Model.read(ribasim_toml)
model.run(ribasim_exe=Path("c:\\ribasim_dev\\ribasim.exe"))
