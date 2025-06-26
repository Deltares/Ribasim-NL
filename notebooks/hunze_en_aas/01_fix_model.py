# %%
import inspect

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet, tabulated_rating_curve

from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table

cloud = CloudStorage()

authority = "HunzeenAas"
name = "hea"
run_model = False

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
database_gpkg = ribasim_toml.with_name("database.gpkg")
ribasim_areas_path = cloud.joinpath(authority, "verwerkt", "4_ribasim", "areas.gpkg")
model_edits_path = cloud.joinpath(authority, "verwerkt", "model_edits.gpkg")

cloud.synchronize(filepaths=[ribasim_dir, ribasim_areas_path, model_edits_path])

# %% read model
model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)

# Load node edit data
# Load area file to fill basin area holes

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


# Area basin 1516 niet OK, te klein, model instabiel
actions = gpd.list_layers(model_edits_aanvoer_gpkg).name.to_list()
for action in actions:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_aanvoer_gpkg, layer=action, fid_as_index=True)
    if "order" in df.columns:
        df.sort_values("order", inplace=True)
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

# %%

# Sanitize node_table
for node_id in model.tabulated_rating_curve.node.df.index:
    model.update_node(node_id=node_id, node_type="Outlet")

# ManningResistance that are duikersifonhevel to outlet
for node_id in model.manning_resistance.node.df[
    model.manning_resistance.node.df["meta_object_type"] == "duikersifonhevel"
].index:
    model.update_node(node_id=node_id, node_type="Outlet")


# basins and outlets we've added do not have category, we fill with hoofdwater
model.basin.node.df.loc[model.basin.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"
model.outlet.node.df.loc[model.outlet.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"

# name-column contains the code we want to keep, meta_name the name we want to have
df = get_data_from_gkw(authority=authority, layers=["gemaal", "stuw", "sluis"])
df.set_index("code", inplace=True)
names = df["naam"]

# set meta_gestuwd in basins
model.basin.node.df["meta_gestuwd"] = False
model.outlet.node.df["meta_gestuwd"] = False
model.pump.node.df["meta_gestuwd"] = True

# set stuwen als gestuwd

model.outlet.node.df.loc[model.outlet.node.df["meta_object_type"] == "stuw", "meta_gestuwd"] = True

# set bovenstroomse basins als gestuwd
node_df = model.node_table().df
node_df = node_df[(node_df["meta_gestuwd"] == True) & node_df["node_type"].isin(["Outlet", "Pump"])]  # noqa: E712

upstream_node_ids = [model.upstream_node_id(i) for i in node_df.index]
basin_mask = model.basin.node.df.index.isin(upstream_node_ids)
model.basin.node.df.loc[basin_mask, "meta_gestuwd"] = True

# set álle benedenstroomse outlets van gestuwde basins als gestuwd (dus ook duikers en andere objecten)
downstream_node_ids = (
    pd.Series([model.downstream_node_id(i) for i in model.basin.node.df[basin_mask].index]).explode().to_numpy()
)
model.outlet.node.df.loc[model.outlet.node.df.index.isin(downstream_node_ids), "meta_gestuwd"] = True


sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie", "meta_gestuwd"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["Basin", "ManningResistance", "LevelBoundary", "FlowBoundary"], "columns": {"name": ""}},
    ],
    names=names,
)


#  %% write model
model.use_validation = True
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{name}.toml")
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %%
# %% Test run model
if run_model:
    result = model.run()
    assert result == 0
