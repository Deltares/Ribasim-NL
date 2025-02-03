# %%
import inspect

import geopandas as gpd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from shapely.geometry import MultiPolygon

from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table

cloud = CloudStorage()

authority = "ValleienVeluwe"
name = "venv"

# %% Check if model exist, otherwise download
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")
ribasim_toml = ribasim_dir / "model.toml"
database_gpkg = ribasim_toml.with_name("database.gpkg")
fix_user_data_gpkg = cloud.joinpath(authority, "verwerkt", "fix_user_data.gpkg")
model_edits_gpkg = cloud.joinpath(authority, "verwerkt", "model_edits.gpkg")

cloud.synchronize(filepaths=[ribasim_dir, fix_user_data_gpkg, model_edits_gpkg])

# %% read model
model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)
split_line_gdf = gpd.read_file(fix_user_data_gpkg, layer="split_basins", fid_as_index=True)

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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/148#issuecomment-2401873626
# Verwijderen duplicate edges

model.edge.df.drop_duplicates(inplace=True)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/148#issuecomment-2401876430

# Toevoegen ontbrekende basins (oplossen topologie)
basin_edges_df = network_validator.edge_incorrect_connectivity()
basin_nodes_df = network_validator.node_invalid_connectivity()

for row in basin_nodes_df.itertuples():
    # maak basin-node
    basin_node = model.basin.add(Node(geometry=row.geometry), tables=basin_data)

    # update edge_table
    model.edge.df.loc[basin_edges_df[basin_edges_df.from_node_id == row.node_id].index, ["from_node_id"]] = (
        basin_node.node_id
    )
    model.edge.df.loc[basin_edges_df[basin_edges_df.to_node_id == row.node_id].index, ["to_node_id"]] = (
        basin_node.node_id
    )


# %% see: https://github.com/Deltares/Ribasim-NL/issues/148#issuecomment-2401959032

# Oplossen verkeerde takrichting
for edge_id in [1353, 933, 373, 401, 4, 1338]:
    model.reverse_edge(edge_id=edge_id)

# model.invalid_topology_at_node().to_file("topo_errors.gpkg")


# %% see: https://github.com/Deltares/Ribasim-NL/issues/148#issuecomment-2402031275

# Veluwemeer at Harderwijk verwijderen
for node_id in [24, 694]:
    model.remove_node(node_id, remove_edges=True)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/148#issuecomment-2402229646

# Veluwemeer at Elburg verwijderen
for node_id in [3, 1277]:
    model.remove_node(node_id, remove_edges=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/148#issuecomment-2402257101

model.fix_unassigned_basin_area()

# %% https://github.com/Deltares/Ribasim-NL/issues/148#issuecomment-2402281396

# Verwijderen basins zonder area of toevoegen/opknippen basin /area
model.split_basin(line=split_line_gdf.at[1, "geometry"])
model.split_basin(line=split_line_gdf.at[2, "geometry"])
model.split_basin(line=split_line_gdf.at[3, "geometry"])
model.merge_basins(basin_id=1150, to_basin_id=1101)
model.merge_basins(basin_id=1196, to_basin_id=1192)
model.merge_basins(basin_id=1202, to_basin_id=1049)
model.merge_basins(basin_id=1207, to_basin_id=837)
model.merge_basins(basin_id=1208, to_basin_id=851, are_connected=False)
model.merge_basins(basin_id=1210, to_basin_id=1090)
model.merge_basins(basin_id=1212, to_basin_id=823)
model.merge_basins(basin_id=1216, to_basin_id=751, are_connected=False)
model.merge_basins(basin_id=1217, to_basin_id=752)
model.merge_basins(basin_id=1219, to_basin_id=814)
model.merge_basins(basin_id=1220, to_basin_id=1118)
model.merge_basins(basin_id=1221, to_basin_id=1170)
model.update_node(1229, "LevelBoundary", data=[level_data])
model.merge_basins(basin_id=1254, to_basin_id=1091, are_connected=False)
model.merge_basins(basin_id=1260, to_basin_id=1125, are_connected=False)
model.merge_basins(basin_id=1263, to_basin_id=863)
model.merge_basins(basin_id=1265, to_basin_id=974)
model.remove_node(node_id=539, remove_edges=True)
model.merge_basins(basin_id=1267, to_basin_id=1177, are_connected=False)
model.remove_node(1268, remove_edges=True)
model.remove_node(360, remove_edges=True)
model.remove_node(394, remove_edges=True)
model.merge_basins(basin_id=1269, to_basin_id=1087)
model.merge_basins(basin_id=1149, to_basin_id=1270, are_connected=False)


model.fix_unassigned_basin_area()
model.basin.area.df = model.basin.area.df[~model.basin.area.df.index.isin(model.unassigned_basin_area.index)]

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


# buffer out small slivers
model.basin.area.df.loc[:, ["geometry"]] = (
    model.basin.area.df.buffer(0.1)
    .buffer(-0.1)
    .apply(lambda x: x if x.geom_type == "MultiPolygon" else MultiPolygon([x]))
)

# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)

# %%
actions = [
    "remove_node",
    "remove_edge",
    "add_basin",
    "add_basin_area",
    "update_basin_area",
    "merge_basins",
    "reverse_edge",
    "move_node",
    "connect_basins",
    "update_node",
]
actions = [i for i in actions if i in gpd.list_layers(model_edits_gpkg).name.to_list()]
for action in actions:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_gpkg, layer=action, fid_as_index=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)

# remove unassigned basin area
model.remove_unassigned_basin_area()

# %% corrigeren knoop-topologie
# ManningResistance bovenstrooms LevelBoundary naar Outlet
for row in network_validator.edge_incorrect_type_connectivity().itertuples():
    model.update_node(row.from_node_id, "Outlet")

# Inlaten van ManningResistance naar Outlet
for row in network_validator.edge_incorrect_type_connectivity(
    from_node_type="LevelBoundary", to_node_type="ManningResistance"
).itertuples():
    model.update_node(row.to_node_id, "Outlet")


# %% sanitize node-table
# TabulatedRatingCurve to Outlet
for row in model.node_table().df[model.node_table().df.node_type == "TabulatedRatingCurve"].itertuples():
    node_id = row.Index
    model.update_node(node_id=node_id, node_type="Outlet")

# basins and outlets we've added do not have category, we fill with hoofdwater
# model.basin.node.df.loc[model.basin.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"
# model.outlet.node.df.loc[model.outlet.node.df["meta_categorie"].isna(), "meta_categorie"] = "hoofdwater"

# somehow Sluis Engelen (beheerregister AAM) has been named Henriettesluis
# model.outlet.node.df.loc[model.outlet.node.df.name == "Henriëttesluis", "name"] = "AKW855"

# name-column contains the code we want to keep, meta_name the name we want to have
df = get_data_from_gkw(authority=authority, layers=["gemaal", "stuw", "sluis"])
df.set_index("code", inplace=True)
names = df["naam"]

sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["LevelBoundary", "FlowBoundary"], "columns": {"meta_name": "name"}},
        {"node_types": ["Basin", "ManningResistance"], "columns": {"name": ""}},
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
result = model.run()
assert result == 0
