# %%
import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl import CloudStorage, Model, NetworkValidator
from shapely.geometry import MultiPolygon

cloud = CloudStorage()

authority = "ValleienVeluwe"
short_name = "venv"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", f"{short_name}.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")
split_line_gdf = gpd.read_file(
    cloud.joinpath(authority, "verwerkt", "fix_user_data.gpkg"), layer="split_basins", fid_as_index=True
)

# %% read model
model = Model.read(ribasim_toml)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model_network", f"{short_name}.toml")
network_validator = NetworkValidator(model)

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
basin_edges_df.to_file("basin_edges.gpkg")
basin_nodes_df.to_file("basin_nodes.gpkg")

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

# basin-profielen updaten

df = pd.DataFrame(
    {
        "node_id": np.repeat(model.basin.node.df.index.to_numpy(), 2),
        "level": [0.0, 1.0] * len(model.basin.node.df),
        "area": [0.01, 1000.0] * len(model.basin.node.df),
    }
)
df.index.name = "fid"
model.basin.profile.df = df

df = model.basin.profile.df.groupby("node_id")[["level"]].max().reset_index()
df.index.name = "fid"
model.basin.state.df = df

# %%
# tabulated_rating_curves updaten
df = pd.DataFrame(
    {
        "node_id": np.repeat(model.tabulated_rating_curve.node.df.index.to_numpy(), 2),
        "level": [0.0, 5] * len(model.tabulated_rating_curve.node.df),
        "flow_rate": [0, 0.1] * len(model.tabulated_rating_curve.node.df),
    }
)
df.index.name = "fid"
model.tabulated_rating_curve.static.df = df


# %%

# level_boundaries updaten
df = pd.DataFrame(
    {
        "node_id": model.level_boundary.node.df.index.to_list(),
        "level": [0.0] * len(model.level_boundary.node.df),
    }
)
df.index.name = "fid"
model.level_boundary.static.df = df

# %%
# manning_resistance updaten
length = len(model.manning_resistance.node.df)
df = pd.DataFrame(
    {
        "node_id": model.manning_resistance.node.df.index.to_list(),
        "length": [100.0] * length,
        "manning_n": [100.0] * length,
        "profile_width": [100.0] * length,
        "profile_slope": [100.0] * length,
    }
)
df.index.name = "fid"
model.manning_resistance.static.df = df


#  %% write model
# model.use_validation = True
model.write(ribasim_toml)

# %%
