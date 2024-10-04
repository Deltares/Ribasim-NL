# %%
import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet
from ribasim_nl import CloudStorage, Model, NetworkValidator
from shapely.geometry import MultiPolygon

cloud = CloudStorage()

authority = "DrentsOverijsselseDelta"
short_name = "dod"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", f"{short_name}.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")
hydroobject_gdf = gpd.read_file(
    cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="hydroobject", fid_as_index=True
)
duikersifonhevel_gdf = gpd.read_file(
    cloud.joinpath(authority, "aangeleverd", "Aanlevering_202311", "HyDAMO_WM_20231117.gpkg"),
    fid_as_index=True,
    layer="duikersifonhevel",
)

# split_line_gdf = gpd.read_file(
#     cloud.joinpath(authority, "verwerkt", "fix_user_data.gpkg"), layer="split_basins", fid_as_index=True
# )

# level_boundary_gdf = gpd.read_file(
#     cloud.joinpath(authority, "verwerkt", "fix_user_data.gpkg"), layer="level_boundary", fid_as_index=True
# )

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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393424844
# Verwijderen duplicate edges

model.edge.df.drop_duplicates(inplace=True)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393458802

# Toevoegen ontbrekende basins (oplossen topologie)
model.remove_node(7, remove_edges=True)
model.remove_node(84, remove_edges=True)
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

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393672367

# Omdraaien edge-richting rondom outlets (inlaten/uitlaten)
# for edge_id in [2282, ]

# https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393731749

# Opruimen Reeve

# basin 2484 wordt LevelBoundary (IJssel)
model.update_node(2484, "LevelBoundary", data=[level_data])

# nodes 1536, 762, 761, 1486 + aangesloten edges gooien we weg
for node_id in [1536, 762, 761, 1486]:
    model.remove_node(node_id, remove_edges=True)

# edges 2841, 2842, 2843, 2846 gooien we weg
model.edge.df = model.edge.df[~model.edge.df.index.isin([2841, 2842, 2843, 2846])]

# duiker 286309 voegen we toe
kdu = duikersifonhevel_gdf.loc[9063]
outlet_node = model.outlet.add(
    Node(geometry=kdu.geometry.interpolate(0.5, normalized=True), name=f"duikersifonhevel.{kdu.objectid}"),
    tables=[outlet_data],
)

model.edge.add(model.level_boundary[10], outlet_node)
model.edge.add(outlet_node, model.basin[2240])
model.edge.add(model.manning_resistance[849], model.basin[2240])
model.edge.add(model.manning_resistance[760], model.basin[2240])
model.edge.add(model.tabulated_rating_curve[187], model.basin[2240])
model.edge.add(model.basin[2240], model.pump[1100])

# %% https://github.com/Deltares/Ribasim-NL/issues/147#issuecomment-2393871075

# Ramsgeul bij Ramspol
for node_id in [81, 82, 839]:
    model.remove_node(node_id, remove_edges=True)

model.update_node(83, "Basin", data=basin_data)
model.move_node(83, hydroobject_gdf.at[21045, "geometry"].boundary.geoms[0])
model.reverse_edge(edge_id=3013)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/146#issuecomment-2382572457

# Administratie basin node_id in node_table en Basin / Area correct maken
# model.fix_unassigned_basin_area()
# model.fix_unassigned_basin_area(method="closest", distance=100)
# model.fix_unassigned_basin_area()

model.unassigned_basin_area.to_file("unassigned_basins.gpkg")
# model.basin.area.df = model.basin.area.df[~model.basin.area.df.node_id.isin(model.unassigned_basin_area.node_id)]

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
# %%
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
model.use_validation = False
model.write(ribasim_toml)

# %%
