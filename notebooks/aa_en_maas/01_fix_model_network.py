# %%
import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "AaenMaas"
short_name = "aam"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", f"{short_name}.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")


# %% read model
model = Model.read(ribasim_toml)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model_network", f"{short_name}.toml")
network_validator = NetworkValidator(model)


hydroobject_gdf = gpd.read_file(
    cloud.joinpath(authority, "verwerkt", "4_ribasim", "hydamo.gpkg"), layer="hydroobject", fid_as_index=True
)


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


# HIER KOMEN ISSUES

# %% https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421617819

# Verwijderen duplicate edges
model.edge.df.drop_duplicates(inplace=True)

# %% https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421959240

# Verwijderen edge met 0m lengte
model.remove_node(34, remove_edges=True)
model.update_node(1568, "LevelBoundary", data=[level_data], node_properties={"name": ""})


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421946693
# toevoegen ontbrekende basins

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

# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2421991252

# Corrigeren netwerk Den Bosch

# Binnenstad
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[2788, "geometry"].boundary.geoms[0]), tables=basin_data)

model.reverse_edge(edge_id=2077)
model.redirect_edge(edge_id=2077, to_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2078, from_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2079, from_node_id=basin_node.node_id)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[4999, "geometry"].interpolate(0.5, normalized=True)), tables=[outlet_data]
)
model.edge.add(model.level_boundary[46], outlet_node)
model.edge.add(outlet_node, basin_node)

# Dommel
basin_node = model.basin.add(
    Node(geometry=hydroobject_gdf.at[9055, "geometry"].interpolate(0.5, normalized=True)), tables=basin_data
)
model.redirect_edge(edge_id=2082, from_node_id=basin_node.node_id)

outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[9055, "geometry"].interpolate(0.1, normalized=True)), tables=[outlet_data]
)

model.edge.add(model.level_boundary[49], outlet_node)
model.edge.add(outlet_node, basin_node)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422078500

# Corrigeren netwerk bij sluis Empel
for node_id in [729, 730, 1990, 1962]:
    model.remove_node(node_id, remove_edges=True)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2431933060
# Omkeren edgerichting
for edge_id in [131, 398, 407, 495, 513, 515, 894]:
    model.reverse_edge(edge_id=edge_id)


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422164355

# Corrigeren netwerk bij Spuisluis Crèvecoeur
model.remove_node(411, remove_edges=True)
model.remove_node(4, remove_edges=True)
model.redirect_edge(edge_id=2018, to_node_id=1950)

outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[4825, "geometry"].boundary.geoms[0], name="Spuisluis Crèvecoeur"),
    tables=[outlet_data],
)

model.edge.add(outlet_node, model.level_boundary[5])
model.edge.add(model.basin[1627], outlet_node)

# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422373708

# Corrigeren Afleidingskanaal bij Holthees

# nabij grens
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[8868, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[4680, "geometry"].interpolate(0.1, normalized=True)), tables=[outlet_data]
)

model.redirect_edge(edge_id=2091, from_node_id=56, to_node_id=outlet_node.node_id)
model.redirect_edge(edge_id=2092, from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2093, from_node_id=653, to_node_id=basin_node.node_id)
model.redirect_edge(edge_id=2094, from_node_id=basin_node.node_id, to_node_id=82)

# nabij afleidingskanaal
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[7466, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[8456, "geometry"].interpolate(0.1, normalized=True)), tables=[outlet_data]
)
model.redirect_edge(edge_id=2089, to_node_id=outlet_node.node_id)
model.redirect_edge(edge_id=2088, from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id)
model.edge.add(model.tabulated_rating_curve[82], basin_node)
model.edge.add(basin_node, model.tabulated_rating_curve[853])

# nabij Maas
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[2385, "geometry"].boundary.geoms[0]), tables=basin_data)
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[2539, "geometry"].boundary.geoms[0]), tables=[outlet_data]
)

model.redirect_edge(edge_id=2054, to_node_id=basin_node.node_id)
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[26])


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2422452167

# Toevoegen basin bij Oude Zuid-Willemsvaart
outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[3174, "geometry"].boundary.geoms[0], name="Sluis 9"), tables=[outlet_data]
)
basin_node = model.basin.add(Node(geometry=hydroobject_gdf.at[6499, "geometry"].boundary.geoms[0]), tables=basin_data)
model.redirect_edge(edge_id=2102, to_node_id=outlet_node.node_id)
model.edge.add(outlet_node, basin_node)
model.redirect_edge(edge_id=2106, to_node_id=2026)

outlet_node = model.outlet.add(
    Node(geometry=hydroobject_gdf.at[646, "geometry"].interpolate(0.9, normalized=True)), tables=[outlet_data]
)
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[66])

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


## UPDATEN STATIC TABLES

# %%
# basin-profielen/state updaten
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


# %% see: https://github.com/Deltares/Ribasim-NL/issues/149#issuecomment-2431933060
node_ids = [280, 335, 373, 879]
model.tabulated_rating_curve.static.df.loc[
    model.tabulated_rating_curve.static.df.node_id.isin([280, 335, 373, 879]), "active"
] = False


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

# %%
# flow boundaries updaten
length = len(model.flow_boundary.node.df)
df = pd.DataFrame(
    {
        "node_id": model.flow_boundary.node.df.index.to_list(),
        "flow_rate": [0.0] * length,
    }
)
df.index.name = "fid"
model.flow_boundary.static.df = df


#  %% write model
model.use_validation = True
model.write(ribasim_toml)
model.report_basin_area()

# %%
