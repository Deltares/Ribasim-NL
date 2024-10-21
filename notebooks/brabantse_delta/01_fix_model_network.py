# %%
import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet

from ribasim_nl import CloudStorage, Model, NetworkValidator
from ribasim_nl.geometry import drop_z

cloud = CloudStorage()

authority = "BrabantseDelta"
short_name = "wbd"

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

# %% https://github.com/Deltares/Ribasim-NL/issues/152#issuecomment-2427492528

# Herstellen verbinding Schelde-Rijnverbinding met KDU02582
model.remove_node(2288, remove_edges=True)
model.redirect_edge(edge_id=2450, to_node_id=955)

# Omkeren edges
edge_ids = [2470, 2468, 2469, 2465, 748, 2476, 2489, 697, 2500, 2487, 2440]

for edge_id in edge_ids:
    model.reverse_edge(edge_id=edge_id)

# toeoegen Donge
basin_node = model.basin.add(
    Node(geometry=drop_z(hydroobject_gdf.at[13091, "geometry"].boundary.geoms[0])), tables=basin_data
)
outlet_node = model.outlet.add(
    Node(geometry=drop_z(hydroobject_gdf.at[13136, "geometry"].boundary.geoms[0])), tables=[outlet_data]
)
model.redirect_edge(edge_id=2477, from_node_id=basin_node.node_id, to_node_id=973)
model.edge.add(basin_node, outlet_node)
model.edge.add(outlet_node, model.level_boundary[31])

outlet_node = model.outlet.add(
    Node(geometry=drop_z(hydroobject_gdf.at[13088, "geometry"].boundary.geoms[0])), tables=[outlet_data]
)
model.redirect_edge(edge_id=2497, to_node_id=outlet_node.node_id)
model.redirect_edge(edge_id=2498, from_node_id=outlet_node.node_id, to_node_id=basin_node.node_id)


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

# %%
