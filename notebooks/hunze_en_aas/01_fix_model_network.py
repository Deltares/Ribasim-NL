# %%
import inspect

import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet, tabulated_rating_curve

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "HunzeenAas"
short_name = "hea"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", f"{short_name}.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")

# %% read model
model = Model.read(ribasim_toml)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model_network", f"{short_name}.toml")
network_validator = NetworkValidator(model)

# Load node edit data
model_edits_url = cloud.joinurl(authority, "verwerkt", "model_edits.gpkg")
model_edits_path = cloud.joinpath(authority, "verwerkt", "model_edits.gpkg")
if not model_edits_path.exists():
    cloud.download_file(model_edits_url)


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

# %%
# outlets updaten
length = len(model.outlet.node.df)
df = pd.DataFrame(
    {
        "node_id": model.outlet.node.df.index.to_list(),
        "flow_rate": [10.0] * length,
    }
)
df.index.name = "fid"
model.outlet.static.df = df


# %%
actions = [
    "remove_node",
    "add_basin",
    "add_basin_area",
    "update_basin_area",
    "merge_basins",
    "reverse_edge",
    "move_node",
    "connect_basins",
    "update_node",
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


#  %% write model
model.use_validation = True
model.write(ribasim_toml)
model.invalid_topology_at_node().to_file(ribasim_toml.with_name("invalid_topology_at_connector_nodes.gpkg"))

# %%
