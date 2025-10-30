# %%

import pandas as pd

from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

authority = "HunzeenAas"
short_name = "hea"

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", f"{short_name}.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")

model = Model.read(ribasim_toml)
network_validator = NetworkValidator(model)

verwerkt_dir = cloud.joinpath(authority, "verwerkt")
verwerkt_dir.mkdir(exist_ok=True)

modelfouten_gpkg = cloud.joinpath(authority, "verwerkt", "modelfouten.gpkg")

# %% verwijderen duplicated edges

duplicated_edges = len(model.link.df[model.link.df.duplicated()])
model.link.df.drop_duplicates(inplace=True)

# %% wegschrijven fouten

# niet-bestaande fouten
mask = model.link.df.to_node_id.isin(model.node_table().df.index) & model.link.df.from_node_id.isin(
    model.node_table().df.index
)

edge_mist_node_df = model.link.df[~mask]
model.link.df = model.link.df[mask]

mask = model.link.df.geometry.length == 0
model.link.df[mask].centroid.to_file(modelfouten_gpkg, layer="edge_zonder_lengte")
model.link.df = model.link.df[~mask]

# niet-gekoppelde areas
model.basin.area.df[~model.basin.area.df.node_id.isin(model.basin.node.df.index)].to_file(
    modelfouten_gpkg, layer="area_niet_een_basin"
)

model.basin.node.df[~model.basin.node.df.index.isin(model.basin.area.df.node_id)].to_file(
    modelfouten_gpkg, layer="basin_zonder_area"
)

# ontbrekende basins
network_validator.node_invalid_connectivity().to_file(modelfouten_gpkg, layer="node_mist")
pd.concat([network_validator.edge_incorrect_connectivity(), edge_mist_node_df]).to_file(
    modelfouten_gpkg, layer="ege_mist_node"
)

# nodes met verkeerde richting

model.invalid_topology_at_node().to_file(modelfouten_gpkg, layer="node_met_verkeerde_instroom_uitstroom_egde")
