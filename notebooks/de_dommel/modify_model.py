# %%
import sqlite3

import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, flow_boundary, level_boundary, manning_resistance
from ribasim_nl import CloudStorage, Model, NetworkValidator

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("DeDommel", "modellen", "DeDommel_2024_6_3", "model.toml")
database_gpkg = ribasim_toml.with_name("database.gpkg")

# %% remove urban_runoff
# Connect to the SQLite database

conn = sqlite3.connect(database_gpkg)

# get table into DataFrame
table = "Basin / static"
df = pd.read_sql_query(f"SELECT * FROM '{table}'", conn)

# drop urban runoff column if exists
if "urban_runoff" in df.columns:
    df.drop(columns="urban_runoff", inplace=True)

    #  Write the DataFrame back to the SQLite table
    df.to_sql(table, conn, if_exists="replace", index=False)

# # Close the connection
conn.close()


# %% read model
model = Model.read(ribasim_toml)

network_validator = NetworkValidator(model)

# %% verwijder duplicated edges
duplicated_edges_df = network_validator.edge_duplicated()

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2288780504
model.edge.df = model.edge.df[~((model.edge.df.from_node_type == "Basin") & (model.edge.df.to_node_type == "Basin"))]

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291081244
duplicated_fids = network_validator.edge_duplicated().index.to_list()
model.edge.df = model.edge.df.drop_duplicates(subset=["from_node_id", "to_node_id"])

if not network_validator.edge_duplicated().empty:
    raise Exception("nog steeds duplicated edges")

# %% toevoegen bovenstroomse knopen

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291091067
edge_mask = model.edge.df.index.isin(duplicated_fids)
node_id = model.next_node_id
edge_fid = next(i for i in duplicated_fids if i in model.edge.df.index)
model.edge.df.loc[edge_mask, ["from_node_type"]] = "FlowBoundary"
model.edge.df.loc[edge_mask, ["from_node_id"]] = node_id

node = Node(node_id, model.edge.df.at[edge_fid, "geometry"].boundary.geoms[0])
data = flow_boundary.Static(flow_rate=[0])
model.flow_boundary.add(node, [data])

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291111647

data = [
    basin.Profile(level=[0.0, 1.0], area=[0.01, 1000.0]),
    basin.Static(
        drainage=[0.0],
        potential_evaporation=[0.001 / 86400],
        infiltration=[0.0],
        precipitation=[0.005 / 86400],
    ),
    basin.State(level=[0]),
]

for row in network_validator.edge_incorrect_connectivity().itertuples():
    area = basin.Area(geometry=model.basin.area[row.from_node_id].geometry.to_list())
    node = Node(row.from_node_id, row.geometry.boundary.geoms[0])
    model.edge.df.loc[row.Index, ["from_node_type"]] = "Basin"
    model.basin.add(node, data + [area])

if not network_validator.edge_incorrect_connectivity().empty:
    raise Exception("nog steeds edges zonder knopen")

# %% verwijderen internal basins

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291271525
for row in network_validator.node_internal_basin().itertuples():
    if row.node_id not in model.basin.area.df.node_id.to_numpy():  # remove or change to level-boundary
        edge_select_df = model.edge.df[model.edge.df.to_node_id == row.node_id]
        if len(edge_select_df) == 1:
            if edge_select_df.iloc[0]["from_node_type"] == "FlowBoundary":
                model.remove_node(row.node_id)
                model.remove_node(edge_select_df.iloc[0]["from_node_id"])
                model.edge.df.drop(index=edge_select_df.index[0], inplace=True)

df = model.node_table().df[model.node_table().df.node_type == "Basin"]

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291876800
boundary_node = Node(node_id=28, geometry=model.flow_boundary[28].geometry)

for edge_id in [1960, 798, 1579, 1959, 797]:
    model.remove_edge(edge_id)

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2292014475
model.remove_edge(edge_id=953)

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2292017813
model.remove_edge(edge_id=1913)
model.remove_edge(edge_id=951)

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2291988317
for edge_id in [799, 1580, 625, 1123, 597, 978]:
    model.reverse_edge(edge_id)

data = level_boundary.Static(level=[0])
model.level_boundary.add(boundary_node, [data])

model.edge.add(model.tabulated_rating_curve[614], model.level_boundary[28])

# see: https://github.com/Deltares/Ribasim-NL/issues/102#issuecomment-2292050862

gdf = gpd.read_file(
    cloud.joinpath("DeDommel", "verwerkt", "1_ontvangen_data", "Geodata", "data_Q42018.gpkg"),
    layer="HydroObject",
    engine="pyogrio",
    fid_as_index=True,
)

geometry = gdf.loc[2751].geometry.interpolate(0.5, normalized=True)
node_id = model.next_node_id
data = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])

model.manning_resistance.add(Node(node_id=node_id, geometry=geometry), [data])

model.edge.df = model.edge.df[~((model.edge.df.from_node_id == 611) & (model.edge.df.to_node_id == 1643))]
model.edge.add(model.basin[1643], model.manning_resistance[node_id])
model.edge.add(model.manning_resistance[node_id], model.basin[1182])
model.edge.add(model.tabulated_rating_curve[611], model.basin[1182])

# for row in df.itertuples():
#     area_select_df = model.basin.area.df[model.basin.area.df.geometry.contains(row.geometry)]
#     if len(area_select_df) == 1:
#         area_id = area_select_df.iloc[0]["node_id"]
#         if row.node_id != area_id:
#             print(f"{row.node_id} == {area_id}")
#     elif area_select_df.empty:
#         raise Exception(f"Basin {row.node_id} not within Area")
#     else:
#         raise Exception(f"Basin {row.node_id} contained by multiple areas")

# %% write model
ribasim_toml = ribasim_toml.parents[1].joinpath("DeDommel", ribasim_toml.name)
model.write(ribasim_toml)

# %% upload model

# cloud.upload_model("DeDommel", "DeDommel")
# %%
