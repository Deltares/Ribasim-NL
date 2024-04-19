# %%
import sqlite3

import pandas as pd
import ribasim
import tomli
import tomli_w
from ribasim import Node
from ribasim.nodes import linear_resistance
from ribasim_nl import CloudStorage


def upgrade_feature(database_path):
    conn = sqlite3.connect(database_path)

    table = "DiscreteControl / condition"
    df = pd.read_sql_query(f"SELECT * FROM '{table}'", conn)
    df_renamed = df.rename(
        columns={
            "listen_feature_type": "listen_node_type",
            "listen_feature_id": "listen_node_id",
        }
    )
    df_renamed.to_sql(table, conn, if_exists="replace", index=False)
    conn.close()


def upgrade_crs(toml_path):
    with open(toml_path, "rb") as f:
        config = tomli.load(f)

    config["crs"] = "EPSG:28992"

    with open(toml_path, "wb") as f:
        tomli_w.dump(config, f)


cloud = CloudStorage()

toml_file = cloud.joinpath(
    "rijkswaterstaat", "modellen", "hws_network_upgraded", "hws.toml"
)
upgrade_crs(toml_file)
# upgrade_feature(toml_file.with_name("database").with_suffix(".gpkg"))

# %%
model = ribasim.Model.read(toml_file)

mask = (model.edge.df.to_node_type == "LevelBoundary") & (
    model.edge.df.from_node_type == "ManningResistance"
)

node_ids = model.edge.df[mask].from_node_id.to_list()


model.edge.df.loc[
    model.edge.df.from_node_id.isin(node_ids), ["from_node_type"]
] = "LinearResistance"
model.edge.df.loc[
    model.edge.df.to_node_id.isin(node_ids), ["to_node_type"]
] = "LinearResistance"

# %% change

nodes = model.node_table().df[model.node_table().df["node_id"].isin(node_ids)]


for attr in model.manning_resistance.__fields__.keys():
    table = getattr(model.manning_resistance, attr)
    table.df = table.df[~table.df["node_id"].isin(node_ids)]


for _, node in nodes.iterrows():
    model.linear_resistance.add(
        Node(**node.to_dict()), [linear_resistance.Static(resistance=[0.005])]
    )

model.write(toml_file)
