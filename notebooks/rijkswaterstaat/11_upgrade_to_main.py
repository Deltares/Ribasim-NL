# %%
import sqlite3

import pandas as pd
import ribasim
import tomli
import tomli_w
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

toml_file_read = cloud.joinpath(
    "rijkswaterstaat", "modellen", "hws_2024_4_0", "hws.toml"
)

model_dir_write = cloud.joinpath("rijkswaterstaat", "modellen", "hws")
toml_file_write = model_dir_write / "hws.toml"
model = ribasim.Model.read(toml_file_read)

mask = (model.network.edge.df.to_node_type == "LevelBoundary") & (
    model.network.edge.df.from_node_type == "ManningResistance"
)

node_ids = model.network.edge.df[mask].from_node_id.to_list()

# %% change

# change type network
model.network.edge.df.loc[mask, ["from_node_type"]] = "LinearResistance"
model.network.node.df.loc[node_ids, ["node_type"]] = "LinearResistance"

# remove from ManningTable
model.manning_resistance.static.df = model.manning_resistance.static.df[
    ~model.manning_resistance.static.df.node_id.isin(node_ids)
]

# add to resistance
df = pd.DataFrame(
    {
        "node_id": node_ids,
        "resistance": [0.005] * len(node_ids),
        "active": [True] * len(node_ids),
        "control_state": [None] * len(node_ids),
    }
)

model.linear_resistance.static.df = pd.concat(
    [model.linear_resistance.static.df, ribasim.LinearResistance(static=df).static.df],
    ignore_index=True,
)

model.write(toml_file_write)

upgrade_feature(model_dir_write / "database.gpkg")
upgrade_crs(toml_file_write)
