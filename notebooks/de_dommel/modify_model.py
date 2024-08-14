# %%
import sqlite3

import pandas as pd
from ribasim import Model
from ribasim_nl import CloudStorage

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

# %% verwijder Basin to Basin edges
model.edge.df = model.edge.df[~((model.edge.df.from_node_type == "Basin") & (model.edge.df.to_node_type == "Basin"))]

# %% write model
ribasim_toml = ribasim_toml.parents[1].joinpath("DeDommel", ribasim_toml.name)
model.write(ribasim_toml)

# %% upload model

# cloud.upload_model("DeDommel", "DeDommel")
# %%
