# %%

import sqlite3

import pandas as pd
from ribasim_nl import CloudStorage, Model, reset_index

cloud = CloudStorage()


def update_database(toml_file):
    database_gpkg = toml_file.with_name("database.gpkg")
    conn = sqlite3.connect(database_gpkg)

    # get table into DataFrame
    table = "Outlet / static"
    df = pd.read_sql_query(f"SELECT * FROM '{table}'", conn)

    # drop urban runoff column if exists
    df.rename(columns={"min_crest_level": "min_upstream_level"}, inplace=True)

    #  Write the DataFrame back to the SQLite table
    df.to_sql(table, conn, if_exists="replace", index=False)

    # # Close the connection
    conn.close()


model_url = cloud.joinurl("AmstelGooienVecht", "modellen", "AmstelGooienVecht_parametrized_2024_8_47")
model_path = cloud.joinpath("AmstelGooienVecht", "modellen", "AmstelGooienVecht_parametrized_2024_8_47")
cloud.download_content(model_url)

# %% update RWS-HWS
model_path = cloud.joinpath("Rijkswaterstaat", "modellen", "hws")
toml_file = model_path / "hws.toml"
update_database(toml_file)
rws_model = Model.read(toml_file)

# some fixes
node_id = 8413
level = rws_model.upstream_profile(node_id).level.min() + 0.1

mask = (rws_model.tabulated_rating_curve.static.df.node_id == node_id) & (
    rws_model.tabulated_rating_curve.static.df.level < level
)
rws_model.tabulated_rating_curve.static.df.loc[mask, ["level"]] = level

# %% reset index
rws_model = reset_index(rws_model)

# write model
rws_model.write(model_path.with_name("hws_temp") / "hws.toml")

# %% update AGV
toml_file = model_path / "ribasim.toml"
update_database(toml_file)
model_to_couple = Model.read(toml_file)

model_to_couple.write(model_path.with_name("AmstelGooienVecht_temp") / "agv.toml")


# %%

node_ids = model_to_couple.level_boundary.static.df[
    model_to_couple.level_boundary.static.df.meta_to_authority == "Rijkswaterstaat"
].node_id.to_list()
