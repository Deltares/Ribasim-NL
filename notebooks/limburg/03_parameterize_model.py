# %%
import time

import pandas as pd

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "Limburg"
short_name = "limburg"

run_model = True

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx, profiles_gpkg], check_on_remote=False)
cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)

# %%

# read
model = Model.read(ribasim_toml)
start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.001
# %%

node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA


# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# run model
if run_model:
    exit_code = model.run()

# # %%
controle_output = Control(ribasim_toml=ribasim_toml)
indicators = controle_output.run_afvoer()
# %%
