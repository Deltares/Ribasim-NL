# %%
import time

import pandas as pd
from peilbeheerst_model.controle_output import Control

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "Limburg"
short_name = "limburg"

run_model = True

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx])

# %%

# read
model = Model.read(ribasim_toml)
start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03
# %%


node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

# %% fixes basins and profiles

basin_level_overrides = [
    ([2309], 31.0),
    ([2492], 31.4),
    ([2495], 27.75),
    ([2418], 27.27),
    ([1873], 27.6),
]

for node_ids, meta_streefpeil in basin_level_overrides:
    mask = model.basin.area.df.node_id.isin(node_ids)
    model.basin.area.df.loc[mask, "meta_streefpeil"] = meta_streefpeil

if model.basin.profile.df is not None and not model.basin.profile.df.empty:
    profile_top = model.basin.profile.df.groupby("node_id")["level"].max()
    for node_ids, meta_streefpeil in basin_level_overrides:
        for node_id in node_ids:
            if node_id not in profile_top.index:
                continue
            level_shift = float(meta_streefpeil) - float(profile_top.at[node_id])
            mask = model.basin.profile.df.node_id.eq(node_id)
            model.basin.profile.df.loc[mask, "level"] = (
                model.basin.profile.df.loc[mask, "level"].astype(float) + level_shift
            )

# Herbereken afgeleide tabellen na handmatige streefpeil-/profiel-overrides.
model.basin.state.df = model.basin.area.df[["node_id", "meta_streefpeil"]].rename(columns={"meta_streefpeil": "level"})

# %%
# Write model
model.basin.area.df.loc[:, "meta_area_m2"] = model.basin.area.df.area
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()

# # %%
controle_output = Control(ribasim_toml=ribasim_toml)
indicators = controle_output.run_afvoer()
# %%
