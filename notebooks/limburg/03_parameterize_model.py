# %%
import time

import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    sync_min_upstream_levels_with_profile_bottoms,
)

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "Limburg"
short_name = "limburg"

run_model = False

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
    ([2408], 28.82),
    ([2309], 31.0),
    ([2495], 31.0),
    ([2418], 27.27),
    ([1873], 27.6),
    ([5434], 30.75),
    ([2492], 30.75),
    ([1553], 30.7),
    ([1995], 30.75),
    ([2028], 27.4),
    ([1953], 28.8),
    ([1606], 28.6),
]

apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

boundary_level_overrides = {
    99: 31.0,
    120: 31.0,
    121: 31.0,
}

for node_id, level in boundary_level_overrides.items():
    mask = model.level_boundary.static.df.node_id == node_id
    model.level_boundary.static.df.loc[mask, "level"] = level

# %%
# Write model
model.basin.area.df.loc[:, "meta_area_m2"] = model.basin.area.df.area
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
sync_min_upstream_levels_with_profile_bottoms(model=model)
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()

    controle_output = Control(ribasim_toml=ribasim_toml)
    indicators = controle_output.run_afvoer()
# %%
