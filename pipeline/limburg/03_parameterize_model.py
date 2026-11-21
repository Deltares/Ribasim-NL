# %%
import time

from ribasim_nl.controle_output import AFVOER_METRICS, Control
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    apply_level_boundary_overrides,
    clear_ungated_outlet_levels,
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

# %%

# read
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")
assert model.manning_resistance.static.df is not None
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03
# %%


clear_ungated_outlet_levels(model)

# %% fixes basins and profiles

basin_level_overrides = [
    ([n("up:S_98244")], 28.82),
    ([n("up:D_23544")], 31.0),
    ([n("loc:191446,372816")], 31.0),
    ([n("up:S_97188")], 27.27),
    ([n("up:D_18736")], 27.6),
    ([n("loc:189487,383501")], 30.75),
    ([n("up:S_97928")], 30.7),
    ([n("up:D_217317")], 30.75),
    ([n("up:S_97479")], 27.4),
    ([n("up:D_19576")], 28.8),
    ([n("up:D_21900")], 28.6),
]

apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

boundary_level_overrides = {
    n("bnd:99"): 31.0,
    n("bnd:120"): 31.0,
    n("bnd:121"): 31.0,
}

apply_level_boundary_overrides(model, boundary_level_overrides)

# %%
# Write model
assert model.basin.area.df is not None
model.basin.area.df.loc[:, "meta_area_m2"] = model.basin.area.df.area
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
sync_min_upstream_levels_with_profile_bottoms(model=model)
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()

    controle_output = Control(ribasim_toml=ribasim_toml)
    indicators = controle_output.run(metrics=AFVOER_METRICS)
# %%
