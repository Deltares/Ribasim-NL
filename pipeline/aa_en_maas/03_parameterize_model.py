# %%
import time

from ribasim import run_ribasim
from ribasim_nl.check_basin_level import add_check_basin_level
from ribasim_nl.controle_output import AFVOER_METRICS, Control

# voeg deze imports toe
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    apply_level_boundary_overrides,
    clear_ungated_outlet_levels,
    sync_min_upstream_levels_with_profile_bottoms,
    update_basin_state,
    update_basin_static,
)

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "AaenMaas"
short_name = "aam"
run_model = False

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# you need the excel, but the model should be local-only by running 01_fix_model.py

# %%
# read
model = Model.read(ribasim_toml)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = model.node_id
start_time = time.time()

# %%
# parameterize
assert model.basin.area.df is not None
model.parameterize(
    static_data_xlsx=static_data_xlsx,
    evaporation_mm_per_day=1,
    profiles_gpkg=profiles_gpkg,
)
print("Elapsed Time:", time.time() - start_time, "seconds")

# %%
# update state op basis van nieuw profiel
update_basin_state(model)

# update forcing opnieuw, zodat nieuwe profile-area ook wordt meegenomen
update_basin_static(
    model=model,
    precipitation_mm_per_day=0,
    evaporation_mm_per_day=1,  # pas aan naar jouw waarde
)

# %%
assert model.manning_resistance.static.df is not None
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03

# Fix afvoer
assert model.outlet.static.df is not None
model.outlet.static.df.loc[model.outlet.static.df.node_id == n("201BGS"), "flow_rate"] = 0.0

# fix basin_levels
basin_level_overrides = [
    # Peil niet consistent voor sturing
    ([n("up:ow:2011330")], 2.83),
    ([n("up:201G")], 11.6),
    ([n("up:217FA")], 9.3),
    ([n("up:2010340___2")], 18.15),
    ([n("up:201DDK")], 18.15),
    ([n("up:2113405___1")], 6.95),
    ([n("up:201R")], 22.95),
    ([n("up:2940212")], 25.7),
    ([n("up:2910006")], 23.5),
    ([n("up:2920069")], 23.5),
    ([n("up:261BR")], 30.75),
    ([n("up:3200005")], 30.75),
    ([n("up:275KDN")], 30.75),
    ([n("up:2751")], 30.75),
]

apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

boundary_level_overrides = {
    n("bnd:37"): 31.0,
    n("bnd:38"): 31.0,
}

apply_level_boundary_overrides(model, boundary_level_overrides)


# Fixes


# make pump nodes
model.update_nodes([n("211VEL"), n("211GOW"), n("204GRW"), n("211HDS")], "Pump")
# %%
clear_ungated_outlet_levels(model)

model.outlet.static.df.loc[
    model.outlet.static.df.node_id.isin([n("117BSL"), n("VRKW118"), n("116HDW")]), "min_upstream_level"
] = 2.2  # peil drongelens kanaal

# %%
# optionele checks
assert model.basin.profile.df is not None
print(model.basin.profile.df.groupby("node_id")["area"].max().head())
assert model.basin.static.df is not None
print(model.basin.static.df[["node_id", "precipitation", "potential_evaporation"]].head())

# Write model
sync_min_upstream_levels_with_profile_bottoms(model=model)
add_check_basin_level(model=model)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%
# run model
if run_model:
    run_ribasim(ribasim_toml)
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run(metrics=AFVOER_METRICS)
# %%
