# %%
import time

import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim import run_ribasim
from ribasim_nl.check_basin_level import add_check_basin_level

# voeg deze imports toe
from ribasim_nl.parametrization.basin_tables import (
    update_basin_state,
    update_basin_static,
)

from ribasim_nl import CloudStorage, Model, settings

cloud = CloudStorage()
authority = "AaenMaas"
short_name = "aam"

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx, qlr_path])

# %%
# read
model = Model.read(ribasim_toml)
start_time = time.time()

# %%
# parameterize
model.basin.area.df.loc[model.basin.area.df.node_id == 1801, "meta_streefpeil"] = -0.25
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
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03

# Fix afvoer
model.outlet.static.df.loc[model.outlet.static.df.node_id == 375, "flow_rate"] = 0.0

# fix basin_levels
basin_level_overrides = [
    # Peil niet consistent voor sturing
    ([1331], 2.83),
    ([1446], 11.6),
    ([1665], 9.3),
    ([1419], 18.15),
    ([1852], 18.15),
    ([1149], 6.95),
    ([1475], 22.95),
    ([1572], 25.7),
    ([1565], 23.5),
    ([1885], 23.5),
    ([1959], 23.5),
]

for node_ids, meta_streefpeil in basin_level_overrides:
    mask = model.basin.area.df.node_id.isin(node_ids)
    model.basin.area.df.loc[mask, "meta_streefpeil"] = meta_streefpeil

# Herbereken afgeleide tabellen na handmatige streefpeil-overrides.
model.basin.state.df = model.basin.area.df[["node_id", "meta_streefpeil"]].rename(columns={"meta_streefpeil": "level"})


# Fixes
def update_nodes(model: Model, node_ids: list[int], node_type: str) -> None:
    for node_id in dict.fromkeys(node_ids):
        model.update_node(node_id=node_id, node_type=node_type)


# make pump nodes
update_nodes(model, [100, 95, 105, 113], "Pump")
# %%
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

model.outlet.static.df.loc[model.outlet.static.df.node_id.isin([98, 103, 221]), "min_upstream_level"] = (
    2.2  # peil drongelens kanaal
)

# %%
# optionele checks
print(model.basin.profile.df.groupby("node_id")["area"].max().head())
print(model.basin.static.df[["node_id", "precipitation", "potential_evaporation"]].head())

# Write model
add_check_basin_level(model=model)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%
# run model
run_ribasim(ribasim_toml, ribasim_home=settings.ribasim_home)

# %%
controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
indicators = controle_output.run_afvoer()
# %%
