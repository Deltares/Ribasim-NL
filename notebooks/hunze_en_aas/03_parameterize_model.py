# %%
import time

import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.check_basin_level import add_check_basin_level
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    sync_min_upstream_levels_with_profile_bottoms,
)
from ribasim_nl.parametrization.manning_level import sync_parameterized_manning_basin_levels

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "HunzeenAas"
short_name = "hea"
run_model = False
static_data_xlsx = cloud.joinpath(authority, "verwerkt/parameters/static_data.xlsx")
aanvoergebieden_gpkg = cloud.joinpath(authority, "verwerkt", "sturing", "aanvoergebieden.gpkg")
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

# # you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx, qlr_path, aanvoergebieden_gpkg])

# %%

# read
model = Model.read(ribasim_toml)
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

start_time = time.time()

# %% fixes basins and profiles

basin_level_overrides = [
    ([1338], 7.1),
    ([1432], 6),
    ([1680], 6),
    ([1617], 1.75),
    ([1325], 5.1),
    ([1311], 3.3),
    ([1832], 3.3),
    ([1416], 17.7),
    ([1832], 11.5),
]

# %%

model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5)
print("Elapsed Time:", time.time() - start_time, "seconds")
protected_basin_node_ids = apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03
sync_parameterized_manning_basin_levels(
    model=model,
    aanvoergebieden_df=aanvoergebieden_df,
    output_gpkg=cloud.joinpath(
        authority,
        "modellen",
        f"{authority}_parameterized_model",
        "manning_level_basin_updates.gpkg",
    ),
    protected_basin_node_ids=protected_basin_node_ids,
)

# %% Flow rates are replaced to max_flow_rate, otherwise it affects the flow ratio
model.outlet.static.df.max_flow_rate = model.outlet.static.df.flow_rate
model.outlet.static.df.flow_rate = 100.0
# %% Fixes
model.pump.static.df.loc[model.pump.static.df.node_id == 27, "max_flow_rate"] = 5.0
model.pump.static.df.loc[model.pump.static.df.node_id == 64, "max_flow_rate"] = 1.0
model.pump.static.df.loc[model.pump.static.df.node_id == 71, "max_flow_rate"] = 5.0
model.pump.static.df.loc[model.pump.static.df.node_id == 82, "max_flow_rate"] = 2.0
model.pump.static.df.loc[model.pump.static.df.node_id == 114, "max_flow_rate"] = 1.0
model.pump.static.df.loc[model.pump.static.df.node_id == 123, "max_flow_rate"] = 5.0
model.pump.static.df.loc[model.pump.static.df.node_id == 29, "max_flow_rate"] = 1.0
model.pump.static.df.loc[model.pump.static.df.node_id == 68, "max_flow_rate"] = 1.0
model.pump.static.df.loc[model.pump.static.df.node_id == 58, "max_flow_rate"] = 5.0
model.pump.static.df.loc[model.pump.static.df.node_id == 59, "max_flow_rate"] = 5.0
model.pump.static.df.loc[model.pump.static.df.node_id == 133, "max_flow_rate"] = 3.0

# %%
# Write model
sync_min_upstream_levels_with_profile_bottoms(model=model)
add_check_basin_level(model=model)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()
# %%
