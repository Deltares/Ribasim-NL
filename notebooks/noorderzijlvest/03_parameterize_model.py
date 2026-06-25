# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl.check_basin_level import add_check_basin_level
from ribasim_nl.parametrization.basin_tables import sync_min_upstream_levels_with_profile_bottoms

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "Noorderzijlvest"
short_name = "nzv"
run_model = False


static_data_xlsx = cloud.joinpath(authority, "verwerkt/parameters/static_data.xlsx")
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

# you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx, qlr_path])

# %%

# read
model = Model.read(ribasim_toml)

start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=10)
print("Elapsed Time:", time.time() - start_time, "seconds")
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03

# %%
manual_basin_level_node_ids = [1132]
model.basin.area.df.loc[model.basin.area.df.node_id == 1132, "meta_streefpeil"] = 3.45

# %%
# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
sync_min_upstream_levels_with_profile_bottoms(model=model)
add_check_basin_level(model=model)
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()
