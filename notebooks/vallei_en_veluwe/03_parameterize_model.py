# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "ValleienVeluwe"
short_name = "venv"
run_model = True

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
# cloud.synchronize(fil
#
# aths=[static_data_xlsx])

# cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)

# %%

# read
model = Model.read(ribasim_toml)

start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=10, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")
model.outlet.static.df.loc[model.outlet.static.df.node_id == 559, "active"] = False
# model.manning_resistance.static.df.loc[model.manning_resistance.static.df.node_id == 665, "active"] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 212, "flow_rate"] = 50
model.outlet.static.df.loc[model.outlet.static.df.node_id == 700, "flow_rate"] = 50
model.outlet.static.df.loc[model.outlet.static.df.node_id == 374, "flow_rate"] = 4
model.outlet.static.df.loc[model.outlet.static.df.node_id == 583, "flow_rate"] = 10
model.outlet.static.df.loc[model.outlet.static.df.node_id == 93, "flow_rate"] = 10
model.outlet.static.df.loc[model.outlet.static.df.node_id == 403, "flow_rate"] = 10
model.outlet.static.df.loc[model.outlet.static.df.node_id == 405, "flow_rate"] = 10
model.outlet.static.df.loc[model.outlet.static.df.node_id == 102, "flow_rate"] = 1
model.pump.static.df.loc[model.pump.static.df.node_id == 242, "active"] = False
model.pump.static.df.loc[model.pump.static.df.node_id == 612, "active"] = False


# model.manning_resistance.static.df.loc[model.manning_resistance.static.df.node_id == 217, "active"] = False
# %%

# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# run model
if run_model:
    exit_code = model.run()
    assert exit_code == 0

# %%

controle_output = Control(ribasim_toml=ribasim_toml)
indicators = controle_output.run_afvoer()
# %%
