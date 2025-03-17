# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "ValleienVeluwe"
short_name = "venv"
run_model = False

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

model.merge_basins(basin_id=1054, to_node_id=925)
model.merge_basins(basin_id=999, to_node_id=1123)
model.merge_basins(basin_id=1098, to_node_id=865)
model.merge_basins(basin_id=997, to_node_id=1012)
model.merge_basins(basin_id=1266, to_node_id=816)
model.merge_basins(basin_id=976, to_node_id=1111)
model.merge_basins(basin_id=1021, to_node_id=1090)
model.basin.profile.df.loc[(model.basin.profile.df.node_id == 1209) & (model.basin.profile.df.area > 0.1), "area"] = (
    10000
)

model.outlet.static.df.loc[model.outlet.static.df.node_id == 312, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 369, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 446, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 576, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 74, "flow_rate"] = 1
# %%

# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# run model
if run_model:
    exit_code = model.run()
    assert exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml)
    indicators = controle_output.run_afvoer()

# %%
