# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model
from ribasim_nl.check_basin_level import add_check_basin_level

cloud = CloudStorage()
authority = "StichtseRijnlanden"
short_name = "hdsr"
run_model = False
static_data_xlsx = cloud.joinpath(
    authority,
    "verwerkt",
    "parameters",
    "static_data_template.xlsx",
)
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens\\QGIS_lyr\\output_controle_vaw_afvoer.qlr")

# # you need the excel, but the model should be local-only by running 01_fix_model.py
# cloud.synchronize(filepaths=[static_data_xlsx])
# cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)

# %%

# read
model = Model.read(ribasim_toml)

start_time = time.time()
# %%
# fixes


# model.merge_basins(basin_id=2044, to_node_id=1850)
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=10)
print("Elapsed Time:", time.time() - start_time, "seconds")


# %% deactivate inlets
node_ids = model.outlet.node.df[model.outlet.node.df.meta_code_waterbeheerder.str.startswith("I")].index.to_numpy()
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "active"] = False
model.solver.maxiters = 100000
# Write model
add_check_basin_level(model=model)
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.005

model.basin.area.df.loc[:, "meta_area_m2"] = model.basin.area.df.area
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# run model
if run_model:
    exit_code = model.run()
    assert exit_code == 0

# %%
controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
indicators = controle_output.run_afvoer()
# %%
