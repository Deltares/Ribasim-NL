# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model
from ribasim_nl.check_basin_level import add_check_basin_level

cloud = CloudStorage()
authority = "BrabantseDelta"
short_name = "wbd"

run_model = False

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
qlr_path = cloud.joinpath("Basisgegevens\\QGIS_lyr\\output_controle_vaw_afvoer.qlr")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx, profiles_gpkg], check_on_remote=False)
cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)

# %%

# read
model = Model.read(ribasim_toml)

start_time = time.time()

# merge basins
model.merge_basins(node_id=2101, to_node_id=2058, are_connected=True)
model.merge_basins(node_id=1885, to_node_id=2114, are_connected=True)
model.merge_basins(node_id=2239, to_node_id=1617, are_connected=True)
model.merge_basins(node_id=1850, to_node_id=2022, are_connected=True)
model.merge_basins(node_id=2271, to_node_id=1766, are_connected=True)
model.merge_basins(node_id=2048, to_node_id=1412, are_connected=True)
model.merge_basins(node_id=2300, to_node_id=2198, are_connected=True)
model.merge_basins(node_id=2001, to_node_id=2273, are_connected=True)
model.merge_basins(node_id=1441, to_node_id=1799, are_connected=True)

# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")

# %%
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.005

# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
add_check_basin_level(model=model)
model.basin.area.df.loc[:, "meta_area"] = model.basin.area.df.area
model.write(ribasim_toml)

# %%

# run model
if run_model:
    exit_code = model.run()
    assert exit_code == 0

    # # %%
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()
# %%
