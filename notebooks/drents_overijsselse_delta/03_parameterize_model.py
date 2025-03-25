# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model
from ribasim_nl.check_basin_level import add_check_basin_level

cloud = CloudStorage()
authority = "DrentsOverijsselseDelta"
short_name = "dod"

run_model = True

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
qlr_path = cloud.joinpath("Basisgegevens\\QGIS_lyr\\output_controle_vaw_afvoer.qlr")


ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
# cloud.synchronize(filepaths=[static_data_xlsx, profiles_gpkg])
# cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)

# %%

# read
model = Model.read(ribasim_toml)

start_time = time.time()
# %%
# parameterize
model.update_node(node_id=1401, node_type="Outlet")
model.parameterize(
    static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=10, profiles_gpkg=profiles_gpkg, max_pump_flow_rate=125
)
print("Elapsed Time:", time.time() - start_time, "seconds")

# %%
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.005
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1401, ["active"]] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1401, ["meta_categorie"]] = "Inlaat"
# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
add_check_basin_level(model=model)
model.write(ribasim_toml)

# %%
# run model
if run_model:
    start_time = time.time()
    exit_code = model.run()
    print("Run Time:", time.time() - start_time, "seconds")
    assert exit_code == 0

# # %%
controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
indicators = controle_output.run_afvoer()

# %%
