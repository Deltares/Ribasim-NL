# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl.check_basin_level import add_check_basin_level

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "BrabantseDelta"
short_name = "wbd"

run_model = True

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx, qlr_path])

# %%

# read
model = Model.read(ribasim_toml)
start_time = time.time()

# %%
# parameterize
# fix basin streefpeilen
manual_basin_level_node_ids = [1354]
model.basin.area.df.loc[model.basin.area.df.node_id == 1354, "meta_streefpeil"] = (
    4.1  # n.a.v. min_upstream_level van outlet 342
)
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")

model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03

model.update_node(node_id=1085, node_type="outlet")

# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
add_check_basin_level(model=model)
model.basin.area.df.loc[:, "meta_area"] = model.basin.area.df.area
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

    # # %%
    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()
# %%
