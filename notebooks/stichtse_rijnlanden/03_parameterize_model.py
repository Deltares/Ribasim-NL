# %%
import time
from pathlib import Path

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model
from ribasim_nl.check_basin_level import add_check_basin_level

cloud = CloudStorage()
ribasim_exe = Path(r"c:\\Ribasim_dev\\ribasim.exe")
authority = "StichtseRijnlanden"
short_name = "hdsr"
run_model = False
static_data_xlsx = cloud.joinpath(
    authority,
    "verwerkt",
    "parameters",
    "static_data.xlsx",
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
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=10)
print("Elapsed Time:", time.time() - start_time, "seconds")

# %%

# fixes
model.remove_node(node_id=1031, remove_edges=True)
model.merge_basins(basin_id=1882, to_node_id=1867)
model.merge_basins(basin_id=1888, to_node_id=1867)
model.merge_basins(basin_id=1676, to_node_id=1375)
model.merge_basins(basin_id=1866, to_node_id=1883)
model.merge_basins(basin_id=1870, to_node_id=1883)
model.merge_basins(basin_id=1695, to_node_id=1800)
model.merge_basins(basin_id=2011, to_node_id=2050)
model.merge_basins(basin_id=2049, to_node_id=2050)
model.merge_basins(basin_id=1892, to_node_id=1947)
model.merge_basins(basin_id=1834, to_node_id=1947)
model.merge_basins(basin_id=2010, to_node_id=1863)

# %% deactivate inlets
node_ids = model.outlet.node.df[model.outlet.node.df.meta_code_waterbeheerder.str.startswith("I")].index.to_numpy()
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "active"] = False
model.solver.maxiters = 100000
# Write model
add_check_basin_level(model=model)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
# model.endtime = model.starttime + timedelta(days=31)
model.write(ribasim_toml)

# %%

# run model
if run_model:
    exit_code = model.run(ribasim_exe=ribasim_exe)
    assert exit_code == 0

# %%
controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
indicators = controle_output.run_afvoer()
# %%
