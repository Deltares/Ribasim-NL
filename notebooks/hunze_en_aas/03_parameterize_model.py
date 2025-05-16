# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model
from ribasim_nl.check_basin_level import add_check_basin_level

cloud = CloudStorage()
authority = "HunzeenAas"
short_name = "hea"
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

# %%fixes
model.remove_node(node_id=1126, remove_edges=True)
model.remove_node(node_id=1023, remove_edges=True)

node_ids = model.outlet.node.df[model.outlet.node.df.meta_code_waterbeheerder.str.startswith("KIN")].index.to_numpy()
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "active"] = False

# %% Fixes
model.outlet.static.df.loc[model.outlet.static.df.node_id == 792, "active"] = True
model.outlet.static.df.loc[model.outlet.static.df.node_id == 634, "active"] = True
model.outlet.static.df.loc[model.outlet.static.df.node_id == 571, "active"] = True
model.outlet.static.df.loc[model.outlet.static.df.node_id == 862, "active"] = True
model.outlet.static.df.loc[model.outlet.static.df.node_id == 603, "active"] = True
model.outlet.static.df.loc[model.outlet.static.df.node_id == 978, "active"] = True
model.outlet.static.df.loc[model.outlet.static.df.node_id == 183, "active"] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1220, "active"] = False


model.outlet.static.df.loc[model.outlet.static.df.node_id == 570, "min_upstream_level"] = -1.27
model.outlet.static.df.loc[model.outlet.static.df.node_id == 815, "min_upstream_level"] = -1.27
model.pump.static.df.loc[model.pump.static.df.node_id == 27, "flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 64, "flow_rate"] = 1
model.pump.static.df.loc[model.pump.static.df.node_id == 71, "flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 82, "flow_rate"] = 2
model.pump.static.df.loc[model.pump.static.df.node_id == 114, "flow_rate"] = 1
model.pump.static.df.loc[model.pump.static.df.node_id == 123, "flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 29, "flow_rate"] = 1
model.pump.static.df.loc[model.pump.static.df.node_id == 68, "flow_rate"] = 1
model.pump.static.df.loc[model.pump.static.df.node_id == 58, "flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 59, "flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 133, "flow_rate"] = 3
model.pump.static.df.loc[model.pump.static.df.node_id == 134, "active"] = False
model.pump.static.df.loc[model.pump.static.df.node_id == 728, "active"] = False
model.pump.static.df.loc[model.pump.static.df.node_id == 62, "active"] = False

model.merge_basins(basin_id=1908, to_basin_id=1372)


# %%
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.005
# Write model
add_check_basin_level(model=model)
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
