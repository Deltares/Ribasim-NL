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

# %% fixes


# %%
# read
model = Model.read(ribasim_toml)

start_time = time.time()


# model.merge_basins(basin_id=2044, to_node_id=1850)
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=10)
print("Elapsed Time:", time.time() - start_time, "seconds")
# %%
# fixes

for edge_id in [2563, 1251, 1238, 2087, 91, 1436, 938, 2196]:
    model.reverse_edge(edge_id=edge_id)

model.outlet.static.df.loc[model.outlet.static.df.node_id == 2103, "flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 975, "flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 555, "flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 544, "flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 644, "flow_rate"] = 2
model.pump.static.df.loc[model.pump.static.df.node_id == 643, "flow_rate"] = 1

model.merge_basins(basin_id=1780, to_basin_id=1642, are_connected=False)
model.merge_basins(basin_id=2023, to_basin_id=1427, are_connected=False)
model.merge_basins(basin_id=1400, to_basin_id=1455)
model.merge_basins(basin_id=1455, to_basin_id=1435, are_connected=False)
model.merge_basins(basin_id=1726, to_basin_id=1744)
model.merge_basins(basin_id=2005, to_basin_id=1833, are_connected=False)
model.merge_basins(basin_id=1884, to_basin_id=1387, are_connected=False)
model.merge_basins(basin_id=1880, to_basin_id=1483, are_connected=False)
model.merge_basins(basin_id=1807, to_basin_id=1467)
model.merge_basins(basin_id=1863, to_basin_id=1860)
model.merge_basins(basin_id=1860, to_basin_id=2050)
model.merge_basins(basin_id=1543, to_basin_id=1524)
model.merge_basins(basin_id=1902, to_basin_id=1545)
model.merge_basins(basin_id=1789, to_basin_id=1792, are_connected=False)
model.merge_basins(basin_id=1435, to_basin_id=1436, are_connected=False)
model.merge_basins(basin_id=1590, to_basin_id=1786, are_connected=False)
# model.merge_basins(basin_id=1587, to_basin_id=1503)
model.merge_basins(basin_id=1957, to_basin_id=1591)
# model.merge_basins(basin_id=1528, to_basin_id=1428, are_connected=False)
model.merge_basins(basin_id=1529, to_basin_id=1428, are_connected=False)
model.merge_basins(basin_id=1587, to_basin_id=1503)
model.merge_basins(basin_id=1389, to_basin_id=1390)

model.outlet.static.df.loc[model.outlet.static.df.node_id == 955, "min_upstream_level"] = -1

model.remove_node(node_id=687, remove_edges=True)
model.remove_node(node_id=764, remove_edges=True)
model.remove_node(node_id=312, remove_edges=True)
model.remove_node(node_id=1051, remove_edges=True)
model.remove_node(node_id=741, remove_edges=True)
model.remove_node(node_id=1343, remove_edges=True)
model.redirect_edge(edge_id=1547, from_node_id=1914)
model.redirect_edge(edge_id=838, to_node_id=1524)

model.merge_basins(basin_id=1944, to_basin_id=1523, are_connected=False)


# %% deactivate inlets
node_ids = model.outlet.node.df[model.outlet.node.df.meta_code_waterbeheerder.str.startswith("I")].index.to_numpy()
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "active"] = False

model.outlet.static.df.loc[model.outlet.static.df.node_id == 206, "active"] = True

model.outlet.static.df.loc[model.outlet.static.df.node_id == 207, "active"] = True
model.outlet.static.df.loc[model.outlet.static.df.node_id == 358, "active"] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 984, "active"] = False

# %%
# model.solver.maxiters = 100000
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
