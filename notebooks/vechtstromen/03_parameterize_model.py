# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model
from ribasim_nl.check_basin_level import add_check_basin_level

cloud = CloudStorage()
authority = "Vechtstromen"
short_name = "vechtstromen"
run_model = False
run_period = None
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

# %% deactivate inlets
node_ids = model.pump.node.df[model.pump.node.df.meta_function.str.startswith("in")].index.to_numpy()
model.pump.static.df.loc[model.pump.static.df.node_id.isin(node_ids), "active"] = False

# Get node IDs where the name contains "inlaat"
# node_ids = model.outlet.node.df[
#    model.outlet.node.df["name"].fillna("").str.lower().str.contains("inlaat")
# ].index.to_numpy()

# Set active = False for these node IDs in the static data
# model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "active"] = False

# node_ids = model.outlet.node.df[model.outlet.node.df.meta_function.str.startswith("inlaat")].index.to_numpy()
# model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "active"] = False


# %%
# Merge basins

model.outlet.static.df.loc[model.outlet.static.df.node_id == 2019, "active"] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2019, ["meta_categorie"]] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1151, "active"] = False
model.remove_node(166, remove_edges=True)
model.remove_node(393, remove_edges=True)
model.remove_node(146, remove_edges=True)
model.remove_node(835, remove_edges=True)
model.remove_node(1370, remove_edges=True)
model.remove_node(358, remove_edges=True)
model.remove_node(188, remove_edges=True)
model.remove_node(219, remove_edges=True)
model.remove_node(345, remove_edges=True)
model.remove_node(654, remove_edges=True)
model.remove_node(1045, remove_edges=True)
model.remove_node(125, remove_edges=True)
model.remove_node(601, remove_edges=True)
model.remove_node(121, remove_edges=True)
model.remove_node(590, remove_edges=True)
model.remove_node(624, remove_edges=True)
model.remove_node(573, remove_edges=True)
model.remove_node(570, remove_edges=True)
model.remove_node(657, remove_edges=True)
model.remove_node(652, remove_edges=True)
model.merge_basins(basin_id=1488, to_basin_id=1834)
model.merge_basins(basin_id=1678, to_basin_id=1700)
model.merge_basins(basin_id=2181, to_basin_id=1700)
model.merge_basins(basin_id=1897, to_basin_id=1700)
model.merge_basins(basin_id=1876, to_basin_id=1633)
model.merge_basins(basin_id=2187, to_basin_id=1873)
model.merge_basins(basin_id=2174, to_basin_id=1873)
model.merge_basins(basin_id=1875, to_basin_id=1879)
model.merge_basins(basin_id=1908, to_basin_id=1879)
model.merge_basins(basin_id=1902, to_basin_id=1879)
model.merge_basins(basin_id=1441, to_basin_id=1879)
model.merge_basins(basin_id=1957, to_basin_id=1879)
model.merge_basins(basin_id=1717, to_basin_id=1528)
model.merge_basins(basin_id=1995, to_basin_id=1442)
model.merge_basins(basin_id=2262, to_basin_id=1431)
model.merge_basins(basin_id=1853, to_basin_id=1852)
model.merge_basins(basin_id=33, to_basin_id=1377)
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.005
model.outlet.static.df.loc[model.outlet.static.df.node_id == 375, "flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 44, "flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 260, "flow_rate"] = 0.1
# %%


# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
add_check_basin_level(model=model)
model.write(ribasim_toml)

# %%

# run model
if run_model:
    if run_period is not None:
        model.endtime = model.starttime + run_period
        model.write(ribasim_toml)
    exit_code = model.run()
    assert exit_code == 0

# %%
controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
indicators = controle_output.run_afvoer()
# %%
