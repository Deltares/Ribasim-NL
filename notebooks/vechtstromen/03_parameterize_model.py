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
model.outlet.node.df.name
# %% deactivate inlets
node_ids = model.pump.node.df[model.pump.node.df.meta_function.str.startswith("in")].index.to_numpy()
model.pump.static.df.loc[model.pump.static.df.node_id.isin(node_ids), "active"] = False

# Get node IDs where the name contains "inlaat"
node_ids = model.outlet.node.df[
    model.outlet.node.df["name"].fillna("").str.lower().str.contains("inlaat")
].index.to_numpy()

# Set active = False for these node IDs in the static data
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "active"] = False


node_ids = model.outlet.node.df[model.outlet.node.df.meta_function.str.startswith("in")].index.to_numpy()
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "active"] = False


# %%
# Merge basins
model.merge_basins(basin_id=2115, to_node_id=1405)
model.merge_basins(basin_id=1378, to_node_id=1431)
model.merge_basins(basin_id=2211, to_node_id=1727)
model.merge_basins(basin_id=1538, to_node_id=33)
model.merge_basins(basin_id=1963, to_node_id=1518)
model.merge_basins(basin_id=2245, to_node_id=1818)
model.merge_basins(basin_id=2026, to_node_id=1818)
model.merge_basins(basin_id=1412, to_node_id=2107)
model.merge_basins(basin_id=1592, to_node_id=1765)
model.merge_basins(basin_id=1765, to_node_id=1817)
model.merge_basins(basin_id=2159, to_node_id=1890)
# model.merge_basins(basin_id=1604, to_node_id=1890)
model.merge_basins(basin_id=1628, to_node_id=2143)
model.merge_basins(basin_id=1821, to_node_id=2143)
model.merge_basins(basin_id=2144, to_node_id=2143)
model.merge_basins(basin_id=2116, to_node_id=1730)
model.merge_basins(basin_id=2177, to_node_id=1730)
model.remove_node(node_id=619, remove_edges=True)
model.remove_node(node_id=660, remove_edges=True)
model.remove_node(node_id=698, remove_edges=True)
model.remove_node(node_id=1243, remove_edges=True)
model.remove_node(node_id=1242, remove_edges=True)
model.remove_node(node_id=836, remove_edges=True)
model.remove_node(node_id=80, remove_edges=True)
model.remove_node(node_id=265, remove_edges=True)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2019, "active"] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2019, ["meta_categorie"]] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1151, "active"] = False
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.005
model.pump.static.df.loc[model.pump.static.df.node_id == 582, "min_upstream_level"] = 6.13
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
