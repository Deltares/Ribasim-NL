# %%

import time

import pandas as pd
from peilbeheerst_model.controle_output import Control

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "StichtseRijnlanden"
short_name = "hdsr"
run_model = False
static_data_xlsx = cloud.joinpath(authority, "verwerkt/parameters/static_data.xlsx")
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")
# model_edits_extra_gpkg = cloud.joinpath(authority, "verwerkt/model_edits_aanvoer.gpkg")
# # you need the excel, but the model should be local-only by running 01_fix_model.py
# cloud.synchronize(filepaths=[static_data_xlsx])
# cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)

# %%
# read
model = Model.read(ribasim_toml)
start_time = time.time()

# %%
model.basin.area.df.loc[model.basin.area.df.node_id == 1975, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1836, "meta_streefpeil"] = -2.08  # Wulverhorst
model.basin.area.df.loc[model.basin.area.df.node_id == 1988, "meta_streefpeil"] = -1.55
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5)
print("Elapsed Time:", time.time() - start_time, "seconds")
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.001

# Fixes
model.basin.area.df.loc[model.basin.area.df.node_id == 1975, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1836, "meta_streefpeil"] = -2.08  # Wulverhorst

model.basin.area.df.loc[model.basin.area.df.node_id == 1698, "meta_streefpeil"] = 0
model.basin.area.df.loc[model.basin.area.df.node_id == 1474, "meta_streefpeil"] = -0.48
model.basin.area.df.loc[model.basin.area.df.node_id == 1492, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1396, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1562, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1387, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1986, "meta_streefpeil"] = -1.55
model.basin.area.df.loc[model.basin.area.df.node_id == 1987, "meta_streefpeil"] = -1.55
model.basin.area.df.loc[model.basin.area.df.node_id == 1516, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1376, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1380, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1572, "meta_streefpeil"] = -2.22
# %%
# fixes

for link_id in [2563, 1238, 91, 1436]:
    model.reverse_link(link_id=link_id)

model.outlet.static.df.loc[model.outlet.static.df.node_id == 2103, "max_flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 809, "max_flow_rate"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 975, "max_flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 906, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 984, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 358, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 751, "max_flow_rate"] = 0.1  # Montfoort sluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 86, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 758, "max_flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 555, "max_flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 544, "max_flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 644, "max_flow_rate"] = 2
model.pump.static.df.loc[model.pump.static.df.node_id == 643, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 747, "max_flow_rate"] = 0.1
model.pump.static.df.loc[model.pump.static.df.node_id == 405, "max_flow_rate"] = 0.1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 955, "min_upstream_level"] = -1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 906, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 342, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 164, "min_upstream_level"] = 0.52
model.outlet.static.df.loc[model.outlet.static.df.node_id == 210, "min_upstream_level"] = 0.52


# %% Alle inlaten op max 0.5m3/s gezet.
node_ids = model.outlet.node.df[model.outlet.node.df.meta_code_waterbeheerder.str.startswith("I")].index.to_numpy()
model.outlet.static.df.loc[model.outlet.static.df.node_id.isin(node_ids), "max_flow_rate"] = 0.1


# %%
# model.solver.maxiters = 100000
# Write model
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

model.basin.area.df.loc[:, "meta_area_m2"] = model.basin.area.df.area
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

# %%
controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
indicators = controle_output.run_afvoer()
# %%
