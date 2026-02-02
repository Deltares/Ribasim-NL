# %%
import time

import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.check_basin_level import add_check_basin_level

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "Vechtstromen"
short_name = "vechtstromen"
run_model = True
run_period = None
static_data_xlsx = cloud.joinpath(authority, "verwerkt/parameters/static_data.xlsx")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

cloud.synchronize(filepaths=[static_data_xlsx, qlr_path])


# %%
# read
model = Model.read(ribasim_toml)

start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=1)
print("Elapsed Time:", time.time() - start_time, "seconds")
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.001


# %%
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2019, ["meta_categorie"]] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 704, "min_upstream_level"] = 10.5
model.pump.static.df.loc[model.pump.static.df.node_id == 672, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 375, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 947, "max_flow_rate"] = 2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 260, "max_flow_rate"] = 0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 532, "max_flow_rate"] = 2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1060, "max_flow_rate"] = 2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 44, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 260, "max_flow_rate"] = 0.5

# %%
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

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
    result = model.run()
    assert result.exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()
# %%
