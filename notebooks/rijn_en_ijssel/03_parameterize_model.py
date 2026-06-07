# %%
import time

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.check_basin_level import add_check_basin_level
from ribasim_nl.parametrization.basin_tables import sync_min_upstream_levels_with_profile_bottoms
from ribasim_nl.parametrization.manning_level import sync_parameterized_manning_basin_levels

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "RijnenIJssel"
short_name = "wrij"

run_model = False

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
aanvoergebieden_gpkg = cloud.joinpath(authority, "verwerkt", "sturing", "aanvoergebieden.gpkg")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")
cloud.synchronize(filepaths=[static_data_xlsx, qlr_path, aanvoergebieden_gpkg])


# %%

# read
model = Model.read(ribasim_toml)
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")

start_time = time.time()
# %%
# %% wat basin-peilen zetten n.a.v. full control-checks
manual_basin_level_node_ids = [1068, 777, 1085, 793, 857]
model.basin.area.df.loc[model.basin.area.df.node_id == 1068, "meta_streefpeil"] = 8.55
model.basin.area.df.loc[model.basin.area.df.node_id == 777, "meta_streefpeil"] = 26.406666666666666
model.basin.area.df.loc[model.basin.area.df.node_id == 1085, "meta_streefpeil"] = 5.5
model.basin.area.df.loc[model.basin.area.df.node_id == 793, "meta_streefpeil"] = 10.7
model.basin.area.df.loc[model.basin.area.df.node_id == 857, "meta_streefpeil"] = 11.60
model.basin.area.df.loc[model.basin.area.df.node_id == 1011, "meta_streefpeil"] = 6.5


# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03
sync_parameterized_manning_basin_levels(
    model=model,
    aanvoergebieden_df=aanvoergebieden_df,
    output_gpkg=cloud.joinpath(
        authority,
        "modellen",
        f"{authority}_parameterized_model",
        "manning_level_basin_updates.gpkg",
    ),
    protected_basin_node_ids=manual_basin_level_node_ids,
)

# %% fixes

model.outlet.static.df.loc[model.outlet.static.df.node_id == 471, "flow_rate"] = 0.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 472, "flow_rate"] = 0.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 119, "min_upstream_level"] = 11.0

# %%

node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

# %%
# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
sync_min_upstream_levels_with_profile_bottoms(model=model)
add_check_basin_level(model=model)
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
