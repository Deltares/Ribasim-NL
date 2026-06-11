# %%

import time

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.parametrization.basin_tables import sync_min_upstream_levels_with_profile_bottoms
from ribasim_nl.parametrization.manning_level import sync_parameterized_manning_basin_levels

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "StichtseRijnlanden"
short_name = "hdsr"
run_model = False
static_data_xlsx = cloud.joinpath(authority, "verwerkt/parameters/static_data.xlsx")
aanvoergebieden_gpkg = cloud.joinpath(authority, "verwerkt", "sturing", "aanvoergebieden.gpkg")
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

cloud.synchronize(filepaths=[static_data_xlsx, qlr_path, aanvoergebieden_gpkg])

# %%
# read
model = Model.read(ribasim_toml)
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")
start_time = time.time()

# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5)
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
)

# Fixes
manual_basin_level_node_ids = [
    1737,
    1452,
    1426,
    1975,
    1836,
    1698,
    1474,
    1492,
    1396,
    1562,
    1387,
    1462,
    1501,
    1986,
    1987,
    1988,
    1516,
    1376,
    1380,
    1572,
    1886,
    1436,
    1847,
    1583,
    1586,
]
model.basin.area.df.loc[model.basin.area.df.node_id == 1737, "meta_streefpeil"] = -0.4
model.basin.area.df.loc[model.basin.area.df.node_id == 1452, "meta_streefpeil"] = 1.32
model.basin.area.df.loc[model.basin.area.df.node_id == 1426, "meta_streefpeil"] = 1.32
model.basin.area.df.loc[model.basin.area.df.node_id == 1975, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1836, "meta_streefpeil"] = -2.08  # Wulverhorst
model.basin.area.df.loc[model.basin.area.df.node_id == 1698, "meta_streefpeil"] = 0
model.basin.area.df.loc[model.basin.area.df.node_id == 1474, "meta_streefpeil"] = -0.48
model.basin.area.df.loc[model.basin.area.df.node_id == 1492, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1396, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1562, "meta_streefpeil"] = 0.52
model.basin.area.df.loc[model.basin.area.df.node_id == 1387, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1462, "meta_streefpeil"] = -1.55
model.basin.area.df.loc[model.basin.area.df.node_id == 1501, "meta_streefpeil"] = -1.55
model.basin.area.df.loc[model.basin.area.df.node_id == 1986, "meta_streefpeil"] = -1.55
model.basin.area.df.loc[model.basin.area.df.node_id == 1987, "meta_streefpeil"] = -1.55
model.basin.area.df.loc[model.basin.area.df.node_id == 1988, "meta_streefpeil"] = -1.55
model.basin.area.df.loc[model.basin.area.df.node_id == 1516, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1376, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1380, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1572, "meta_streefpeil"] = -2.22
model.basin.area.df.loc[model.basin.area.df.node_id == 1886, "meta_streefpeil"] = -2.1
model.basin.area.df.loc[model.basin.area.df.node_id == 1436, "meta_streefpeil"] = -0.1  # check streefpeil!
model.basin.area.df.loc[model.basin.area.df.node_id == 1847, "meta_streefpeil"] = -2.35  # check streefpeil!
model.basin.area.df.loc[model.basin.area.df.node_id == 1583, "meta_streefpeil"] = -0.25  # check streefpeil!
model.basin.area.df.loc[model.basin.area.df.node_id == 1586, "meta_streefpeil"] = 1.85  # check streefpeil!


# stadswater Utrecht beneden peil
node_ids = [1401, 1406, 1414, 1422, 1426, 1452, 1576, 1588, 1654, 1660, 1668, 1673, 1757, 1760, 1766, 1778]
manual_basin_level_node_ids.extend(node_ids)
model.basin.area.df.loc[model.basin.area.df.node_id.isin(node_ids), "meta_streefpeil"] = 0.58

# set basin-state op meta-streefpeil
model.basin.state.df = model.basin.area.df[["node_id", "meta_streefpeil"]].rename(columns={"meta_streefpeil": "level"})

# %%
# Write model
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

sync_min_upstream_levels_with_profile_bottoms(model=model)
model.basin.area.df.loc[:, "meta_area_m2"] = model.basin.area.df.area
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# run model
if run_model:
    result = model.run()
    assert result.exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()
# %%
