# %%

import time

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    sync_min_upstream_levels_with_profile_bottoms,
)
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
basin_level_overrides = [
    ([1737], -0.4),
    ([1975], 0.52),
    ([1836], -2.08),  # Wulverhorst
    ([1698], 0.0),
    ([1474], -0.48),
    ([1492], 0.52),
    ([1396], 0.52),
    ([1562], 0.52),
    ([1387], -2.22),
    ([1462], -1.55),
    ([1501], -1.55),
    ([1986], -1.55),
    ([1987], -1.55),
    ([1988], -1.55),
    ([1516], -2.22),
    ([1376], -2.22),
    ([1380], -2.22),
    ([1572], -2.22),
    ([1886], -2.1),
    ([1436], -0.1),  # check streefpeil!
    ([1847], -2.35),  # check streefpeil!
    ([1583], -0.25),  # check streefpeil!
    ([1586], 1.85),  # check streefpeil!
]

# stadswater Utrecht beneden peil
node_ids = [1401, 1406, 1414, 1422, 1426, 1452, 1576, 1588, 1654, 1660, 1668, 1673, 1757, 1760, 1766, 1778]
basin_level_overrides.append((node_ids, 0.58))
apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

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
