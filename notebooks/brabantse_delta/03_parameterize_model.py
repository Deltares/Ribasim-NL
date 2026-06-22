# %%
import time

import geopandas as gpd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.check_basin_level import add_check_basin_level
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    sync_min_upstream_levels_with_profile_bottoms,
)
from ribasim_nl.parametrization.manning_level import sync_parameterized_manning_basin_levels

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "BrabantseDelta"
short_name = "wbd"

run_model = False

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
aanvoergebieden_gpkg = cloud.joinpath(authority, "verwerkt", "sturing", "aanvoergebieden.gpkg")
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx, qlr_path, aanvoergebieden_gpkg])

# %%

# read
model = Model.read(ribasim_toml)
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")
start_time = time.time()

# %% fixes basins and profiles

basin_level_overrides = [
    ([1354], 4.1),
    ([1431], 10),
    (
        [
            1407,
            1428,
            1491,
            1496,
            1497,
            1557,
            1579,
            1602,
            1604,
            1608,
            1613,
            1656,
            1668,
            1688,
            1694,
            1767,
            1770,
            1774,
            1786,
            1791,
            1808,
            1820,
            1821,
            1829,
            1830,
            1837,
            1870,
            1889,
            1899,
            1909,
            1919,
            1933,
            1938,
            1973,
            1975,
            1976,
            1978,
            1996,
            1998,
            2003,
            2013,
            2060,
            2061,
            2072,
            2091,
            2103,
            2107,
            2145,
            2217,
            2240,
            2241,
            2242,
            2243,
            2244,
            2245,
            2246,
            2248,
            2270,
            2289,
            2291,
            2309,
        ],
        0.0,
    ),
]

model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")

protected_basin_node_ids = apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

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
    protected_basin_node_ids=protected_basin_node_ids,
)

model.update_node(node_id=1085, node_type="outlet")
model.update_node(node_id=668, node_type="pump")
sync_min_upstream_levels_with_profile_bottoms(model=model)
# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
add_check_basin_level(model=model)
model.basin.area.df.loc[:, "meta_area"] = model.basin.area.df.area
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
