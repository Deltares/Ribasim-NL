# %%
import time

import geopandas as gpd
import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.check_basin_level import add_check_basin_level
from ribasim_nl.parametrization.basin_tables import (
    apply_basin_level_overrides,
    sync_min_upstream_levels_with_profile_bottoms,
)
from ribasim_nl.parametrization.level_boundary_table import update_level_boundary_static
from ribasim_nl.parametrization.manning_level import sync_parameterized_manning_basin_levels

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "Vechtstromen"
short_name = "vechtstromen"
run_model = False
run_period = None
static_data_xlsx = cloud.joinpath(authority, "verwerkt/parameters/static_data.xlsx")
aanvoergebieden_gpkg = cloud.joinpath(authority, "verwerkt", "sturing", "aanvoergebieden.gpkg")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
parameterized_model_dir = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

cloud.synchronize(filepaths=[static_data_xlsx, qlr_path, aanvoergebieden_gpkg])

# %%
# read
model = Model.read(ribasim_toml)

start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=1)
print("Elapsed Time:", time.time() - start_time, "seconds")
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.03
aanvoergebieden_df = gpd.read_file(aanvoergebieden_gpkg, fid_as_index=True).dissolve(by="aanvoergebied")


# %%
model.outlet.static.df.loc[model.outlet.static.df.node_id == 2019, ["meta_categorie"]] = "Inlaat"

basin_level_overrides = [
    ([2157, 1561, 1660], 10),
    ([1493], 10.1),
    ([1643], 6.8),
    ([2192], 5.7),
    ([1844], 10.22),
    ([2030, 1433, 2085], 5.75),
    ([1768, 1681], 10),
    ([1605], 17.7),
    ([1864, 1659], 10.5),
    ([1160, 1723, 1554, 1623], 7.35),
    ([2238, 1388, 1428, 1421, 1479, 1670, 1834, 1839, 2156], 4.39),
    ([1635, 1544, 1634, 1823, 1830, 2147], 8.35),
    ([2150, 1461, 1534, 1540, 1574], 7.35),
    ([1393, 1513], 2.65),
    ([1744, 2153], 3.73),
    ([1405, 1730, 2178, 2003, 2163, 2222, 1700, 1633, 1881, 1843], 9.1),
    ([1852, 1448, 1847, 2308], 7.1),
    ([1637, 1495], 5.60),
    ([1593], 4.50),
    ([2158, 2180], 2.65),
    ([1862], 11.2),
    ([1879, 1878, 1873, 2061, 1621, 1644], 12.95),
    ([1856, 1518, 1442, 1528], 17.7),
    ([2340], 9.7),
    ([1600], 17.7),
]
protected_basin_node_ids = apply_basin_level_overrides(model=model, basin_level_overrides=basin_level_overrides)

sync_parameterized_manning_basin_levels(
    model=model,
    aanvoergebieden_df=aanvoergebieden_df,
    output_gpkg=parameterized_model_dir / "manning_level_basin_updates.gpkg",
    excluded_manning_node_ids=[1151],
    protected_basin_node_ids=protected_basin_node_ids,
)

update_level_boundary_static(
    model=model,
    static_data_xlsx=static_data_xlsx,
    code_column="meta_code_waterbeheerder",
)

# Outlet 704 ligt benedenstrooms van basin 1545; gebruik het streefpeil van dat basin als drempelpeil.
model.outlet.static.df.loc[model.outlet.static.df.node_id == 704, "min_upstream_level"] = 10.5


# %%
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA
sync_min_upstream_levels_with_profile_bottoms(model=model)

# %%
# Write model
ribasim_toml = parameterized_model_dir / f"{short_name}.toml"
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
