# %%
import time
from pathlib import Path

import geopandas as gpd

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model
from ribasim_nl.check_basin_level import add_check_basin_level

cloud = CloudStorage()
authority = "ValleienVeluwe"
short_name = "venv"
run_model = False

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt", "parameters")
static_data_xlsx = parameters_dir / "static_data_template.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
qlr_path = cloud.joinpath("Basisgegevens\\QGIS_lyr\\output_controle_vaw_afvoer.qlr")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"


# # you need the excel, but the model should be local-only by running 01_fix_model.py
# cloud.synchronize(filepaths=[static_data_xlsx])
# cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)


# %%

# read
model = Model.read(ribasim_toml)

start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=10, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")

model.basin.profile.df.loc[(model.basin.profile.df.node_id == 1209) & (model.basin.profile.df.area > 0.1), "area"] = (
    10000
)
model.merge_basins(basin_id=1187, to_node_id=1028)
model.merge_basins(basin_id=759, to_node_id=990)
model.merge_basins(basin_id=992, to_node_id=990)
model.merge_basins(basin_id=1024, to_node_id=990)
model.merge_basins(basin_id=1064, to_node_id=990)
model.merge_basins(basin_id=826, to_node_id=1140)
model.merge_basins(basin_id=789, to_node_id=1137)
model.merge_basins(basin_id=1077, to_node_id=1137)
model.merge_basins(basin_id=938, to_node_id=1137)
model.merge_basins(basin_id=1147, to_node_id=1103)
model.merge_basins(basin_id=828, to_node_id=1103)
model.merge_basins(basin_id=1142, to_node_id=1066)
model.merge_basins(basin_id=900, to_node_id=959)
model.merge_basins(basin_id=1099, to_node_id=811)
model.merge_basins(basin_id=1033, to_node_id=811)

model.remove_node(740, remove_edges=True)
model.remove_node(736, remove_edges=True)
model.pump.static.df.loc[model.pump.static.df.node_id == 1282, ["active"]] = False
model.pump.static.df.loc[model.pump.static.df.node_id == 1282, ["meta_categorie"]] = "Inlaat"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 634, ["active"]] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 443, ["active"]] = False
model.pump.static.df.loc[model.pump.static.df.node_id == 1279, ["active"]] = False
model.pump.static.df.loc[model.pump.static.df.node_id == 252, ["active"]] = False
model.pump.static.df.loc[model.pump.static.df.node_id == 418, ["active"]] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 306, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 312, "min_upstream_level"] = 2.85
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 369, "min_upstream_level"] = 2.85
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 576, "min_upstream_level"] = 2.85
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 555, "min_upstream_level"] = 1.7
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 542, "min_upstream_level"] = 3.9
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 74, "flow_rate"] = 1
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 289, "min_upstream_level"] = 12
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 169, "min_upstream_level"] = -0.75
# model.pump.static.df.loc[model.pump.static.df.node_id == 266, "min_upstream_level"] = 4.2
# model.basin.area.df.loc[model.basin.area.df.node_id == 1006, "meta_streefpeil"] = 3.9
# model.basin.area.df.loc[model.basin.area.df.node_id == 1028, "meta_streefpeil"] = 4.2

model.manning_resistance.static.df.loc[:, "manning_n"] = 0.005
# %%

# Write model
add_check_basin_level(model=model)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# run model
if run_model:
    exit_code = model.run()
    assert exit_code == 0

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_afvoer()

# %%
# code voor neeltje


peilgebieden_path = Path(
    r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\ValleienVeluwe\verwerkt\1_ontvangen_data\20250428\Peilvakken.shp"
)
node = model.outlet[78]
peilgebieden_df = gpd.read_file(peilgebieden_path)
tolerance = 10  # afstand voor zoeken bovenstrooms
node_id = node.node_id
node_geometry = node.geometry


# haal bovenstroomse en bendenstroomse links op
line_to_node = model.link.df.set_index("to_node_id").at[node_id, "geometry"]
line_from_node = model.link.df.set_index("from_node_id").at[node_id, "geometry"]

# bepaal een punt 10 meter bovenstrooms node
containing_point = line_to_node.interpolate(line_to_node.length - tolerance)

# filter peilgebieden met intersect bovenstroomse link
peilgebieden_select_df = peilgebieden_df[peilgebieden_df.contains(containing_point)]

# als meerdere gevonden: filter verder met niet-intersect benedenstoomse link
if peilgebieden_select_df.empty:
    raise ValueError(f"No peilgebied found within {tolerance}m upstream of {node_id}")

peilgebied = peilgebieden_select_df.iloc[0]

# VALIDEREN!!!
level = peilgebied["WS_MAX_PEI"]

model.outlet.static.df.loc[model.outlet.static.df.node_id == node_id, "min_upstream_level"] = level

# %%
