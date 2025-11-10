# %%
import time

import pandas as pd
from peilbeheerst_model.controle_output import Control

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "ValleienVeluwe"
short_name = "venv"
run_model = True

parameters_dir = static_data_xlsx = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens\\QGIS_lyr\\output_controle_vaw_afvoer.qlr")

# # you need the excel, but the model should be local-only by running 01_fix_model.py
# cloud.synchronize(filpaths=[static_data_xlsx])
# cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)

# %%

# read
model = Model.read(ribasim_toml)
start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")

model.basin.profile.df.loc[(model.basin.profile.df.node_id == 1209) & (model.basin.profile.df.area > 0.1), "area"] = (
    10000
)

node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

# Fixes aanvoer
model.outlet.static.df.loc[model.outlet.static.df.node_id == 407, "max_flow_rate"] = 1

# Grebbesluis: flow_rate: 2.85m3/s
model.outlet.static.df.loc[model.outlet.static.df.node_id == 288, "max_flow_rate"] = 2.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 378, "max_flow_rate"] = 5.9
model.outlet.static.df.loc[model.outlet.static.df.node_id == 580, "min_upstream_level"] = 1.55
model.basin.area.df.loc[model.basin.area.df.node_id == 786, "meta_streefpeil"] = 1.55
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 44, "level"] = 999

# set upstream level boundaries at 999 meters
# boundary_node_ids = [i for i in model.level_boundary.node.df.index if not model.upstream_node_id(i) is not None]
# model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id.isin(boundary_node_ids), "level"] = 999

# Gemaal Maatpolder
model.pump.static.df.loc[model.pump.static.df.node_id == 1283, "max_downstream_level"] = -0.9

# manning_node, wrong basin (anders lek)
model.manning_resistance.static.df.loc[model.manning_resistance.static.df.node_id == 646, "active"] = False

# Duikers naast hoofdwaterloop inactive (anders lek)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 588, "active"] = False

# Schele Duiker (HKV, 2009)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 400, "max_flow_rate"] = 1

# Inlaatduiker Arkersluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 112, "max_flow_rate"] = 1
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 37, "level"] = 999

model.pump.static.df.loc[model.pump.static.df.node_id == 1282, "max_flow_rate"] = 1

# Gemaal Malesluis toevoegen
model.pump.static.df.loc[model.pump.static.df.node_id == 1284, "max_flow_rate"] = 0.17
model.pump.static.df.loc[model.pump.static.df.node_id == 1284, "max_downstream_level"] = -0.69
model.pump.static.df.loc[model.pump.static.df.node_id == 1284, "min_upstream_level"] = -1.01

# Inlaatduiker
model.outlet.static.df.loc[model.outlet.static.df.node_id == 599, "max_downstream_level"] = -0.99

# Inlaat Malesuis toevoegen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1286, "flow_rate"] = 5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1286, "max_downstream_level"] = -0.44
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1286, "min_upstream_level"] = pd.NA

# Inlaatduiker bij levelboundary
model.outlet.static.df.loc[model.outlet.static.df.node_id == 169, "min_upstream_level"] = -0.35
model.outlet.static.df.loc[model.outlet.static.df.node_id == 169, "max_flow_rate"] = 1
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 38, "level"] = 999

# Inlaat Mr Baron van der Feltz
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "max_downstream_level"] = -0.9
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "min_upstream_level"] = -1.01

# Inactive, basin niet ok, lek
model.outlet.static.df.loc[model.outlet.static.df.node_id == 495, "active"] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 389, "active"] = False

# Afvoergemaal Nijkerk en Hertog Reijnout no downstream_waterlevel
model.pump.static.df.loc[model.pump.static.df.node_id == 267, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 244, "max_downstream_level"] = pd.NA

# Laakse Duiker
model.outlet.static.df.loc[model.outlet.static.df.node_id == 479, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 479, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 479, "max_downstream_level"] = -0.2
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 39, "level"] = 999

# Kooisluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 272, "min_upstream_level"] = -0.21
model.outlet.static.df.loc[model.outlet.static.df.node_id == 272, "max_downstream_level"] = -0.99

# Inlaatstuw De Laak
model.outlet.static.df.loc[model.outlet.static.df.node_id == 300, "min_upstream_level"] = -0.21
model.outlet.static.df.loc[model.outlet.static.df.node_id == 300, "max_downstream_level"] = -0.69

# Kleine Melm
model.reverse_direction_at_node(91)  # Kleine Melm
model.outlet.static.df.loc[model.outlet.static.df.node_id == 91, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 91, "max_downstream_level"] = -0.59

# Kantelstuw Haarbrug
model.outlet.static.df.loc[model.outlet.static.df.node_id == 563, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 563, "max_downstream_level"] = -0.99

# Gemaal de Wenden
model.pump.static.df.loc[model.pump.static.df.node_id == 468, "max_downstream_level"] = pd.NA

# Inlaat de Wenden
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 1, "level"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1286, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1286, "max_downstream_level"] = -0.3
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1286, "min_upstream_level"] = pd.NA

# Update nieuw duikers (voorheen Manning)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 681, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 681, "min_upstream_level"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 701, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 701, "min_upstream_level"] = 1.4
model.outlet.static.df.loc[model.outlet.static.df.node_id == 747, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 747, "min_upstream_level"] = 1.4
model.outlet.static.df.loc[model.outlet.static.df.node_id == 339, "min_upstream_level"] = -0.81

# Havensluis Elburg
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 1106, "level"] = 999
model.reverse_direction_at_node(477)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 477, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 477, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 477, "max_downstream_level"] = 0.19

# some customs
model.outlet.static.df.loc[model.outlet.static.df.node_id == 478, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 478, "min_upstream_level"] = -0.05
model.outlet.static.df.loc[model.outlet.static.df.node_id == 279, "min_upstream_level"] = -0.51
model.outlet.static.df.loc[model.outlet.static.df.node_id == 319, "min_upstream_level"] = -0.51
model.outlet.static.df.loc[model.outlet.static.df.node_id == 121, "min_upstream_level"] = -0.51
model.outlet.static.df.loc[model.outlet.static.df.node_id == 279, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 319, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 121, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 418, "max_downstream_level"] = pd.NA

# Inlaat Eektermerksluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 606, "meta_code"] = "KSL-8"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 606, "meta_name"] = "Eektermerksluis"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 606, "max_downstream_level"] = -0.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 606, "min_upstream_level"] = pd.NA
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 2, "level"] = -0.05
model.outlet.static.df.loc[model.outlet.static.df.node_id == 606, "max_flow_rate"] = 1

# Stuw Vlieterweg
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 1298, "min_upstream_level"] = 3.45
# Stuw KST-4284
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1299, "min_upstream_level"] = 12.68

# Stuw Vlieterweg
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1300, "min_upstream_level"] = 11.65
# Stuw KST-4284
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1301, "min_upstream_level"] = 11.3

# Stuw Vloeddijk
model.outlet.static.df.loc[model.outlet.static.df.node_id == 556, "min_upstream_level"] = 2.99
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1287, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1287, "min_upstream_level"] = 2.99
model.outlet.static.df.loc[model.outlet.static.df.node_id == 446, "min_upstream_level"] = 2.99

# Aanvoergemaal Emsterbroek
model.pump.static.df.loc[model.pump.static.df.node_id == 1290, "max_downstream_level"] = 1.81

# Aanvoergemaal Emsterbroek
model.pump.static.df.loc[model.pump.static.df.node_id == 1291, "max_downstream_level"] = 1.76

# Aanvoergemaal Nijbroek
model.pump.static.df.loc[model.pump.static.df.node_id == 1292, "max_downstream_level"] = 2.01

# Aanvoergemaal Hoenwaard
model.pump.static.df.loc[model.pump.static.df.node_id == 1293, "max_downstream_level"] = 0.43

# Aanvoergemaal Antlia
model.pump.static.df.loc[model.pump.static.df.node_id == 1294, "max_downstream_level"] = 0.16

# Gemaal Kleine Gat
model.pump.static.df.loc[model.pump.static.df.node_id == 232, "max_downstream_level"] = -1

# Gemaal Veluwe verkeerd min_upstream_level
model.pump.static.df.loc[model.pump.static.df.node_id == 257, "min_upstream_level"] = 1.2
model.pump.static.df.loc[model.pump.static.df.node_id == 257, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 307, "min_upstream_level"] = 1.2

# Afvoergemaal Luttenbroek
model.pump.static.df.loc[model.pump.static.df.node_id == 265, "max_downstream_level"] = pd.NA
# Afvoergemaal Nijoever
model.pump.static.df.loc[model.pump.static.df.node_id == 252, "max_downstream_level"] = pd.NA
# Duiker
model.outlet.static.df.loc[model.outlet.static.df.node_id == 457, "max_downstream_level"] = pd.NA
# Stuw Assendorpbroek
model.outlet.static.df.loc[model.outlet.static.df.node_id == 498, "max_downstream_level"] = pd.NA
# Stuw Assendorp
model.outlet.static.df.loc[model.outlet.static.df.node_id == 355, "max_downstream_level"] = pd.NA
# Stuw Assendorp
model.outlet.static.df.loc[model.outlet.static.df.node_id == 556, "max_downstream_level"] = pd.NA

# Duiker
model.outlet.static.df.loc[model.outlet.static.df.node_id == 137, "max_downstream_level"] = -1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 67, "max_downstream_level"] = -1

# Grote Melm
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1295, "max_downstream_level"] = pd.NA

model.pump.static.df.loc[model.pump.static.df.node_id == 240, "max_downstream_level"] = pd.NA

# Fixes afvoer:
model.outlet.static.df.loc[model.outlet.static.df.node_id == 312, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 369, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 446, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 576, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 555, "min_upstream_level"] = 1.7
model.outlet.static.df.loc[model.outlet.static.df.node_id == 74, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 289, "min_upstream_level"] = 12
model.outlet.static.df.loc[model.outlet.static.df.node_id == 169, "min_upstream_level"] = -0.75
model.pump.static.df.loc[model.pump.static.df.node_id == 512, "max_flow_rate"] = 0.5
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.001

# %%
# Write model
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
