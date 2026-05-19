# %%
import time

from peilbeheerst_model.controle_output import Control

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "ValleienVeluwe"
short_name = "venv"
run_model = True

parameters_dir = cloud.joinpath(authority, "verwerkt/parameters")
static_data_xlsx = parameters_dir / "static_data.xlsx"
profiles_gpkg = parameters_dir / "profiles.gpkg"
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_afvoer.qlr")

# you need the excel, but the model should be local-only by running 01_fix_model.py
cloud.synchronize(filepaths=[static_data_xlsx, qlr_path])

# %%

# read
model = Model.read(ribasim_toml)
start_time = time.time()
# %%
# parameterize
series = model.basin.node.df["meta_categorie"]
uncategorized_basins = series[series.isna()].index.values
if len(uncategorized_basins) > 0:
    print(f"uncategorized basins: {uncategorized_basins}, will be set to doorgaand")
    # pyrefly: ignore[missing-attribute]
    model.node.df.loc[uncategorized_basins, "meta_categorie"] = "doorgaand"

model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=5, profiles_gpkg=profiles_gpkg)
print("Elapsed Time:", time.time() - start_time, "seconds")

model.basin.area.df.loc[model.basin.area.df.node_id == 864, "meta_streefpeil"] = -0.5
model.basin.area.df.loc[model.basin.area.df.node_id == 1271, "meta_streefpeil"] = 1.2
model.basin.area.df.loc[model.basin.area.df.node_id == 786, "meta_streefpeil"] = 1.55

model.basin.profile.df.loc[(model.basin.profile.df.node_id == 1209) & (model.basin.profile.df.area > 0.1), "area"] = (
    10000
)

model.manning_resistance.static.df.loc[:, "manning_n"] = 0.04

# Havensluis Elburg
model.reverse_direction_at_node(477)

# Inlaat Eektermerksluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 606, "meta_code"] = "KSL-8"
model.outlet.static.df.loc[model.outlet.static.df.node_id == 606, "meta_name"] = "Eektermerksluis"

# Inlaat gemaal de Haar
model.outlet.static.df.loc[model.outlet.static.df.node_id == 232, "meta_name"] = "Aanvoergemaal de Haar"

# Fixes aanvoer
# Grebbesluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 580, "min_upstream_level"] = 1.55

# Gemaal Maatpolder
model.pump.static.df.loc[model.pump.static.df.node_id == 1283, "max_downstream_level"] = -0.9

# manning_node, wrong basin (anders lek)
model.manning_resistance.static.df.loc[model.manning_resistance.static.df.node_id == 646, "manning_n"] = 100.0

# Gemaal Malesluis toevoegen
model.pump.static.df.loc[model.pump.static.df.node_id == 1284, "max_downstream_level"] = -0.69
model.pump.static.df.loc[model.pump.static.df.node_id == 1284, "min_upstream_level"] = -1.01

# Inlaatduiker
model.outlet.static.df.loc[model.outlet.static.df.node_id == 599, "max_downstream_level"] = -0.99

# Inlaat Malesuis toevoegen
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1286, "flow_rate"] = 5.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1286, "max_downstream_level"] = -0.44

# Inlaatduiker bij levelboundary
model.outlet.static.df.loc[model.outlet.static.df.node_id == 169, "min_upstream_level"] = -0.35

# Inlaat Mr Baron van der Feltz
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "max_downstream_level"] = -0.9
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "min_upstream_level"] = -1.01

# Inactive, basin niet ok, lek
model.outlet.static.df.loc[model.outlet.static.df.node_id == 495, "flow_rate"] = 0.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 389, "flow_rate"] = 0.0

# Laakse Duiker
model.outlet.static.df.loc[model.outlet.static.df.node_id == 479, "max_downstream_level"] = -0.2

# Kooisluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 272, "min_upstream_level"] = -0.21
model.outlet.static.df.loc[model.outlet.static.df.node_id == 272, "max_downstream_level"] = -0.99

# Inlaatstuw De Laak
model.outlet.static.df.loc[model.outlet.static.df.node_id == 300, "min_upstream_level"] = -0.21
model.outlet.static.df.loc[model.outlet.static.df.node_id == 300, "max_downstream_level"] = -0.69

# Kleine Melm
model.reverse_direction_at_node(91)  # Kleine Melm
model.outlet.static.df.loc[model.outlet.static.df.node_id == 91, "max_downstream_level"] = -0.59

# Kantelstuw Haarbrug
model.outlet.static.df.loc[model.outlet.static.df.node_id == 563, "max_downstream_level"] = -0.99

# Inlaat de Wenden
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 1, "level"] = 0.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1286, "max_downstream_level"] = -0.3

# Update nieuw duikers (voorheen Manning)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 681, "min_upstream_level"] = 0.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 701, "min_upstream_level"] = 1.4
model.outlet.static.df.loc[model.outlet.static.df.node_id == 747, "min_upstream_level"] = 1.4
model.outlet.static.df.loc[model.outlet.static.df.node_id == 339, "min_upstream_level"] = -0.81

# Havensluis Elburg
model.reverse_direction_at_node(477)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 477, "max_downstream_level"] = 0.19

# some customs
model.outlet.static.df.loc[model.outlet.static.df.node_id == 478, "min_upstream_level"] = -0.05
model.outlet.static.df.loc[model.outlet.static.df.node_id == 279, "min_upstream_level"] = -0.51
model.outlet.static.df.loc[model.outlet.static.df.node_id == 319, "min_upstream_level"] = -0.51
model.outlet.static.df.loc[model.outlet.static.df.node_id == 121, "min_upstream_level"] = -0.51

# Inlaat Eektermerksluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 606, "max_downstream_level"] = -0.5
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 2, "level"] = -0.05

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
model.pump.static.df.loc[model.pump.static.df.node_id == 232, "max_downstream_level"] = -1.0

# Gemaal Veluwe verkeerd min_upstream_level
model.pump.static.df.loc[model.pump.static.df.node_id == 257, "min_upstream_level"] = 1.2
model.outlet.static.df.loc[model.outlet.static.df.node_id == 307, "min_upstream_level"] = 1.2

# Duiker
model.outlet.static.df.loc[model.outlet.static.df.node_id == 137, "max_downstream_level"] = -1.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 67, "max_downstream_level"] = -1.0

# Fixes afvoer:
model.outlet.static.df.loc[model.outlet.static.df.node_id == 312, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 369, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 446, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 576, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 555, "min_upstream_level"] = 1.7
model.outlet.static.df.loc[model.outlet.static.df.node_id == 289, "min_upstream_level"] = 12.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 169, "min_upstream_level"] = -0.75

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
