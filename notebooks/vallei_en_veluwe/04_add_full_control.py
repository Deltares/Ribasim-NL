# %%

import pandas as pd
from peilbeheerst_model.controle_output import Control
from ribasim_nl.from_to_nodes_and_levels import add_from_to_nodes_and_levels
from ribasim_nl.parametrization.basin_tables import update_basin_static

from peilbeheerst_model import ribasim_parametrization
from ribasim_nl import CloudStorage, Model, check_basin_level

# execute model run
MODEL_EXEC: bool = False

# model settings
AUTHORITY: str = "ValleienVeluwe"
SHORT_NAME: str = "venv"
MODEL_ID: str = "2025_5_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect relevant data from the GoodCloud
ribasim_model_dir = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_parameterized_model")
ribasim_toml = ribasim_model_dir / f"{SHORT_NAME}.toml"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr/output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(AUTHORITY, "verwerkt/1_ontvangen_data/Na_levering_202401/wateraanvoer/Inlaatgebieden.shp")


cloud.synchronize(
    filepaths=[
        aanvoer_path,
    ]
)

# read model
model = Model.read(ribasim_toml)
original_model = model.model_copy(deep=True)
update_basin_static(model=model, evaporation_mm_per_day=1)
# update_basin_static(model=model, precipitation_mm_per_day=10)
# TODO: Remember to set the forcing conditions to be representative for a drought ('aanvoer'-conditions), or for
#  changing conditions (e.g., 1/3 precipitation, 2/3 evaporation).
# set forcing conditions

model.basin.area.df.loc[model.basin.area.df.node_id == 864, "meta_streefpeil"] = -0.5
model.basin.area.df.loc[model.basin.area.df.node_id == 1271, "meta_streefpeil"] = 1.2

# alle niet-gecontrolleerde basins krijgen een meta_streefpeil uit de final state van de parameterize_model.py
update_levels = model.basin_outstate.df.set_index("node_id")["level"]
basin_ids = model.basin.node.df[model.basin.node.df["meta_gestuwd"] == "False"].index
mask = model.basin.area.df["node_id"].isin(basin_ids)
model.basin.area.df.loc[mask, "meta_streefpeil"] = model.basin.area.df[mask]["node_id"].apply(
    lambda x: update_levels[x]
)
add_from_to_nodes_and_levels(model)

# re-parameterize
ribasim_parametrization.set_aanvoer_flags(model, str(aanvoer_path), overruling_enabled=False)
ribasim_parametrization.determine_min_upstream_max_downstream_levels(model, AUTHORITY)
check_basin_level.add_check_basin_level(model=model)

# TODO: The addition of `ContinuousControl`-nodes is subsequently a minor modification:
"""To allow the addition of `ContinuousControl`-nodes, the branch 'continuous_control' must be merged first to access
the required function: `ribasim_parametrization.add_continuous_control(<model>)`. The expansion of adding the continuous
control requires a proper working schematisation of both 'afvoer'- and 'aanvoer'-situations, and so these should be
fixed and up-and-running beforehand.
"""
# ribasim_parametrization.add_continuous_control(model)

"""For the addition of `ContinuousControl`-nodes, it might be necessary to set `model.basin.static.df=None`, as the
`ContinuousControl`-nodes require `Time`-tables instead of `Static`-tables. If both are defined (for the same node,
Ribasim will raise an error and thus not execute.
"""
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.04
mask = model.outlet.static.df["meta_aanvoer"] == 0
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA

model.outlet.static.df.flow_rate = original_model.outlet.static.df.flow_rate
model.pump.static.df.flow_rate = original_model.pump.static.df.flow_rate
model.outlet.static.df.max_flow_rate = original_model.outlet.static.df.max_flow_rate
model.pump.static.df.max_flow_rate = original_model.pump.static.df.max_flow_rate

# %% sturing uit alle niet-gestuwde outlets halen
node_ids = model.outlet.node.df[model.outlet.node.df["meta_gestuwd"] == "False"].index
non_control_mask = model.outlet.static.df["node_id"].isin(node_ids)
model.outlet.static.df.loc[non_control_mask, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[non_control_mask, "max_downstream_level"] = pd.NA
# %%
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

# Gemaal De Groep: pump rate = 0.1m3/s (HKV, 2009)
# model.pump.static.df.loc[model.pump.static.df.node_id == 1283, "max_downstream_level"] = -0.9

# manning_node, wrong basin (anders lek)
model.manning_resistance.static.df.loc[model.manning_resistance.static.df.node_id == 646, "manning_n"] = 100.0

# Duikers naast hoofdwaterloop inactive (anders lek)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 588, "flow_rate"] = 0.0

# Schele Duiker (HKV, 2009)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 400, "max_flow_rate"] = 1

# Inlaatduiker Arkersluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 112, "max_flow_rate"] = 1
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 37, "level"] = 999

# Gemaal Meentweg toevoegen
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

# Gemaal Oostsingel toevoegen
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "max_flow_rate"] = 1
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "max_downstream_level"] = -0.9
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "min_upstream_level"] = -1.01

# Duiker
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "max_downstream_level"] = -0.9

# Inactive, basin niet ok, lek
model.outlet.static.df.loc[model.outlet.static.df.node_id == 495, "flow_rate"] = 0.0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 389, "flow_rate"] = 0.0

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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1288, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1288, "max_downstream_level"] = -0.3
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1288, "min_upstream_level"] = pd.NA

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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1289, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1289, "min_upstream_level"] = 2.99
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
model.pump.static.df.loc[model.pump.static.df.node_id == 232, "min_upstream_level"] = -0.11

# Inlaat Goorpomp
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 4, "level"] = 999
model.outlet.static.df.loc[model.outlet.static.df.node_id == 470, "max_flow_rate"] = 0.1

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

# Grote Melm
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1295, "max_downstream_level"] = pd.NA

# Stuw Hierdense beek
# model.outlet.static.df.loc[model.outlet.static.df.node_id == 1297, "min_upstream_level"] = 6.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1297, "max_flow_rate"] = 1

# Inlaat Mr Baron van der Feltz
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1287, "max_flow_rate"] = 5
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 58, "level"] = 999
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1287, "max_downstream_level"] = 3.01
model.pump.static.df.loc[model.pump.static.df.node_id == 240, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1287, "min_upstream_level"] = 3

# Fixes afvoer:
model.outlet.static.df.loc[model.outlet.static.df.node_id == 312, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 369, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 446, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 576, "min_upstream_level"] = 2.85
model.outlet.static.df.loc[model.outlet.static.df.node_id == 555, "min_upstream_level"] = 1.7
model.outlet.static.df.loc[model.outlet.static.df.node_id == 74, "max_flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 289, "min_upstream_level"] = 12
model.outlet.static.df.loc[model.outlet.static.df.node_id == 169, "min_upstream_level"] = -0.75

# %%
# Hoofdinlaten krijgen 10m3/s
model.outlet.static.df.loc[
    model.outlet.static.df.node_id.isin(model.upstream_connection_node_ids(node_type="Outlet")), "flow_rate"
] = 10
model.pump.static.df.loc[
    model.pump.static.df.node_id.isin(model.upstream_connection_node_ids(node_type="Pump")), "flow_rate"
] = 10
model.outlet.static.df.loc[
    model.outlet.static.df.node_id.isin(model.upstream_connection_node_ids(node_type="Outlet")), "max_flow_rate"
] = 10
model.pump.static.df.loc[
    model.pump.static.df.node_id.isin(model.upstream_connection_node_ids(node_type="Pump")), "max_flow_rate"
] = 10

# %%
# write model
ribasim_toml = cloud.joinpath(AUTHORITY, "modellen", f"{AUTHORITY}_full_control_model", f"{SHORT_NAME}.toml")

model.pump.static.df["meta_func_afvoer"] = 1
model.pump.static.df["meta_func_aanvoer"] = 0
model.write(ribasim_toml)

# run model
if MODEL_EXEC:
    # TODO: Different ways of executing the model; choose the one that suits you best:
    ribasim_parametrization.tqdm_subprocess(["ribasim", ribasim_toml], print_other=False, suffix="init")
    # exit_code = model.run()

    # assert exit_code == 0

    """Note that currently, the Ribasim-model is unstable but it does execute, i.e., the model re-parametrisation is
    successful. This might be due to forcing the schematisation with precipitation while setting the 'sturing' of the
    outlets on 'aanvoer' instead of the more suitable 'afvoer'. This should no longer be a problem once the next step of
    adding `ContinuousControl`-nodes is implemented.
    """

    controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    indicators = controle_output.run_all()
