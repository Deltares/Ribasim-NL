# %%
"""
Addition of settings for 'wateraanvoer' by means of `ContinuousControl`-nodes and 'wateraanvoergebieden'.

NOTE: This is a non-working dummy file to provide guidance on how to implement these workflows.

Author: Gijs G. Hendrickx
"""

import inspect

import geopandas as gpd
import pandas as pd

from peilbeheerst_model import ribasim_parametrization
from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model, check_basin_level
from ribasim_nl.parametrization.basin_tables import update_basin_static

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
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_lyr", "output_controle_vaw_aanvoer.qlr")
aanvoer_path = cloud.joinpath(
    AUTHORITY, "verwerkt", "1_ontvangen_data", "Na_levering_202401", "wateraanvoer", "Inlaatgebieden.shp"
)
model_edits_aanvoer_gpkg = cloud.joinpath(AUTHORITY, "verwerkt", "model_edits_aanvoer.gpkg")

cloud.synchronize(
    filepaths=[
        aanvoer_path,
    ]
)

# read model
model = Model.read(ribasim_toml)
original_model = model.model_copy(deep=True)
update_basin_static(model=model, evaporation_mm_per_day=1)
# TODO: Remember to set the forcing conditions to be representative for a drought ('aanvoer'-conditions), or for
#  changing conditions (e.g., 1/3 precipitation, 2/3 evaporation).
# set forcing conditions

model.basin.area.df.loc[model.basin.area.df.node_id == 864, "meta_streefpeil"] = -0.5
model.basin.area.df.loc[model.basin.area.df.node_id == 1271, "meta_streefpeil"] = 1.2

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
model.manning_resistance.static.df.loc[:, "manning_n"] = 0.001

mask = model.outlet.static.df["meta_aanvoer"] == 0
model.outlet.static.df.loc[mask, "max_downstream_level"] = pd.NA
model.outlet.static.df.flow_rate = original_model.outlet.static.df.flow_rate
model.pump.static.df.flow_rate = original_model.pump.static.df.flow_rate

model.outlet.static.df.loc[model.outlet.static.df.node_id == 407, "flow_rate"] = 1


# Grebbesluis: flow_rate: 2.85m3/s
model.outlet.static.df.loc[model.outlet.static.df.node_id == 288, "flow_rate"] = 2.5
model.outlet.static.df.loc[model.outlet.static.df.node_id == 378, "flow_rate"] = 5.9
model.outlet.static.df.loc[model.outlet.static.df.node_id == 580, "min_upstream_level"] = 1.55
model.basin.area.df.loc[model.basin.area.df.node_id == 786, "meta_streefpeil"] = 1.55
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 44, "level"] = 999

# Valleikanaal verkeerd geschematiseerd
model.redirect_edge(edge_id=138, to_node_id=1095)
model.redirect_edge(edge_id=137, to_node_id=1095)
model.redirect_edge(edge_id=136, to_node_id=1095)
model.redirect_edge(edge_id=24, to_node_id=1115)
model.redirect_edge(edge_id=560, to_node_id=1)
model.redirect_edge(edge_id=745, from_node_id=1120)


# fix boundary levels so we can get inflow
model.reverse_direction_at_node(271)  # sluis Dieren is not an inlet
model.reverse_direction_at_node(228)  # Inlaatgemaat diepe Gracht
model.reverse_direction_at_node(470)  # Inlaatgemaat Goorpomp
model.reverse_direction_at_node(302)  #
model.reverse_direction_at_node(479)  # Laakse Duiker


actions = gpd.list_layers(model_edits_aanvoer_gpkg).name.to_list()
for action in actions:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_aanvoer_gpkg, layer=action, fid_as_index=True)
    if "order" in df.columns:
        df.sort_values("order", inplace=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)


# set upstream level boundaries at 999 meters
# boundary_node_ids = [i for i in model.level_boundary.node.df.index if not model.upstream_node_id(i) is not None]
# model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id.isin(boundary_node_ids), "level"] = 999

# Gemaal Maatpolder
model.pump.static.df.loc[model.pump.static.df.node_id == 1283, "max_downstream_level"] = -0.9

# Gemaal De Groep: pump rate = 0.1m3/s (HKV, 2009)
model.pump.static.df.loc[model.pump.static.df.node_id == 1283, "max_downstream_level"] = -0.9

# manning_node, wrong basin (anders lek)
model.manning_resistance.static.df.loc[model.manning_resistance.static.df.node_id == 646, "active"] = False

# Duikers naast hoofdwaterloop inactive (anders lek)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 588, "active"] = False

# Schele Duiker (HKV, 2009)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 400, "flow_rate"] = 1

# Inlaatduiker Arkersluis
model.outlet.static.df.loc[model.outlet.static.df.node_id == 112, "flow_rate"] = 1
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 37, "level"] = 999

# Gemaal Meentweg toevoegen
model.pump.static.df.loc[model.pump.static.df.node_id == 1284, "flow_rate"] = 0.17
model.pump.static.df.loc[model.pump.static.df.node_id == 1284, "max_downstream_level"] = -0.69
model.pump.static.df.loc[model.pump.static.df.node_id == 1284, "min_upstream_level"] = -1.01

# Inlaatduiker
model.outlet.static.df.loc[model.outlet.static.df.node_id == 599, "max_downstream_level"] = -0.99

# Inlaat Malesuis toevoegen
model.pump.static.df.loc[model.pump.static.df.node_id == 1286, "flow_rate"] = 5
model.pump.static.df.loc[model.pump.static.df.node_id == 1286, "max_downstream_level"] = -0.44
model.pump.static.df.loc[model.pump.static.df.node_id == 1286, "min_upstream_level"] = pd.NA

# Inlaatduiker bij levelboundary
model.outlet.static.df.loc[model.outlet.static.df.node_id == 169, "min_upstream_level"] = -0.35
model.outlet.static.df.loc[model.outlet.static.df.node_id == 169, "flow_rate"] = 1
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 38, "level"] = 999

# Gemaal Oostsingel toevoegen
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "flow_rate"] = 1
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "max_downstream_level"] = -0.9
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "min_upstream_level"] = -1.01

# Duiker
model.pump.static.df.loc[model.pump.static.df.node_id == 1285, "max_downstream_level"] = -0.9

# Inactive, basin te groot. Anders lekt hoofdwaterloop
model.outlet.static.df.loc[model.outlet.static.df.node_id == 495, "active"] = False
model.outlet.static.df.loc[model.outlet.static.df.node_id == 389, "active"] = False

# Afvoergemaal Nijkerk en Hertog Reijnout no downstream_waterlevel
model.pump.static.df.loc[model.pump.static.df.node_id == 267, "max_downstream_level"] = pd.NA
model.pump.static.df.loc[model.pump.static.df.node_id == 244, "max_downstream_level"] = pd.NA

# Laakse Duiker
model.outlet.static.df.loc[model.outlet.static.df.node_id == 479, "flow_rate"] = 1
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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1288, "flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1288, "max_downstream_level"] = -0.3
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1288, "min_upstream_level"] = pd.NA

# Update nieuw duikers (voorheen Manning)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 681, "flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 681, "min_upstream_level"] = 0
model.outlet.static.df.loc[model.outlet.static.df.node_id == 701, "flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 701, "min_upstream_level"] = 1.4
model.outlet.static.df.loc[model.outlet.static.df.node_id == 747, "flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 747, "min_upstream_level"] = 1.4


model.outlet.static.df.loc[model.outlet.static.df.node_id == 339, "min_upstream_level"] = -0.81

# Havensluis Elburg
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 1106, "level"] = 999
model.reverse_direction_at_node(477)
model.outlet.static.df.loc[model.outlet.static.df.node_id == 477, "flow_rate"] = 1
model.outlet.static.df.loc[model.outlet.static.df.node_id == 477, "min_upstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 477, "max_downstream_level"] = 0.19

# some customs
model.outlet.static.df.loc[model.outlet.static.df.node_id == 478, "flow_rate"] = 1
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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 606, "flow_rate"] = 1


# merge basins
# model.merge_basins(basin_id=1044, to_basin_id=1103)
model.merge_basins(basin_id=910, to_basin_id=988)
model.merge_basins(basin_id=1078, to_basin_id=1193)
model.merge_basins(basin_id=1193, to_basin_id=1170)
model.merge_basins(basin_id=1170, to_basin_id=1123)
model.merge_basins(basin_id=1115, to_basin_id=1123)
model.merge_basins(basin_id=883, to_basin_id=989)
model.merge_basins(basin_id=1145, to_basin_id=1088)
model.merge_basins(basin_id=927, to_basin_id=1117)
model.merge_basins(basin_id=1036, to_basin_id=848)
model.merge_basins(basin_id=804, to_basin_id=928)
model.merge_basins(basin_id=928, to_basin_id=977)
model.merge_basins(basin_id=806, to_basin_id=1185)
model.merge_basins(basin_id=1177, to_basin_id=1111)
model.merge_basins(basin_id=834, to_basin_id=904)
model.merge_basins(basin_id=904, to_basin_id=862)
model.merge_basins(basin_id=1051, to_basin_id=895)
model.merge_basins(basin_id=1044, to_basin_id=1103)

# Stuw Vloeddijk
model.outlet.static.df.loc[model.outlet.static.df.node_id == 556, "min_upstream_level"] = 2.99
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1289, "flow_rate"] = 1
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
model.outlet.static.df.loc[model.outlet.static.df.node_id == 470, "flow_rate"] = 0.1

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

# Inlaat Nijoever
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1295, "max_downstream_level"] = 0.8
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1295, "flow_rate"] = 1

# Inlaat Nijoever
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1296, "max_downstream_level"] = 0.8
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1296, "flow_rate"] = 1

# Stuw Hierdense beek
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1297, "min_upstream_level"] = 6.55
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1297, "flow_rate"] = 1

# Inlaat Mr Baron van der Feltz
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1287, "flow_rate"] = 5
model.level_boundary.static.df.loc[model.level_boundary.static.df.node_id == 58, "level"] = 999
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1287, "max_downstream_level"] = 3.01
model.pump.static.df.loc[model.pump.static.df.node_id == 240, "max_downstream_level"] = pd.NA
model.outlet.static.df.loc[model.outlet.static.df.node_id == 1287, "min_upstream_level"] = 3

# remove node (wrong connection)
model.remove_node(478, remove_edges=True)
model.remove_node(672, remove_edges=True)
model.remove_node(671, remove_edges=True)
model.remove_node(16, remove_edges=True)
model.remove_node(17, remove_edges=True)
model.remove_node(18, remove_edges=True)
model.remove_node(738, remove_edges=True)
model.remove_node(443, remove_edges=True)
model.remove_node(595, remove_edges=True)
model.remove_node(199, remove_edges=True)
model.remove_node(585, remove_edges=True)
model.remove_node(631, remove_edges=True)
model.remove_node(611, remove_edges=True)
model.remove_node(710, remove_edges=True)
model.remove_node(292, remove_edges=True)
model.remove_node(125, remove_edges=True)
model.remove_node(696, remove_edges=True)
model.remove_node(706, remove_edges=True)
model.remove_node(737, remove_edges=True)
model.remove_node(182, remove_edges=True)
model.remove_node(185, remove_edges=True)  # Duiker vervangen door pomp Oostsingel
model.remove_node(581, remove_edges=True)  # Stuw Asschat, zit er 2 keer in
model.remove_node(496, remove_edges=True)  # stuw de Groep, zit er 2 keer in
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
