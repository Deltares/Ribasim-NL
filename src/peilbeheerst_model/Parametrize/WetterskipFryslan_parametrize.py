"""Parameterisation of water board: Fryslan."""

import datetime
import os
import warnings

import ribasim
import ribasim.nodes
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import Point

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model import supply
from peilbeheerst_model.add_storage_basins import AddStorageBasins
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim_nl import CloudStorage

AANVOER_CONDITIONS: bool = True

# model settings
waterschap = "WetterskipFryslan"
base_model_versie = "2024_12_8"

# connect with the GoodCloud
cloud = CloudStorage()

# collect data from the base model, feedback form, waterauthority & RWS border
ribasim_base_model_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_boezemmodel_{base_model_versie}")
FeedbackFormulier_path = cloud.joinpath(
    waterschap, "verwerkt", "Feedback Formulier", f"feedback_formulier_{waterschap}.xlsx"
)
FeedbackFormulier_LOG_path = cloud.joinpath(
    waterschap, "verwerkt", "Feedback Formulier", f"feedback_formulier_{waterschap}_LOG.xlsx"
)
ws_grenzen_path = cloud.joinpath("Basisgegevens", "RWS_waterschaps_grenzen", "waterschap.gpkg")
RWS_grenzen_path = cloud.joinpath("Basisgegevens", "RWS_waterschaps_grenzen", "Rijkswaterstaat.gpkg")
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_qlr", "output_controle_202502.qlr")
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd", "Na_levering", "Wateraanvoer", "MIPWA_20230907WF.gpkg")

cloud.synchronize(
    filepaths=[
        ribasim_base_model_dir,
        FeedbackFormulier_path,
        ws_grenzen_path,
        RWS_grenzen_path,
        qlr_path,
        aanvoer_path,
    ]
)

# download the feedback forms, overwrite the old ones
cloud.download_verwerkt(authority=waterschap, overwrite=True)

# set paths to the TEMP working directory
work_dir = cloud.joinpath(waterschap, "verwerkt", "Work_dir", f"{waterschap}_parameterized")
ribasim_gpkg = work_dir.joinpath("database.gpkg")
ribasim_work_dir_model_toml = work_dir.joinpath("ribasim.toml")

# set path to base model toml
ribasim_base_model_toml = ribasim_base_model_dir.joinpath("ribasim.toml")

# create work_dir/parameterized
parameterized = os.path.join(work_dir, f"{waterschap}_parameterized/")
os.makedirs(parameterized, exist_ok=True)

# define variables and model
# basin area percentage
regular_percentage = 10
boezem_percentage = 90
unknown_streefpeil = (
    0.00012345  # we need a streefpeil to create the profiles, Q(h)-relations, and af- and aanslag peil for pumps
)

# forcing settings
starttime = datetime.datetime(2024, 1, 1)
endtime = datetime.datetime(2025, 1, 1)
saveat = 3600 * 24
timestep_size = "d"
timesteps = 2
delta_crest_level = 0.1  # delta waterlevel of boezem compared to streefpeil till no water can flow through an outlet

default_level = 10 if AANVOER_CONDITIONS else -2.3456  # default LevelBoundary level

# process the feedback form
name = "HKV"
processor = RibasimFeedbackProcessor(
    name,
    waterschap,
    base_model_versie,
    FeedbackFormulier_path,
    ribasim_base_model_toml,
    work_dir,
    FeedbackFormulier_LOG_path,
    use_validation=True,
)
processor.run()

# load model
with warnings.catch_warnings():
    warnings.simplefilter(action="ignore", category=FutureWarning)
    ribasim_model = ribasim.Model(filepath=ribasim_work_dir_model_toml)

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# model specific tweaks
# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

# change high initial states to 0
ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df["level"] == 9.999, "level"] = 0
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == 9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(158166, 553915)), [level_boundary.Static(level=[default_level])]
)

tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(158166, 553915)),
    [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
)

ribasim_model.edge.add(ribasim_model.basin[89], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)

new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(156824, 552856)), [level_boundary.Static(level=[default_level])]
)

tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(156831, 552967)),
    [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
)

ribasim_model.edge.add(ribasim_model.basin[861], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)

new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(155989, 563067)), [level_boundary.Static(level=[default_level])]
)

tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(156038, 563080)),
    [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
)

ribasim_model.edge.add(ribasim_model.basin[149], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)

new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(154647, 567409)), [level_boundary.Static(level=[default_level])]
)

tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(154644, 567399)),
    [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
)

ribasim_model.edge.add(ribasim_model.basin[149], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)

new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(156173, 576551)), [level_boundary.Static(level=[default_level])]
)

tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(157109, 576125)),
    [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
)

ribasim_model.edge.add(ribasim_model.basin[254], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)

# add a zeegemaal
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(170894, 591637)), [level_boundary.Static(level=[default_level])]
)

pump = ribasim_model.pump.add(Node(new_node_id + 1, Point(170902, 591432)), [pump.Static(flow_rate=[700 / 60])])

ribasim_model.edge.add(ribasim_model.basin[4], pump)
ribasim_model.edge.add(pump, level_boundary_node)

ribasim_model.level_boundary.node.df.meta_node_id = ribasim_model.level_boundary.node.df.index
ribasim_model.tabulated_rating_curve.node.df.meta_node_id = ribasim_model.tabulated_rating_curve.node.df.index
ribasim_model.pump.node.df.meta_node_id = ribasim_model.pump.node.df.index

# insert standard profiles to each basin: these are [depth_profiles] meter deep, defined from the streefpeil
ribasim_param.insert_standard_profile(
    ribasim_model,
    unknown_streefpeil=unknown_streefpeil,
    regular_percentage=regular_percentage,
    boezem_percentage=boezem_percentage,
    depth_profile=2,
)

add_storage_basins = AddStorageBasins(
    ribasim_model=ribasim_model, exclude_hoofdwater=True, additional_basins_to_exclude=[]
)

add_storage_basins.create_bergende_basins()

# TODO: Create a stable model for 'aanvoer'-conditions with E=10
# set static forcing
forcing_dict = {
    "precipitation": ribasim_param.convert_mm_day_to_m_sec(0 if AANVOER_CONDITIONS else 10),
    "potential_evaporation": ribasim_param.convert_mm_day_to_m_sec(0.1 if AANVOER_CONDITIONS else 0),
    "drainage": ribasim_param.convert_mm_day_to_m_sec(0),
    "infiltration": ribasim_param.convert_mm_day_to_m_sec(0),
}

ribasim_param.set_static_forcing(timesteps, timestep_size, starttime, forcing_dict, ribasim_model)

# set pump capacity for each pump
ribasim_model.pump.static.df["flow_rate"] = 0.16667  # 10 kuub per minuut

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add the default levels
ribasim_model.level_boundary.static.df.level = default_level

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

# prepare 'aanvoergebieden'
if AANVOER_CONDITIONS:
    aanvoergebieden = supply.special_load_geometry(
        f_geometry=str(aanvoer_path),
        method="extract",
        layer="DAMO_W_AfvoergebiedAanvoergebied",
        key="functieGebied",
        value="2",
    )
else:
    aanvoergebieden = None

# add control, based on the meta_categorie
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(ribasim_model, aanvoergebieden, processor, aanvoer_enabled=AANVOER_CONDITIONS)
# ribasim_param.add_discrete_control(ribasim_model, waterschap, default_level)
ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)

# Manning resistance
# there is a MR without geometry and without edges for some reason
ribasim_model.manning_resistance.node.df = ribasim_model.manning_resistance.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 25
ribasim_model.manning_resistance.static.df.manning_n = 0.01

# last formatting of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)

# set numerical settings
# write model output
ribasim_model.use_validation = True
ribasim_model.starttime = starttime
ribasim_model.endtime = endtime
ribasim_model.solver.saveat = saveat
ribasim_model.write(ribasim_work_dir_model_toml)

# run model
ribasim_param.tqdm_subprocess(
    ["C:/ribasim_windows/ribasim/ribasim.exe", ribasim_work_dir_model_toml], print_other=False, suffix="init"
)

# model performance
controle_output = Control(work_dir=work_dir, qlr_path=qlr_path)
indicators = controle_output.run_all()

# # write model
# ribasim_param.write_ribasim_model_GoodCloud(
#     ribasim_model=ribasim_model,
#     work_dir=work_dir,
#     waterschap=waterschap,
#     include_results=True,
# )
