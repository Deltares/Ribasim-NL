"""Parameterisation of water board: Zuiderzeeland."""

import datetime
import os
import warnings

from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import Point

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.add_storage_basins import AddStorageBasins
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim_nl import CloudStorage, Model

AANVOER_CONDITIONS: bool = True

# model settings
waterschap = "Zuiderzeeland"
base_model_versie = "2024_12_0"

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
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd", "Na_levering", "peilgebieden.gpkg")

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
default_level = (
    0.40 if AANVOER_CONDITIONS else -0.40
)  # default LevelBoundary level, similar to surrounding IJsselmeer, Markermeer and Randmeren

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
    ribasim_model = Model(filepath=ribasim_work_dir_model_toml)

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# model specific tweaks
# 1 Add gemaal at blocq van kuffeler
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(143912, 492256)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(143959, 492198)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[31], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 1 Add gemaal Wortman
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(157201, 501796)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(157251, 501708)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[31], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# Inlaat (hevel) toevoegen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(193502, 526518)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(193491, 526526)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[98])

# Inlaat (hevel) toevoegen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(185725, 533120)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(185725, 533098)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[116])

# merge the smallest basins together
ribasim_model.merge_basins(node_id=30, to_node_id=29)  # 4363 m2
ribasim_model.merge_basins(node_id=66, to_node_id=21)  # 4745 m2
ribasim_model.merge_basins(node_id=191, to_node_id=288)  # 4964 m2
ribasim_model.merge_basins(node_id=288, to_node_id=140)
ribasim_model.merge_basins(node_id=192, to_node_id=289)
ribasim_model.merge_basins(node_id=289, to_node_id=140)
ribasim_model.merge_basins(node_id=206, to_node_id=284)  # 7831 m2
ribasim_model.merge_basins(node_id=29, to_node_id=195)  # 8398 m2

ribasim_model.merge_basins(node_id=137, to_node_id=80)  # stedelijk gebied
ribasim_model.merge_basins(node_id=103, to_node_id=217)  # stedelijk gebied
ribasim_model.merge_basins(node_id=80, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=231, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=238, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=239, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=213, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=91, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=179, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=174, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=202, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=259, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=264, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=175, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=76, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=77, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=234, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=235, to_node_id=136)  # stedelijk gebied
ribasim_model.merge_basins(node_id=176, to_node_id=136)  # stedelijk gebied

ribasim_model.merge_basins(node_id=190, to_node_id=73)  # los snippertje
ribasim_model.merge_basins(node_id=224, to_node_id=73)  # los snippertje

ribasim_model.merge_basins(node_id=246, to_node_id=133)  # Noordelijkst puntje veel kleine gebieden
ribasim_model.merge_basins(node_id=250, to_node_id=133)  # Noordelijkst puntje veel kleine gebieden
ribasim_model.merge_basins(node_id=162, to_node_id=133)  # Noordelijkst puntje veel kleine gebieden
ribasim_model.merge_basins(node_id=119, to_node_id=133)  # Noordelijkst puntje veel kleine gebieden
ribasim_model.merge_basins(node_id=65, to_node_id=124)  # Noordelijkst puntje veel kleine gebieden
ribasim_model.merge_basins(node_id=150, to_node_id=124)  # Noordelijkst puntje veel kleine gebieden

ribasim_model.merge_basins(node_id=83, to_node_id=220)  # oostelijke NOP
ribasim_model.merge_basins(node_id=84, to_node_id=220)  # oostelijke NOP

ribasim_model.merge_basins(node_id=196, to_node_id=73)  # klein gebiedje in lage afdeling
ribasim_model.merge_basins(node_id=89, to_node_id=73)  # klein gebiedje in lage afdeling
ribasim_model.merge_basins(node_id=42, to_node_id=123)  # klein gebiedjes met twee MR


ribasim_model.merge_basins(node_id=68, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=158, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=256, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=163, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=69, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=177, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=78, to_node_id=96)  # natuurgebiedje NOP

ribasim_model.merge_basins(node_id=257, to_node_id=138)  # natuurgebiedje NOP

ribasim_model.merge_basins(node_id=115, to_node_id=6)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=135, to_node_id=6)  # natuurgebiedje NOP

ribasim_model.merge_basins(node_id=227, to_node_id=10)
ribasim_model.merge_basins(node_id=156, to_node_id=10)

ribasim_model.merge_basins(node_id=4, to_node_id=32)  # vakantieparkje(?)
ribasim_model.merge_basins(node_id=108, to_node_id=32)  # vakantieparkje(?)

ribasim_model.merge_basins(node_id=107, to_node_id=198)  # klein gebiedje
ribasim_model.merge_basins(node_id=67, to_node_id=21)  # klein gebiedje
ribasim_model.merge_basins(node_id=281, to_node_id=51)  # klein gebiedje

ribasim_model.merge_basins(node_id=11, to_node_id=16)  # klein gebiedje in hoge vaart
ribasim_model.merge_basins(node_id=35, to_node_id=16)  # klein gebiedje in hoge vaart
ribasim_model.merge_basins(node_id=97, to_node_id=16)  # klein gebiedje in hoge vaart
ribasim_model.merge_basins(node_id=164, to_node_id=16)  # klein gebiedje in hoge vaart
ribasim_model.merge_basins(node_id=75, to_node_id=16)  # klein gebiedje in hoge vaart

ribasim_model.merge_basins(node_id=40, to_node_id=31)  # klein gebiedje in lage vaart
ribasim_model.merge_basins(node_id=25, to_node_id=31)  # klein gebiedje in lage vaart
ribasim_model.merge_basins(node_id=160, to_node_id=31)  # klein gebiedje in lage vaart

ribasim_model.merge_basins(node_id=152, to_node_id=36)  # klein gebiedjes in polder, noordelijk
ribasim_model.merge_basins(node_id=209, to_node_id=36)  # klein gebiedjes in polder, noordelijk
ribasim_model.merge_basins(node_id=208, to_node_id=36)  # klein gebiedjes in polder, noordelijk


# TODO: Own interpretation (GH)
# change afvoergemaal to aanvoergemaal
node_ids = 319, 333, 380, 433, 650
for n in node_ids:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_aanvoer"] = 1

# (re)set 'meta_node_id'-values
ribasim_model.level_boundary.node.df.meta_node_id = ribasim_model.level_boundary.node.df.index
ribasim_model.tabulated_rating_curve.node.df.meta_node_id = ribasim_model.tabulated_rating_curve.node.df.index
ribasim_model.pump.node.df.meta_node_id = ribasim_model.pump.node.df.index

# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

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

# set static forcing
forcing_dict = {
    "precipitation": ribasim_param.convert_mm_day_to_m_sec(0 if AANVOER_CONDITIONS else 10),
    "potential_evaporation": ribasim_param.convert_mm_day_to_m_sec(10 if AANVOER_CONDITIONS else 0),
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

# add control, based on the meta_categorie
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=AANVOER_CONDITIONS)
# ribasim_param.add_discrete_control(ribasim_model, waterschap, default_level)
ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.manning_resistance.node.df = ribasim_model.manning_resistance.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 10
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

# write model
ribasim_param.write_ribasim_model_GoodCloud(
    ribasim_model=ribasim_model,
    work_dir=work_dir,
    waterschap=waterschap,
    include_results=True,
)
