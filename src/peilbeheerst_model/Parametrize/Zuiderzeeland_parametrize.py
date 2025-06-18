"""Parameterisation of water board: Zuiderzeeland."""

import datetime
import os
import warnings

from ribasim import Node
from ribasim.config import Solver
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import Point

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.add_storage_basins import AddStorageBasins
from peilbeheerst_model.assign_authorities import AssignAuthorities
from peilbeheerst_model.assign_parametrization import AssignMetaData
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim_nl import CloudStorage, Model
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# enlarge (relative) tolerance to smoothen any possible instabilities
solver = Solver(abstol=1e-9, reltol=1e-9)

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
qlr_path = cloud.joinpath(
    "Basisgegevens", "QGIS_qlr", "output_controle_cc.qlr" if MIXED_CONDITIONS else "output_controle_202502.qlr"
)
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
    ribasim_model = Model(filepath=ribasim_work_dir_model_toml, solver=solver)

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

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

ribasim_model.merge_basins(node_id=18, to_node_id=260)  # stedelijk gebied

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

ribasim_model.merge_basins(node_id=273, to_node_id=282)  # klein gebied NOP
ribasim_model.merge_basins(node_id=215, to_node_id=143)  # klein gebied NOP
ribasim_model.merge_basins(node_id=123, to_node_id=205, are_connected=False)  # klein gebied NOP

ribasim_model.merge_basins(node_id=130, to_node_id=73)  # klein gebied NOP

ribasim_model.merge_basins(node_id=68, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=158, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=256, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=163, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=69, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=177, to_node_id=96)  # natuurgebiedje NOP
ribasim_model.merge_basins(node_id=78, to_node_id=96)  # natuurgebiedje NOP

ribasim_model.merge_basins(node_id=268, to_node_id=73)  # klein gebied NOP

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

ribasim_model.merge_basins(node_id=286, to_node_id=283)  # klein gebiedje in stedelijk gebied

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

ribasim_model.merge_basins(node_id=212, to_node_id=101)  # ivm wateraanvoer. Klein onlogisch gebied.

ribasim_model.merge_basins(node_id=216, to_node_id=262)  # industriegebied Almere,
ribasim_model.merge_basins(node_id=262, to_node_id=31)  # industriegebied Almere,

ribasim_model.merge_basins(node_id=140, to_node_id=19)  # klein gebiedje Lelystad,
ribasim_model.merge_basins(node_id=173, to_node_id=53, are_connected=False)  # klein gebiedje zonder streefpeil

# There are +- 30 locations where a pump has been added in the FF,
# these pumps are generally not in the delivered data but are placed by i.e. farmers
# Convert these pumps to inlaat_pumps
inlaat_structures = []
inlaat_pump = []

# model specific tweaks
# 1 Add gemaal at blocq van kuffeler
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(143912, 492256)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(143959, 492198)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[31], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 1 Gemaal near Almere
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(143895, 492250)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(143927, 492229)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[16], pump_node)
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
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

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
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen near Lelystad 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(161109, 507280)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(161113, 507190)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[31])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen near Lelystad 2
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(155928, 500745)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(155977, 500692)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[34])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen near Urk: 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(168944, 520125)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(168957, 520126)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[136])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen near Urk: 2
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(168920, 520306)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(168956, 520306)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[73])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at Ketelhaven
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(180145, 510253)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(180131, 510204)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[16])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: east 1. According data it should go to basin 199, but 45 seems more logical
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(195210, 519077)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(195198, 519068)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[45])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: east 2
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(193142, 525102)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(193131, 525102)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[64])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: east 3
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(193293, 522692)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(193259, 522698)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[220])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: south
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(189485, 515337)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(189473, 515427)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[39])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: south east
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(192223, 521469)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(192219, 521474)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[141])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: north east
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(185723, 533122)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(185719, 533122)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[88])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on


# Inlaat (hevel) toevoegen at NOP: north 1 (not in data, but mentioned by RV)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(176393, 538604)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(176400, 538569)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[241])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: north 2 (not in data, but mentioned by RV)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(176389, 538605)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(176392, 538568)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[26])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(163582, 482871)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[16], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[51])
inlaat_pump.append(pump_node.node_id)

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(181525, 517580)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[8], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[96])
inlaat_pump.append(pump_node.node_id)

# switching direction prohibited due to "old" terminal-node
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(169590, 518954)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(169592, 518954)), [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])]
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[73])

# [from_node, to_node, x_coordinate, y_coordinate]
farmer_pumps = [
    [16, 222, 163266, 487745],
    [31, 53, 162476, 493070],
    [31, 161, 160482, 495540],
    [31, 147, 163155, 496109],
    [31, 232, 167346, 493712],
    [31, 180, 167553, 493696],
    [31, 245, 161132, 497790],
    [31, 219, 161413, 498612],
    [31, 20, 159725, 499946],
    [31, 117, 157082, 501506],
    [31, 244, 163603, 498905],
    [31, 243, 164112, 499846],
    [31, 204, 164702, 501743],
    [16, 229, 180860, 499866],
    [16, 221, 180899, 499914],
    [16, 155, 182273, 498826],
    [16, 101, 184918, 504045],
    [16, 102, 184013, 504227],
    [16, 263, 182081, 504226],
    [31, 278, 174932, 504574],
    [129, 279, 175637, 507063],
    [31, 129, 176616, 508796],
    [31, 92, 172251, 506176],
    [31, 28, 175874, 509405],
    [31, 210, 174223, 510437],
    [31, 48, 167508, 510888],
    [8, 47, 184856, 515922],
    [63, 201, 186402, 515281],
    [199, 41, 192906, 519400],
    [88, 110, 183948, 532356],
    [73, 43, 180285, 531935],
    [73, 184, 177791, 532821],
    [16, 79, 180975, 509873],
    [79, 56, 183547, 508147],
]

for farmer_pump in farmer_pumps:
    pump_node = ribasim_model.pump.add(
        Node(geometry=Point(farmer_pump[2], farmer_pump[3])), [pump.Static(flow_rate=[0.1])]
    )  # create node
    ribasim_model.link.add(ribasim_model.basin[farmer_pump[0]], pump_node)  # create link
    ribasim_model.link.add(pump_node, ribasim_model.basin[farmer_pump[1]])  # create link
    inlaat_pump.append(pump_node.node_id)  # add inlaat category

# change afvoergemaal to aanvoergemaal
inlaat_pump += [319, 333, 380, 433, 650]

for n in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_aanvoer"] = 1
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_afvoer"] = 0

# TODO: Temporary fixes
# All set and done!

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

# set forcing
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_forcing(ribasim_model, starttime, endtime, 10)
else:
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
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(ribasim_model, starttime, endtime, -0.4, 0.4)
else:
    ribasim_model.level_boundary.static.df.level = default_level

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)
for node in inlaat_structures:
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_aanvoer"] = 1

# assign metadata for pumps and basins
assign_metadata = AssignMetaData(
    authority=waterschap,
    model_name=ribasim_model,
    param_name=f"{waterschap}.gpkg",
)
assign_metadata.add_meta_to_pumps(
    layer="gemaal",
    mapper={
        "meta_name": {"node": ["name"]},
        "meta_capaciteit": {"static": ["flow_rate", "max_flow_rate"]},
    },
    max_distance=100,
    factor_flowrate=1 / 60,  # m3/min -> m3/s
)
assign_metadata.add_meta_to_basins(
    layer="aggregation_area",
    mapper={"meta_name": {"node": ["name"]}},
    min_overlap=0.95,
)

offline_budgets = AssignOfflineBudgets()
offline_budgets.compute_budgets(ribasim_model)

# add control, based on the meta_categorie
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=AANVOER_CONDITIONS)
ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
ribasim_param.add_continuous_control(ribasim_model, dy=-50)

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.manning_resistance.node.df = ribasim_model.manning_resistance.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 100
ribasim_model.manning_resistance.static.df.manning_n = 0.01

# last formatting of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)
if MIXED_CONDITIONS:
    ribasim_model.basin.static.df = None
    ribasim_param.set_dynamic_min_upstream_max_downstream(ribasim_model)

# add the water authority column to couple the model with
assign = AssignAuthorities(
    ribasim_model=ribasim_model,
    waterschap=waterschap,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    custom_nodes={
        821: "WetterskipFryslan",
        823: "WetterskipFryslan",
        2889: "WetterskipFryslan",
        820: "Rijkswaterstaat",
        834: "Rijkswaterstaat",
        857: "Rijkswaterstaat",
        873: "Rijkswaterstaat",
        875: "Rijkswaterstaat",
        879: "Rijkswaterstaat",
    },
)
ribasim_model = assign.assign_authorities()

# set numerical settings
# write model output
ribasim_model.use_validation = True
ribasim_model.starttime = starttime
ribasim_model.endtime = endtime
ribasim_model.solver.saveat = saveat
ribasim_model.write(ribasim_work_dir_model_toml)

# run model
ribasim_param.tqdm_subprocess(["ribasim", ribasim_work_dir_model_toml], print_other=False, suffix="init")

# model performance
controle_output = Control(work_dir=work_dir, qlr_path=qlr_path)
indicators = controle_output.run_dynamic_forcing() if MIXED_CONDITIONS else controle_output.run_all()

# write model
ribasim_param.write_ribasim_model_GoodCloud(
    ribasim_model=ribasim_model,
    work_dir=work_dir,
    waterschap=waterschap,
    include_results=True,
)
