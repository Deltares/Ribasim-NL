"""Parameterisation of water board: Hollandse Delta."""

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
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim_nl import CloudStorage, Model

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# enlarge (relative) tolerance to smoothen any possible instabilities
solver = Solver(abstol=1e-9, reltol=1e-9)

# model settings
waterschap = "HollandseDelta"
base_model_versie = "2024_12_3"

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
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd", "Na_levering", "Wateraanvoer", "aanvoergebieden_v2.shp")

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
if AANVOER_CONDITIONS:
    default_level = 1.24
else:
    default_level = -0.42

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

# model specific tweaks
# merge small basins into larger basins for numerical stability
ribasim_model.merge_basins(node_id=149, to_node_id=21)
ribasim_model.merge_basins(node_id=559, to_node_id=120)
ribasim_model.merge_basins(node_id=7, to_node_id=54)
ribasim_model.merge_basins(node_id=720, to_node_id=54)
# basins too small: aggregate basins
ribasim_model.merge_basins(node_id=81, to_node_id=357)
ribasim_model.merge_basins(node_id=95, to_node_id=99)
ribasim_model.merge_basins(node_id=715, to_node_id=721)
ribasim_model.merge_basins(node_id=778, to_node_id=393)
ribasim_model.merge_basins(node_id=675, to_node_id=170)
ribasim_model.merge_basins(node_id=636, to_node_id=232)
ribasim_model.merge_basins(node_id=610, to_node_id=15)
ribasim_model.merge_basins(node_id=286, to_node_id=203)
ribasim_model.merge_basins(node_id=417, to_node_id=418)
ribasim_model.merge_basins(node_id=65, to_node_id=326)
ribasim_model.merge_basins(node_id=714, to_node_id=281)
ribasim_model.merge_basins(node_id=580, to_node_id=165)
ribasim_model.merge_basins(node_id=702, to_node_id=668)
ribasim_model.merge_basins(node_id=755, to_node_id=234)
ribasim_model.merge_basins(node_id=230, to_node_id=488)
ribasim_model.merge_basins(node_id=6, to_node_id=299, are_connected=False)
ribasim_model.merge_basins(node_id=431, to_node_id=299)
ribasim_model.merge_basins(node_id=782, to_node_id=429)
ribasim_model.merge_basins(node_id=257, to_node_id=258)
ribasim_model.merge_basins(node_id=602, to_node_id=393)
ribasim_model.merge_basins(node_id=423, to_node_id=654)
ribasim_model.merge_basins(node_id=530, to_node_id=393)
ribasim_model.merge_basins(node_id=224, to_node_id=393)
ribasim_model.merge_basins(node_id=263, to_node_id=310)
ribasim_model.merge_basins(node_id=497, to_node_id=393)
ribasim_model.merge_basins(node_id=565, to_node_id=12)
ribasim_model.merge_basins(node_id=679, to_node_id=472)
ribasim_model.merge_basins(node_id=721, to_node_id=271)
ribasim_model.merge_basins(node_id=191, to_node_id=21)
ribasim_model.merge_basins(node_id=706, to_node_id=219)
ribasim_model.merge_basins(node_id=117, to_node_id=576)
ribasim_model.merge_basins(node_id=169, to_node_id=189)
ribasim_model.merge_basins(node_id=302, to_node_id=303)
ribasim_model.merge_basins(node_id=303, to_node_id=189)
ribasim_model.merge_basins(node_id=324, to_node_id=655)
ribasim_model.merge_basins(node_id=98, to_node_id=99)
ribasim_model.merge_basins(node_id=538, to_node_id=294)
ribasim_model.merge_basins(node_id=558, to_node_id=32)
ribasim_model.merge_basins(node_id=704, to_node_id=453)
ribasim_model.merge_basins(node_id=300, to_node_id=116)
ribasim_model.merge_basins(node_id=624, to_node_id=37)
ribasim_model.merge_basins(node_id=250, to_node_id=248)
ribasim_model.merge_basins(node_id=92, to_node_id=49)
ribasim_model.merge_basins(node_id=687, to_node_id=304)
ribasim_model.merge_basins(node_id=468, to_node_id=556)
ribasim_model.merge_basins(node_id=488, to_node_id=709)
ribasim_model.merge_basins(node_id=709, to_node_id=251)
ribasim_model.merge_basins(node_id=249, to_node_id=251)
ribasim_model.merge_basins(node_id=449, to_node_id=453)
ribasim_model.merge_basins(node_id=387, to_node_id=75)
ribasim_model.merge_basins(node_id=499, to_node_id=402)
ribasim_model.merge_basins(node_id=180, to_node_id=15)
ribasim_model.merge_basins(node_id=641, to_node_id=434)
ribasim_model.merge_basins(node_id=432, to_node_id=320)
ribasim_model.merge_basins(node_id=36, to_node_id=614)
ribasim_model.merge_basins(node_id=480, to_node_id=55)
ribasim_model.merge_basins(node_id=410, to_node_id=453)
ribasim_model.merge_basins(node_id=726, to_node_id=591)
ribasim_model.merge_basins(node_id=374, to_node_id=371)
ribasim_model.merge_basins(node_id=514, to_node_id=184)
ribasim_model.merge_basins(node_id=690, to_node_id=556)
ribasim_model.merge_basins(node_id=354, to_node_id=384)
ribasim_model.merge_basins(node_id=125, to_node_id=124)
ribasim_model.merge_basins(node_id=623, to_node_id=753)
ribasim_model.merge_basins(node_id=462, to_node_id=31)
ribasim_model.merge_basins(node_id=555, to_node_id=232)
ribasim_model.merge_basins(node_id=341, to_node_id=26)
ribasim_model.merge_basins(node_id=618, to_node_id=701)
ribasim_model.merge_basins(node_id=705, to_node_id=682)
ribasim_model.merge_basins(node_id=643, to_node_id=262)
ribasim_model.merge_basins(node_id=151, to_node_id=47)
ribasim_model.merge_basins(node_id=57, to_node_id=353)
ribasim_model.merge_basins(node_id=121, to_node_id=85)
ribasim_model.merge_basins(node_id=510, to_node_id=512)
ribasim_model.merge_basins(node_id=311, to_node_id=383)
ribasim_model.merge_basins(node_id=349, to_node_id=26)
ribasim_model.merge_basins(node_id=40, to_node_id=175)
ribasim_model.merge_basins(node_id=197, to_node_id=242)
ribasim_model.merge_basins(node_id=685, to_node_id=615)
ribasim_model.merge_basins(node_id=668, to_node_id=21)
ribasim_model.merge_basins(node_id=261, to_node_id=305)
ribasim_model.merge_basins(node_id=406, to_node_id=405)
ribasim_model.merge_basins(node_id=733, to_node_id=632)
ribasim_model.merge_basins(node_id=100, to_node_id=512)
ribasim_model.merge_basins(node_id=737, to_node_id=632)
ribasim_model.merge_basins(node_id=146, to_node_id=139)
ribasim_model.merge_basins(node_id=99, to_node_id=728)
ribasim_model.merge_basins(node_id=144, to_node_id=352)
ribasim_model.merge_basins(node_id=442, to_node_id=229)
ribasim_model.merge_basins(node_id=528, to_node_id=268)
ribasim_model.merge_basins(node_id=64, to_node_id=47)
ribasim_model.merge_basins(node_id=693, to_node_id=60)
ribasim_model.merge_basins(node_id=576, to_node_id=606)
ribasim_model.merge_basins(node_id=603, to_node_id=13)
ribasim_model.merge_basins(node_id=211, to_node_id=320)
ribasim_model.merge_basins(node_id=319, to_node_id=320)
ribasim_model.merge_basins(node_id=369, to_node_id=732)
ribasim_model.merge_basins(node_id=757, to_node_id=476)
ribasim_model.merge_basins(node_id=147, to_node_id=518)
ribasim_model.merge_basins(node_id=186, to_node_id=60)
ribasim_model.merge_basins(node_id=70, to_node_id=674)
ribasim_model.merge_basins(node_id=531, to_node_id=60)
ribasim_model.merge_basins(node_id=708, to_node_id=31)
ribasim_model.merge_basins(node_id=762, to_node_id=344)
ribasim_model.merge_basins(node_id=657, to_node_id=129)
ribasim_model.merge_basins(node_id=697, to_node_id=597)
ribasim_model.merge_basins(node_id=585, to_node_id=198)
ribasim_model.merge_basins(node_id=742, to_node_id=198)
ribasim_model.merge_basins(node_id=48, to_node_id=242)
ribasim_model.merge_basins(node_id=492, to_node_id=233)
ribasim_model.merge_basins(node_id=87, to_node_id=340)
ribasim_model.merge_basins(node_id=612, to_node_id=162)
ribasim_model.merge_basins(node_id=22, to_node_id=752)
ribasim_model.merge_basins(node_id=274, to_node_id=752)
ribasim_model.merge_basins(node_id=187, to_node_id=420)
ribasim_model.merge_basins(node_id=361, to_node_id=113)
ribasim_model.merge_basins(node_id=703, to_node_id=357)
ribasim_model.merge_basins(node_id=451, to_node_id=75)
ribasim_model.merge_basins(node_id=200, to_node_id=75)
ribasim_model.merge_basins(node_id=486, to_node_id=522)
ribasim_model.merge_basins(node_id=545, to_node_id=393)
ribasim_model.merge_basins(node_id=393, to_node_id=31)
ribasim_model.merge_basins(node_id=663, to_node_id=570)
ribasim_model.merge_basins(node_id=347, to_node_id=660)
ribasim_model.merge_basins(node_id=628, to_node_id=415)
ribasim_model.merge_basins(node_id=583, to_node_id=518)
ribasim_model.merge_basins(node_id=779, to_node_id=126)
ribasim_model.merge_basins(node_id=372, to_node_id=422)
ribasim_model.merge_basins(node_id=784, to_node_id=701)
ribasim_model.merge_basins(node_id=457, to_node_id=365)
ribasim_model.merge_basins(node_id=215, to_node_id=297)
ribasim_model.merge_basins(node_id=297, to_node_id=312)
ribasim_model.merge_basins(node_id=212, to_node_id=133)
ribasim_model.merge_basins(node_id=358, to_node_id=175)
ribasim_model.merge_basins(node_id=412, to_node_id=266)
ribasim_model.merge_basins(node_id=350, to_node_id=476)
ribasim_model.merge_basins(node_id=707, to_node_id=384)
ribasim_model.merge_basins(node_id=342, to_node_id=155)
ribasim_model.merge_basins(node_id=798, to_node_id=9)
ribasim_model.merge_basins(node_id=592, to_node_id=254)
ribasim_model.merge_basins(node_id=67, to_node_id=9)
ribasim_model.merge_basins(node_id=193, to_node_id=401, are_connected=False)
ribasim_model.merge_basins(node_id=699, to_node_id=75)
ribasim_model.merge_basins(node_id=776, to_node_id=21)
ribasim_model.merge_basins(node_id=614, to_node_id=31)
ribasim_model.merge_basins(node_id=691, to_node_id=47)
ribasim_model.merge_basins(node_id=294, to_node_id=388)
ribasim_model.merge_basins(node_id=318, to_node_id=647)
ribasim_model.merge_basins(node_id=574, to_node_id=282)
ribasim_model.merge_basins(node_id=473, to_node_id=453)
ribasim_model.merge_basins(node_id=648, to_node_id=207)
ribasim_model.merge_basins(node_id=461, to_node_id=340)
ribasim_model.merge_basins(node_id=383, to_node_id=266)
ribasim_model.merge_basins(node_id=751, to_node_id=174)
ribasim_model.merge_basins(node_id=591, to_node_id=150)
ribasim_model.merge_basins(node_id=216, to_node_id=320)
ribasim_model.merge_basins(node_id=541, to_node_id=37)
ribasim_model.merge_basins(node_id=596, to_node_id=605)
ribasim_model.merge_basins(node_id=710, to_node_id=519)
ribasim_model.merge_basins(node_id=777, to_node_id=348)
ribasim_model.merge_basins(node_id=239, to_node_id=465)
ribasim_model.merge_basins(node_id=513, to_node_id=512)
ribasim_model.merge_basins(node_id=107, to_node_id=771)
ribasim_model.merge_basins(node_id=309, to_node_id=481)
ribasim_model.merge_basins(node_id=723, to_node_id=566)
ribasim_model.merge_basins(node_id=692, to_node_id=237)
ribasim_model.merge_basins(node_id=638, to_node_id=50)
ribasim_model.merge_basins(node_id=321, to_node_id=53)
ribasim_model.merge_basins(node_id=595, to_node_id=75)
# merge overlapping basins
ribasim_model.merge_basins(node_id=633, to_node_id=475)
ribasim_model.merge_basins(node_id=329, to_node_id=325)

# check basin area
ribasim_param.validate_basin_area(ribasim_model)


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

# an error occured in the feedback form, which prevented a LevelBoundary and pump being schematized. Add it here.
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(55992, 424882)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(55982, 424908)), [pump.Static(flow_rate=[0.1])])
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_afvoer"] = 0
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[170])

# both an afvoer as well as aanvoer gemaal. Aanvoer gemaal already in model, add afvoer
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(81148, 418947)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(81179, 419027)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[270], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 7th biggest gemaal of Hollandse Delta is missing: add it
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(81551, 425469)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(81526, 425553)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[205], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add gemaal and LB near Brienenoordbrug
pump_node = ribasim_model.pump.add(Node(geometry=Point(96675, 434551)), [pump.Static(flow_rate=[20])])
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(96690, 434593)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[338], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add a TRC and LB near the south of Rotterdam
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(93596, 434790)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(93610.26, 434788.89)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[2], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# add gemaal and LB at a football field
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(102168, 421137)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(102158, 421104)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[348])

# add gemaal and LB
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(91963, 414083)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(91929, 414495)), [pump.Static(flow_rate=[5])])
ribasim_model.link.add(ribasim_model.basin[71], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add afvoergemaal
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(78275, 423949)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(78251, 423919)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[789], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add a TRC and LB to a small harbour
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(103964, 429864)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(103927.41, 429888.69)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[116])

# add gemaal and LB at the Voornse Meer
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(64560, 439130)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(64555, 439130)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[801], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# changes after samenwerkdag of February
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(110570.5, 421238.1)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(110535.2, 421208.5)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[38])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(87175.8, 415515.9)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(87187.6, 415538.6)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[435])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(89336.4, 415678.2)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(89318.7, 415662.7)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[669])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(89352.6, 415703.5)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(89348.1, 415718)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[684])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(77197.2, 424267.6)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(77197.2, 424241.6)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[739])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(93464.1, 428403.3)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(93464.1, 428423.3)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[47])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(104356.5, 430002.6)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(104336.5, 430002.6)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[616])

# addition of LLB-TRC combination
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(73429, 420133)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(73429, 420123)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[391])

# TEMP add LB
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(88565, 429038)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(88566.5, 429040)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[563])

# TEMP add gemaal and LB
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(79312, 424826)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(79325, 424862)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[26])

# (re) set 'meta_node_id'
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

# implement standard profile and a storage basin
# insert standard profiles to each basin. These are [depth_profiles] meter deep, defined from the streefpeil
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
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_forcing(ribasim_model, starttime, endtime, 2)
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
    ribasim_param.set_hypothetical_dynamic_level_boundaries(ribasim_model, starttime, endtime, -0.42, 1.24)
else:
    ribasim_model.level_boundary.static.df.level = default_level

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

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

# last formating of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)
if MIXED_CONDITIONS:
    ribasim_model.basin.static.df = None

# add the water authority column to couple the model with
assign = AssignAuthorities(
    ribasim_model=ribasim_model,
    waterschap=waterschap,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    RWS_buffer=10000,  # is only neighbouring RWS, so increase buffer
    custom_nodes={
        9141: None,  # dunes
        2687: None,  # dunes
    },
)
ribasim_model = assign.assign_authorities()
if MIXED_CONDITIONS:
    # TODO: Embed the correct usage of `static` v. `time` dataframes in `AssignAuthorities`
    assign.from_static_to_time_df(ribasim_model, clear_static=True)

# set numerical settings
ribasim_model.use_validation = True
ribasim_model.starttime = starttime
ribasim_model.endtime = endtime
ribasim_model.solver.saveat = saveat
ribasim_model.write(ribasim_work_dir_model_toml)

# run model
ribasim_param.tqdm_subprocess(["ribasim", ribasim_work_dir_model_toml], print_other=False, suffix="init")

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
