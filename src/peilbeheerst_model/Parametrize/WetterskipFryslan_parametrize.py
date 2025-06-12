"""Parameterisation of water board: Fryslan."""

import datetime
import os
import warnings

from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import Point

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model import supply
from peilbeheerst_model.add_storage_basins import AddStorageBasins
from peilbeheerst_model.assign_authorities import AssignAuthorities
from peilbeheerst_model.assign_parametrization import AssignMetaData
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim_nl import CloudStorage, Model

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "WetterskipFryslan"
base_model_versie = "2025_5_1"

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
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd", "Na_levering", "Wateraanvoer", "aanvoer.gpkg")

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
endtime = datetime.datetime(2024, 3, 1)
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
    ribasim_model = Model(filepath=ribasim_work_dir_model_toml)

# Uitlaat toevoegen at Ter Schelling
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(155665, 601591)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(155658, 601976)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[993], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# Uitlaat toevoegen at Vlieland
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(128115, 586097)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(127144, 585944)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[1016], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# Uitlaat toevoegen at Vlieland
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(129831, 587677)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(129804, 587869)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[1021], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# numerical stability
ribasim_model.merge_basins(node_id=388, to_node_id=238)  # 569 m2
ribasim_model.merge_basins(node_id=321, to_node_id=121)  # 1737 m2
ribasim_model.merge_basins(node_id=912, to_node_id=36)  # 1933 m2
ribasim_model.merge_basins(node_id=941, to_node_id=322)  # 1939 m2
ribasim_model.merge_basins(node_id=807, to_node_id=6)  # 2044 m2
ribasim_model.merge_basins(node_id=936, to_node_id=464)
ribasim_model.merge_basins(node_id=464, to_node_id=467)  # 2617 m2
ribasim_model.merge_basins(node_id=909, to_node_id=16)  # 2637 m2
ribasim_model.merge_basins(node_id=529, to_node_id=635)  # 2880 m2
ribasim_model.merge_basins(node_id=572, to_node_id=176)  # 2883 m2
ribasim_model.merge_basins(node_id=730, to_node_id=329)  # 3012 m2
ribasim_model.merge_basins(node_id=481, to_node_id=6)  # 3878 m2
ribasim_model.merge_basins(node_id=944, to_node_id=19)  # 4407 m2
ribasim_model.merge_basins(node_id=908, to_node_id=6)  # 5274 m2
ribasim_model.merge_basins(node_id=950, to_node_id=16)  # 5863 m2
ribasim_model.merge_basins(node_id=965, to_node_id=195)  # 7283 m2
ribasim_model.merge_basins(node_id=296, to_node_id=16)  # 7709 m2
ribasim_model.merge_basins(node_id=957, to_node_id=527)  # 7920 m2
ribasim_model.merge_basins(node_id=887, to_node_id=195)  # 8514 m2
ribasim_model.merge_basins(node_id=141, to_node_id=6)  # 10273 m2
ribasim_model.merge_basins(node_id=577, to_node_id=527)  # 10299 m2
ribasim_model.merge_basins(node_id=507, to_node_id=613)  # 11880 m2
ribasim_model.merge_basins(node_id=669, to_node_id=547)  # 10299 m2
ribasim_model.merge_basins(node_id=802, to_node_id=80)  # 13571 m2
ribasim_model.merge_basins(node_id=894, to_node_id=729)  # 13853 m2
ribasim_model.merge_basins(node_id=739, to_node_id=951)  # 14022 m2
ribasim_model.merge_basins(node_id=573, to_node_id=159)  # 14121 m2
ribasim_model.merge_basins(node_id=762, to_node_id=29)  # 14121 m2
ribasim_model.merge_basins(node_id=748, to_node_id=185)  # 15085 m2

ribasim_model.merge_basins(node_id=964, to_node_id=982)  # small area + MR
ribasim_model.merge_basins(node_id=982, to_node_id=864)  # small area + MR
ribasim_model.merge_basins(node_id=741, to_node_id=864)  # small area + MR

ribasim_model.merge_basins(node_id=320, to_node_id=75)  # 15138 m2
ribasim_model.merge_basins(node_id=758, to_node_id=6)  # 15486 m2
ribasim_model.merge_basins(node_id=851, to_node_id=64)  # 16396 m2
ribasim_model.merge_basins(node_id=847, to_node_id=519, are_connected=False)  # 17464 m2
# basins below 20.000 m2 have been merged, whenever hydrologically possible

# water supply
ribasim_model.merge_basins(node_id=927, to_node_id=16)  # boezem stukje
ribasim_model.merge_basins(node_id=200, to_node_id=253)  # boezem stuk
ribasim_model.merge_basins(node_id=249, to_node_id=787)  # boezem stukje
ribasim_model.merge_basins(node_id=838, to_node_id=330, are_connected=False)  # boezem stukje
ribasim_model.merge_basins(node_id=989, to_node_id=330)  # boezem stukje
ribasim_model.merge_basins(node_id=1015, to_node_id=799)  # boezem stukje
ribasim_model.merge_basins(node_id=721, to_node_id=7)  # basin bij boezem
ribasim_model.merge_basins(node_id=1014, to_node_id=799)  # basin bij boezem
ribasim_model.merge_basins(node_id=1012, to_node_id=727)  # basin omringt door boezem
ribasim_model.merge_basins(node_id=901, to_node_id=16, are_connected=False)  # basin bij boezem
ribasim_model.merge_basins(node_id=1005, to_node_id=16)  # basin bij boezem
ribasim_model.merge_basins(node_id=999, to_node_id=206)  # klein gebied

ribasim_model.merge_basins(node_id=777, to_node_id=80)  # veel Manning knopen bij boezem, ook zelfde streefpeil
ribasim_model.merge_basins(node_id=559, to_node_id=80)  # veel Manning knopen bij boezem, ook zelfde streefpeil
ribasim_model.merge_basins(node_id=175, to_node_id=21)  # veel Manning knopen bij boezem, ook zelfde streefpeil
ribasim_model.merge_basins(node_id=299, to_node_id=21)  # veel Manning knopen bij boezem, ook zelfde streefpeil
ribasim_model.merge_basins(node_id=530, to_node_id=21)  # veel Manning knopen bij boezem, ook zelfde streefpeil

ribasim_model.merge_basins(node_id=1007, to_node_id=253)  # klein gebied in de buurt van de boezem
ribasim_model.merge_basins(node_id=1008, to_node_id=558)  # klein gebied aan de rand van model
ribasim_model.merge_basins(node_id=1029, to_node_id=366, are_connected=False)  # klein gebied midden in groter gebied
ribasim_model.merge_basins(node_id=934, to_node_id=977)  # klein gebied naast groter gebied
ribasim_model.merge_basins(node_id=383, to_node_id=22)  # klein gebied
ribasim_model.merge_basins(node_id=963, to_node_id=252)  # klein gebied naast boezem
ribasim_model.merge_basins(node_id=1028, to_node_id=252)  # klein gebied naast boezem
ribasim_model.merge_basins(node_id=801, to_node_id=64)  # klein gebied naast boezem
ribasim_model.merge_basins(node_id=1020, to_node_id=6)  # klein gebied naast boezem
ribasim_model.merge_basins(node_id=746, to_node_id=5)  # klein gebied in groter gebied
ribasim_model.merge_basins(node_id=821, to_node_id=21, are_connected=False)  # klein gebied in groter gebied
ribasim_model.merge_basins(node_id=1017, to_node_id=21)  # klein gebied in groter gebied

ribasim_model.merge_basins(node_id=1025, to_node_id=158, are_connected=False)  # havenachtig gebied
ribasim_model.merge_basins(node_id=955, to_node_id=158)  # havenachtig gebied
ribasim_model.merge_basins(node_id=574, to_node_id=21)  # havenachtig gebied
ribasim_model.merge_basins(node_id=1009, to_node_id=21)  # havenachtig gebied

ribasim_model.merge_basins(node_id=125, to_node_id=21)  # boezemland met veel MR
ribasim_model.merge_basins(node_id=986, to_node_id=21)  # boezemland met veel MR
ribasim_model.merge_basins(node_id=877, to_node_id=21)  # boezemland met veel MR
ribasim_model.merge_basins(node_id=540, to_node_id=16, are_connected=False)  # boezem

ribasim_model.merge_basins(node_id=991, to_node_id=519)  # ontbreekt gemaal / foutieve streefpeil
ribasim_model.merge_basins(
    node_id=497, to_node_id=930, are_connected=False
)  # boezem via duiker verbonden aan andere boezem

ribasim_model.merge_basins(node_id=922, to_node_id=431)  # vrijafstromend
ribasim_model.merge_basins(node_id=923, to_node_id=431)  # vrijafstromend
ribasim_model.merge_basins(node_id=520, to_node_id=431)  # vrijafstromend
ribasim_model.merge_basins(node_id=240, to_node_id=431)  # vrijafstromend
ribasim_model.merge_basins(node_id=474, to_node_id=242)  # vrijafstromend

ribasim_model.merge_basins(node_id=570, to_node_id=428)  # vrijafstromend
ribasim_model.merge_basins(node_id=427, to_node_id=428)  # vrijafstromend
ribasim_model.merge_basins(node_id=243, to_node_id=428)  # vrijafstromend
ribasim_model.merge_basins(node_id=304, to_node_id=428)  # vrijafstromend
ribasim_model.merge_basins(node_id=305, to_node_id=428)  # vrijafstromend

ribasim_model.merge_basins(node_id=1023, to_node_id=891)  # vrijafstromend

ribasim_model.merge_basins(node_id=872, to_node_id=632)  # vrijafstromend
ribasim_model.merge_basins(node_id=352, to_node_id=252)  # vrijafstromend
ribasim_model.merge_basins(node_id=933, to_node_id=252)  # vrijafstromend

# water supply
ribasim_model.merge_basins(node_id=869, to_node_id=444)  # klein gebied
ribasim_model.merge_basins(node_id=972, to_node_id=199)  # klein gebied
ribasim_model.merge_basins(node_id=984, to_node_id=366)  # klein gebied
ribasim_model.merge_basins(node_id=726, to_node_id=961)  # klein gebied
ribasim_model.merge_basins(node_id=966, to_node_id=595)  # klein gebied
ribasim_model.merge_basins(node_id=873, to_node_id=917, are_connected=False)  # klein gebied

ribasim_model.merge_basins(node_id=743, to_node_id=997)  # samenvoegen voor wateraanvoer
ribasim_model.merge_basins(node_id=997, to_node_id=861)  # samenvoegen voor wateraanvoer
ribasim_model.merge_basins(node_id=979, to_node_id=16)  # samenvoegen voor wateraanvoer

ribasim_model.merge_basins(node_id=182, to_node_id=16)  # part of the boezem

inlaat_pump = []  # pumps
inlaat_structures = []  # weirs / outlets

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(207500, 587902)), [pump.Static(flow_rate=[1.8])])
ribasim_model.link.add(ribasim_model.basin[322], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[917])
inlaat_pump.append(pump_node.node_id)

# add gemaal where basins were added twice
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(206469, 592738)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(206449, 592819)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[938], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# Inlaat (hevel) toevoegen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(206516, 592761)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(206508, 592803)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[938])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(155919, 563047)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(156038, 563082)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[295])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Ropta-pumps as 'aanvoer'
inlaat_pump += [3709, 2751]

# embed inlaat information
for n in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_aanvoer"] = 1
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_afvoer"] = 0

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

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

# (re)set 'meta_node_id'-values
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

# set static forcing
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_forcing(ribasim_model, starttime, endtime, 1)
else:
    forcing_dict = {
        "precipitation": ribasim_param.convert_mm_day_to_m_sec(0 if AANVOER_CONDITIONS else 10),
        "potential_evaporation": ribasim_param.convert_mm_day_to_m_sec(10 if AANVOER_CONDITIONS else 0),
        "drainage": ribasim_param.convert_mm_day_to_m_sec(0),
        "infiltration": ribasim_param.convert_mm_day_to_m_sec(0),
    }
    ribasim_param.set_static_forcing(timesteps, timestep_size, starttime, forcing_dict, ribasim_model)

# set pump capacity for each pump
# ribasim_model.pump.static.df["flow_rate"] = 0.16667  # 10 kuub per minuut

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(ribasim_model, starttime, endtime, -2.3456, 10)
else:
    ribasim_model.level_boundary.static.df.level = default_level

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

inlaat_structures += [3685]  # add some more outlets (created due to FB, hence not in FF)
# for node in inlaat_structures:
#     ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_aanvoer"] = 1

# prepare 'aanvoergebieden'
if AANVOER_CONDITIONS:
    aanvoergebieden = supply.special_load_geometry(
        f_geometry=str(aanvoer_path),
        method="extract",
        # layer="aanvoer",
        key="aanvoer",
        value=1,
    )
else:
    aanvoergebieden = None

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

# add control, based on the meta_categorie
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(
    ribasim_model,
    aanvoergebieden,
    processor,
    outlet_aanvoer_on=tuple(inlaat_structures),
    aanvoer_enabled=AANVOER_CONDITIONS,
)
# ribasim_param.add_discrete_control(ribasim_model, waterschap, default_level)
ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap, aanvoer_upstream_offset=0.04)
ribasim_param.add_continuous_control(ribasim_model, dy=-50, node_id_raiser=50000)

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.manning_resistance.node.df = ribasim_model.manning_resistance.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 25
ribasim_model.manning_resistance.static.df.manning_n = 0.02

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
    custom_nodes=None,
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
