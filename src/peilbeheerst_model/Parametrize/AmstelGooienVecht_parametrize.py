"""Parameterisation of water board: Amstel, Gooi en Vecht."""

import datetime
import os
import warnings

from ribasim import Node
from ribasim.config import Solver
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import Point

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model import supply
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
waterschap = "AmstelGooienVecht"
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
aanvoer_path = cloud.joinpath(
    waterschap, "aangeleverd", "Na_levering", "Wateraanvoer", "afvoergebiedaanvoergebied.gpkg"
)

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

# download the Feedback Formulieren, overwrite the old ones
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
default_level = -0.42  # default LevelBoundary level

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

# merge basins
ribasim_model.merge_basins(node_id=162, to_node_id=177, are_connected=False)
ribasim_model.merge_basins(node_id=178, to_node_id=177, are_connected=True)

# model specific tweaks
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(125905, 486750)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(125958, 486838)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[225])

# add additional pump and LB to ARK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(129850, 480894)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(129829, 480893)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[229], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add outlet and LB from ARK-NZK to Loosdrechtse Plassen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(129097, 468241)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(129097, 468241)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[59])

# add outlet to Gooimeer
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(146641, 479856)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(146592, 479749)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[215], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# add additional pump and LB to ARK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(129677, 482929)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(129674, 482974)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[228], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(119234, 492570)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(119234, 492587)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[9])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(123115, 489284)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(123112, 489351)), [pump.Static(flow_rate=[20])])
# TODO: Verify setting to 'aan- & afvoergemaal'
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[12])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(124470, 489070)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(124465, 489070)), [tabulated_rating_curve.Static(level=[0, 0.1234], flow_rate=[0, 0.1234])]
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[182])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(127630, 485961)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(127621, 485961)), [tabulated_rating_curve.Static(level=[0, 0.1234], flow_rate=[0, 0.1234])]
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[174])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(148516, 478866)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(148397, 478753)), [pump.Static(flow_rate=[20])])
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[168])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(148550, 478832)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(148437, 478733)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(ribasim_model.basin[168], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

ribasim_model.remove_node(350, True)
aanvoer_pump_ids = [430]
for pid in aanvoer_pump_ids:
    ribasim_param.change_pump_func(ribasim_model, pid, "aanvoer", 1)
    ribasim_param.change_pump_func(ribasim_model, pid, "afvoer", 0)

# TODO: Temporary fixes
ribasim_model.remove_node(350, True)
aanvoer_pump_ids = 251, 430, 644, 673, 769, 784, 842
for pid in aanvoer_pump_ids:
    ribasim_param.change_pump_func(ribasim_model, pid, "aanvoer", 1)
    ribasim_param.change_pump_func(ribasim_model, pid, "afvoer", 0)

#  a multipolygon occurs in some basins (88, 62). Only retain the largest value
multipolygon_basins = [88, 62]
for basin_to_explode in multipolygon_basins:
    exploded_basins = ribasim_model.basin.area.df.loc[
        ribasim_model.basin.area.df["node_id"] == basin_to_explode
    ].explode(index_parts=False)
    exploded_basins["area"] = exploded_basins.area
    largest_polygon = exploded_basins.sort_values(by="area", ascending=False).iloc[0]
    ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df.node_id == basin_to_explode, "geometry"] = (
        largest_polygon["geometry"]
    )

# set all 'meta_node_id'-values
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

ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

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

# set forcing
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
ribasim_model.pump.static.df["flow_rate"] = 0.16667  # 10 kuub per minuut

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(ribasim_model, starttime, endtime, -0.42, 0.42)
else:
    ribasim_model.level_boundary.static.df.level = default_level

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

# prepare 'aanvoergebieden'
if AANVOER_CONDITIONS:
    aanvoergebieden = supply.special_load_geometry(
        f_geometry=aanvoer_path, method="inverse", layers=("afvoergebiedaanvoergebied", "afwateringsgebied")
    )
else:
    aanvoergebieden = None

# add control, based on the meta_categorie
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(
    ribasim_model,
    aanvoergebieden,
    processor,
    basin_aanvoer_on=38,
    basin_aanvoer_off=(1, 53, 134, 144, 196, 222),
    aanvoer_enabled=AANVOER_CONDITIONS,
)
ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
ribasim_param.add_continuous_control(ribasim_model, dy=-200)

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 100
ribasim_model.manning_resistance.static.df.manning_n = 0.01

# last formating of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)

if MIXED_CONDITIONS:
    ribasim_model.basin.static.df = None
    ribasim_param.set_dynamic_min_upstream_max_downstream(ribasim_model)

# assign authorities
assign = AssignAuthorities(
    ribasim_model=ribasim_model,
    waterschap=waterschap,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    custom_nodes={
        907: "HollandsNoorderkwartier",
        1050: "HollandsNoorderkwartier",
        908: "HollandsNoorderkwartier",
        905: "Rijkswaterstaat",
        909: "Rijkswaterstaat",
        931: "Rijkswaterstaat",
        966: "Rijkswaterstaat",
        2956: "Rijkswaterstaat",
        994: "Rijnland",
    },
)
ribasim_model = assign.assign_authorities()
if MIXED_CONDITIONS:
    ribasim_model = assign.from_static_to_time_df(ribasim_model, clear_static=True)

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
