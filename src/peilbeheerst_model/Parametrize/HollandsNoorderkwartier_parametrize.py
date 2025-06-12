"""Parameterisation of water board: Hollands Noorderkwartier."""

import datetime
import os
import warnings

from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import Point

import peilbeheerst_model.ribasim_parametrization as ribasim_param
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
waterschap = "HollandsNoorderkwartier"
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
    waterschap, "aangeleverd", "Na_levering", "20240618_peilgebieden_en_polders", "Polders_export_2024-06-18.shp"
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
default_level = 0.42 if AANVOER_CONDITIONS else -0.42  # default LevelBoundary level

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
# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

# add spui Schermerboezem Den Helder (near gemaal Helsdeur)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(114728, 551405)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(114698, 551327)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[3], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# add hevel near Andijk
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(141549, 529097)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(141591, 529071)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[66])

# add hevel near stedelijk gebied ARK-NZK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(113959, 493654)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(113933, 493655)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[160])

# add hevel near stedelijk gebied ARK-NZK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(109989, 494265)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(110059, 494258)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[221])

# add gemaal and LB at boezemgemaal Monnickendam. Zowel af- als aanvoer. Dit blok: aanvoer
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(131027, 497603)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(130877, 497615)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[3])

# add gemaal and LB at boezemgemaal Monnickendam. Zowel af- als aanvoer. Dit blok: afvoer
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(131027, 497613)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(130877, 497625)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(ribasim_model.basin[3], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add gemaal and LB at Waterlandseboezem .
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(132115, 495290)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(132040, 495282)), [pump.Static(flow_rate=[10])])
ribasim_model.link.add(ribasim_model.basin[2], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add hevelat Schermersluis (ARK-NZK --> boezem)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(111930, 494827)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(111934, 494910)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[3])

ribasim_model.merge_basins(node_id=201, to_node_id=79, are_connected=False)  # klein snippertje vlakbij IJsselmeer
ribasim_model.merge_basins(node_id=192, to_node_id=17)  # klein snippertje vlakbij Markermeer
ribasim_model.merge_basins(node_id=182, to_node_id=2)  # klein snippertje
ribasim_model.merge_basins(node_id=214, to_node_id=3)  # klein snippertje in boezem
ribasim_model.merge_basins(node_id=117, to_node_id=6)  # klein gebiedje in enclave peilgebied
ribasim_model.merge_basins(node_id=218, to_node_id=3)  # klein gebiedje vlakbij boezem
ribasim_model.merge_basins(node_id=113, to_node_id=3)  # klein gebiedje in boezem
ribasim_model.merge_basins(node_id=203, to_node_id=21)  # klein gebiedje in duinen

# TODO: Temporary fixes
# Changed direction of three pumps: See updated feedback form.

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
    ribasim_param.set_hypothetical_dynamic_level_boundaries(ribasim_model, starttime, endtime, -0.42, 0.42)
else:
    ribasim_model.level_boundary.static.df.level = default_level

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

# prepare 'aanvoergebieden'
# fmt: off
basin_aanvoer_off = (
    48, 131, 20, 180, 111,  # Texel
    5, 75,  # Wieringermeer
    163, 88, 46, 78, 129,  # duinen
)
# fmt: on

# assign metadata for pumps and basins
assign_metadata = AssignMetaData(
    authority=waterschap,
    model_name=ribasim_model,
    param_name="Noorderkwartier.gpkg",
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
    ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=AANVOER_CONDITIONS, basin_aanvoer_off=basin_aanvoer_off
)
ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
ribasim_param.add_continuous_control(ribasim_model, dy=-50)

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.manning_resistance.node.df.dropna(subset="geometry", inplace=True)

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 10
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
    RWS_buffer=3000,  # due to harbour
    custom_nodes={
        4565: "AmstelGooienVecht",
        1394: "AmstelGooienVecht",
        3657: "AmstelGooienVecht",
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
