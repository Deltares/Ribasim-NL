# %%
"""Parameterisation of water board: Hollandse Delta."""

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
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_qlr", "output_controle_202502.qlr")
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
default_level = 1.24 if AANVOER_CONDITIONS else -0.42  # default LevelBoundary level

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
# merge small basins into larger basins for numerical stability
ribasim_model.merge_basins(node_id=149, to_node_id=21)  # too small basin
ribasim_model.merge_basins(node_id=559, to_node_id=120)  # small basin + deviations
ribasim_model.merge_basins(node_id=7, to_node_id=54)  # small basin causes numerical instabilities
ribasim_model.merge_basins(node_id=720, to_node_id=54)  # small basin causes numerical instabilities
# merge overlapping basins
ribasim_model.merge_basins(node_id=633, to_node_id=475)
ribasim_model.merge_basins(node_id=329, to_node_id=325)


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
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[675])

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
ribasim_model.link.add(pump_node, ribasim_model.basin[777])

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
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[300])

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
ribasim_param.set_aanvoer_flags(ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=AANVOER_CONDITIONS)
# ribasim_param.add_discrete_control(ribasim_model, waterschap, default_level)
ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.manning_resistance.node.df = ribasim_model.manning_resistance.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 100
ribasim_model.manning_resistance.static.df.manning_n = 0.01

# last formating of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)

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

# set numerical settings
ribasim_model.use_validation = True
ribasim_model.starttime = datetime.datetime(2024, 1, 1)
ribasim_model.endtime = datetime.datetime(2025, 1, 1)
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
