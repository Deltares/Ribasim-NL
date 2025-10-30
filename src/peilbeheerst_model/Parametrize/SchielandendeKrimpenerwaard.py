"""Parameterisation of water board: Schieland en de Krimpenerwaard."""

import datetime
import os
import warnings

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.add_storage_basins import AddStorageBasins
from peilbeheerst_model.assign_authorities import AssignAuthorities
from peilbeheerst_model.assign_flushing import Flushing
from peilbeheerst_model.assign_parametrization import AssignMetaData
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets
from shapely import Point

from peilbeheerst_model import supply
from ribasim_nl import CloudStorage, Model, SetDynamicForcing

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True
DYNAMIC_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "SchielandendeKrimpenerwaard"
base_model_versie = "2024_12_1"

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
    waterschap, "aangeleverd", "Na_levering", "Wateraanvoer", "HyDamo_metWasverzachter_20230905.gpkg"
)
meteo_path = cloud.joinpath("Basisgegevens", "WIWB")

cloud.synchronize(
    filepaths=[
        ribasim_base_model_dir,
        FeedbackFormulier_path,
        ws_grenzen_path,
        RWS_grenzen_path,
        qlr_path,
        aanvoer_path,
        meteo_path,
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
starttime = datetime.datetime(2017, 1, 1)
endtime = datetime.datetime(2018, 1, 1)
saveat = 3600 * 24
timestep_size = "d"
timesteps = 2
delta_crest_level = 0.1  # delta waterlevel of boezem compared to streefpeil till no water can flow through an outlet

default_level = 0.75 if AANVOER_CONDITIONS else -0.75  # default LevelBoundary level, similar to surrounding Maas

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
    ribasim_model.set_crs("EPSG:28992")

# model specific tweaks
# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

inlaat_structures = []
inlaat_pump = []
# add the LevelBoundaries and Pumps
# 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(89792, 437301)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(89992, 437341)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[116], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 2
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(92237, 435452)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(92144, 435533)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[110])

# 3
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(95111, 436428)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(95431, 436488)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[157], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 4
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(95111, 436418)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(95736, 436211)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[143], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 5 not sure whether this one is correct. FF was not clear
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(94966, 437011)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(94966, 437011)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[25], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 6
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(92476, 435889)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(92334, 436428)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[142], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 7
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(104547, 443432)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(104485, 443506)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[19], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 7
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(104555, 443434)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(104527, 443455)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[2], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 8
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(102079, 438209)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(102230, 438082)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[29], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 9, added by RB as it is quiet clear a levelboundary is missing.
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(91451, 439245)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(91514, 439286)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[28], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# 10. cooperation day 12 dec 2024: Erik suggested adding an additional pump which already exists, but this would prevent two entire basins interacting with eachother if only one basin would get a pump, while this pump discharges both basins
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(89786, 437309)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(89985, 437345)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[115], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)


# Inlaat (hevel) toevoegen at Delfshaven
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(91122, 436003)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(91107, 436042)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[116])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at north of HHSK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(105536, 448858)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(105485, 448858)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[27])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at Leuvehaven
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(92798, 436794)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(92801, 437070)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[139])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at Schilthuis
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(93880, 437435)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(93776, 437680)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[97])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# # # add gemaal between two basins in Rotterdam. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(95653, 436055)), [pump.Static(flow_rate=[2.5 / 60])])
ribasim_model.link.add(ribasim_model.basin[143], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[157])
inlaat_pump.append(pump_node.node_id)

afvoer_pumps = [230, 387, 579, 366]
for afvoer_pump in afvoer_pumps:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == afvoer_pump, "meta_func_afvoer"] = 1
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == afvoer_pump, "meta_func_aanvoer"] = 0

for n in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_aanvoer"] = 1

ribasim_model.merge_basins(node_id=16, to_node_id=8)  # small (boezem)
ribasim_model.merge_basins(node_id=145, to_node_id=2)  # small (boezem)

# Flow directions have been changed: See feedback form
ribasim_model.remove_node(478, True)
ribasim_model.remove_node(641, False)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(93810, 437547)),
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(93804, 437547)), [pump.Static(flow_rate=[20])])
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "aanvoer", 1)
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "afvoer", 0)
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[97])

# (re)set 'meta_node_id'-values
ribasim_model.level_boundary.node.df.meta_node_id = ribasim_model.level_boundary.node.df.index
ribasim_model.tabulated_rating_curve.node.df.meta_node_id = ribasim_model.tabulated_rating_curve.node.df.index
ribasim_model.pump.node.df.meta_node_id = ribasim_model.pump.node.df.index

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check target levels at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

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
if DYNAMIC_CONDITIONS:
    # Add dynamic meteo
    forcing = SetDynamicForcing(
        model=ribasim_model,
        cloud=cloud,
        startdate=starttime,
        enddate=endtime,
    )

    ribasim_model = forcing.add()

    # Add dynamic groundwater
    offline_budgets = AssignOfflineBudgets()
    offline_budgets.compute_budgets(ribasim_model)

elif MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_forcing(ribasim_model, starttime, endtime, 1)

else:
    forcing_dict = {
        "precipitation": ribasim_param.convert_mm_day_to_m_sec(0 if AANVOER_CONDITIONS else 10),
        "potential_evaporation": ribasim_param.convert_mm_day_to_m_sec(10 if AANVOER_CONDITIONS else 0),
        "drainage": ribasim_param.convert_mm_day_to_m_sec(0),
        "infiltration": ribasim_param.convert_mm_day_to_m_sec(0),
    }
    ribasim_param.set_static_forcing(timesteps, timestep_size, starttime, forcing_dict, ribasim_model)

# reset pump capacity for each pump
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(
        ribasim_model, starttime, endtime, -0.75, 0.75, DYNAMIC_CONDITIONS
    )
else:
    ribasim_model.level_boundary.static.df.level = default_level

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

for node in inlaat_structures:
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_aanvoer"] = 1

# prepare 'aanvoergebieden'
if AANVOER_CONDITIONS:
    aanvoergebieden = supply.special_load_geometry(
        f_geometry=str(aanvoer_path), method="extract", layer="peilbesluitgebied", key="statusobject", value="3"
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
    basin_aanvoer_off=104,
    aanvoer_enabled=AANVOER_CONDITIONS,
)
ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
ribasim_param.add_continuous_control(ribasim_model, dy=-50)

# assign metadata for pumps and basins
assign_metadata = AssignMetaData(
    authority=waterschap,
    model_name=ribasim_model,
    param_name="HHSK.gpkg",
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

increase_flow_rate_pumps = [395]
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), "flow_rate"
] *= 60

# set the max_flow_rate to the flow_rate
ribasim_model.pump.static.df.max_flow_rate = ribasim_model.pump.static.df.flow_rate.copy()

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
    RWS_buffer=400,  # polygons match relatively good, lower buffer
    custom_nodes=None,
)
ribasim_model = assign.assign_authorities()

# Add flushing data
flush = Flushing(ribasim_model)
flush.add_flushing()

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
