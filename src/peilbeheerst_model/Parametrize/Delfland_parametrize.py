"""Parametrisation of water board: Delfland."""

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
from ribasim.nodes import level_boundary, pump
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets
from shapely import Point

from ribasim_nl import CloudStorage, Model, SetDynamicForcing

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True
DYNAMIC_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "Delfland"
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
    waterschap, "aangeleverd", "Na_levering", "Wateraanvoer", "Delfland_aanvoergebiedafvoergebied.gdb"
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
starttime = datetime.datetime(2017, 1, 1)
endtime = datetime.datetime(2018, 1, 1)
saveat = 3600 * 24
timestep_size = "d"
timesteps = 2
delta_crest_level = 0.1  # delta waterlevel of boezem compared to streefpeil till no water can flow through an outlet
if AANVOER_CONDITIONS:
    default_level = 0.42
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
    ribasim_model = Model(filepath=ribasim_work_dir_model_toml)
    ribasim_model.set_crs("EPSG:28992")

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

inlaat_pump = []

# change gemaal function Brielse Meer to aanvoer. Find node_id first, as it is added in the feedback form
BrielseMeerNodes = ribasim_model.link.df.loc[
    ribasim_model.link.df.from_node_id == 98, "to_node_id"
]  # array of all values from basin 98
BrielseMeerAanvoerPump = ribasim_model.link.df.loc[
    (ribasim_model.link.df.from_node_id.isin(BrielseMeerNodes)) & (ribasim_model.link.df.to_node_id == 10),
    "from_node_id",
]

# convert Brielse Meer from afvoer to aanvoer
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id.isin(BrielseMeerAanvoerPump), "meta_func_afvoer"
] = 0
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id.isin(BrielseMeerAanvoerPump), "meta_func_aanvoer"
] = 1

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(81619, 439852)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[2], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[113])
inlaat_pump.append(pump_node.node_id)

# add gemaal near Rijnland (Dolk). Dont use FF as it is an aanvoergemaal + boundary
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(87256, 455139)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(87082, 455089)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[9])
inlaat_pump.append(pump_node.node_id)

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(70197, 444207)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[41], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[53])
inlaat_pump.append(pump_node.node_id)

for n in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_aanvoer"] = 1

ribasim_model.merge_basins(node_id=73, to_node_id=10, are_connected=True)
ribasim_model.merge_basins(node_id=67, to_node_id=2, are_connected=True)
ribasim_model.merge_basins(node_id=62, to_node_id=2, are_connected=True)
ribasim_model.merge_basins(node_id=61, to_node_id=2, are_connected=True)
ribasim_model.merge_basins(node_id=63, to_node_id=2, are_connected=True)
ribasim_model.merge_basins(node_id=89, to_node_id=16, are_connected=False)
ribasim_model.merge_basins(node_id=71, to_node_id=9, are_connected=True)
ribasim_model.merge_basins(node_id=88, to_node_id=2, are_connected=True)
ribasim_model.merge_basins(node_id=32, to_node_id=50, are_connected=True)
ribasim_model.merge_basins(node_id=54, to_node_id=1, are_connected=True)

ribasim_param.change_pump_func(ribasim_model, 158, "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, 300, "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, 300, "afvoer", 1)
ribasim_model.remove_node(160, True)

# remove Brielse Meer as `Basin` and add as `LevelBoundary`-`Pump`-combination
ribasim_model.remove_node(98, True)
ribasim_model.remove_node(565, True)
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == 460, "meta_func_aanvoer"] = 1
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == 460, "meta_func_afvoer"] = 0
ribasim_model.link.add(ribasim_model.pump[460], ribasim_model.basin[10])

# (re)set 'meta_node_id'-values
ribasim_model.level_boundary.node.df.meta_node_id = ribasim_model.level_boundary.node.df.index
ribasim_model.tabulated_rating_curve.node.df.meta_node_id = ribasim_model.tabulated_rating_curve.node.df.index
ribasim_model.pump.node.df.meta_node_id = ribasim_model.pump.node.df.index

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
        ribasim_model, starttime, endtime, -0.42, -0.4, DYNAMIC_CONDITIONS
    )
else:
    ribasim_model.level_boundary.static.df.level = default_level

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

# add control, based on the meta_categorie
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(
    ribasim_model,
    str(aanvoer_path),
    processor,
    load_geometry_kw={"layer": "Aanvoergebied_Afvoergebied_polders"},
    aanvoer_enabled=AANVOER_CONDITIONS,
)
ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
ribasim_param.add_continuous_control(ribasim_model, dy=-50)

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

# presumably wrong conversion of flow capacity in the data
increase_flow_rate_pumps = [463, 232]
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), "flow_rate"
] *= 60

# set the flow_rate to the max_flow_rate
ribasim_model.pump.static.df.max_flow_rate = ribasim_model.pump.static.df.flow_rate.copy()


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
    ribasim_param.set_dynamic_min_upstream_max_downstream(ribasim_model)

# add the water authority column to couple the model with
assign = AssignAuthorities(
    ribasim_model=ribasim_model,
    waterschap=waterschap,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    custom_nodes={
        532: "Rijkswaterstaat",
    },
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
