"""Parameterisation of water board: Rijnland."""

import datetime
import warnings

import peilbeheerst_model.ribasim_parametrization as ribasim_param
import xarray as xr
from peilbeheerst_model.assign_authorities import AssignAuthorities
from peilbeheerst_model.assign_parametrization import AssignMetaData
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.outlet_pump_scaler import OutletPumpScalingConfig, scale_outlets_pumps
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import Node, cli
from ribasim.nodes import level_boundary, pump
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets
from ribasim_nl.control import (
    add_controllers_to_connector_nodes,
    add_function_to_peilbeheerst_node_table,
    get_node_table_with_from_to_node_ids,
    remove_duplicate_controls,
    set_node_functions,
)
from ribasim_nl.profiles import implement
from shapely import Point

from peilbeheerst_model import supply
from ribasim_nl import CloudStorage, Model, SetDynamicForcing

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True
DYNAMIC_CONDITIONS: bool = False
RESCALE_FLOW_CAPACITIES: bool = False

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

MIXED_CONDITIONS_DESIGN_P = 12
MIXED_CONDITIONS_DESIGN_E = 2

# model settings
waterschap = "Rijnland"
base_model_versie = "2024_12_3"

# connect with the GoodCloud
cloud = CloudStorage()

# collect data from the base model, feedback form, waterauthority & RWS border
ribasim_base_model_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_boezemmodel_{base_model_versie}")
FeedbackFormulier_path = cloud.joinpath(
    waterschap, "verwerkt/Feedback Formulier", f"feedback_formulier_{waterschap}.xlsx"
)
FeedbackFormulier_LOG_path = cloud.joinpath(
    waterschap, "verwerkt/Feedback Formulier", f"feedback_formulier_{waterschap}_LOG.xlsx"
)
ws_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")
RWS_grenzen_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/Rijkswaterstaat.gpkg")
qlr_name = "output_controle_cc.qlr" if MIXED_CONDITIONS else "output_controle_202502.qlr"
qlr_path = cloud.joinpath("Basisgegevens/QGIS_qlr", qlr_name)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/RL_aanvoer.shp")
meteo_path = cloud.joinpath("Basisgegevens/WIWB")
profiles_path = cloud.joinpath(waterschap, "verwerkt/profielen")

# cloud.synchronize(
#     filepaths=[
#         ribasim_base_model_dir,
#         FeedbackFormulier_path,
#         ws_grenzen_path,
#         RWS_grenzen_path,
#         qlr_path,
#         aanvoer_path,
#         meteo_path,
#         profiles_path,
#     ]
# )

# refresh only the feedback form from cloud
cloud.download_file(cloud.file_url(FeedbackFormulier_path))

# set paths to the TEMP working directory
work_dir = cloud.joinpath(waterschap, "verwerkt/Work_dir", f"{waterschap}_parameterized")
ribasim_gpkg = work_dir.joinpath("database.gpkg")
ribasim_work_dir_model_toml = work_dir.joinpath("ribasim.toml")

# set path to base model toml
ribasim_base_model_toml = ribasim_base_model_dir.joinpath("ribasim.toml")

# # create work_dir/parameterized
# parameterized = os.path.join(work_dir, f"{waterschap}_parameterized/")
# os.makedirs(parameterized, exist_ok=True)

# define variables and model
# basin area percentage
# regular_percentage = 10
# boezem_percentage = 90
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
default_level = 2 if AANVOER_CONDITIONS else -2  # default LevelBoundary level

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
    ribasim_model = Model.read(ribasim_work_dir_model_toml)
    ribasim_model.set_crs("EPSG:28992")

inlaat_pump = []

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(88007, 469350)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[22], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[27])
inlaat_pump.append(pump_node.node_id)

# add levelboundary to avoid incorrect coupling of water authorities
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(110865, 446289)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(110884, 446307)), [pump.Static(flow_rate=[1.5])])
ribasim_model.link.add(ribasim_model.basin[338], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

for n in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_aanvoer"] = 1

# (re)set 'meta_node_id'-values
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# model specific tweaks
# merge basins
ribasim_model.merge_basins(node_id=106, to_node_id=93, are_connected=True)  # (too) small area
ribasim_model.merge_basins(node_id=235, to_node_id=151, are_connected=True)  # (too) small area
ribasim_model.merge_basins(node_id=166, to_node_id=22, are_connected=True)  # (too) small area
ribasim_model.merge_basins(node_id=79, to_node_id=22, are_connected=True)  # (too) small area
# unconnected basins
ribasim_model.merge_basins(node_id=308, to_node_id=22, are_connected=False)
ribasim_model.merge_basins(node_id=332, to_node_id=138, are_connected=False)

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(88007, 469350)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[22], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[27])
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1

# (re)set 'meta_node_id'-values
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

# # insert standard profiles to each basin: these are [depth_profiles] meter deep, defined from the streefpeil
# ribasim_param.insert_standard_profile(
#     ribasim_model,
#     unknown_streefpeil=unknown_streefpeil,
#     regular_percentage=regular_percentage,
#     boezem_percentage=boezem_percentage,
#     depth_profile=2,
# )
#
# add_storage_basins = AddStorageBasins(
#     ribasim_model=ribasim_model, exclude_hoofdwater=True, additional_basins_to_exclude=[]
# )
#
# add_storage_basins.create_bergende_basins()

implement.set_basin_profiles(ribasim_model, waterschap, cloud=cloud, min_area=10)

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
    if offline_budgets.lhm_budget_path.exists():
        offline_budgets._sync_files = lambda model: (xr.open_zarr(str(offline_budgets.lhm_budget_path)), model)
    offline_budgets.compute_budgets(ribasim_model)

elif MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_forcing(
        ribasim_model, starttime, endtime, MIXED_CONDITIONS_DESIGN_P, MIXED_CONDITIONS_DESIGN_E
    )

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
        ribasim_model, starttime, endtime, -2, 2, DYNAMIC_CONDITIONS
    )
else:
    ribasim_model.level_boundary.static.df.level = default_level

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

# prepare 'aanvoergebieden'
if AANVOER_CONDITIONS:
    aanvoergebieden = supply.special_load_geometry(f_geometry=aanvoer_path, method="extract", key="aanvoer", value=1)
    # remove double-polygons
    aanvoergebieden = aanvoergebieden[aanvoergebieden["node_id"] == aanvoergebieden["meta_node_"]]
else:
    aanvoergebieden = None

# add control, based on the meta_categorie
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(ribasim_model, aanvoergebieden, processor, aanvoer_enabled=AANVOER_CONDITIONS)
supply.SupplyOutlet(ribasim_model).exec(overruling_enabled=True)
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

LEVEL_DIFFERENCE_THRESHOLD = 0.02
ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

from_to_node_table = get_node_table_with_from_to_node_ids(ribasim_model)
from_to_node_function_table = add_function_to_peilbeheerst_node_table(ribasim_model, from_to_node_table)
from_to_node_function_table["demand"] = None

to_drain = (
    525,
    530,
    1096,
    1307,
    1456,
)
to_flow_control = (
    487,
    625,
    630,
    885,
    963,
    1148,
    1179,
    1180,
)
to_supply = (
    398,
    619,
    690,
    1032,
    1255,
    1349,
    1454,
    1471,
)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)


outlet_copy = ribasim_model.outlet.static.df[
    [
        "node_id",
        "meta_categorie",
        "meta_from_node_id",
        "meta_to_node_id",
        "meta_from_level",
        "meta_to_level",
        "meta_aanvoer",
    ]
].copy()
pump_copy = ribasim_model.pump.static.df[
    [
        "node_id",
        "meta_categorie",
        "meta_func_afvoer",
        "meta_func_aanvoer",
        "meta_func_circulatie",
        "meta_from_node_id",
        "meta_to_node_id",
        "meta_from_level",
        "meta_to_level",
    ]
].copy()

# flush = Flushing(
#     ribasim_model,
#     lhm_flushing_path="Rivierenland/aangeleverd/Na_levering/Doorspoeling.gpkg",
#     flushing_layer="aanvoergebieden",
#     flushing_id="UniekID",
# )
# _, df_demand = flush.add_flushing(df_function=from_to_node_function_table)
# from_to_node_function_table = flush.update_function_table(df_demand, from_to_node_function_table)

add_controllers_to_connector_nodes(
    ribasim_model, from_to_node_function_table, LEVEL_DIFFERENCE_THRESHOLD, drain_capacity=20
)
remove_duplicate_controls(ribasim_model)

outlet_columns_to_add_back = [
    "meta_categorie",
    "meta_from_node_id",
    "meta_to_node_id",
    "meta_from_level",
    "meta_to_level",
    "meta_aanvoer",
]
pump_columns_to_add_back = [
    "meta_categorie",
    "meta_func_afvoer",
    "meta_func_aanvoer",
    "meta_func_circulatie",
    "meta_from_node_id",
    "meta_to_node_id",
    "meta_from_level",
    "meta_to_level",
]
ribasim_model.outlet.static.df = ribasim_model.outlet.static.df.drop(columns=outlet_columns_to_add_back).merge(
    outlet_copy, on="node_id", how="left"
)
ribasim_model.pump.static.df = ribasim_model.pump.static.df.drop(columns=pump_columns_to_add_back).merge(
    pump_copy, on="node_id", how="left"
)

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

# according data flow_rate of 0
zero_flow_pumps = [1436, 1282, 1472]
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"].isin(zero_flow_pumps), "flow_rate"] = 25

# presumably wrong conversion of flow capacity in the data
increase_flow_rate_pumps = [793, 754, 987, 354, 463, 1179, 496, 781]
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), "flow_rate"
] *= 60

# set the flow_rate to the max_flow_rate
ribasim_model.pump.static.df.max_flow_rate = ribasim_model.pump.static.df.flow_rate.copy()

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")

# # set LevelBoundary-nodes level to AGV-basins
# lb_static = ribasim_model.level_boundary.static.df.copy()
# # > Rijnland -> Zuider Legmeerpolder (AGV)
# lb_ids = 1137, 1147, 1213, 1247, 1261, 1270, 1299
# lb_static.loc[lb_static['node_id'].isin(lb_ids), 'level'] = -5.97
# # > Rijnland -> Binnendijkse Buitenvelderse Polder (AGV)
# lb_ids = 1028, 1317
# lb_static.loc[lb_static['node_id'].isin(lb_ids), 'level'] = -2.
# # > Rijnland -> Riekerpolder (AGV)
# lb_ids = 1095, 1303
# lb_static.loc[lb_static['node_id'].isin(lb_ids), 'level'] = -.6
# # > Rijnland -> Middelveldse Akerpolder (AGV)
# lb_ids = 1065,
# lb_static.loc[lb_static['node_id'].isin(lb_ids), 'level'] = -4.45
# # > Rijnland -> Osdorperbinnenpolder (AGV)
# lb_ids = 1125, 1127
# lb_static.loc[lb_static['node_id'].isin(lb_ids), 'level'] = -2.17
# # > update LevelBoundary-Static table
# ribasim_model.level_boundary.static.df = lb_static.copy()
# del lb_static

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 10
ribasim_model.manning_resistance.static.df.manning_n = 0.01

# last formatting of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)
if MIXED_CONDITIONS:
    ribasim_model.basin.static.df = None
    ribasim_param.set_dynamic_min_upstream_max_downstream(ribasim_model)

ribasim_model.outlet.static.df["meta_known_flow_rate"] = False
ribasim_model.pump.static.df["meta_known_flow_rate"] = True
ribasim_model.pump.static.df.loc[
    (ribasim_model.pump.static.df["max_flow_rate"].isna()) | (ribasim_model.pump.static.df["max_flow_rate"] == 0),
    "meta_known_flow_rate",
] = False

ribasim_model, from_to_node_table = scale_outlets_pumps(
    OutletPumpScalingConfig(
        ribasim_model_path=ribasim_work_dir_model_toml,
        ribasim_model=ribasim_model,
        from_to_node_function_table=from_to_node_function_table,
        waterschap=waterschap,
        cloud=cloud,
        rescale_flow_capacities=RESCALE_FLOW_CAPACITIES,
        max_iterations=12,
        design_precipitation_event=MIXED_CONDITIONS_DESIGN_P,
        design_potential_evaporation_event=MIXED_CONDITIONS_DESIGN_E,
    )
)

# add the water authority column to couple the model with
assign = AssignAuthorities(
    ribasim_model=ribasim_model,
    waterschap=waterschap,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    custom_nodes={
        1367: "Delfland",
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
cli.run_ribasim(ribasim_work_dir_model_toml)

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
