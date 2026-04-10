"""Parameterisation of water board: Scheldestromen."""

import datetime
import warnings

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.assign_authorities import AssignAuthorities
from peilbeheerst_model.assign_parametrization import AssignMetaData
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.outlet_pump_scaler import OutletPumpScalingConfig, scale_outlets_pumps
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import Node, cli
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
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
RESCALE_FLOW_CAPACITIES: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

MIXED_CONDITIONS_DESIGN_P = 12
MIXED_CONDITIONS_DESIGN_E = 2

# model settings
waterschap = "Scheldestromen"
base_model_versie = "2024_12_0"

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
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/WSS_aanvoergebieden.shp")
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

# # refresh only the feedback form from cloud
# cloud.download_file(cloud.file_url(FeedbackFormulier_path))

# set paths to the TEMP working directory
work_dir = cloud.joinpath(waterschap, "verwerkt/Work_dir", f"{waterschap}_parameterized")
work_dir.mkdir(parents=True, exist_ok=True)
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
    ribasim_model = Model.read(ribasim_work_dir_model_toml)
    ribasim_model.set_crs("EPSG:28992")

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check target levels at both sides of the Manning Nodes
ribasim_param.validate_manning_basins(ribasim_model)

# model specific tweaks
# the vrij-afwaterende basins are a multipolygon, in a single basin (189). Only retain the largest value
exploded_basins = ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["node_id"] == 189].explode(
    index_parts=False
)
exploded_basins["area"] = exploded_basins.area
largest_polygon = exploded_basins.sort_values(by="area", ascending=False).iloc[0]
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df.node_id == 189, "geometry"] = largest_polygon["geometry"]

# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

inlaat_structures = []
# add an TRC and links to the newly created level boundary
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(74861, 382484)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(geometry=Point(74504, 382443)), [pump.Static(flow_rate=[0.1])])
ribasim_model.node.df.loc[pump_node.node_id, "meta_node_id"] = pump_node.node_id
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[133])

# add a pump and links to a newly created level boundary
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(65450, 374986)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(65429, 374945)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[148], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add a TRC and LB from Belgium
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(43290, 356428)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(43486, 357740)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[1])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# connection with Belgium
ribasim_model.remove_node(29, True)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(35147, 362794)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(level_boundary_node, ribasim_model.tabulated_rating_curve[491])
ribasim_model.link.add(level_boundary_node, ribasim_model.tabulated_rating_curve[547])
ribasim_model.link.add(level_boundary_node, ribasim_model.tabulated_rating_curve[334])
ribasim_model.link.add(level_boundary_node, ribasim_model.tabulated_rating_curve[554])
ribasim_model.link.add(ribasim_model.tabulated_rating_curve[227], level_boundary_node)
ribasim_model.link.add(ribasim_model.tabulated_rating_curve[381], level_boundary_node)
inlaat_structures.extend([491, 547, 334, 554])
inlaat_structures.append(309)

# (re) set 'meta_node_id'
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

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
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(
        ribasim_model, starttime, endtime, -0.42, 0.42, DYNAMIC_CONDITIONS
    )
    ribasim_model.level_boundary.time.df.loc[ribasim_model.level_boundary.time.df["node_id"] == 583, "level"] = -2
    ribasim_model.level_boundary.time.df.loc[ribasim_model.level_boundary.time.df["node_id"] == 585, "level"] = -2
    ribasim_model.level_boundary.time.df.loc[
        (ribasim_model.level_boundary.time.df.node_id == 635) & (ribasim_model.level_boundary.time.df.level == -0.4),
        "level",
    ] = 0.4  # change value of a single LB during summer time
else:
    ribasim_model.level_boundary.static.df.level = default_level
    ribasim_model.level_boundary.static.df.loc[ribasim_model.level_boundary.static.df.node_id == 583, "level"] = -2
    ribasim_model.level_boundary.static.df.loc[ribasim_model.level_boundary.static.df.node_id == 585, "level"] = -2

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

# add control, based on the meta_categorie
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=AANVOER_CONDITIONS)
supply.SupplyOutlet(ribasim_model).exec(overruling_enabled=True)
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

LEVEL_DIFFERENCE_THRESHOLD = 0.02
ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

for node in inlaat_structures:
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_aanvoer"] = 1
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_afvoer"] = 0

from_to_node_table = get_node_table_with_from_to_node_ids(ribasim_model)
from_to_node_function_table = add_function_to_peilbeheerst_node_table(ribasim_model, from_to_node_table)
from_to_node_function_table["demand"] = None

to_drain = (
    194,
    271,
    302,
    316,
    322,
    412,
    413,
    414,
    415,
)
to_flow_control = (505,)
to_supply = (
    206,
    252,
    265,
    312,
)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

# check for flow_control-/supply-nodes outside supplied-basins
supply_connectors = (
    234,
    241,
    260,
    309,
    334,
    384,
    422,
    505,
    526,  # non-official supplying pumps?
    258,
    266,
    452,
    462,
    473,
    477,
    486,
    491,
    531,
    546,
    547,
    554,
    640,  # inflow from Belgium
    252,
    265,
    206,  # unknown
    312,
    357,
    359,
    363,
    364,
    432,
    474,
    487,
    211,
    220,
    242,
    253,
    283,
    305,
    306,
    310,
    316,
    323,
    343,
    344,
    346,
    347,
    353,
    370,
    425,
    426,
    427,
    428,
    429,
    430,
    431,
    435,
    464,
    631,
    632,
    634,
    371,
    523,
    636,
)
if not all(
    from_to_node_function_table[from_to_node_function_table["function"] == "supply"].index.isin(supply_connectors)
):
    for i in from_to_node_function_table[from_to_node_function_table["function"] == "supply"].index.values:
        if i not in supply_connectors:
            print(f"{i:4d}: invalid supply")
if not all(
    from_to_node_function_table[from_to_node_function_table["function"] == "flow_control"].index.isin(supply_connectors)
):
    for i in from_to_node_function_table[from_to_node_function_table["function"] == "flow_control"].index.values:
        if i not in supply_connectors:
            print(f"{i:4d}: invalid flow-control")

assert all(
    from_to_node_function_table[from_to_node_function_table["function"] == "supply"].index.isin(supply_connectors)
), f"supply:\n{from_to_node_function_table[from_to_node_function_table['function'] == 'supply']}\n"
assert all(
    from_to_node_function_table[from_to_node_function_table["function"] == "flow_control"].index.isin(supply_connectors)
), f"flow control:\n{from_to_node_function_table[from_to_node_function_table['function'] == 'flow_control']}\n"

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

# flush = Flushing(ribasim_model)
# _, df_demand = flush.add_flushing(df_function=from_to_node_function_table)
# from_to_node_function_table = flush.update_function_table(df_demand, from_to_node_function_table)

# set undefined pumps to 'afvoer'
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df[["meta_func_afvoer", "meta_func_aanvoer", "meta_func_circulatie"]].isna().all(axis=1),
    "meta_func_afvoer",
] = 1

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

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")


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
    RWS_buffer=2000,  # mainly neighbouring RWS, so increase buffer. Not too much, due to nodes within Belgium.
    custom_nodes={
        584: "Rijkswaterstaat",  # Westerschelde
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
