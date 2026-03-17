"""Parameterisation of water board: Amstel, Gooi en Vecht."""

import datetime
import os
import warnings

import peilbeheerst_model.ribasim_parametrization as ribasim_param
import xarray as xr
from peilbeheerst_model.add_storage_basins import AddStorageBasins
from peilbeheerst_model.assign_authorities import AssignAuthorities
from peilbeheerst_model.assign_parametrization import AssignMetaData
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.outlet_pump_scaler import OutletPumpScalingConfig, scale_outlets_pumps
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets
from ribasim_nl.control import (
    add_controllers_to_connector_nodes,
    add_function_to_peilbeheerst_node_table,
    get_node_table_with_from_to_node_ids,
)
from shapely import Point

from peilbeheerst_model import supply
from ribasim_nl import CloudStorage, Model, SetDynamicForcing

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True
DYNAMIC_CONDITIONS: bool = True
RESCALE_FLOW_CAPACITIES: bool = False

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "AmstelGooienVecht"
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
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/afvoergebiedaanvoergebied.gpkg")
meteo_path = cloud.joinpath("Basisgegevens/WIWB")

# cloud.synchronize(
#     filepaths=[
#         ribasim_base_model_dir,
#         FeedbackFormulier_path,
#         ws_grenzen_path,
#         RWS_grenzen_path,
#         qlr_path,
#         aanvoer_path,
#         meteo_path,
#     ]
# )

# download the Feedback Formulieren, overwrite the old ones
cloud.download_file(cloud.file_url(FeedbackFormulier_path))

# set paths to the TEMP working directory
work_dir = cloud.joinpath(waterschap, "verwerkt/Work_dir", f"{waterschap}_parameterized")
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
    ribasim_model = Model(filepath=ribasim_work_dir_model_toml)
    ribasim_model.set_crs("EPSG:28992")

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

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
ribasim_model.remove_node(673, True)
ribasim_model.remove_node(1000, False)
ribasim_param.change_pump_func(ribasim_model, 430, "aanvoer", 1)
ribasim_param.change_pump_func(ribasim_model, 430, "afvoer", 0)

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

# prepare 'aanvoergebieden'
if AANVOER_CONDITIONS:
    aanvoergebieden = supply.special_load_geometry(
        f_geometry=aanvoer_path, method="inverse", layers=("afvoergebiedaanvoergebied", "afwateringsgebied")
    )
else:
    aanvoergebieden = None

# add control, based on the meta_categorie
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(
    ribasim_model,
    aanvoergebieden,
    processor,
    basin_aanvoer_on=38,
    basin_aanvoer_off=(1, 53, 134, 144, 196, 222),
    outlet_aanvoer_on=856,
    aanvoer_enabled=AANVOER_CONDITIONS,
)
# Apply outlet meta_aanvoer labelling and overrule non-hoofdwater routes when direct hoofdwater supply exists.
supply.SupplyOutlet(ribasim_model).exec(overruling_enabled=True)

ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)

# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

LEVEL_DIFFERENCE_THRESHOLD = 0.04
ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

from_to_node_table = get_node_table_with_from_to_node_ids(ribasim_model)
from_to_node_function_table = add_function_to_peilbeheerst_node_table(ribasim_model, from_to_node_table)
from_to_node_function_table["demand"] = None

# change function to inlaat
to_inlaat = [338, 390, 417, 435, 450, 456, 518, 539, 541, 548, 632, 679, 710, 722, 738, 741, 774, 789, 861, 1014, 1018]
from_to_node_function_table.loc[from_to_node_function_table.index.isin(to_inlaat), "function"] = "supply"

# change function to flow_control
to_flow_control = [290, 557, 762, 1013, 1033]
from_to_node_function_table.loc[from_to_node_function_table.index.isin(to_flow_control), "function"] = "flow_control"

# change function to drain
to_drain = [256, 626]
from_to_node_function_table.loc[from_to_node_function_table.index.isin(to_drain), "function"] = "drain"

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

# update node_ids
# ribasim_model = ribasim_model._update_used_ids()
ribasim_model._used_node_ids.max_node_id = ribasim_model.node_table().df.index.max()

add_controllers_to_connector_nodes(
    model=ribasim_model,
    node_functions_df=from_to_node_function_table,
    level_difference_threshold=LEVEL_DIFFERENCE_THRESHOLD,
    target_level_column="meta_streefpeil",
    drain_capacity=20,
)

# replace the meta_data to the pump and outlet tables again, as the add_controllers_to_connector_nodes function might have changed/added node_id's
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

# there are some duplicates in the discrete control? Remove them
control = ribasim_model.link.df[ribasim_model.link.df.link_type == "control"]
dup_control = []
all_nodes = ribasim_model.node_table().df[["node_type"]]
for to_node_id, group in control.groupby("to_node_id"):
    if len(group) == 1:
        continue
    elif len(group) == 2:
        group = group.merge(all_nodes, left_on="from_node_id", right_index=True, how="inner")
        if set(group.node_type.tolist()) == {"DiscreteControl", "FlowDemand"}:
            continue
        else:
            dup_control.append(group.from_node_id.iat[0])
    else:
        raise ValueError(
            f"found {len(group)} incoming control links for {to_node_id=} from {set(group.from_node_id.tolist())}"
        )

for duplicate in dup_control:
    ribasim_model.remove_node(duplicate, True)
    print(f"Removed duplicate control node {duplicate}")


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

# manually assign better names (as suggested by JWV in FF)
ribasim_model.basin.node.df.loc[19, "name"] = "Kromme Mijdrecht"
ribasim_model.basin.node.df.loc[97, "name"] = "Amstel-Drecht kanaal"
ribasim_model.basin.node.df.loc[20, "name"] = "Amstel"
ribasim_model.basin.node.df.loc[85, "name"] = "Amstel"
ribasim_model.basin.node.df.loc[86, "name"] = "Amstel"


ribasim_model.pump.static.df.flow_rate = ribasim_model.pump.static.df.flow_rate.fillna(value=25)
ribasim_model.pump.static.df.max_flow_rate = ribasim_model.pump.static.df.max_flow_rate.fillna(value=25)
increase_flow_rate_pumps = [412, 146]

# presumably wrong conversion of flow capacity in the data
for pump_id in increase_flow_rate_pumps:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_id, "flow_rate"] *= 60

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 100
ribasim_model.manning_resistance.static.df.manning_n = 0.01

# last formating of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)

if MIXED_CONDITIONS:
    ribasim_model.basin.static.df = None
    ribasim_param.set_dynamic_min_upstream_max_downstream(ribasim_model)

# set the pumps and outlets with unknown flow capacities to have unknown flow capacities in the model, so they can be scaled in the next step.
ribasim_model.outlet.static.df.meta_known_flow_capacities = False
ribasim_model.pump.static.df.loc[
    (ribasim_model.pump.static.df.max_flow_rate.isna()) | (ribasim_model.pump.static.df.max_flow_rate == 0),
    "meta_known_flow_capacities",
] = False

# kan weg als er geschaald wordt
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.max_flow_rate == 0, "max_flow_rate"] = 1.0
# If RESCALE_FLOW_CAPACITIES: scale max_flow_rates of the connector nodes which have no predefined max_flow_rates. If not, load the from_to_node_function_table with the scaled max flow rates from GoodCloud.
ribasim_model, from_to_node_function_table = scale_outlets_pumps(
    OutletPumpScalingConfig(
        ribasim_model_path=ribasim_work_dir_model_toml,
        ribasim_model=ribasim_model,
        from_to_node_function_table=from_to_node_function_table,
        apply_temporary_debug_changes=True,
        waterschap=waterschap,
        cloud=cloud,
        rescale_flow_capacities=True,
        max_iterations=20,
    )
)

# manually assign max_flow_rates (as suggested by JWV in FF)
ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df.node_id == 826, "max_flow_rate"] = 0.05

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
