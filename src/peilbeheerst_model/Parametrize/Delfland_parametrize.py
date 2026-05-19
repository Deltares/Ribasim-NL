"""Parametrisation of water board: Delfland."""

import datetime
import warnings

import peilbeheerst_model.ribasim_parametrization as ribasim_param
import xarray as xr
from peilbeheerst_model.assign_authorities import AssignAuthorities
from peilbeheerst_model.assign_parametrization import AssignMetaData
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.network_snapping import snap_model
from peilbeheerst_model.outlet_pump_scaler import OutletPumpScalingConfig, scale_outlets_pumps
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import Node, run_ribasim
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from ribasim_nl.assign_lhm_fractions import assign_lhm_fractions
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets
from ribasim_nl.control import (
    add_controllers_to_connector_nodes,
    add_function_to_peilbeheerst_node_table,
    get_node_table_with_from_to_node_ids,
    set_node_functions,
)
from ribasim_nl.profiles import implement
from ribasim_nl.split_basins import NodeMetaCache, SplitBasins
from shapely import Point

from peilbeheerst_model import supply
from ribasim_nl import CloudStorage, Model, SetDynamicForcing, junctionify, merge_rwzi_model

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True
DYNAMIC_CONDITIONS: bool = True
RESCALE_FLOW_CAPACITIES: bool = True
ADD_LHM_FRACTIONS: bool = True
ADD_RWZI: bool = True
ADD_JUNCTIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

mixed_conditions_design_P = 12
mixed_conditions_design_E = 1.5

# model settings
waterschap = "Delfland"
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
aanvoer_path = cloud.joinpath(
    waterschap, "aangeleverd/Na_levering/Wateraanvoer/Aanvoergebied_Afvoergebied_polders.gpkg"
)
meteo_path = cloud.joinpath("Basisgegevens/WIWB")
profiles_path = cloud.joinpath(waterschap, "verwerkt/profielen")

splitted_basin_2_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_2.gpkg")
splitted_basin_9_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_9.gpkg")
splitted_basin_10_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_10.gpkg")

cloud.synchronize(
    filepaths=[
        ribasim_base_model_dir,
        FeedbackFormulier_path,
        ws_grenzen_path,
        RWS_grenzen_path,
        qlr_path,
        aanvoer_path,
        meteo_path,
        profiles_path,
        splitted_basin_2_path,
        splitted_basin_9_path,
        splitted_basin_10_path,
    ]
)

# refresh only the feedback form from cloud (instead of all "verwerkt" files)
# cloud.download_file(cloud.file_url(FeedbackFormulier_path))

work_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_parameterized")
work_dir.mkdir(parents=True, exist_ok=True)

ribasim_work_dir_model_toml = work_dir.joinpath("ribasim.toml")

# set path to base model toml
ribasim_base_model_toml = ribasim_base_model_dir.joinpath("ribasim.toml")

unknown_streefpeil = (
    0.00012345  # we need a streefpeil to create the profiles, Q(h)-relations, and af- and aanslag peil for pumps
)

# forcing settings
starttime = datetime.datetime(2017, 1, 1)
endtime = datetime.datetime(2020, 1, 1)
saveat = 3600 * 24
timestep_size = "d"
timesteps = 2
delta_crest_level = 0.1  # delta waterlevel of boezem compared to streefpeil till no water can flow through an outlet
default_level = 0.42 if AANVOER_CONDITIONS else -0.42

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

# model specific tweaks

# should have been boezem
ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df.node_id == 3, "meta_categorie"] = "hoofdwater"

# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

# change high initial states to 0
ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df["level"] == 9.999, "level"] = 0.0
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

# move node for improved recognition of the system
ribasim_model.move_node(node_id=564, geometry=Point(79320, 437067))

# add riool gemaal Vlaardingen West
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(81712, 435370), name="Rioolgemaal Vlaardingen West"),
    [pump.Static(flow_rate=[192 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(81737, 435104)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[82], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add another riool gemaal Vettenoord
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(80831, 435239), name="Rioolgemaal Vettenoord"),
    [pump.Static(flow_rate=[29 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(80824, 434959)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[82], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Vlaardingen Oost
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(84440, 436203), name="Rioolgemaal Vlaardingen Oost"),
    [pump.Static(flow_rate=[32 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(84531, 435937)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[31], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Schiedam Oost
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(88064, 436633), name="Rioolgemaal Schiedam Oost"),
    [pump.Static(flow_rate=[62 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(88008, 436382)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[114], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Spangen
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(88312, 437389), name="Rioolgemaal Spangen"),
    [pump.Static(flow_rate=[34 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(88309, 436358)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[102], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Oud-Mathenesse
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(88657, 436591), name="Rioolgemaal Oud-Mathenesse"),
    [pump.Static(flow_rate=[7.2 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(88617, 436314)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[27], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Poldervaartpolder
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(85395, 436393), name="Rioolgemaal Poldervaartpolder"),
    [pump.Static(flow_rate=[1.0])],  # unknown, geusstimate
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(85578, 435846)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[59], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

inlaat_structures = []
# add inlaat Bergsluis. Do not add inlaat Schiegemaal, as there is already an inlaat
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(91595, 439326)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(91506, 439278), name="Inlaat Bergsluis"),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[9])
inlaat_structures.append(tabulated_rating_curve_node.node_id)


for n in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_aanvoer"] = 1

ribasim_model.node._update_used_ids()  # pyrefly: ignore[not-callable]
ribasim_model.merge_basins(node_id=115, to_node_id=9, are_connected=False)
ribasim_model.merge_basins(node_id=116, to_node_id=13, are_connected=False)
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
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

for node in inlaat_structures:
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_aanvoer"] = 1

ribasim_param.clean_tables(ribasim_model, waterschap)

# split large basins: "boezems"
node_cache = NodeMetaCache(ribasim_model)
for splitted_basin_path, basin_id in zip(
    [splitted_basin_2_path, splitted_basin_9_path, splitted_basin_10_path], [2, 9, 10], strict=True
):
    # split basins to improve model convergence
    splitter = SplitBasins(
        model=ribasim_model, splitted_basin_path=splitted_basin_path, basin_node_id_to_split=basin_id
    )
    ribasim_model = splitter.run()

node_cache.set_meta_category(ribasim_model)
ribasim_model.write(ribasim_work_dir_model_toml)
del node_cache

# add junctions and network snapping
if ADD_JUNCTIONS:
    ribasim_model = snap_model(ribasim_model, profiles_path)
    ribasim_model = junctionify(ribasim_model)

# set basin profiles
implement.set_basin_profiles(ribasim_model, waterschap, cloud=cloud)  # , min_area=100

# check if meta_categorie in the basin.node.df is completely filled
missing_meta_categorie_node_ids = ribasim_model.basin.node.df.loc[
    ribasim_model.basin.node.df["meta_categorie"].isna()
].index.tolist()
if missing_meta_categorie_node_ids:
    raise ValueError(
        "Not all basins have a meta_categorie assigned. "
        f"Missing meta_categorie for basin node IDs: {missing_meta_categorie_node_ids}"
    )

# set forcing
if DYNAMIC_CONDITIONS:
    # Add dynamic meteo and groundwater from LHM zarr
    lhm_budget_path = cloud.joinpath("Basisgegevens/LHM/4.3/results/LHM_433_budgets_update_makkink")
    cloud.synchronize(filepaths=[lhm_budget_path], overwrite=False)
    budgets = xr.open_zarr(str(lhm_budget_path)).sel(time=slice(starttime, endtime))
    offline_budgets = AssignOfflineBudgets(budgets)

    forcing = SetDynamicForcing(
        model=ribasim_model,
        budgets=budgets,
        startdate=starttime,
        enddate=endtime,
    )
    ribasim_model = forcing.add()
    offline_budgets.compute_budgets(ribasim_model)
    assign_validation_path = work_dir / "results" / "assign_validation.png"
    assign_validation_path.parent.mkdir(parents=True, exist_ok=True)
    offline_budgets.plot_assign_validation(ribasim_model, path=assign_validation_path)


elif MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_forcing(
        ribasim_model, starttime, endtime, mixed_conditions_design_P, mixed_conditions_design_E
    )
else:
    forcing_dict = {
        "precipitation": ribasim_param.convert_mm_day_to_m_sec(0 if AANVOER_CONDITIONS else mixed_conditions_design_P),
        "potential_evaporation": ribasim_param.convert_mm_day_to_m_sec(
            mixed_conditions_design_E if AANVOER_CONDITIONS else 0
        ),
        "drainage": ribasim_param.convert_mm_day_to_m_sec(0),
        "infiltration": ribasim_param.convert_mm_day_to_m_sec(0),
    }
    ribasim_param.set_static_forcing(timesteps, timestep_size, starttime, forcing_dict, ribasim_model)

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(
        ribasim_model, starttime, endtime, -0.42, -0.4, DYNAMIC_CONDITIONS
    )
else:
    ribasim_model.level_boundary.static.df["level"] = default_level


# add control, based on the meta_categorie
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(
    ribasim_model,
    str(aanvoer_path),
    processor,
    aanvoer_enabled=AANVOER_CONDITIONS,
)
# Apply outlet meta_aanvoer labelling and overrule non-hoofdwater routes when direct hoofdwater supply exists.
supply.SupplyOutlet(ribasim_model).exec(overruling_enabled=True)

ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)

# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)
from_to_node_table = get_node_table_with_from_to_node_ids(ribasim_model)
from_to_node_function_table = add_function_to_peilbeheerst_node_table(ribasim_model, from_to_node_table)
from_to_node_function_table["demand"] = None
# manual adjustments to control settings
to_supply = (
    150,
    163,
    167,
    179,
    201,
    223,
    224,
    239,
    245,
    247,
    258,
    306,
    353,
    371,
    377,
    403,
    468,
    475,
    525,
)
set_node_functions(from_to_node_function_table, to_supply=to_supply)

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

# # Add flushing data
# flush = Flushing(ribasim_model)
# _, df_demand = flush.add_flushing()
# from_to_node_function_table = flush.update_function_table(df_demand, from_to_node_function_table)

add_controllers_to_connector_nodes(
    model=ribasim_model,
    node_functions_df=from_to_node_function_table,
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

# wateraanvoer node to other waterboard. Set max downstream level to a low value to prevent unwanted control actions
ribasim_model.outlet.static.df.loc[
    ribasim_model.outlet.static.df.node_id == 433, "max_downstream_level"
] = -0.63  # 2 cm below Rijnlands streefpeil, to avoid too much water entering from Delfland


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
increase_flow_rate_pumps = [474, 298]
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), "flow_rate"
] *= 60

# set the flow_rate to the max_flow_rate
ribasim_model.pump.static.df["max_flow_rate"] = ribasim_model.pump.static.df["flow_rate"].copy()

ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 559, "max_flow_rate"] = (
    1.5  # TODO: Guessed value, ask Delfland
)
# set the pumps and outlets with unknown flow capacities to have unknown flow capacities in the model, so they can be scaled in the next step.
ribasim_model.outlet.static.df["meta_known_flow_capacities"] = False
ribasim_model.pump.static.df.loc[
    (ribasim_model.pump.static.df.max_flow_rate.isna()) | (ribasim_model.pump.static.df.max_flow_rate == 0),
    "meta_known_flow_capacities",
] = False

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")
# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df["length"] = 100.0
ribasim_model.manning_resistance.static.df["manning_n"] = 0.01

# decrease aanslagpeil for Dolkgemaal
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 569, "max_downstream_level"] -= (
    0.05  # 5 cm lower than streefpeil to make sure Winsemius pumps first
)
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 3118, "threshold_high"
] -= 0.05
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 3118, "threshold_low"
] -= 0.05


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
        530: "Noordzee",
        532: "Rijkswaterstaat",
        543: "Noordzee",
    },
    fill_na_authority="Rijkswaterstaat",
)
ribasim_model = assign.assign_authorities()

# merge RWZI model
if ADD_RWZI:
    ribasim_model = merge_rwzi_model(ribasim_model, cloud.joinpath("Rijkswaterstaat/modellen/rwzi/rwzi.toml"))

# add LHM fractions
if ADD_LHM_FRACTIONS:
    assign_lhm_fractions(ribasim_model)

# set the pumps and outlets with unknown flow capacities to have unknown flow capacities in the model, so they can be scaled in the next step.
ribasim_model.outlet.static.df["meta_known_flow_rate"] = False
ribasim_model.pump.static.df["meta_known_flow_rate"] = True
ribasim_model.pump.static.df.loc[
    (ribasim_model.pump.static.df.max_flow_rate.isna()) | (ribasim_model.pump.static.df.max_flow_rate == 0),
    "meta_known_flow_rate",
] = False

# If RESCALE_FLOW_CAPACITIES: scale max_flow_rates of the connector nodes which have no predefined max_flow_rates. If not, load the from_to_node_function_table with the scaled max flow rates from GoodCloud.
ribasim_model, from_to_node_function_table = scale_outlets_pumps(
    OutletPumpScalingConfig(
        ribasim_model_path=ribasim_work_dir_model_toml,
        ribasim_model=ribasim_model,
        from_to_node_function_table=from_to_node_function_table,
        waterschap=waterschap,
        cloud=cloud,
        rescale_flow_capacities=RESCALE_FLOW_CAPACITIES,
        design_precipitation_event=mixed_conditions_design_P,
        design_potential_evaporation_event=mixed_conditions_design_E,
    )
)

# remove outlets where both aanvoer as well as afvoer state have a max_flow_rate of 0.001 or lower
outlet_static = ribasim_model.outlet.static.df.copy()
afvoer_node_ids = outlet_static.loc[
    (outlet_static["control_state"] == "afvoer") & (outlet_static["max_flow_rate"] <= 0.001),
    "node_id",
]
aanvoer_node_ids = outlet_static.loc[
    (outlet_static["control_state"] == "aanvoer") & (outlet_static["max_flow_rate"] <= 0.001),
    "node_id",
]
too_low_max_flow_rates_outlets = afvoer_node_ids[afvoer_node_ids.isin(aanvoer_node_ids)].unique()
flow_control_links = ribasim_model.link.df.loc[
    ribasim_model.link.df.to_node_id.isin(too_low_max_flow_rates_outlets)
    & (ribasim_model.link.df.link_type == "flow_control"),
    ["from_node_id", "to_node_id"],
].drop_duplicates()

for flow_control_link in flow_control_links.itertuples(index=False):
    ribasim_model.remove_node(node_id=flow_control_link.to_node_id, remove_links=True)
    ribasim_model.remove_node(node_id=flow_control_link.from_node_id, remove_links=True)

# check if meta_categorie in the basin.node.df is completely filled
missing_meta_categorie_node_ids = ribasim_model.basin.node.df.loc[
    ribasim_model.basin.node.df["meta_categorie"].isna()
].index.tolist()
if missing_meta_categorie_node_ids:
    raise ValueError(
        "Not all basins have a meta_categorie assigned. "
        f"Missing meta_categorie for basin node IDs: {missing_meta_categorie_node_ids}"
    )

# set numerical settings
# write model output
ribasim_model.use_validation = True
ribasim_model.starttime = starttime
ribasim_model.endtime = endtime
ribasim_model.solver.saveat = saveat
ribasim_model.write(ribasim_work_dir_model_toml)

# run model
run_ribasim(ribasim_work_dir_model_toml)
ribasim_model.update_state()
ribasim_model.basin.state.write()

# model performance
controle_output = Control(work_dir=work_dir, qlr_path=qlr_path)
indicators = controle_output.run_dynamic_forcing() if MIXED_CONDITIONS else controle_output.run_all()
