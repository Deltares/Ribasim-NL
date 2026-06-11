"""Parameterisation of water board: Hollandse Delta."""

import datetime

import peilbeheerst_model.ribasim_parametrization as ribasim_param
import xarray as xr
from peilbeheerst_model.assign_authorities import AssignAuthorities
from peilbeheerst_model.assign_parametrization import AssignMetaData
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.network_snapping import snap_model
from peilbeheerst_model.outlet_pump_scaler import OutletPumpScalingConfig, scale_outlets_pumps
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import run_ribasim
from ribasim_nl.assign_lhm_fractions import assign_lhm_fractions
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets
from ribasim_nl.control import (
    add_controllers_to_connector_nodes,
    add_function_to_peilbeheerst_node_table,
    get_node_table_with_from_to_node_ids,
    remove_duplicate_controls,
    set_node_functions,
)

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

MIXED_CONDITIONS_DESIGN_P = 12
MIXED_CONDITIONS_DESIGN_E = 4

# model settings
waterschap = "HollandseDelta"
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
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/aanvoergebieden_v2.shp")
profiles_path = cloud.joinpath(waterschap, "verwerkt/profielen")

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

# refresh only the feedback form from cloud (instead of all "verwerkt" files)
# cloud.download_file(cloud.file_url(FeedbackFormulier_path))

work_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_profiles")
work_dir.mkdir(parents=True, exist_ok=True)

output_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_forcing")

ribasim_work_dir_model_toml = work_dir.joinpath("ribasim.toml")
output_dir_model_toml = output_dir.joinpath("ribasim.toml")

# set path to base model toml
ribasim_base_model_toml = ribasim_base_model_dir.joinpath("ribasim.toml")

unknown_streefpeil = (
    0.00012345  # we need a streefpeil to create the profiles, Q(h)-relations, and af- and aanslag peil for pumps
)
starttime = datetime.datetime(2017, 1, 1)
endtime = datetime.datetime(2020, 1, 1)
saveat = 3600 * 24
timestep_size = "d"
timesteps = 2
delta_crest_level = 0.1  # delta waterlevel of boezem compared to streefpeil till no water can flow through an outlet
default_level = 1.24 if AANVOER_CONDITIONS else -0.42

# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
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

ribasim_model = Model.read(ribasim_work_dir_model_toml)

# add junctions and network snapping
if ADD_JUNCTIONS:
    ribasim_model = snap_model(ribasim_model, profiles_path)
    ribasim_model = junctionify(ribasim_model)

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
    assign_validation_path = output_dir / "results" / "assign_validation.png"
    assign_validation_path.parent.mkdir(parents=True, exist_ok=True)
    offline_budgets.plot_assign_validation(ribasim_model, path=assign_validation_path)


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

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(
        ribasim_model, starttime, endtime, -0.42, 1.24, DYNAMIC_CONDITIONS
    )
else:
    ribasim_model.level_boundary.static.df["level"] = default_level

# add control, based on the meta_categorie
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")

# filter processor basin IDs to only those present in the model
basin_ids = set(ribasim_model.basin.node.df.index)
processor._basin_aanvoer_on = tuple(n for n in (processor.basin_aanvoer_on or ()) if n in basin_ids)
processor._basin_aanvoer_off = tuple(n for n in (processor.basin_aanvoer_off or ()) if n in basin_ids)

ribasim_param.set_aanvoer_flags(ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=AANVOER_CONDITIONS)
supply.SupplyOutlet(ribasim_model).exec(overruling_enabled=True)

ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50, exclude_outlets=(1265, 1371))

# # update meta_categorie ("Regulier gemaal" not accepted)
# ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df['node_id'].isin((1176, 1381, 2055)), 'meta_categorie'] = "Afvoer gemaal peilgebied peilgebied"

ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

from_to_node_table = get_node_table_with_from_to_node_ids(ribasim_model)
from_to_node_function_table = add_function_to_peilbeheerst_node_table(ribasim_model, from_to_node_table)
from_to_node_function_table["demand"] = None

to_drain = (
    868,
    922,
    960,
    1015,
    1118,
    1172,
    1154,
    1347,
    1378,
    1443,
    1455,
    1535,
    1589,
    1611,
    1690,
    1700,
    1771,
    1791,  # rondpompen
    1800,
    1820,
    1824,
    1833,
    1895,
    1956,
    2034,
    2055,
    2103,
    2206,
    2290,
    2302,
    2386,
    2411,
    2512,
    2532,
)
to_flow_control = (
    840,
    841,
    846,
    917,
    1004,
    1058,
    1168,
    1184,
    1193,
    1292,
    1297,
    1300,
    1311,
    1312,
    1321,
    1407,
    1422,
    1577,
    1688,
    1706,
    1758,
    1860,
    1876,
    2007,
    2107,
    2419,
    2552,
    2581,
)
to_supply = (
    830,
    839,
    946,
    954,  # rondpompen
    998,
    1032,  # rondpompen
    1082,
    1091,
    1098,  # rondpompen
    1120,
    1281,
    1283,  # rondpompen
    1313,  # rondpompen
    1354,
    1451,
    1490,
    1513,
    1540,
    1595,
    1723,
    1726,
    1757,
    1794,
    1815,
    1840,
    1936,
    1941,
    2001,
    2006,
    2065,
    2109,
    2154,
    2155,
    2170,
    2187,
    2200,
    2203,
    2256,
    2268,
    2344,
    2367,
    2371,
    2458,
    2466,
    2494,
    2499,
    2507,
    2572,
    2738,
    2750,
    2763,
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
#     lhm_flushing_path="HollandseDelta/aangeleverd/Na_levering/DoorspoelingWSHD.gpkg",
#     flushing_layer="Doorspoeling",
#     flushing_id="OBJECTID",
# )
# _, df_demand = flush.add_flushing(df_function=from_to_node_function_table)
# from_to_node_function_table = flush.update_function_table(df_demand, from_to_node_function_table)

add_controllers_to_connector_nodes(ribasim_model, from_to_node_function_table, drain_capacity=20)
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
# there is an MR without geometry and without links for some reason
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df["length"] = 100.0
ribasim_model.manning_resistance.static.df["manning_n"] = 0.01

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
    RWS_buffer=10000,  # is only neighbouring RWS, so increase buffer
    custom_nodes={
        # 9141: None,  # dunes
        # 2687: None,  # dunes
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


# There are no reliable flow rates for the pumps (data is mixed in m3/s and m3/min, and most dont have a value). Scale it all, include additional safety margin for the pumps afterwards
ribasim_model.outlet.static.df["meta_known_flow_rate"] = False
ribasim_model.pump.static.df["meta_known_flow_rate"] = False

ribasim_model, from_to_node_table = scale_outlets_pumps(
    OutletPumpScalingConfig(
        ribasim_model_path=ribasim_work_dir_model_toml,
        ribasim_model=ribasim_model,
        from_to_node_function_table=from_to_node_function_table,
        waterschap=waterschap,
        cloud=cloud,
        rescale_flow_capacities=RESCALE_FLOW_CAPACITIES,
        design_precipitation_event=MIXED_CONDITIONS_DESIGN_P,
        design_potential_evaporation_event=MIXED_CONDITIONS_DESIGN_E,
        initial_guess_flow_rate_pump=15.0,  # set higher as no pump capacity is known
    )
)

ribasim_model.pump.static.df.max_flow_rate *= 1.25  # safety factor

# TODO: find out why the flow rate is 0 at WSHD but not at other water boards. Temp fix for validation session.
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["flow_rate"] == 0,
    "flow_rate",
] = ribasim_model.pump.static.df["max_flow_rate"]

ribasim_model.outlet.static.df.loc[
    ribasim_model.outlet.static.df["flow_rate"] == 0,
    "flow_rate",
] = ribasim_model.outlet.static.df["max_flow_rate"]

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
ribasim_model.write(output_dir_model_toml)

if not ribasim_model.use_validation:
    raise ValueError(f"Abort execution of Ribasim: {ribasim_model.use_validation=}")

# run model
run_ribasim(output_dir_model_toml)
ribasim_model.update_state()
ribasim_model.basin.state.write()

# model performance
controle_output = Control(work_dir=output_dir, qlr_path=qlr_path)
indicators = controle_output.run_dynamic_forcing() if MIXED_CONDITIONS else controle_output.run_all()
