"""Parameterisation of water board: Hollandse Delta."""

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.assign_authorities import AssignAuthorities
from peilbeheerst_model.assign_parametrization import AssignMetaData
from peilbeheerst_model.network_snapping import snap_model
from peilbeheerst_model.outlet_pump_scaler import OutletPumpScalingConfig, scale_outlets_pumps
from peilbeheerst_model.workflow import (
    ENDTIME,
    STARTTIME,
    WaterBoardPaths,
    check_basin_meta_categorie,
    preserve_connector_meta,
    set_forcing,
    write_and_run_forcing_model,
)
from ribasim_nl.assign_lhm_fractions import assign_lhm_fractions
from ribasim_nl.control import (
    add_controllers_to_connector_nodes,
    add_function_to_peilbeheerst_node_table,
    get_node_table_with_from_to_node_ids,
    remove_duplicate_controls,
    set_node_functions,
)

from peilbeheerst_model import supply
from ribasim_nl import CloudStorage, Model, merge_rwzi_model

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True
DYNAMIC_CONDITIONS: bool = True
RESCALE_FLOW_CAPACITIES: bool = True
ADD_LHM_FRACTIONS: bool = True
ADD_RWZI: bool = True
ADD_JUNCTIONS: bool = False

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

MIXED_CONDITIONS_DESIGN_P = 12
MIXED_CONDITIONS_DESIGN_E = 4

# model settings
waterschap = "HollandseDelta"
base_model_versie = "2024_12_3"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
qlr_path = paths.qlr(MIXED_CONDITIONS)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/aanvoergebieden_v2.shp")

output_dir = paths.model_dir("forcing")
default_level = 1.24 if AANVOER_CONDITIONS else -0.42

# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# network snapping (junctions are added at the very end, just before writing, so they stay
# transparent to all parametrization, classification and validation steps)
if ADD_JUNCTIONS:
    ribasim_model = snap_model(ribasim_model, paths.profielen_dir)

# check if meta_categorie in the basin.node.df is completely filled
check_basin_meta_categorie(ribasim_model)

# set forcing
ribasim_model = set_forcing(
    ribasim_model,
    cloud,
    dynamic_conditions=DYNAMIC_CONDITIONS,
    mixed_conditions=MIXED_CONDITIONS,
    aanvoer_conditions=AANVOER_CONDITIONS,
    design_precipitation=MIXED_CONDITIONS_DESIGN_P,
    design_evaporation=MIXED_CONDITIONS_DESIGN_E,
    assign_validation_png=output_dir / "results" / "assign_validation.png",
)

# reset pump capacity for each pump
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(
        ribasim_model, STARTTIME, ENDTIME, -0.42, 1.24, DYNAMIC_CONDITIONS
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

# restore the meta data of pumps and outlets, as adding controllers might change or add node_ids
with preserve_connector_meta(ribasim_model):
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

# assign metadata for pumps and basins
assign_metadata = AssignMetaData(
    authority=waterschap,
    model_name=ribasim_model,
    param_name=f"{waterschap}.gpkg",
    sync=False,
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
    ws_grenzen_path=paths.ws_grenzen,
    RWS_grenzen_path=paths.rws_grenzen,
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
    ribasim_model = merge_rwzi_model(
        ribasim_model,
        cloud.joinpath("Rijkswaterstaat/modellen/rwzi/rwzi.toml"),
        cloud.joinpath("Rijkswaterstaat/modellen/rwzi/meta/RWZI_coordinates_model_coverage.geojson"),
        output_dir / "meta/RWZI_coordinates_lhm_coverage.geojson",
    )

# add LHM fractions
if ADD_LHM_FRACTIONS:
    assign_lhm_fractions(ribasim_model)


# There are no reliable flow rates for the pumps (data is mixed in m3/s and m3/min, and most dont have a value). Scale it all, include additional safety margin for the pumps afterwards
ribasim_model.outlet.static.df["meta_known_flow_rate"] = False
ribasim_model.pump.static.df["meta_known_flow_rate"] = False

ribasim_model, from_to_node_table = scale_outlets_pumps(
    OutletPumpScalingConfig(
        ribasim_model_path=output_dir / "scaler" / "ribasim.toml",  # keep the profiles model unchanged
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

# check if meta_categorie in the basin.node.df is completely filled
check_basin_meta_categorie(ribasim_model)

# write and run the model
write_and_run_forcing_model(ribasim_model, paths, mixed_conditions=MIXED_CONDITIONS, add_junctions=ADD_JUNCTIONS)
