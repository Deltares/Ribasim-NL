"""Parameterisation of water board: Scheldestromen."""

import math

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
MIXED_CONDITIONS_DESIGN_E = 2

# model settings
waterschap = "Scheldestromen"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
qlr_path = paths.qlr(MIXED_CONDITIONS)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/WSS_aanvoergebieden.shp")

output_dir = paths.model_dir("forcing")
default_level = 0.42 if AANVOER_CONDITIONS else -0.42  # default LevelBoundary level

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
        ribasim_model, STARTTIME, ENDTIME, -0.42, 0.42, DYNAMIC_CONDITIONS
    )
    ribasim_model.level_boundary.time.df.loc[ribasim_model.level_boundary.time.df["node_id"] == 583, "level"] = -2.0
    ribasim_model.level_boundary.time.df.loc[ribasim_model.level_boundary.time.df["node_id"] == 585, "level"] = -2.0
    ribasim_model.level_boundary.time.df.loc[
        (ribasim_model.level_boundary.time.df.node_id == 635) & (ribasim_model.level_boundary.time.df.level == -0.4),
        "level",
    ] = 0.4  # change value of a single LB during summer time
else:
    ribasim_model.level_boundary.static.df["level"] = default_level
    ribasim_model.level_boundary.static.df.loc[ribasim_model.level_boundary.static.df.node_id == 583, "level"] = -2.0
    ribasim_model.level_boundary.static.df.loc[ribasim_model.level_boundary.static.df.node_id == 585, "level"] = -2.0

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
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

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
to_flow_control = (252, 505)
to_supply = (
    206,
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

# restore the meta data of pumps and outlets, as adding controllers might change or add node_ids
with preserve_connector_meta(ribasim_model):
    # flush = Flushing(ribasim_model)
    # _, df_demand = flush.add_flushing(df_function=from_to_node_function_table)
    # from_to_node_function_table = flush.update_function_table(df_demand, from_to_node_function_table)

    # set undefined pumps to 'afvoer'
    ribasim_model.pump.static.df.loc[
        ribasim_model.pump.static.df[["meta_func_afvoer", "meta_func_aanvoer", "meta_func_circulatie"]]
        .isna()
        .all(axis=1),
        "meta_func_afvoer",
    ] = 1

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
# there is a MR without geometry and without links for some reason
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df["length"] = 10.0
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
    RWS_buffer=2000,  # mainly neighbouring RWS, so increase buffer. Not too much, due to nodes within Belgium.
    custom_nodes={
        567: "Rijkswaterstaat",
        578: "Rijkswaterstaat",
        584: "Rijkswaterstaat",  # Westerschelde
        639: "Buitenland",
        641: "Buitenland",
        1119: "Buitenland",
        1188: "Buitenland",
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

ribasim_model.outlet.static.df["meta_known_flow_rate"] = False
ribasim_model.pump.static.df["meta_known_flow_rate"] = True
ribasim_model.pump.static.df.loc[
    (ribasim_model.pump.static.df["max_flow_rate"].isna()) | (ribasim_model.pump.static.df["max_flow_rate"] == 0),
    "meta_known_flow_rate",
] = False

ribasim_model, from_to_node_table = scale_outlets_pumps(
    OutletPumpScalingConfig(
        ribasim_model_path=output_dir / "scaler" / "ribasim.toml",  # keep the profiles model unchanged
        ribasim_model=ribasim_model,
        from_to_node_function_table=from_to_node_function_table,
        waterschap=waterschap,
        cloud=cloud,
        rescale_flow_capacities=RESCALE_FLOW_CAPACITIES,
        initial_guess_flow_rate_pump=15,
        design_precipitation_event=MIXED_CONDITIONS_DESIGN_P,
        design_potential_evaporation_event=MIXED_CONDITIONS_DESIGN_E,
        simulation_days=365,  # avoid empty basins which causes convergence issues. Lower max_days
        max_exceedance_days=5,
    )
)

# Gemaal Postweg pumps into basin 142, which aggregates peilgebieden with target levels up to -1.1 m NAP,
# so do not limit it by the -1.8 m NAP target level of that basin, see #844
postweg = 1189
assert ribasim_model.node.df.at[postweg, "name"] == "Gemaal Postweg, Kapelle", "Node ID of Gemaal Postweg changed"
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == postweg, "max_downstream_level"] = math.inf

# check if meta_categorie in the basin.node.df is completely filled
check_basin_meta_categorie(ribasim_model)

# write and run the model
write_and_run_forcing_model(ribasim_model, paths, mixed_conditions=MIXED_CONDITIONS, add_junctions=ADD_JUNCTIONS)
