"""Parameterisation of water board: Fryslan."""

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
    set_flow_rate,
    set_node_functions,
)
from shapely.geometry import Point

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

MIXED_CONDITIONS_DESIGN_P = 11
MIXED_CONDITIONS_DESIGN_E = 2.6  # 0.3 L/s/ha

# model settings
waterschap = "WetterskipFryslan"
base_model_versie = "2025_5_1"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
qlr_path = paths.qlr(MIXED_CONDITIONS)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/aanvoer.gpkg")

output_dir = paths.model_dir("forcing")
default_level = 10 if AANVOER_CONDITIONS else -2.3456  # default LevelBoundary level

# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# Resolve geometry-based drain node lookups before snapping relocates these nodes, so the
# hard-coded coordinates still match the original node locations (used much further below).
_drain_points = [Point(206421, 592530), Point(206360, 592679)]
to_drain_node_ids = []
for _drain_point in _drain_points:
    _drain_candidates = ribasim_model.node.df.loc[ribasim_model.node.df.geometry.distance(_drain_point) < 1]
    if len(_drain_candidates) != 1:
        raise ValueError(
            f"Expected exactly 1 node within 1 m of drain location {_drain_point.wkt}, "
            f"but found {len(_drain_candidates)}: {_drain_candidates.index.tolist()}"
        )
    to_drain_node_ids.append(_drain_candidates.index[0])
to_drain_node_ids = tuple(to_drain_node_ids)

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

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(
        ribasim_model, STARTTIME, ENDTIME, -2.3456, 10, DYNAMIC_CONDITIONS
    )
else:
    ribasim_model.level_boundary.static.df["level"] = default_level

# prepare 'aanvoergebieden'
if AANVOER_CONDITIONS:
    aanvoergebieden = supply.special_load_geometry(
        f_geometry=str(aanvoer_path),
        method="extract",
        # layer="aanvoer",
        key="aanvoer",
        value=1,
    )
else:
    aanvoergebieden = None

# add control, based on the meta_categorie
# Read outlet_aanvoer_on BEFORE find_upstream_downstream_target_levels strips meta columns
outlet_aanvoer_on = tuple(
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["meta_func_aanvoer"] == 1, "node_id"]
)
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(
    ribasim_model,
    aanvoergebieden,
    processor,
    outlet_aanvoer_on=outlet_aanvoer_on,
    aanvoer_enabled=AANVOER_CONDITIONS,
)
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)

# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

# remove non-free flowing outlets (which flow against gravity) based on the difference in streefpeil and the threshold
ribasim_model = ribasim_param.remove_non_free_flowing_outlets(
    ribasim_model=ribasim_model,
    to_exclude=[2183, 1394, 3302, 2625, 2910, 3497, 2209, 2547, 2705, 1123, 2398, 1037, 2209],
    threshold=0.02,
    printing=True,
)

ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

# create a table with from and to node ids, and the function of the node (supply, flow_control, drain)
from_to_node_table = get_node_table_with_from_to_node_ids(ribasim_model)
from_to_node_function_table = add_function_to_peilbeheerst_node_table(ribasim_model, from_to_node_table)
from_to_node_function_table["demand"] = None

# manually change the function of some nodes based upon model inspection
to_supply = (
    1312,
    1347,
    1512,
    1596,
    1647,
    1747,
    2128,
    2398,
    2434,
    2445,
    2616,
    2624,
    2670,
    2773,
    2783,
    2812,
    2857,
    2982,
    3023,
    3150,
    3188,
    3228,
    3272,
    3376,
    3411,
    3452,
    3760,
    3880,
    3882,
    3884,
    3888,
)
to_flow_control = (2452, 3064, 3065, 3068)

to_drain = (2147, 2751, 2944, 3041, 3494, 3568, 3709, *to_drain_node_ids)

from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

# retain meta data of the outlets and pumps
# restore the meta data of pumps and outlets, as adding controllers might change or add node_ids
with preserve_connector_meta(ribasim_model, method="combine_first"):
    # ribasim_model.update_used_ids()

    # # Add flushing data
    # flush = Flushing(
    #     ribasim_model,
    #     lhm_flushing_path="WetterskipFryslan/aangeleverd/Na_levering/WetterskipFryslan_doorspoeling.gpkg",
    #     flushing_layer="WetterskipFryslan_doorspoeling",
    #     flushing_id="flushing_id",
    #     flushing_col="doorsp_mmj",
    # )
    # _, df_demand = flush.add_flushing(df_function=from_to_node_function_table)
    # from_to_node_function_table = flush.update_function_table(df_demand, from_to_node_function_table)

    add_controllers_to_connector_nodes(
        model=ribasim_model,
        node_functions_df=from_to_node_function_table,
        target_level_column="meta_streefpeil",
        drain_capacity=20,
    )
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

ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 2700, "max_flow_rate"] = 172 / 60
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 3891, "max_flow_rate"] = 7340 / 60

# Manning resistance
# there is a MR without geometry and without links for some reason
mr_null_geom = ribasim_model.manning_resistance.node.df[ribasim_model.manning_resistance.node.df.geometry.isna()].index
ribasim_model.node.df = ribasim_model.node.df.drop(mr_null_geom)

# increase aanslagpeil for Woudagemaal
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 2436, "min_upstream_level"
] = -0.48  # 5 cm higher than streefpeil
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 14970, "threshold_high"
] = -0.48 + 0.025
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 14970, "threshold_low"
] = -0.48 - 0.025

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
    custom_nodes={
        3820: "Noordzee",
        5022: "Rijkswaterstaat",
        10958: "Rijkswaterstaat",
    },
    fill_na_authority="Noordzee",
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

# set the pumps and outlets with unknown flow capacities to have unknown flow capacities in the model, so they can be scaled in the next step.
ribasim_model.outlet.static.df["meta_known_flow_rate"] = False
ribasim_model.pump.static.df["meta_known_flow_rate"] = True
ribasim_model.pump.static.df.loc[
    (ribasim_model.pump.static.df.max_flow_rate.isna()) | (ribasim_model.pump.static.df.max_flow_rate == 0),
    "meta_known_flow_rate",
] = False

ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 3863, "meta_known_flow_rate"] = (
    False  # unknown capacity, set temp value
)
set_flow_rate(ribasim_model.pump.static.df, [3863], 0.1)

# rescaling of outlets (and pumps)
ribasim_model, from_to_node_function_table = scale_outlets_pumps(
    OutletPumpScalingConfig(
        ribasim_model_path=output_dir / "scaler" / "ribasim.toml",  # keep the profiles model unchanged
        ribasim_model=ribasim_model,
        from_to_node_function_table=from_to_node_function_table,
        waterschap=waterschap,
        cloud=cloud,
        rescale_flow_capacities=RESCALE_FLOW_CAPACITIES,
        design_precipitation_event=MIXED_CONDITIONS_DESIGN_P,
        design_potential_evaporation_event=MIXED_CONDITIONS_DESIGN_E,
    )
)

# the North Sea and Wadden Sea boundaries stay in the LHM, so give them mean sea level instead of the
# hypothetical drainage and supply levels, see https://github.com/Deltares/Ribasim-NL/issues/660
lb_node_df = ribasim_model.level_boundary.node.df
ribasim_param.set_static_level_boundaries(
    ribasim_model, lb_node_df.index[lb_node_df["meta_couple_authority"] == "Noordzee"], level=0.0
)

# check if meta_categorie in the basin.node.df is completely filled
check_basin_meta_categorie(ribasim_model)

# write and run the model
write_and_run_forcing_model(ribasim_model, paths, mixed_conditions=MIXED_CONDITIONS, add_junctions=ADD_JUNCTIONS)
