"""Parameterisation of water board: Rivierenland."""

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

MIXED_CONDITIONS_DESIGN_P = 12
MIXED_CONDITIONS_DESIGN_E = 2

# model settings
waterschap = "Rivierenland"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
qlr_path = paths.qlr(MIXED_CONDITIONS)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/Aanvoergebieden_detail.shp")

output_dir = paths.model_dir("forcing")
default_level = 12.4 if AANVOER_CONDITIONS else -0.60  # default LevelBoundary level, +- level at Kinderdijk

# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# Resolve geometry-based inlaat node lookups before snapping relocates these nodes, so the
# hard-coded coordinates still match the original node locations (used much further below).
_inlaat_points = [Point(174615, 440126), Point(103334, 433570), Point(103446, 433601), Point(198568, 434184)]
inlaten = []
for _inlaat_point in _inlaat_points:
    _inlaat_candidates = ribasim_model.node.df.loc[ribasim_model.node.df.geometry.distance(_inlaat_point) < 1]
    if len(_inlaat_candidates) != 1:
        raise ValueError(
            f"Expected exactly 1 node within 1 m of inlaat location {_inlaat_point.wkt}, "
            f"but found {len(_inlaat_candidates)}: {_inlaat_candidates.index.tolist()}"
        )
    inlaten.append(_inlaat_candidates.index[0])
inlaat_Kuijk = inlaten[0]

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
        ribasim_model, STARTTIME, ENDTIME, -0.6, 12.4, DYNAMIC_CONDITIONS
    )
    ribasim_model.level_boundary.time.df.loc[
        (ribasim_model.level_boundary.time.df.node_id.isin([963, 957]))
        & (ribasim_model.level_boundary.time.df.level == 12.4),
        "level",
    ] = 2.5  # change value of a single LB during summer time, so it can still discharge excess water

else:
    ribasim_model.level_boundary.static.df["level"] = default_level

# add control, based on the meta_categorie
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(
    ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=AANVOER_CONDITIONS, basin_aanvoer_off=(204)
)
# change the control of the outlet at Kinderdijk
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 280, "meta_categorie"] = (
    "Inlaat boezem, afvoer gemaal"
)
supply.SupplyOutlet(ribasim_model).exec(overruling_enabled=True)

ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
# # ribasim_param.add_discrete_control(ribasim_model, waterschap, default_level)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

from_to_node_table = get_node_table_with_from_to_node_ids(ribasim_model)
from_to_node_function_table = add_function_to_peilbeheerst_node_table(ribasim_model, from_to_node_table)
from_to_node_function_table["demand"] = None

to_drain = (
    303,
    363,
    494,
    522,
    525,
    565,
    664,
    668,
    723,
    785,
    786,
    814,
    833,
    840,
)
to_flow_control = (
    270,
    322,
    384,
    386,
    388,
    468,
    574,
    856,
    879,
)


to_supply = (
    *inlaten,  # add all manually added inlaten
    245,
    325,
    338,
    478,
    557,
    566,
    575,
    625,
    822,
    886,
    988,
    990,
    999,
    1002,
)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)


# restore the meta data of pumps and outlets, as adding controllers might change or add node_ids
with preserve_connector_meta(ribasim_model):
    # flush = Flushing(
    #     ribasim_model,
    #     lhm_flushing_path="Rivierenland/aangeleverd/Na_levering/Doorspoeling.gpkg",
    #     flushing_layer="aanvoergebieden",
    #     flushing_id="UniekID",
    # )
    # _, df_demand = flush.add_flushing(df_function=from_to_node_function_table)
    # from_to_node_function_table = flush.update_function_table(df_demand, from_to_node_function_table)

    add_controllers_to_connector_nodes(ribasim_model, from_to_node_function_table, drain_capacity=20)

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
    max_distance=25,
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
    custom_nodes={
        973: "Rijkswaterstaat",
        940: "Rijkswaterstaat",
        939: "Rijkswaterstaat",
        1004: "Rijkswaterstaat",
        1005: "Rijkswaterstaat",
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

# for some reason, some connector nodes of the Linge are incorrectly scaled. Keep using the original (high) values of the flow_rate to allow for better discharge of the system.
Linge_nodes = [433, 612, 666]
ribasim_model.outlet.static.df.loc[
    ribasim_model.outlet.static.df["node_id"].isin(Linge_nodes), "meta_known_flow_rate"
] = True

# add fixed max flow rate for inlaat Kuijk
ribasim_model.outlet.static.df.loc[
    ribasim_model.outlet.static.df["node_id"] == inlaat_Kuijk, ["max_flow_rate", "meta_known_flow_rate"]
] = [20.0, True]

# fix pumps from Linge to ARK
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == 1025, "max_flow_rate"] = 8.0
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == 664, "max_flow_rate"] = 16.0

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

set_flow_rate(ribasim_model.pump.static.df, [664], 8.0)  # average flow rate in the winter

# check if meta_categorie in the basin.node.df is completely filled
check_basin_meta_categorie(ribasim_model)

# write and run the model
write_and_run_forcing_model(ribasim_model, paths, mixed_conditions=MIXED_CONDITIONS, add_junctions=ADD_JUNCTIONS)
