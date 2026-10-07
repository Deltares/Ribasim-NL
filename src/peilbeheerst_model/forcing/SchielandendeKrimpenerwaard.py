"""Parameterisation of water board: Schieland en de Krimpenerwaard."""

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

MIXED_CONDITIONS_DESIGN_P = 18
MIXED_CONDITIONS_DESIGN_E = 5

# model settings
waterschap = "SchielandendeKrimpenerwaard"
base_model_versie = "2024_12_1"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
qlr_path = paths.qlr(MIXED_CONDITIONS)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/HyDamo_metWasverzachter_20230905.gpkg")

output_dir = paths.model_dir("forcing")
default_level = 0.75 if AANVOER_CONDITIONS else -0.75  # default LevelBoundary level, similar to surrounding Maas

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
        ribasim_model, STARTTIME, ENDTIME, -0.75, 0.75, DYNAMIC_CONDITIONS
    )
else:
    ribasim_model.level_boundary.static.df["level"] = default_level

# prepare 'aanvoergebieden'
if AANVOER_CONDITIONS:
    aanvoergebieden = supply.special_load_geometry(
        f_geometry=str(aanvoer_path), method="extract", layer="peilbesluitgebied", key="statusobject", value="3"
    )
else:
    aanvoergebieden = None

# add control, based on the meta_categorie
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")

# filter processor basin IDs to only those present in the model
basin_ids = set(ribasim_model.basin.node.df.index)
processor._basin_aanvoer_on = tuple(n for n in (processor.basin_aanvoer_on or ()) if n in basin_ids)
processor._basin_aanvoer_off = tuple(n for n in (processor.basin_aanvoer_off or ()) if n in basin_ids)

ribasim_param.set_aanvoer_flags(
    ribasim_model,
    aanvoergebieden,
    processor,
    basin_aanvoer_off=104,
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

# change outlet functions
to_supply = (
    182,
    183,
    196,
    201,
    356,
    468,
    476,
    479,
    480,
    489,
    494,
    504,
    516,
    524,
    527,
    531,
    534,
    554,
    669,
    691,
    702,
    704,
    705,
    709,
    711,
)
to_flow_control = (
    # 182,
    # 183,
    220,  # basin die boezem voedt
    341,  # basin die boezem voedt
    354,
    358,
    373,  # basin die boezem voedt
    561,
    703,
)
to_drain = (
    364,  # labelled as "aanvoergemaal" but directed from polder to boezem
    612,
    467,
)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

# restore the meta data of pumps and outlets, as adding controllers might change or add node_ids
with preserve_connector_meta(ribasim_model):
    # TODO: Add flushing
    # Add flushing data
    # flush = Flushing(ribasim_model)
    # _, df_demand = flush.add_flushing(df_function=from_to_node_function_table)
    # from_to_node_function_table = flush.update_function_table(df_demand, from_to_node_function_table)

    add_controllers_to_connector_nodes(
        model=ribasim_model,
        node_functions_df=from_to_node_function_table,
        target_level_column="meta_streefpeil",
        drain_capacity=20,
    )

    # # set discharge to 0 in supply state as suggested by D2Hydro
    # flow_demand_nodes = ribasim_model.flow_demand.node.df.index.unique()
    # pump_with_flow_demand = ribasim_model.link.df.loc[ribasim_model.link.df.from_node_id.isin(flow_demand_nodes), "to_node_id"].unique()
    # ribasim_model.pump.static.df.loc[
    #     (ribasim_model.pump.static.df.node_id.isin(pump_with_flow_demand))
    #     & (ribasim_model.pump.static.df.control_state == "aanvoer"),
    #     ["flow_rate", "max_flow_rate"],
    # ] = 0

# # there are some duplicates in the discrete control? Remove them
# control = ribasim_model.link.df[ribasim_model.link.df.link_type == "control"]
# dup_control = []
# all_nodes = ribasim_model.node.df[["node_type"]]
# for to_node_id, group in control.groupby("to_node_id"):
#     if len(group) == 1:
#         continue
#     elif len(group) == 2:
#         group = group.merge(all_nodes, left_on="from_node_id", right_index=True, how="inner")
#         if set(group.node_type.tolist()) == {"DiscreteControl", "FlowDemand"}:
#             continue
#         else:
#             dup_control.append(group.from_node_id.iat[0])
#     else:
#         raise ValueError(
#             f"found {len(group)} incoming control links for {to_node_id=} from {set(group.from_node_id.tolist())}"
#         )
#
# for duplicate in dup_control:
#     ribasim_model.remove_node(duplicate, True)
#     print(f"Removed duplicate control node {duplicate}")


# assign metadata for pumps and basins
assign_metadata = AssignMetaData(
    authority=waterschap,
    model_name=ribasim_model,
    param_name="HHSK.gpkg",
    sync=False,
)
assign_metadata.add_meta_to_pumps(
    layer="gemaal",
    mapper={
        "meta_name": {"node": ["name"]},
        "meta_capaciteit": {"static": ["flow_rate", "max_flow_rate"]},
    },
    max_distance=10,
    factor_flowrate=1 / 60,  # m3/min -> m3/s
)
assign_metadata.add_meta_to_basins(
    layer="aggregation_area",
    mapper={"meta_name": {"node": ["name"]}},
    min_overlap=0.95,
)

# increase_flow_rate_pumps = [395]
# ribasim_model.pump.static.df.loc[
#     ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), "flow_rate"
# ] *= 60

# last formatting of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.manning_resistance.node.df.dropna(subset="geometry", inplace=True)

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df["length"] = 10.0
ribasim_model.manning_resistance.static.df["manning_n"] = 0.01

if MIXED_CONDITIONS:
    ribasim_model.basin.static.df = None
    ribasim_param.set_dynamic_min_upstream_max_downstream(ribasim_model)

# add the water authority column to couple the model with
assign = AssignAuthorities(
    ribasim_model=ribasim_model,
    waterschap=waterschap,
    ws_grenzen_path=paths.ws_grenzen,
    RWS_grenzen_path=paths.rws_grenzen,
    RWS_buffer=400,  # polygons match relatively good, lower buffer
    custom_nodes=None,
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

# set the pumps and outlets with unknown flow capacities to have unknown flow capacities in the model, so they can be scaled in the next step.
ribasim_model.outlet.static.df["meta_known_flow_rate"] = False
ribasim_model.pump.static.df["meta_known_flow_rate"] = True
ribasim_model.pump.static.df.loc[
    (ribasim_model.pump.static.df.max_flow_rate.isna()) | (ribasim_model.pump.static.df.max_flow_rate == 0),
    "meta_known_flow_rate",
] = False

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 363,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 5, True  # actually not known, but its otherwise too low

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 166,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 10, True  # actually not known, but its otherwise too low

ribasim_model.outlet.static.df.loc[
    ribasim_model.outlet.static.df.node_id == 685,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 2.0, True  # actually not known, but its otherwise too low

# rescaling of outlets (and pumps)
if RESCALE_FLOW_CAPACITIES:
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
else:
    print(f"No scaling of outlets/pumps: {RESCALE_FLOW_CAPACITIES=}")

ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df.node_id == 342, "max_flow_rate"] = 1.0


# check if meta_categorie in the basin.node.df is completely filled
check_basin_meta_categorie(ribasim_model)

# set numerical settings
# write model output

# write and run the model
write_and_run_forcing_model(ribasim_model, paths, mixed_conditions=MIXED_CONDITIONS, add_junctions=ADD_JUNCTIONS)
