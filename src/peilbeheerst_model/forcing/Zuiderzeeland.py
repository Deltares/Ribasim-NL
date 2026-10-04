"""Parameterisation of water board: Zuiderzeeland."""

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

MIXED_CONDITIONS_DESIGN_P = 16
MIXED_CONDITIONS_DESIGN_E = 4

# model settings
waterschap = "Zuiderzeeland"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
qlr_path = paths.qlr(MIXED_CONDITIONS)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/peilgebieden.gpkg")

output_dir = paths.model_dir("forcing")
default_level = (
    0.40 if AANVOER_CONDITIONS else -0.40
)  # default LevelBoundary level, similar to surrounding IJsselmeer, Markermeer and Randmeren

# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# network snapping (junctions are added at the very end, just before writing, so they stay
# transparent to all parametrization, classification and validation steps)
if ADD_JUNCTIONS:
    ribasim_model = snap_model(ribasim_model, paths.profielen_dir)

# set forcing
ribasim_model = set_forcing(
    ribasim_model,
    cloud,
    dynamic_conditions=DYNAMIC_CONDITIONS,
    mixed_conditions=MIXED_CONDITIONS,
    aanvoer_conditions=AANVOER_CONDITIONS,
    design_precipitation=MIXED_CONDITIONS_DESIGN_P,
    design_evaporation=MIXED_CONDITIONS_DESIGN_E,
)

# reset pump capacity for each pump
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(
        ribasim_model, STARTTIME, ENDTIME, -0.42, -0.4, DYNAMIC_CONDITIONS
    )
else:
    ribasim_model.level_boundary.static.df["level"] = default_level

# add control, based on the meta_categorie
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=AANVOER_CONDITIONS)
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)

ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

from_to_node_table = get_node_table_with_from_to_node_ids(ribasim_model)
from_to_node_function_table = add_function_to_peilbeheerst_node_table(ribasim_model, from_to_node_table)
from_to_node_function_table["demand"] = None

to_supply = (453, 493, 525, 659, 664, 672)
to_flow_control = (617,)
to_drain = ()
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

######


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
    factor_flowrate=1,  # m3/s
)
assign_metadata.add_meta_to_basins(
    layer="aggregation_area",
    mapper={"meta_name": {"node": ["name"]}},
    min_overlap=0.95,
)

# data availability (and thus delivery) is not complete for all pumps. Hard code the flow rates based on some emails.
ribasim_model.pump.static.df["meta_known_flow_rate"] = False

# based on Excel for D-HYDRO model. Also set the meta_known_flow_rate to True for these pumps, as we have data for these.
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 364,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 4.44, True
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 708,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 2.78, True
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 517,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 3.00, True
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 564,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 1.22, True

# based on Gemalen stichting
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 719,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 1, True  # unknown capacity, 1 m3/s based on expert judgement
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 856,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 50, True  # Wortman
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 853,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 770 * 2 / 60, True  # Blocq van Kuffeler, Lage Vaart
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 855,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 935 * 2 / 60, True  # Blocq van Kuffeler, Hoge Vaart
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 787,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 500 * 2 / 60, True  # Colijn, Lage Vaart

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 788,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 580 / 60, True  # Colijn, Hoge Vaart

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 823,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 580 * 2 / 60, True  # Lovink

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 814,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 800 * 3 / 60, True  # Vissering


ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 834,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 620 * 2 / 60, True  # Smeenge
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 815,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 720 * 3 / 60, True  # Buma

# Manning resistance
# there is a MR without geometry and without links for some reason
mr_null_geom = ribasim_model.manning_resistance.node.df[ribasim_model.manning_resistance.node.df.geometry.isna()].index
ribasim_model.node.df = ribasim_model.node.df.drop(mr_null_geom)

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df["length"] = 100.0
ribasim_model.manning_resistance.static.df["manning_n"] = 0.01

# increase aanslagpeil for gemaal Wortman
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 2436, "min_upstream_level"] += (
    0.05  # 5 cm higher than streefpeil
)
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 14970, "threshold_high"
] += 0.05
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 14970, "threshold_low"
] += 0.05

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
        820: "Rijkswaterstaat",
        834: "Rijkswaterstaat",
        857: "Rijkswaterstaat",
        873: "Rijkswaterstaat",
        875: "Rijkswaterstaat",
        876: "Rijkswaterstaat",
        879: "Rijkswaterstaat",
        858: "Rijkswaterstaat",
        872: "Rijkswaterstaat",
        874: "Rijkswaterstaat",
        880: "Rijkswaterstaat",
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

# set the pumps and outlets with unknown flow capacities to have unknown flow capacities in the model, so they can be scaled in the next step.
ribasim_model.outlet.static.df["meta_known_flow_rate"] = False
ribasim_model.pump.static.df.loc[
    (ribasim_model.pump.static.df.max_flow_rate.isna()) | (ribasim_model.pump.static.df.max_flow_rate == 0),
    "meta_known_flow_rate",
] = False

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

# increase max flow rate of some specific outlets which have high drainage rates
for static_df, node_ids in (
    (ribasim_model.outlet.static.df, [337, 371, 416, 474]),
    (ribasim_model.pump.static.df, [914]),
):
    set_flow_rate(static_df, node_ids, 1.0)
    static_df.loc[static_df.node_id.isin(node_ids), "max_flow_rate"] = 1.0

# check if meta_categorie in the basin.node.df is completely filled
check_basin_meta_categorie(ribasim_model)

# write and run the model
write_and_run_forcing_model(ribasim_model, paths, mixed_conditions=MIXED_CONDITIONS, add_junctions=ADD_JUNCTIONS)
