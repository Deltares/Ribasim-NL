"""Parameterisation of water board: Hollands Noorderkwartier."""

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
from ribasim import Node
from ribasim.nodes import level_boundary, manning_resistance, pump
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

MIXED_CONDITIONS_DESIGN_P = 12
MIXED_CONDITIONS_DESIGN_E = 2

# model settings
waterschap = "HollandsNoorderkwartier"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
qlr_path = paths.qlr(MIXED_CONDITIONS)
aanvoer_path = cloud.joinpath(
    waterschap, "aangeleverd/Na_levering/20240618_peilgebieden_en_polders/Polders_export_2024-06-18.shp"
)

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

# add gemaal Kadoelen which is removed due to the merging
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(122405, 491275)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(122475, 491317)), [pump.Static(flow_rate=[11.67])])
ribasim_model.link.add(ribasim_model.basin[2], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)
ribasim_model.node.df.loc[level_boundary_node.node_id, "meta_node_id"] = level_boundary_node.node_id
ribasim_model.node.df.loc[pump_node.node_id, "meta_node_id"] = pump_node.node_id

# a manning node misses at splitted boezem due to awkward basin shape
manning_node = ribasim_model.manning_resistance.add(
    Node(geometry=Point(110789, 518114)),
    [manning_resistance.Static(length=[10.0], manning_n=[0.01], profile_width=[10.0], profile_slope=[3.0])],
)
ribasim_model.link.add(ribasim_model.basin[2769], manning_node)
ribasim_model.link.add(manning_node, ribasim_model.basin[2757])
ribasim_model.node.df.loc[manning_node.node_id, "meta_node_id"] = manning_node.node_id

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
        ribasim_model, STARTTIME, ENDTIME, -0.42, -0.4, DYNAMIC_CONDITIONS
    )
else:
    ribasim_model.level_boundary.static.df["level"] = default_level

# prepare 'aanvoergebieden'
# fmt: off
basin_aanvoer_off = (
    48, 131, 20, 180, 111,  # Texel
    5, 75,  # Wieringermeer
    163, 88, 46, 78, 129,  # duinen
)
# fmt: on

# add control, based on the meta_categorie
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")

# filter processor basin IDs to only those present in the model
basin_ids = set(ribasim_model.basin.node.df.index)
processor._basin_aanvoer_on = tuple(n for n in (processor.basin_aanvoer_on or ()) if n in basin_ids)
processor._basin_aanvoer_off = tuple(n for n in (processor.basin_aanvoer_off or ()) if n in basin_ids)
basin_aanvoer_off = tuple(n for n in basin_aanvoer_off if n in basin_ids)

ribasim_param.set_aanvoer_flags(
    ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=AANVOER_CONDITIONS, basin_aanvoer_off=basin_aanvoer_off
)
supply.SupplyOutlet(ribasim_model).exec(overruling_enabled=True)
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

# remove defying-gravity-outlets
ribasim_model = ribasim_param.remove_non_free_flowing_outlets(ribasim_model, printing=True)

ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

from_to_node_table = get_node_table_with_from_to_node_ids(ribasim_model)

# unique_connections = from_to_node_table.reset_index(drop=False).groupby(["from_node_id", "to_node_id"]).first()
# double_connections = from_to_node_table[~from_to_node_table.index.isin(unique_connections["node_id"])]
# for i in double_connections.index.values:
#     ribasim_model.remove_node(i, True)
#
# from_to_node_table = from_to_node_table[from_to_node_table.index.isin(unique_connections["node_id"])].copy()

from_to_node_function_table = add_function_to_peilbeheerst_node_table(ribasim_model, from_to_node_table)
from_to_node_function_table["demand"] = None

to_drain = (
    279,
    588,
    781,
    1422,
)
to_flow_control = (
    239,
    241,
    245,
    292,
    345,
    347,
    350,
    376,
    385,
    472,
    484,
    510,
    525,
    528,
    548,
    555,
    574,
    610,
    648,
    653,
    670,
    674,
    701,
    741,
    758,
    848,
    902,
    987,
    990,
    1039,
    1052,
    1116,
    1176,
    1212,
    1256,
    1261,
    1273,
)
to_supply = (
    240,
    247,
    255,
    258,
    263,
    265,
    274,
    296,
    312,
    315,
    321,
    325,
    331,
    335,
    342,
    344,
    355,
    356,
    387,
    415,
    427,
    499,
    531,
    549,
    627,
    695,
    700,
    712,
    728,
    732,
    748,
    750,
    795,
    803,
    808,
    828,
    868,
    878,
    919,
    922,
    929,
    935,
    986,
    991,
    1043,
    1049,
    1077,
    1113,  # rondpompen
    1115,
    1177,
    1214,
    1229,
    1263,
    1296,
)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

# restore the meta data of pumps and outlets, as adding controllers might change or add node_ids
with preserve_connector_meta(ribasim_model):
    # flush = Flushing(ribasim_model)
    # _, df_demand = flush.add_flushing(df_function=from_to_node_function_table)
    # from_to_node_function_table = flush.update_function_table(df_demand, from_to_node_function_table)

    add_controllers_to_connector_nodes(ribasim_model, from_to_node_function_table, drain_capacity=20)
    remove_duplicate_controls(ribasim_model)

# assign metadata for pumps and basins
assign_metadata = AssignMetaData(
    authority=waterschap,
    model_name=ribasim_model,
    param_name="Noorderkwartier.gpkg",
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

# presumably wrong conversion of flow capacity in the data
set_flow_rate(ribasim_model.pump.static.df, [452], 0.4)
set_flow_rate(ribasim_model.pump.static.df, [895, 1144], 1.17)

# according data flow_rate of 0
zero_flow_pumps = [1293, 677]
set_flow_rate(ribasim_model.pump.static.df, zero_flow_pumps, 25.0)

increase_flow_rate_pumps = [1183, 827, 1108, 300, 735, 1010, 611, 1042, 392, 424, 626, 1144, 895, 536, 1048, 1132]
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), ["flow_rate", "max_flow_rate"]
] *= 60

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
    RWS_buffer=3000,  # due to harbour
    custom_nodes={
        4565: "AmstelGooienVecht",
        1394: "AmstelGooienVecht",
        3657: "AmstelGooienVecht",
        1309: "Noordzee",
        1312: "Noordzee",
        1322: "Noordzee",
        1337: "Noordzee",
        1338: "Noordzee",
        1345: "Noordzee",
        1355: "Noordzee",
        1371: "Noordzee",
        1376: "Noordzee",
        1382: "Noordzee",
        1390: "Noordzee",
        1392: "Noordzee",
        1393: "Noordzee",
        1396: "Noordzee",
        1421: "Noordzee",
        2088: "Noordzee",
        2457: "Noordzee",
        2517: "Noordzee",
        2563: "Noordzee",
        2717: "Noordzee",
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
        design_precipitation_event=MIXED_CONDITIONS_DESIGN_P,
        design_potential_evaporation_event=MIXED_CONDITIONS_DESIGN_E,
    )
)

# check if meta_categorie in the basin.node.df is completely filled
check_basin_meta_categorie(ribasim_model)

# write and run the model
write_and_run_forcing_model(ribasim_model, paths, mixed_conditions=MIXED_CONDITIONS, add_junctions=ADD_JUNCTIONS)
