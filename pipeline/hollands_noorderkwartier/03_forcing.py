"""Parameterisation of water board: Hollands Noorderkwartier."""

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim import Node
from ribasim.nodes import level_boundary, manning_resistance, pump
from ribasim_nl.control import set_flow_rate, set_node_functions
from ribasim_nl.design_capacities import set_design_capacities
from ribasim_nl.workflow import (
    WaterBoardPaths,
    add_controllers,
    assign_pump_and_basin_names,
    categorize_nodes,
    complete_parametrization,
    keep_model_supply_basins,
    node_function_table,
    set_boundary_conditions,
    set_manning_resistance_outside_bergend,
    write_and_run_forcing_model,
)
from shapely.geometry import Point

from ribasim_nl import CloudStorage, Model

# model settings
waterschap = "HollandsNoorderkwartier"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
aanvoer_path = cloud.joinpath(
    waterschap, "aangeleverd/Na_levering/20240618_peilgebieden_en_polders/Polders_export_2024-06-18.shp"
)

# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# add gemaal Kadoelen which is removed due to the merging
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(122405, 491275)), [level_boundary.Static(level=[0.42])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(122475, 491317)), [pump.Static(flow_rate=[11.67])])
ribasim_model.link.add(ribasim_model.basin[2], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)
assert ribasim_model.node.df is not None
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

# reset pump capacity for each pump
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-0.42, -0.4))

# prepare 'aanvoergebieden'
# fmt: off
basin_aanvoer_off = (
    48, 131, 20, 180, 111,  # Texel
    5, 75,  # Wieringermeer
    163, 88, 46, 78, 129,  # duinen
)
# fmt: on

# add control, based on the meta_categorie
keep_model_supply_basins(processor, ribasim_model)
assert ribasim_model.basin.node is not None and ribasim_model.basin.node.df is not None
basin_ids = set(ribasim_model.basin.node.df.index)
basin_aanvoer_off = tuple(n for n in basin_aanvoer_off if n in basin_ids)

ribasim_param.set_aanvoer_flags(
    ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=True, basin_aanvoer_off=basin_aanvoer_off
)
categorize_nodes(ribasim_model)

# remove defying-gravity-outlets
ribasim_model = ribasim_param.remove_non_free_flowing_outlets(ribasim_model, printing=True)

from_to_node_function_table = node_function_table(ribasim_model)

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

add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=True)

assign_pump_and_basin_names(ribasim_model, paths, param_name="Noorderkwartier.gpkg")

# presumably wrong conversion of flow capacity in the data
assert ribasim_model.pump.static.df is not None
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
assert ribasim_model.node.df is not None
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node, except those of storing basins
set_manning_resistance_outside_bergend(ribasim_model, length=10.0, manning_n=0.01)

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    rws_buffer=3000,  # due to harbour
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
)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)
write_and_run_forcing_model(ribasim_model, paths)
