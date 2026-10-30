"""Parameterisation of water board: Hollandse Delta."""

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim_nl.control import (
    set_node_functions,
)
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

from ribasim_nl import CloudStorage, Model

# model settings
waterschap = "HollandseDelta"
base_model_versie = "2024_12_3"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/aanvoergebieden_v2.shp")


# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# reset pump capacity for each pump
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-0.42, 1.24))

# add control, based on the meta_categorie

keep_model_supply_basins(processor, ribasim_model)

ribasim_param.set_aanvoer_flags(ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=True)

categorize_nodes(ribasim_model)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50, exclude_outlets=(1265, 1371))

# # update meta_categorie ("Regulier gemaal" not accepted)
# ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df['node_id'].isin((1176, 1381, 2055)), 'meta_categorie'] = "Afvoer gemaal peilgebied peilgebied"

from_to_node_function_table = node_function_table(ribasim_model)

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

add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=True)

assign_pump_and_basin_names(ribasim_model, paths)

# Manning resistance
# there is an MR without geometry and without links for some reason
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node, except those of storing basins
set_manning_resistance_outside_bergend(ribasim_model, length=100.0, manning_n=0.01)

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    rws_buffer=10000,  # is only neighbouring RWS, so increase buffer
    known_pump_flow_rates=False,
)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.max_flow_rate *= 1.25  # safety factor

write_and_run_forcing_model(ribasim_model, paths)
