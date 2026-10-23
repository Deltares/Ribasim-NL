"""Parameterisation of water board: Rijnland."""

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim_nl.control import (
    set_flow_rate,
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

from ribasim_nl import CloudStorage, Model, supply

# model settings
waterschap = "Rijnland"
base_model_versie = "2024_12_3"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/RL_aanvoer.shp")


# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# reset pump capacity for each pump
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-2, 2))

# prepare 'aanvoergebieden'
aanvoergebieden = supply.special_load_geometry(f_geometry=aanvoer_path, method="extract", key="aanvoer", value=1)
# remove double-polygons
aanvoergebieden = aanvoergebieden[aanvoergebieden["node_id"] == aanvoergebieden["meta_node_"]]

# add control, based on the meta_categorie

keep_model_supply_basins(processor, ribasim_model)

ribasim_param.set_aanvoer_flags(ribasim_model, aanvoergebieden, processor, aanvoer_enabled=True)
categorize_nodes(ribasim_model)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

from_to_node_function_table = node_function_table(ribasim_model)

to_drain = (
    525,
    530,
    609,
    1096,
    1179,
    1307,
    1455,
    1456,
    1474,
)
to_flow_control = (
    487,
    619,
    625,
    963,
    1148,
    1180,
)
to_supply = (
    398,
    535,
    630,
    690,
    885,
    889,
    901,
    1030,
    1032,
    1255,
    1347,
    1349,
    1439,
    1454,
    1471,
)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)


add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=True)

assign_pump_and_basin_names(ribasim_model, paths)

# according data flow_rate of 0
zero_flow_pumps = [460, 736, 1282, 1435, 1436, 1471, 1472]
assert ribasim_model.pump.static.df is not None
set_flow_rate(ribasim_model.pump.static.df, zero_flow_pumps, 25.0)

# presumably wrong conversion of flow capacity in the data
increase_flow_rate_pumps = [354, 463, 496, 754, 781, 793, 987, 1179]
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), ["flow_rate", "max_flow_rate"]
] *= 60

# Manning resistance
# there is a MR without geometry and without links for some reason
assert ribasim_model.node.df is not None
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")

# # set LevelBoundary-nodes level to AGV-basins
# lb_static = ribasim_model.level_boundary.static.df.copy()
# # > Rijnland -> Zuider Legmeerpolder (AGV)
# lb_ids = 1137, 1147, 1213, 1247, 1261, 1270, 1299
# lb_static.loc[lb_static['node_id'].isin(lb_ids), 'level'] = -5.97
# # > Rijnland -> Binnendijkse Buitenvelderse Polder (AGV)
# lb_ids = 1028, 1317
# lb_static.loc[lb_static['node_id'].isin(lb_ids), 'level'] = -2.
# # > Rijnland -> Riekerpolder (AGV)
# lb_ids = 1095, 1303
# lb_static.loc[lb_static['node_id'].isin(lb_ids), 'level'] = -.6
# # > Rijnland -> Middelveldse Akerpolder (AGV)
# lb_ids = 1065,
# lb_static.loc[lb_static['node_id'].isin(lb_ids), 'level'] = -4.45
# # > Rijnland -> Osdorperbinnenpolder (AGV)
# lb_ids = 1125, 1127
# lb_static.loc[lb_static['node_id'].isin(lb_ids), 'level'] = -2.17
# # > update LevelBoundary-Static table
# ribasim_model.level_boundary.static.df = lb_static.copy()
# del lb_static

# lower the difference in waterlevel for each manning node, except those of storing basins
set_manning_resistance_outside_bergend(ribasim_model, length=10.0, manning_n=0.01)

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    custom_nodes={
        1354: "Noordzee",
        1367: "Delfland",
        1481: "StichtseRijnlanden",
        1485: "StichtseRijnlanden",
        1486: "StichtseRijnlanden",
        1487: "StichtseRijnlanden",
        1488: "StichtseRijnlanden",
        1489: "StichtseRijnlanden",
        1490: "StichtseRijnlanden",
    },
)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

write_and_run_forcing_model(ribasim_model, paths)
