"""Parameterisation of water board: Rijnland."""

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim import Node
from ribasim.nodes import flow_demand
from ribasim_nl.control import (
    DEFAULT_LEVEL_STEP_HYSTERESIS,
    set_discrete_level_control,
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
from shapely.geometry import Point

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
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

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
    n("403-036-00022"),
    n("160-036-00028"),
    n("180-056-00500"),
    n("078-036-00022"),
    n("180-036-00049"),
    n("418-056-00025"),
    n("loc:95198,455864"),
    n("loc:104703,453092"),
    n("loc:113030,489579"),
)
to_flow_control = (
    n("ow:247-058-00828-01_31622"),
    n("416-056-00001"),
    n("ow:180-033-04433_11074"),
    n("250-056-00036"),
    n("180-056-00563"),
)
to_supply = (
    n("ow:250-058-02136-01_40731"),
    n("180-056-00125"),
    n("180-056-00297"),
    n("255-036-00021"),
    n("180-056-00820"),
    n("263-056-00007"),
    n("ow:010-058-00017-01_180548"),
    n("ow:097-033-00012_5252"),
    n("ow:155-033-00106_5425"),
    n("180-056-00599"),
    n("ow:180-033-05435_35928"),
    n("180-056-00577"),
    n("loc:113465,448516"),
    n("loc:85918,460954"),
    n("loc:102318,491416"),
)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)


add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=True)

assign_pump_and_basin_names(ribasim_model, paths)

# according data flow_rate of 0
zero_flow_pumps = [
    n("180-036-00073"),
    n("426-036-00021"),
    n("423-036-00021"),
    n("loc:88593,464205"),
    n("loc:93134,471673"),
    n("loc:102318,491416"),
    n("loc:105373,486990"),
]
assert ribasim_model.pump.static.df is not None
set_flow_rate(ribasim_model.pump.static.df, zero_flow_pumps, 25.0)

# presumably wrong conversion of flow capacity in the data
increase_flow_rate_pumps = [
    n("180-036-00048"),
    n("180-036-00046"),
    n("180-036-00057"),
    n("169-036-00028"),
    n("242-036-00022"),
    n("155-036-00022"),
    n("180-036-00105"),
    n("180-036-00049"),
]
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), ["flow_rate", "max_flow_rate"]
] *= 60

# Manning resistance
# there is a MR without geometry and without links for some reason
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
        n("bnd:448-036-00021|RIJNLANDSBOEZEM_dummy_id_280>"): "Noordzee",
        n("bnd:ow:247-033-00011_550"): "Delfland",
        n("loc:110865,446289"): "StichtseRijnlanden",
        n("loc:111786,446255"): "StichtseRijnlanden",
        n("loc:113790,446726"): "StichtseRijnlanden",
        n("loc:112442,446074"): "StichtseRijnlanden",
        n("loc:113639,446227"): "StichtseRijnlanden",
        n("loc:111784,446219"): "StichtseRijnlanden",
        n("loc:111811,446252"): "StichtseRijnlanden",
    },
)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

# The boezemgemalen drain the boezem in steps of the level of the Basin in the middle of the boezem, rather than of
# the Basins next to them, see https://github.com/Deltares/Ribasim-NL/issues/906 and
# https://github.com/Deltares/Ribasim-NL/pull/928#issuecomment-6443645940
BOEZEM_BASIN = n("RIJNLANDSBOEZEM_dummy_id_281/286")  # streefpeil -0.61 m NAP
# The Basins next to the gemalen are about 1 cm lower than that Basin. With the min_upstream_level of -0.61 m NAP
# they had, the gemalen would only pump a fraction of their steps, so it is a safety limit below the streefpeil.
BOEZEM_MIN_UPSTREAM_LEVEL = -0.65
# (node_id, name, levels [m+NAP] at which the steps switch on, flow rates [m3/s] below and above them, hysteresis [m])
BOEZEMGEMALEN: tuple[tuple[int, str, list[float], list[float], float | list[float]], ...] = (
    (
        n("448-036-00021|RIJNLANDSBOEZEM_dummy_id_280>"),
        "Katwijk",
        [-0.595, -0.585, -0.575, -0.57, -0.565, -0.56],
        [0.0, 15.0, 30.0, 45.0, 60.0, 75.0, 94.0],
        DEFAULT_LEVEL_STEP_HYSTERESIS,
    ),
    # Halfweg switches on before Spaarndam
    (n("464-036-00021"), "Spaarndam", [-0.605], [0.0, 32.0], DEFAULT_LEVEL_STEP_HYSTERESIS),
    (n("loc:113030,489579"), "Halfweg", [-0.615], [0.0, 33.0], DEFAULT_LEVEL_STEP_HYSTERESIS),
    # Gouda starts at -0.59 m NAP and stops below -0.595 m NAP
    (n("433-036-00021"), "Gouda", [-0.59, -0.57], [0.0, 34.5, 40.0], [0.005, DEFAULT_LEVEL_STEP_HYSTERESIS]),
)
for node_id, name, levels, flow_rates, hysteresis in BOEZEMGEMALEN:
    pump_name = ribasim_model.node.df.at[node_id, "name"]
    assert pump_name == f"Boezemgemaal {name}", pump_name
    set_discrete_level_control(
        ribasim_model,
        node_id,
        BOEZEM_BASIN,
        level=levels,
        flow_rate=flow_rates,
        hysteresis=hysteresis,
        min_upstream_level=BOEZEM_MIN_UPSTREAM_LEVEL,
        name=f"peil boezem: {'/'.join(f'{level:g}' for level in levels)} [m+NAP]",
    )

# Katwijk flushes at least 1.07 m3/s, which a FlowDemand on a Pump sets as its minimum flow rate
katwijk = ribasim_model.pump[n("448-036-00021|RIJNLANDSBOEZEM_dummy_id_280>")]
doorspoeling = ribasim_model.flow_demand.add(
    Node(geometry=Point(katwijk.geometry.x, katwijk.geometry.y - 20), name="doorspoeling Katwijk"),
    [flow_demand.Static(demand=[1.07], demand_priority=[1])],
)
ribasim_model.link.add(doorspoeling, katwijk)

write_and_run_forcing_model(ribasim_model, paths)
