"""Parameterisation of water board: Scheldestromen."""

import math

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim_nl.control import (
    add_controllers_to_connector_nodes,
    remove_duplicate_controls,
    set_node_functions,
)
from ribasim_nl.design_capacities import set_design_capacities
from ribasim_nl.workflow import (
    WaterBoardPaths,
    assign_pump_and_basin_names,
    categorize_nodes,
    complete_parametrization,
    keep_model_supply_basins,
    node_function_table,
    preserve_connector_meta,
    set_boundary_conditions,
    set_manning_resistance_outside_bergend,
    write_and_run_forcing_model,
)

from ribasim_nl import CloudStorage, Model

# model settings
waterschap = "Scheldestromen"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/WSS_aanvoergebieden.shp")


# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# reset pump capacity for each pump
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-0.42, 0.42))

assert ribasim_model.level_boundary.time.df is not None
ribasim_model.level_boundary.time.df.loc[ribasim_model.level_boundary.time.df["node_id"] == 583, "level"] = -2.0
ribasim_model.level_boundary.time.df.loc[ribasim_model.level_boundary.time.df["node_id"] == 585, "level"] = -2.0
ribasim_model.level_boundary.time.df.loc[
    (ribasim_model.level_boundary.time.df.node_id == 635) & (ribasim_model.level_boundary.time.df.level == -0.4),
    "level",
] = 0.4  # change value of a single LB during summer time

# add control, based on the meta_categorie

keep_model_supply_basins(processor, ribasim_model)

ribasim_param.set_aanvoer_flags(ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=True)
categorize_nodes(ribasim_model)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

from_to_node_function_table = node_function_table(ribasim_model)

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
assert ribasim_model.pump.static.df is not None
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

assign_pump_and_basin_names(ribasim_model, paths)

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node, except those of storing basins
set_manning_resistance_outside_bergend(ribasim_model, length=10.0, manning_n=0.01)

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    rws_buffer=2000,  # mainly neighbouring RWS, so increase buffer. Not too much, due to nodes within Belgium.
    custom_nodes={
        567: "Rijkswaterstaat",
        578: "Rijkswaterstaat",
        584: "Rijkswaterstaat",  # Westerschelde
        639: "Buitenland",
        641: "Buitenland",
        1119: "Buitenland",
        1188: "Buitenland",
    },
)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

# Gemaal Postweg pumps into basin 142, which aggregates peilgebieden with target levels up to -1.1 m NAP,
# so do not limit it by the -1.8 m NAP target level of that basin, see #844
postweg = 1189
assert ribasim_model.node.df.at[postweg, "name"] == "Gemaal Postweg, Kapelle", "Node ID of Gemaal Postweg changed"
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == postweg, "max_downstream_level"] = math.inf

write_and_run_forcing_model(ribasim_model, paths)
