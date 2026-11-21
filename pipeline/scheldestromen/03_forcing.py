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
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

# reset pump capacity for each pump
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-0.42, 0.42))

assert ribasim_model.level_boundary.time.df is not None
ribasim_model.level_boundary.time.df.loc[
    ribasim_model.level_boundary.time.df["node_id"] == n("bnd:ow:dummy_globalid_hydroobject_70945"), "level"
] = -2.0
ribasim_model.level_boundary.time.df.loc[
    ribasim_model.level_boundary.time.df["node_id"] == n("bnd:ow:dummy_globalid_hydroobject_70947"), "level"
] = -2.0
ribasim_model.level_boundary.time.df.loc[
    (ribasim_model.level_boundary.time.df.node_id == n("loc:74861,382484"))
    & (ribasim_model.level_boundary.time.df.level == -0.4),
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
    n("ow:dummy_globalid_hydroobject_39536"),
    n("Gemaal Catharinapolder"),
    n("KST559"),
    n("KST307"),
    n("Gemaal Walsoorden"),
    n("ow:dummy_globalid_hydroobject_70964"),
    n("ow:dummy_globalid_hydroobject_70965"),
    n("ow:dummy_globalid_hydroobject_38515"),
    n("ow:dummy_globalid_hydroobject_21"),
)
to_flow_control = (n("KST976"), n("Gemaal Langeweg"))
to_supply = (
    n("KST822"),
    n("KST479"),
    n("ow:KDU71741"),
)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

# check for flow_control-/supply-nodes outside supplied-basins
supply_connectors = (
    n("Gemaal Philippinekanaal"),
    n("Gemaal Dijckmeesterpolder"),
    n("Gemaal Pietersdijk"),
    n("ow:KDU2752"),
    n("ow:dummy_globalid_hydroobject_38780"),
    n("Gemaal Angelinapolder"),
    n("Gemaal Goese Sas"),
    n("Gemaal Langeweg"),
    n("Gemaal Spanjaardweg"),  # non-official supplying pumps?
    n("ow:dummy_globalid_hydroobject_4778"),
    n("ow:dummy_globalid_hydroobject_5115"),
    n("ow:KDU65290"),
    n("ow:KDU65398"),
    n("ow:KDU64977"),
    n("ow:KDU35357"),
    n("ow:KDU64982"),
    n("ow:KDU51133"),
    n("ow:KDU65284"),
    n("ow:KDU35393"),
    n("ow:KDU49150"),
    n("ow:KDU25204"),
    n("loc:43486,357740"),  # inflow from Belgium
    n("KST976"),
    n("KST479"),
    n("KST822"),  # unknown
    n("ow:KDU71741"),
    n("Inlaat Campweg"),
    n("Gemaal Lageweg, Bruintjeskreek"),
    n("KST19"),
    n("KST15"),
    n("Gemaal De Luyster"),
    n("ow:KDU71954"),
    n("KST1146"),
    n("ow:KDU1858"),
    n("KST118"),
    n("KST78"),
    n("KST1179"),
    n("KST112"),
    n("Gemaal Bosweg, noord"),
    n("KST101"),
    n("Gemaal Kadijk"),
    n("Gemaal Koningsweg"),
    n("KST81"),
    n("KST305"),
    n("Gemaal Derde Dijk"),
    n("Gemaal Postweg, Tholen"),
    n("Gemaal Poortvlietsedijk"),
    n("KST632"),
    n("Inlaat Deurloo"),
    n("Gemaal De Eendracht"),
    n("Inlaat Oud Kijkuit"),
    n("Gemaal Drie Grote Polders"),
    n("Inlaat Drie Grote Polders"),
    n("Gemaal Van Haaften"),
    n("Inlaat Van Haaften"),
    n("Gemaal Loohoek"),
    n("KST76"),
    n("loc:61949,396914"),
    n("loc:61951,396911"),
    n("loc:61946,396912"),
    n("KST56"),
    n("KST58"),
    n("loc:74504,382443"),
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
        n("bnd:Gemaal Cadzand"): "Rijkswaterstaat",
        n("bnd:ow:dummy_globalid_hydroobject_37373"): "Rijkswaterstaat",
        n("bnd:ow:dummy_globalid_hydroobject_70946"): "Rijkswaterstaat",  # Westerschelde
        n("loc:43290,356428"): "Buitenland",
        n("loc:35147,362794"): "Buitenland",
        n("loc:14328,372314"): "Buitenland",
        n("loc:14370,370568"): "Buitenland",
    },
)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

# Gemaal Postweg pumps into basin 142, which aggregates peilgebieden with target levels up to -1.1 m NAP,
# so do not limit it by the -1.8 m NAP target level of that basin, see #844
postweg = n("loc:57403,390161")
assert ribasim_model.node.df.at[postweg, "name"] == "Gemaal Postweg, Kapelle", "Expected Gemaal Postweg at this node"
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == postweg, "max_downstream_level"] = math.inf

write_and_run_forcing_model(ribasim_model, paths)
