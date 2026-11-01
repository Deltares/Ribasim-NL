"""Parameterisation of water board: Schieland en de Krimpenerwaard."""

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

from ribasim_nl import CloudStorage, Model, supply

# model settings
waterschap = "SchielandendeKrimpenerwaard"
base_model_versie = "2024_12_1"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/HyDamo_metWasverzachter_20230905.gpkg")


# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# reset pump capacity for each pump
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-0.75, 0.75))

# prepare 'aanvoergebieden'
aanvoergebieden = supply.special_load_geometry(
    f_geometry=str(aanvoer_path), method="extract", layer="peilbesluitgebied", key="statusobject", value="3"
)

# add control, based on the meta_categorie

keep_model_supply_basins(processor, ribasim_model)

ribasim_param.set_aanvoer_flags(
    ribasim_model,
    aanvoergebieden,
    processor,
    basin_aanvoer_off=104,
    aanvoer_enabled=True,
)

categorize_nodes(ribasim_model)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)


from_to_node_function_table = node_function_table(ribasim_model)

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

add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=False)

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


assign_pump_and_basin_names(ribasim_model, paths, param_name="HHSK.gpkg", pump_max_distance=10)

# increase_flow_rate_pumps = [395]
# ribasim_model.pump.static.df.loc[
#     ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), "flow_rate"
# ] *= 60

ribasim_model.manning_resistance.node.df.dropna(subset="geometry", inplace=True)
set_manning_resistance_outside_bergend(ribasim_model, length=10.0, manning_n=0.01)

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    rws_buffer=400,  # polygons match relatively good, lower buffer
)

assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 363,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 5, True  # actually not known, but its otherwise too low

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 166,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 10, True  # actually not known, but its otherwise too low

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

assert ribasim_model.outlet.static.df is not None
ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df.node_id == 342, "max_flow_rate"] = 1.0


write_and_run_forcing_model(ribasim_model, paths)
