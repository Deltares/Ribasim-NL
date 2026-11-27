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
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

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
    basin_aanvoer_off=n("GPG-1283_98"),
    aanvoer_enabled=True,
)

categorize_nodes(ribasim_model)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)


from_to_node_function_table = node_function_table(ribasim_model)

# change outlet functions
to_supply = (
    n("ow:Inlaat|GPG-1204_4>GPG-1227_103"),
    n("ow:Inlaat|GPG-1204_4>GPG-1292b_6"),
    n("KST-1704"),
    n("ow:Inlaat|GPG-1205_28>GPG-1219_143"),
    n("ow:KPL-235"),
    n("KST-1375|GPG-1204_4>GPG-1292a_90"),
    n("ow:INL-462"),
    n("ow:INL-260"),
    n("ow:INL-683"),
    n("KST-27"),
    n("ow:INL-249"),
    n("ow:INL-498"),
    n("ow:INL-459"),
    n("KST-1785"),
    n("ow:INL-423"),
    n("ow:INL-262"),
    n("KST-1862"),
    n("ow:Duiker|GPG-1205_28>GPG-1218_132"),
    n("loc:93034,442796"),
    n("loc:99402,450923"),
    n("loc:97253,444840"),
    n("loc:95290,445253"),
    n("loc:94837,442938"),
    n("loc:95742,441955"),
    n("loc:89792,437301"),
)
to_flow_control = (
    # 182,
    # 183,
    n("KGM-12"),  # basin die boezem voedt
    n("KGM-108|GPG-1011_36>GPG-1012_56"),  # basin die boezem voedt
    n("KGM-71|GPG-1134_63>GPG-1360_149"),
    n("KGM-34"),
    n("KGM-108|GPG-1011_36>GPG-1005_20"),  # basin die boezem voedt
    n("KGM-227"),
    n("loc:94551,440803"),
)
to_drain = (
    n("KGM-31"),  # labelled as "aanvoergemaal" but directed from polder to boezem
    n("ow:Duiker|GPG-1024_137>GPG-1184_17"),
    n("KST-1859"),
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

set_manning_resistance_outside_bergend(ribasim_model, length=10.0, manning_n=0.01)

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    rws_buffer=400,  # polygons match relatively good, lower buffer
)

assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == n("KGM-87"),
    ("max_flow_rate", "meta_known_flow_rate"),
] = 5, True  # actually not known, but its otherwise too low

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == n("KGM-118"),
    ("max_flow_rate", "meta_known_flow_rate"),
] = 10, True  # actually not known, but its otherwise too low

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

assert ribasim_model.outlet.static.df is not None
ribasim_model.outlet.static.df.loc[
    ribasim_model.outlet.static.df.node_id == n("KST-1375|GPG-1298_97>GPG-1204_4"), "max_flow_rate"
] = 1.0


write_and_run_forcing_model(ribasim_model, paths)
