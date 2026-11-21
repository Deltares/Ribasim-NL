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
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

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
    n("45874ST"),
    n("ow:T43866"),
    n("ow:T49404"),
    n("ow:D39230"),
    n("10510GM"),
    n("03400ST"),
    n("12431ST"),
    n("ow:H19585"),
    n("13428ST"),
    n("06166ST"),
    n("18753ST"),
    n("05549ST"),
    n("18616ST|P-V06.017>P-V06.016"),
    n("07578ST"),
    n("03834ST"),
    n("12464GM|P-G13.004>P-G46.002"),
    n("05729ST|P-V01.084>P-V01.093"),  # rondpompen
    n("26464ST"),
    n("45822ST"),
    n("15985ST"),
    n("30637ST"),
    n("02156ST"),
    n("19316ST|P-P01.005>P-P01.003"),
    n("13996GM"),
    n("03363GM|P-H14.002>P-H19.001"),
    n("11901ST"),
    n("03399ST"),
    n("07923ST"),
    n("30539ST"),
    n("ow:W27036"),
    n("ow:T21964"),
    n("02157ST"),
)
to_flow_control = (
    n("13910GM|P-Y07.036>P-Y07.029"),
    n("13910GM|P-Y07.031>P-Y07.036"),
    n("17610GM"),
    n("02353ST"),
    n("45632ST|P-H14.011>P-H14.002"),
    n("19777GM|P-Y12.020>P-Y12.024"),
    n("30728ST"),
    n("13142ST"),
    n("09404ST"),
    n("30142GM|P-H17.009>P-H17.019"),
    n("01346GM"),
    n("08928GM"),
    n("03376ST"),
    n("17963ST|P-H02.020S>P-H02.013"),
    n("05720ST"),
    n("ow:T40959|P-Y12.024>P-Y12.026"),
    n("20189GM|P-Y07.018>P-Y07.030"),
    n("13190GM"),
    n("02354ST"),
    n("03092ST"),
    n("30149GM|NL40_54_3>P-G12.001"),
    n("10747ST"),
    n("04453ST"),
    n("30060GM"),
    n("04388GM"),
    n("30196GM"),
    n("04597ST"),
    n("30218GM"),
)
to_supply = (
    n("18724GM"),
    n("ow:H01042"),
    n("30209GM"),
    n("45843ST"),  # rondpompen
    n("20200ST"),
    n("09857ST"),  # rondpompen
    n("08022GM"),
    n("06036GM"),
    n("45657ST"),  # rondpompen
    n("ow:B48964"),
    n("03353ST"),
    n("03199ST"),  # rondpompen
    n("02974GM"),  # rondpompen
    n("11313GM|P-Y12.001>P-Y12.013"),
    n("09816GM"),
    n("ow:T51457"),
    n("05753ST"),
    n("11818ST"),
    n("30564ST"),
    n("30232GM|P-Y08.001>P-Y08.026"),
    n("07590ST|P-H19.001>P-H17.002"),
    n("30149GM|P-G12.008>P-G12.001"),
    n("06658GM"),
    n("03397ST"),
    n("ow:T38682"),
    n("20570GM"),
    n("03194GM"),
    n("05768GM|P-V04.003>P-V03.034"),
    n("09644ST"),
    n("18112GM"),
    n("30795ST"),
    n("13355GM|P-V03.004>P-V03.001"),
    n("13355GM|P-V03.004>P-V03.003"),
    n("16679GM"),
    n("15973GM"),
    n("ow:T22094"),
    n("30194GM"),
    n("04364ST"),
    n("05528ST"),
    n("07601GM"),
    n("16699GM"),
    n("26919GM"),
    n("07773ST"),
    n("29454ST"),
    n("03837ST"),
    n("26996GM"),
    n("08508GM"),
    n("loc:98711,419695"),
    n("loc:94646,428722"),
    n("loc:75454,431916"),
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
