"""Parameterisation of water board: Zuiderzeeland."""

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
    node_function_table,
    set_boundary_conditions,
    set_manning_resistance_outside_bergend,
    write_and_run_forcing_model,
)

from ribasim_nl import CloudStorage, Model

# model settings
waterschap = "Zuiderzeeland"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/peilgebieden.gpkg")


# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# reset pump capacity for each pump
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-0.42, -0.4))

# add control, based on the meta_categorie
ribasim_param.set_aanvoer_flags(ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=True)
categorize_nodes(ribasim_model, supply_outlets=False)

from_to_node_function_table = node_function_table(ribasim_model)

to_supply = (453, 493, 525, 659, 664, 672)
to_flow_control = (617,)
to_drain = ()
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=False)

######


assign_pump_and_basin_names(ribasim_model, paths, pump_flow_rate_factor=1)

# data availability (and thus delivery) is not complete for all pumps. Hard code the flow rates based on some emails.
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df["meta_known_flow_rate"] = False

# based on Excel for D-HYDRO model. Also set the meta_known_flow_rate to True for these pumps, as we have data for these.
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 364,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 4.44, True
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 708,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 2.78, True
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 517,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 3.00, True
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 564,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 1.22, True

# based on Gemalen stichting
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 719,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 1, True  # unknown capacity, 1 m3/s based on expert judgement
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 856,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 50, True  # Wortman
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 853,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 770 * 2 / 60, True  # Blocq van Kuffeler, Lage Vaart
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 855,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 935 * 2 / 60, True  # Blocq van Kuffeler, Hoge Vaart
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 787,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 500 * 2 / 60, True  # Colijn, Lage Vaart

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 788,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 580 / 60, True  # Colijn, Hoge Vaart

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 823,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 580 * 2 / 60, True  # Lovink

ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 814,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 800 * 3 / 60, True  # Vissering


ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 834,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 620 * 2 / 60, True  # Smeenge
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == 815,
    ("max_flow_rate", "meta_known_flow_rate"),
] = 720 * 3 / 60, True  # Buma

# Manning resistance
# there is a MR without geometry and without links for some reason
assert ribasim_model.manning_resistance.node is not None and ribasim_model.manning_resistance.node.df is not None
mr_null_geom = ribasim_model.manning_resistance.node.df[ribasim_model.manning_resistance.node.df.geometry.isna()].index
assert ribasim_model.node.df is not None
ribasim_model.node.df = ribasim_model.node.df.drop(mr_null_geom)

# lower the difference in waterlevel for each manning node, except those of storing basins
set_manning_resistance_outside_bergend(ribasim_model, length=100.0, manning_n=0.01)

# increase aanslagpeil for gemaal Wortman
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 2436, "min_upstream_level"] += (
    0.05  # 5 cm higher than streefpeil
)
assert ribasim_model.discrete_control.condition.df is not None
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 14970, "threshold_high"
] += 0.05
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 14970, "threshold_low"
] += 0.05

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    custom_nodes={
        820: "Rijkswaterstaat",
        834: "Rijkswaterstaat",
        857: "Rijkswaterstaat",
        873: "Rijkswaterstaat",
        875: "Rijkswaterstaat",
        876: "Rijkswaterstaat",
        879: "Rijkswaterstaat",
        858: "Rijkswaterstaat",
        872: "Rijkswaterstaat",
        874: "Rijkswaterstaat",
        880: "Rijkswaterstaat",
    },
    known_pump_flow_rates=None,
)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

# increase max flow rate of some specific outlets which have high drainage rates
for static_df, node_ids in (
    (ribasim_model.outlet.static.df, [337, 371, 416, 474]),
    (ribasim_model.pump.static.df, [914]),
):
    assert static_df is not None
    set_flow_rate(static_df, node_ids, 1.0)
    static_df.loc[static_df.node_id.isin(node_ids), "max_flow_rate"] = 1.0

write_and_run_forcing_model(ribasim_model, paths)
