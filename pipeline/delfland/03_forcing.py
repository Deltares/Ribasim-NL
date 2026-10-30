"""Parametrisation of water board: Delfland."""

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim_nl.control import set_node_functions
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
waterschap = "Delfland"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
aanvoer_path = cloud.joinpath(
    waterschap, "aangeleverd/Na_levering/Wateraanvoer/Aanvoergebied_Afvoergebied_polders.gpkg"
)

# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-0.42, -0.4))

# add control, based on the meta_categorie
ribasim_param.set_aanvoer_flags(ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=True)
categorize_nodes(ribasim_model)
from_to_node_function_table = node_function_table(ribasim_model)
# manual adjustments to control settings
to_supply = 167, 371, 239, 223, 258, 306, 525, 377, 150, 224, 468, 163, 201, 475
set_node_functions(from_to_node_function_table, to_supply=to_supply)

add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=False)

# wateraanvoer node to other waterboard. Set max downstream level to a low value to prevent unwanted control actions
assert ribasim_model.outlet.static.df is not None
ribasim_model.outlet.static.df.loc[
    ribasim_model.outlet.static.df.node_id == 433, "max_downstream_level"
] = -0.63  # 2 cm below Rijnlands streefpeil, to avoid too much water entering from Delfland


assign_pump_and_basin_names(ribasim_model, paths)

# presumably wrong conversion of flow capacity in the data
increase_flow_rate_pumps = [474, 298]
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), ["flow_rate", "max_flow_rate"]
] *= 60

ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 559, "max_flow_rate"] = (
    1.5  # TODO: Guessed value, ask Delfland
)
# set the pumps and outlets with unknown flow capacities to have unknown flow capacities in the model, so they can be scaled in the next step.
ribasim_model.outlet.static.df["meta_known_flow_capacities"] = False
ribasim_model.pump.static.df.loc[
    (ribasim_model.pump.static.df.max_flow_rate.isna()) | (ribasim_model.pump.static.df.max_flow_rate == 0),
    "meta_known_flow_capacities",
] = False

# Manning resistance
# there is a MR without geometry and without links for some reason
assert ribasim_model.node.df is not None
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")
# lower the difference in waterlevel for each manning node, except those of storing basins
set_manning_resistance_outside_bergend(ribasim_model, length=100.0, manning_n=0.01)

# decrease aanslagpeil for Dolkgemaal
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 569, "max_downstream_level"] -= (
    0.05  # 5 cm lower than streefpeil to make sure Winsemius pumps first
)
assert ribasim_model.discrete_control.condition.df is not None
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 3118, "threshold_high"
] -= 0.05
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == 3118, "threshold_low"
] -= 0.05


ribasim_model = complete_parametrization(
    ribasim_model, paths, custom_nodes={530: "Noordzee", 532: "Rijkswaterstaat", 543: "Noordzee"}
)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

write_and_run_forcing_model(ribasim_model, paths)
