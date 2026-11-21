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
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-0.42, -0.4))

# add control, based on the meta_categorie
ribasim_param.set_aanvoer_flags(ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=True)
categorize_nodes(ribasim_model)
from_to_node_function_table = node_function_table(ribasim_model)
# manual adjustments to control settings
to_supply = (
    n("221205"),
    n("ow:20AAEC35-6170-4C45-B722-B75A7144B862"),
    n("201418"),
    n("201417"),
    n("ow:79979021-C26D-4AB2-AFF6-02CBBEABB55D"),
    n("ow:902B17FB-0D5B-4248-9C29-FD44BCB9E474"),
    n("207405"),
    n("21520396"),
    n("ow:0B052802-E003-4E4B-9221-ABD2BACC4E48"),
    n("21304697"),
    n("ow:7A0CDAA7-AA62-4CB3-B422-2E75AAA13E0F"),
    n("20220585"),
    n("221432"),
    n("ow:1A457FDF-12DD-4AAD-82B0-5804804781D4"),
)
set_node_functions(from_to_node_function_table, to_supply=to_supply)

add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=False)

# wateraanvoer node to other waterboard. Set max downstream level to a low value to prevent unwanted control actions
assert ribasim_model.outlet.static.df is not None
ribasim_model.outlet.static.df.loc[
    ribasim_model.outlet.static.df.node_id == n("ow:F563CD94-AA4C-4073-B405-6888D2620506"), "max_downstream_level"
] = -0.63  # 2 cm below Rijnlands streefpeil, to avoid too much water entering from Delfland


assign_pump_and_basin_names(ribasim_model, paths)

# presumably wrong conversion of flow capacity in the data
increase_flow_rate_pumps = [n("173D7D8D-BFCE-4446-B9FF-F9C2DB03D701"), n("B862F86E-5DA7-42C5-859B-9CB9780C1D22")]
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"].isin(increase_flow_rate_pumps), ["flow_rate", "max_flow_rate"]
] *= 60

ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == n("loc:87620,438500"), "max_flow_rate"] = (
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
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")
# lower the difference in waterlevel for each manning node, except those of storing basins
set_manning_resistance_outside_bergend(ribasim_model, length=100.0, manning_n=0.01)

# decrease aanslagpeil for Dolkgemaal
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == n("loc:87082,455089"), "max_downstream_level"
] -= 0.05  # 5 cm lower than streefpeil to make sure Winsemius pumps first
assert ribasim_model.discrete_control.condition.df is not None
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == n("discrete_control:loc:87082,455089"), "threshold_high"
] -= 0.05
ribasim_model.discrete_control.condition.df.loc[
    ribasim_model.discrete_control.condition.df.node_id == n("discrete_control:loc:87082,455089"), "threshold_low"
] -= 0.05


ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    custom_nodes={
        n("bnd:ow:AF1B3C05-A9A1-4B08-92DC-EEE8543E45D7"): "Noordzee",
        n("bnd:AA63DDF1-D7E2-49E5-B89C-D984CA469074|OME>"): "Rijkswaterstaat",
        n("bnd:CB812DC4-0F96-4BBB-9520-F001440BA7C8"): "Noordzee",
    },
)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

write_and_run_forcing_model(ribasim_model, paths)
