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

KNOWN_PUMP_CAPACITIES = {
    # based on Excel for D-HYDRO model
    364: 4.44,
    708: 2.78,
    517: 3.00,
    564: 1.22,
    # based on Gemalen stichting, https://www.gemalen.nl/gemaal_per_waterschap.asp?ws=Waterschap+Zuiderzeeland
    719: 1,  # Leeghwater, unknown capacity, 1 m3/s based on expert judgement
    857: 500 * 4 / 60,  # Wortman, also the capacity on the website of the water board and in its delivery
    853: 770 * 2 / 60,  # Blocq van Kuffeler, Lage Vaart
    855: 935 * 2 / 60,  # Blocq van Kuffeler, Hoge Vaart
    787: 500 * 2 / 60,  # Colijn, Lage Vaart
    788: 580 / 60,  # Colijn, Hoge Vaart
    766: 580 * 2 / 60,  # Lovink; the website of the water board has 550 m3/min per pump, its delivery 16.67 m3/s
    814: 800 * 3 / 60,  # Vissering
    527: 620 * 2 / 60,  # Smeenge, also on the website of the water board; its delivery has 20 m3/s
    815: 720 * 3 / 60,  # Buma
}
for node_id, capacity in KNOWN_PUMP_CAPACITIES.items():
    assert ribasim_model.get_node_type(node_id) == "Pump", f"Expected a Pump for node {node_id}"
    ribasim_model.pump.static.df.loc[
        ribasim_model.pump.static.df.node_id == node_id,
        ("max_flow_rate", "meta_known_flow_rate"),
    ] = capacity, True

# Manning resistance
# there is a MR without geometry and without links for some reason
mr_null_geom = ribasim_model.manning_resistance.node.df[ribasim_model.manning_resistance.node.df.geometry.isna()].index
ribasim_model.node.df = ribasim_model.node.df.drop(mr_null_geom)

# lower the difference in waterlevel for each manning node, except those of storing basins
set_manning_resistance_outside_bergend(ribasim_model, length=100.0, manning_n=0.01)

# Gemaal Wortman only starts 12 cm above the -6.20 m NAP of the Lage Vaart, for sustainability and capacity, see
# https://www.zuiderzeeland.nl/wat-doet-het-waterschap/gemalen-in-flevoland/gemaal-wortman/
WORTMAN, WORTMAN_CONTROL, WORTMAN_START_LEVEL = 857, 5556, -6.20 + 0.12
assert ribasim_model.upstream_node_id(WORTMAN, link_type="control") == WORTMAN_CONTROL
condition_df = ribasim_model.discrete_control.condition.df
assert condition_df is not None
wortman_condition = condition_df.node_id == WORTMAN_CONTROL
assert wortman_condition.sum() == 1, f"Expected one condition of DiscreteControl {WORTMAN_CONTROL}"
wortman_shift = WORTMAN_START_LEVEL - condition_df.loc[wortman_condition, "threshold_high"].item()
assert wortman_shift > 0, "Expected the start level of Wortman above its threshold from the target level"
condition_df.loc[wortman_condition, ["threshold_high", "threshold_low"]] += wortman_shift
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == WORTMAN, "min_upstream_level"] += wortman_shift

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
