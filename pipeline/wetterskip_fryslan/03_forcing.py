"""Parameterisation of water board: Fryslan."""

from typing import Literal, TypedDict

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim_nl.control import (
    set_continuous_level_control,
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
    write_and_run_forcing_model,
)
from shapely.geometry import Point

from ribasim_nl import CloudStorage, Model, supply

# model settings
waterschap = "WetterskipFryslan"
base_model_versie = "2025_5_1"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/aanvoer.gpkg")


# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# Resolve geometry-based drain node lookups before snapping relocates these nodes, so the
# hard-coded coordinates still match the original node locations (used much further below).
_drain_points = [Point(206421, 592530), Point(206360, 592679)]
to_drain_node_ids = []
for _drain_point in _drain_points:
    _drain_candidates = ribasim_model.node.df.loc[ribasim_model.node.df.geometry.distance(_drain_point) < 1]
    if len(_drain_candidates) != 1:
        raise ValueError(
            f"Expected exactly 1 node within 1 m of drain location {_drain_point.wkt}, "
            f"but found {len(_drain_candidates)}: {_drain_candidates.index.tolist()}"
        )
    to_drain_node_ids.append(_drain_candidates.index[0])
to_drain_node_ids = tuple(to_drain_node_ids)

# Appelscha, see https://github.com/Deltares/Ribasim-NL/issues/831
# Basin 340 (AP-051, AP-143, GPG-F13117) with its bergend Basin 8125, and Basin 1001 (GPG-F13118) are already
# represented by Drents Overijsselse Delta, so they and the doorlaten 3064 and 3065 between them are removed.
for node_id in (3064, 3065, 15882, 8125, 340, 1001):
    ribasim_model.remove_node(node_id, remove_links=True)
# Basin 554 (AP-141, AP-142) drains via outlets 2018 and 2236 into Drents Overijsselse Delta, instead of being supplied
APPELSCHA_OUTLETS = (2018, 2236)
for link_id in (4907, 918, 4912, 1120):  # LevelBoundary -> Outlet -> Basin 554
    ribasim_model.reverse_link(link_id=link_id)

# Read outlet_aanvoer_on BEFORE set_boundary_conditions, of which find_upstream_downstream_target_levels strips meta columns
assert ribasim_model.outlet.static.df is not None
outlet_aanvoer_on = tuple(
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["meta_func_aanvoer"] == 1, "node_id"]
)

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-2.3456, 10))

# prepare 'aanvoergebieden'
aanvoergebieden = supply.special_load_geometry(
    f_geometry=str(aanvoer_path),
    method="extract",
    # layer="aanvoer",
    key="aanvoer",
    value=1,
)

# add control, based on the meta_categorie
ribasim_param.set_aanvoer_flags(
    ribasim_model,
    aanvoergebieden,
    processor,
    outlet_aanvoer_on=outlet_aanvoer_on,
    aanvoer_enabled=True,
)
categorize_nodes(ribasim_model, supply_outlets=False)

# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

# remove non-free flowing outlets (which flow against gravity) based on the difference in streefpeil and the threshold
ribasim_model = ribasim_param.remove_non_free_flowing_outlets(
    ribasim_model=ribasim_model,
    to_exclude=[2183, 1394, 3302, 2625, 2910, 3497, 2209, 2547, 2705, 1123, 2398, 1037, 2209],
    threshold=0.02,
    printing=True,
)

from_to_node_function_table = node_function_table(ribasim_model)

# manually change the function of some nodes based upon model inspection
to_supply = (
    1312,
    1347,
    1512,
    1596,
    1647,
    1747,
    2128,
    2398,
    2434,
    2445,
    2616,
    2624,
    2670,
    2773,
    2783,
    2812,
    2857,
    2982,
    3023,
    3150,
    3188,
    3228,
    3272,
    3376,
    3411,
    3452,
    3760,
    3880,
    3882,
    3884,
    3888,
)
to_flow_control = (2452, 3068)

to_drain = (2147, 2751, 2944, 3041, 3494, 3568, 3709, *to_drain_node_ids, *APPELSCHA_OUTLETS)

from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

# retain meta data of the outlets and pumps
add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=True, method="combine_first")

assign_pump_and_basin_names(ribasim_model, paths)

assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 2700, "max_flow_rate"] = 172 / 60
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 3891, "max_flow_rate"] = 7340 / 60

# Manning resistance
# there is a MR without geometry and without links for some reason
mr_null_geom = ribasim_model.manning_resistance.node.df[ribasim_model.manning_resistance.node.df.geometry.isna()].index
ribasim_model.node.df = ribasim_model.node.df.drop(mr_null_geom)

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    custom_nodes={
        3820: "Noordzee",
        5022: "Rijkswaterstaat",
        10958: "Rijkswaterstaat",
    },
    fill_na_authority="Noordzee",
)

assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 3863, "meta_known_flow_rate"] = (
    False  # unknown capacity, set temp value
)
set_flow_rate(ribasim_model.pump.static.df, [3863], 0.1)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

# the North Sea and Wadden Sea boundaries stay in the LHM, so give them mean sea level instead of the
# hypothetical drainage and supply levels, see https://github.com/Deltares/Ribasim-NL/issues/660
lb_node_df = ribasim_model.level_boundary.node.df
ribasim_param.set_static_level_boundaries(
    ribasim_model, lb_node_df.index[lb_node_df["meta_couple_authority"] == "Noordzee"], level=0.0
)

# Appelscha uitlaten: no supply, and drain at most 1 m3/s, see #831. They fill the Basins downstream to 9.38 m NAP,
# above their streefpeil of 9.35 m NAP, so these drain themselves, see #879
outlet_static_df = ribasim_model.outlet.static.df
assert outlet_static_df is not None
for control_state, max_flow_rate, max_downstream_level in (("aanvoer", 0.0, None), ("afvoer", 1.0, 9.38)):
    mask = outlet_static_df.node_id.isin(APPELSCHA_OUTLETS) & (outlet_static_df.control_state == control_state)
    assert mask.sum() == len(APPELSCHA_OUTLETS), f"Expected one {control_state} row per Appelscha outlet"
    outlet_static_df.loc[mask, ["flow_rate", "max_flow_rate"]] = max_flow_rate
    if max_downstream_level is not None:
        outlet_static_df.loc[mask, "max_downstream_level"] = max_downstream_level
control_link_df = ribasim_model.link.df[ribasim_model.link.df.link_type == "control"]
appelscha_control_ids = control_link_df.loc[control_link_df.to_node_id.isin(APPELSCHA_OUTLETS), "from_node_id"]
assert len(appelscha_control_ids) == len(APPELSCHA_OUTLETS), "Expected one DiscreteControl per Appelscha outlet"
ribasim_model.node.df.loc[appelscha_control_ids, "name"] = "uitlaat: 9.40/9.38 [m+NAP]"

# Boezem in- and outlets are controlled on the level of the Pikmeer, deeper in the boezem than the Basins next to them,
# with the relations of the Landelijk Sobek Model (LSM3), see https://github.com/Deltares/Ribasim-NL/issues/878.
# The Pikmeer is far from these structures in the model, so the Basins next to them get safety limits around the
# streefpeil of -0.52 m NAP, with which inlets do not overfill them and drains do not empty them.
# Makkum, Workum and Hindelopen keep their DiscreteControl.
PIKMEER_BASIN = 7669


class LevelControlOptions(TypedDict, total=False):
    """The keyword arguments of `set_continuous_level_control` used here."""

    node_type: Literal["Outlet", "Pump"]
    min_upstream_level: float
    max_downstream_level: float


INLET_LIMIT: LevelControlOptions = {"max_downstream_level": -0.45}
DRAIN_LIMIT: LevelControlOptions = {"min_upstream_level": -0.60}
LAUWERSMEER_LIMIT: LevelControlOptions = {"max_downstream_level": -0.60}
# (node_id, name, [(level [m+NAP], flow_rate [m3/s]), ...], keyword arguments)
BOEZEM_CONTROLS: tuple[tuple[int, str, list[tuple[float, float]], LevelControlOptions], ...] = (
    (3892, "inlaat: Lemmer/Teroelsterkolk", [(-1.0, 46), (-0.55, 46), (-0.53, 23), (-0.52, 0)], INLET_LIMIT),
    (3023, "inlaat: Tacozijl", [(-1.0, 23), (-0.56, 23), (-0.54, 11), (-0.53, 0)], INLET_LIMIT),
    (
        3897,
        "inlaat: Stavoren",
        [(-1.0, 33), (-0.65, 33), (-0.61, 22), (-0.58, 11), (-0.56, 5.5), (-0.54, 0)],
        INLET_LIMIT,
    ),
    (2436, "uitlaat: Woudagemaal", [(-1.0, 0), (-0.5, 0), (-0.47, 65), (-0.42, 72)], DRAIN_LIMIT),
    (3894, "uitlaat: Hooglandgemaal", [(-1.0, 0), (-0.5, 0), (-0.47, 76), (-0.42, 116)], DRAIN_LIMIT),
    # The drains below are closed under -0.52 m NAP, so level control takes priority over flushing (doorspoeling).
    # Harlingen drains the tide-averaged spui to the Waddenzee at mean sea level, so it is a Pump instead of an Outlet.
    (
        3041,
        "uitlaat: Harlingen",
        [(-1.0, 0), (-0.53, 0), (-0.52, 5.1), (-0.50, 10), (-0.42, 30)],
        DRAIN_LIMIT | {"node_type": "Pump"},
    ),
    (
        3899,
        "uitlaat: Dokkumer Nieuwe Zijlen schutsluis",
        [(-1.0, 0), (-0.53, 0), (-0.52, 1.8), (-0.50, 5.8), (-0.42, 12)],
        DRAIN_LIMIT | LAUWERSMEER_LIMIT,
    ),
    (
        3901,
        "uitlaat: Dokkumer Nieuwe Zijlen stuw",
        [(-1.0, 0), (-0.53, 0), (-0.52, 4), (-0.50, 6), (-0.42, 22)],
        DRAIN_LIMIT | LAUWERSMEER_LIMIT,
    ),
)
for node_id, name, points, kwargs in BOEZEM_CONTROLS:
    levels, flow_rates = zip(*points, strict=True)
    set_continuous_level_control(
        ribasim_model, node_id, PIKMEER_BASIN, level=levels, flow_rate=flow_rates, name=name, **kwargs
    )

write_and_run_forcing_model(ribasim_model, paths)
