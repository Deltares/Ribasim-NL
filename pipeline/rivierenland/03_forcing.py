"""Parameterisation of water board: Rivierenland."""

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
from shapely.geometry import Point

from ribasim_nl import CloudStorage, Model

# model settings
waterschap = "Rivierenland"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/Aanvoergebieden_detail.shp")


# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# Resolve geometry-based inlaat node lookups before snapping relocates these nodes, so the
# hard-coded coordinates still match the original node locations (used much further below).
_inlaat_points = [Point(174615, 440126), Point(103334, 433570), Point(103446, 433601), Point(198568, 434184)]
inlaten = []
for _inlaat_point in _inlaat_points:
    _inlaat_candidates = ribasim_model.node.df.loc[ribasim_model.node.df.geometry.distance(_inlaat_point) < 1]
    if len(_inlaat_candidates) != 1:
        raise ValueError(
            f"Expected exactly 1 node within 1 m of inlaat location {_inlaat_point.wkt}, "
            f"but found {len(_inlaat_candidates)}: {_inlaat_candidates.index.tolist()}"
        )
    inlaten.append(_inlaat_candidates.index[0])
inlaat_Kuijk = inlaten[0]

# reset pump capacity for each pump
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df["flow_rate"] = 10 / 60  # 10m3/min

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-0.6, 12.4))

assert ribasim_model.level_boundary.time.df is not None
ribasim_model.level_boundary.time.df.loc[
    (ribasim_model.level_boundary.time.df.node_id.isin([963, 957]))
    & (ribasim_model.level_boundary.time.df.level == 12.4),
    "level",
] = 2.5  # change value of a single LB during summer time, so it can still discharge excess water

# add control, based on the meta_categorie
ribasim_param.set_aanvoer_flags(
    ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=True, basin_aanvoer_off=(204)
)
# change the control of the outlet at Kinderdijk
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 280, "meta_categorie"] = (
    "Inlaat boezem, afvoer gemaal"
)

categorize_nodes(ribasim_model)
# # ribasim_param.add_discrete_control(ribasim_model, waterschap, default_level)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

from_to_node_function_table = node_function_table(ribasim_model)

to_drain = (
    303,
    363,
    494,
    522,
    525,
    565,
    664,
    668,
    723,
    785,
    786,
    814,
    833,
    840,
)
to_flow_control = (
    270,
    322,
    384,
    386,
    388,
    468,
    574,
    856,
    879,
)


to_supply = (
    *inlaten,  # add all manually added inlaten
    245,
    325,
    338,
    478,
    557,
    566,
    575,
    625,
    822,
    886,
    988,
    990,
    999,
    1002,
)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)


add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=False)

assign_pump_and_basin_names(ribasim_model, paths, pump_max_distance=25)

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.node.df = ribasim_model.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node, except those of storing basins
set_manning_resistance_outside_bergend(ribasim_model, length=10.0, manning_n=0.01)

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    custom_nodes={
        973: "Rijkswaterstaat",
        940: "Rijkswaterstaat",
        939: "Rijkswaterstaat",
        1004: "Rijkswaterstaat",
        1005: "Rijkswaterstaat",
    },
)

# for some reason, some connector nodes of the Linge are incorrectly scaled. Keep using the original (high) values of the flow_rate to allow for better discharge of the system.
Linge_nodes = [433, 612, 666]
assert ribasim_model.outlet.static.df is not None
ribasim_model.outlet.static.df.loc[
    ribasim_model.outlet.static.df["node_id"].isin(Linge_nodes), "meta_known_flow_rate"
] = True

# add fixed max flow rate for inlaat Kuijk
ribasim_model.outlet.static.df.loc[
    ribasim_model.outlet.static.df["node_id"] == inlaat_Kuijk, ["max_flow_rate", "meta_known_flow_rate"]
] = [20.0, True]

# fix pumps from Linge to ARK
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == 1025, "max_flow_rate"] = 8.0
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == 664, "max_flow_rate"] = 16.0

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

set_flow_rate(ribasim_model.pump.static.df, [664], 8.0)  # average flow rate in the winter

write_and_run_forcing_model(ribasim_model, paths)
