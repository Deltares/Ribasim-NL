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
from shapely.geometry import Point, box

from ribasim_nl import CloudStorage, Model

# model settings
waterschap = "Rivierenland"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(
    cloud, waterschap, base_model_versie, extra_waterways=(cloud.joinpath("Basisgegevens/GSK3E/gsk3e.gpkg"),)
)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/Aanvoergebieden_detail.shp")


# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

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
    (ribasim_model.level_boundary.time.df.node_id.isin([n("bnd:STW_063448"), n("bnd:ow:WTG_141314")]))
    & (ribasim_model.level_boundary.time.df.level == 12.4),
    "level",
] = 2.5  # change value of a single LB during summer time, so it can still discharge excess water

# add control, based on the meta_categorie
ribasim_param.set_aanvoer_flags(
    ribasim_model,
    str(aanvoer_path),
    processor,
    aanvoer_enabled=True,
    basin_aanvoer_off=(n("Culemborg-West_dummy_id_45")),
)
# change the control of the outlet at Kinderdijk
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == n("GML_000850"), "meta_categorie"] = (
    "Inlaat boezem, afvoer gemaal"
)

categorize_nodes(ribasim_model)
# # ribasim_param.add_discrete_control(ribasim_model, waterschap, default_level)
# ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
# ribasim_param.add_continuous_control(ribasim_model, dy=-50)

from_to_node_function_table = node_function_table(ribasim_model)

to_drain = (
    n("ow:WTG_129704_11"),
    n("GML_000839"),
    n("ow:WTG_124990"),
    n("STW_023569"),
    n("ow:WTG_111181_1"),
    n("STW_033944"),
    n("ow:WTG_145108"),
    n("GML_023194"),
    n("STW_000984"),
    n("STW_056494"),
    n("STW_032830B"),
    n("STW_044239B"),
    n("STW_001193"),
    n("STW_001151"),
)
to_flow_control = (
    n("GML_000860"),
    n("STW_001478"),
    n("STW_001447"),
    n("STW_001337"),
    n("STW_001400"),
    n("STW_028737"),
    n("STW_023261"),
    n("STW_085344B"),
    n("STW_001539|Huibert_dummy_id_89>D.W. het Lam_Autena_dummy_id_53"),
)


to_supply = (
    *inlaten,  # add all manually added inlaten
    n("STW_044193B"),
    n("GML_067107"),
    n("STW_001646"),
    n("GML_034368"),
    n("ow:WTG_099901"),
    n("GML_000900"),
    n("GML_000901"),
    n("ow:WTG_111235"),
    n("STW_001278"),
    n("STW_044017B"),
    n("loc:120255,426955"),
    n("loc:134985,444744"),
    n("loc:156123,431987"),
    n("loc:185469,440535"),
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
        n("bnd:GML_024103"): "Rijkswaterstaat",
        n("bnd:GML_000919"): "Rijkswaterstaat",
        n("bnd:GML_032793"): "Rijkswaterstaat",
        n("loc:188596,437809"): "Rijkswaterstaat",
        n("loc:137891,431581"): "Rijkswaterstaat",
    },
)

# for some reason, some connector nodes of the Linge are incorrectly scaled. Keep using the original (high) values of the flow_rate to allow for better discharge of the system.
Linge_nodes = [n("STW_023874"), n("ow:WTG_099981_6")]
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
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n("loc:132313,422591"), "max_flow_rate"] = (
    8.0
)
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n("ow:WTG_145108"), "max_flow_rate"] = 16.0

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

set_flow_rate(ribasim_model.pump.static.df, [n("ow:WTG_145108")], 8.0)  # average flow rate in the winter

# the outlets near Groesbeek drain via Germany into the Ooijpolder, so its links are routed through there, see #995
write_and_run_forcing_model(
    ribasim_model, paths, routing_areas={n("dummy_globalid_peilgebied_380"): box(192500, 420500, 199500, 428500)}
)
