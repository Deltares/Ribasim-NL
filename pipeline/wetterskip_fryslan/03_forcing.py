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
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

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
# The Basins of PBH0090243 (AP-051, AP-143, GPG-F13117) and PBH0198500 (GPG-F13118) are already represented by
# Drents Overijsselse Delta, so they and the two doorlaten between them are removed.
for node_id in (n("ow:KDU0038503"), n("ow:None_281"), n("PBH0090243"), n("PBH0198500")):
    ribasim_model.remove_node(node_id, remove_links=True)
# The Basin of PBH0088673 (AP-141, AP-142) drains via two outlets into Drents Overijsselse Delta, instead of being
# supplied
APPELSCHA_OUTLETS = (n("ow:KDU0020889"), n("ow:OAF-S3065"))
# LevelBoundary -> Outlet -> Basin, by their nodes, since the feedback form numbers the links anew
for from_node_id, to_node_id in (
    (n("bnd:ow:KDU0020889"), n("ow:KDU0020889")),
    (n("ow:KDU0020889"), n("PBH0088673")),
    (n("bnd:ow:OAF-S3065"), n("ow:OAF-S3065")),
    (n("ow:OAF-S3065"), n("PBH0088673")),
):
    ribasim_model.reverse_link(from_node_id=from_node_id, to_node_id=to_node_id)

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
    to_exclude=[
        n("ow:OAF-W2156"),
        n("ow:KDU0013220"),
        n("ow:KDU0024902"),
        n("Code_onbekend_3084"),
        n("ow:KDU0013941"),
        n("ow:None_3249"),
        n("ow:KDU0023693"),
        n("KST0050159"),
        n("KST0006695"),
        n("ow:None_9641"),
        n("ow:OVK0128641"),
        n("ow:KDU0023693"),
    ],
    threshold=0.02,
    printing=True,
)

from_to_node_function_table = node_function_table(ribasim_model)

# manually change the function of some nodes based upon model inspection
to_supply = (
    n("ow:OVK0134991"),
    n("ow:OVK0136074"),
    n("ow:OVK0139927"),
    n("ow:OAF0183279"),
    n("ow:KDU0041885"),
    n("KST0007211"),
    n("ow:None_9641"),
    n("KST0008708"),
    n("ow:OVK0140396"),
    n("ow:OVK0194473|PBH0252717>PBH0099287"),
    n("Code_onbekend_5644"),
    n("ow:OVK0166269"),
    n("ow:OAF-F1020450"),
    n("ow:OAF-F1021304"),
    n("ow:OVK0237869"),
    n("ow:KDU0014875"),
    n("ow:KDU0015727"),
    n("ow:OAF-F1021392"),
    n("ow:KDU0012166"),
    n("Code_onbekend_4056"),
    n("ow:None_759"),
    n("ow:KDU0035574"),
    n("ow:KDU0011383"),
    n("ow:KDU0022894"),
    n("ow:KDU0014829"),
    n("ow:None_9717"),
    n("loc:128115,586097"),
    n("loc:129831,587677"),
    n("loc:207500,587902"),
    n("loc:206508,592803"),
)
to_flow_control = (n("KST0006157"), n("ow:KDU0015040"))

to_drain = (
    n("ow:OAF-F1019555"),
    n("KGM0000153"),
    n("ow:OAF-F1019650"),
    n("ow:OAF-F1021117"),
    n("ow:KDU0047366"),
    n("KGM0001380"),
    n("KGM0003006"),
    *to_drain_node_ids,
    *APPELSCHA_OUTLETS,
)

from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

# retain meta data of the outlets and pumps
add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=True, method="combine_first")

assign_pump_and_basin_names(ribasim_model, paths)

assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == n("KGM0003563"), "max_flow_rate"] = 172 / 60
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == n("loc:174976,539741"), "max_flow_rate"] = (
    7340 / 60
)

# Manning resistance
# there is a MR without geometry and without links for some reason
mr_null_geom = ribasim_model.manning_resistance.node.df[ribasim_model.manning_resistance.node.df.geometry.isna()].index
ribasim_model.node.df = ribasim_model.node.df.drop(mr_null_geom)

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    custom_nodes={
        n("bnd:ow:OAF-F1021117"): "Noordzee",
        n("loc:158166,553915"): "Rijkswaterstaat",
    },
    fill_na_authority="Noordzee",
)

assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id == n("loc:171210,558399"), "meta_known_flow_rate"
] = False  # unknown capacity, set temp value
set_flow_rate(ribasim_model.pump.static.df, [n("loc:171210,558399")], 0.1)

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
# Makkum, Workum and Hindelopen keep their DiscreteControl. The relations are tuned to the measured discharges, see
# https://github.com/Deltares/Ribasim-NL/issues/1060 and https://github.com/Deltares/Ribasim-NL/pull/1076.
PIKMEER_BASIN = n("PBH0248142/1")


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
    (
        n("loc:174961,539814"),
        "inlaat: Lemmer/Teroelsterkolk",
        [(-1.0, 46), (-0.55, 46), (-0.53, 23), (-0.52, 0)],
        INLET_LIMIT,
    ),
    (
        n("ow:OAF-F1021392"),
        "inlaat: Tacozijl",
        [(-1.0, 35), (-0.56, 30), (-0.54, 23), (-0.53, 11), (-0.52, 0)],
        INLET_LIMIT,
    ),
    (
        n("loc:153406,543502"),
        "inlaat: Stavoren",
        [(-1.0, 33), (-0.65, 33), (-0.61, 22), (-0.58, 11), (-0.56, 5.5), (-0.54, 3), (-0.53, 0)],
        INLET_LIMIT,
    ),
    # The Woudagemaal only pumps when the other gemalen fall short
    (
        n("KGM0000014"),
        "uitlaat: Woudagemaal",
        [(-1.0, 0), (-0.5, 0), (-0.40, 0), (-0.39, 72)],
        DRAIN_LIMIT,
    ),
    (
        n("loc:153456,543428"),
        "uitlaat: Hooglandgemaal",
        [(-1.0, 0), (-0.51, 0), (-0.49, 116), (-0.44, 122.5)],
        DRAIN_LIMIT,
    ),
    # The drains below are closed under -0.52 m NAP, so level control takes priority over flushing (doorspoeling).
    # Harlingen drains the tide-averaged spui to the Waddenzee at mean sea level, so it is a Pump instead of an Outlet.
    (
        n("ow:OAF-F1021117"),
        "uitlaat: Harlingen",
        [(-1.0, 0), (-0.52, 0), (-0.51, 5.1), (-0.49, 10), (-0.39, 30)],
        DRAIN_LIMIT | {"node_type": "Pump"},
    ),
    # The stuw drains before the schutsluis, which only takes the peaks
    (
        n("loc:206421,592530"),
        "uitlaat: Dokkumer Nieuwe Zijlen schutsluis",
        [(-1.0, 0), (-0.54, 0), (-0.53, 0), (-0.51, 0), (-0.41, 30)],
        DRAIN_LIMIT | LAUWERSMEER_LIMIT,
    ),
    (
        n("loc:206360,592679"),
        "uitlaat: Dokkumer Nieuwe Zijlen stuw",
        [(-1.0, 0), (-0.53, 0), (-0.52, 5.8), (-0.50, 22), (-0.40, 60)],
        DRAIN_LIMIT | LAUWERSMEER_LIMIT,
    ),
)
for node_id, name, points, kwargs in BOEZEM_CONTROLS:
    levels, flow_rates = zip(*points, strict=True)
    set_continuous_level_control(
        ribasim_model, node_id, PIKMEER_BASIN, level=levels, flow_rate=flow_rates, name=name, **kwargs
    )

# The inlets from the IJsselmeer have no name in the source data, see https://github.com/Deltares/Ribasim-NL/issues/1010
INLET_NAMES = {
    n("ow:OAF-F1021304"): "Hindeloopen",
    n("ow:OAF-F1021392"): "Inlaat Tacozijl",
    n("ow:KDU0011599"): "Makkum-inlaat Suderseewei",
    n("loc:156038,563082"): "Sluis Makkum",
    n("loc:174961,539814"): "Inlaat Teroelsterkolk",
    n("loc:153406,543502"): "Inlaat Johan Friso sluis",
}
for node_id, name in INLET_NAMES.items():
    node_type = ribasim_model.get_node_type(node_id)
    assert node_type == "Outlet", f"Expected Outlet {node_id} for {name}, got {node_type}"
    ribasim_model.node.df.loc[node_id, "name"] = name

write_and_run_forcing_model(ribasim_model, paths)
