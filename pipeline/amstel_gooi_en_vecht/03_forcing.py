"""Parameterisation of water board: Amstel, Gooi en Vecht."""

import math
from typing import cast

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim import Node
from ribasim.nodes import level_boundary, outlet
from ribasim_nl.control import set_flow_rate, set_node_functions
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
from shapely.geometry import Point

from ribasim_nl import CloudStorage, Model, supply

# model settings
waterschap = "AmstelGooienVecht"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/Wateraanvoer/afvoergebiedaanvoergebied.gpkg")

# recreate the feedback form for set_aanvoer_flags
# TODO, see if we can move set_aanvoer_flags to the feedback stage so we don't need this object
processor = paths.feedback_processor(work_dir=paths.model_dir("profiles"))

ribasim_model = Model.read(paths.model_dir("profiles") / "ribasim.toml")

# capture the zeesluis node-id by its geometry before snapping relocates it (see flushing step below)
zeesluis_point = Point(133336, 482556)
zeesluis_candidates = ribasim_model.node.df.loc[ribasim_model.node.df.geometry.distance(zeesluis_point) < 1]
if len(zeesluis_candidates) != 1:
    raise ValueError(
        f"Expected exactly 1 node within 1 m of the zeesluis location {zeesluis_point.wkt}, "
        f"but found {len(zeesluis_candidates)}: {zeesluis_candidates.index.tolist()}"
    )
zeesluis_node_id = zeesluis_candidates.index[0]

ribasim_model = set_boundary_conditions(ribasim_model, paths, level_boundary_levels=(-0.42, -0.20))

# prepare 'aanvoergebieden'
aanvoergebieden = supply.special_load_geometry(
    f_geometry=aanvoer_path, method="inverse", layers=("afvoergebiedaanvoergebied", "afwateringsgebied")
)

# add control, based on the meta_categorie
keep_model_supply_basins(processor, ribasim_model)

ribasim_param.set_aanvoer_flags(
    ribasim_model,
    aanvoergebieden,
    processor,
    basin_aanvoer_on=38,
    basin_aanvoer_off=(53, 134, 144),  # 1, 196 and 222 have been merged together, remove from tuple
    outlet_aanvoer_on=856,
    aanvoer_enabled=True,
)
categorize_nodes(ribasim_model)
from_to_node_function_table = node_function_table(ribasim_model)

# change function to inlaat
to_supply = (
    338,
    348,
    390,
    425,
    435,
    450,
    456,
    489,
    518,
    539,
    541,
    548,
    632,
    679,
    710,
    722,
    738,
    741,
    774,
    789,
    861,
    1012,
    1018,
)
to_flow_control = (290, 417, 557, 762, 1013, 1032, 1033)
to_drain = (256, 626, 863, 1014)
from_to_node_function_table = set_node_functions(
    from_to_node_function_table, to_supply=to_supply, to_flow_control=to_flow_control, to_drain=to_drain
)

add_controllers(ribasim_model, from_to_node_function_table, remove_duplicates=False)

# add increased flushing at the location of Zeesluis
from_to_node_function_table.loc[from_to_node_function_table.index == zeesluis_node_id, "demand"] = 5  # 5 m3/s

# there are some duplicates in the discrete control? Remove them
control = ribasim_model.link.df[ribasim_model.link.df.link_type == "control"]
dup_control = []
all_nodes = ribasim_model.node.df[["node_type"]]
for to_node_id, group in control.groupby("to_node_id"):
    if len(group) == 1:
        continue
    elif len(group) == 2:
        group = group.merge(all_nodes, left_on="from_node_id", right_index=True, how="inner")
        if set(group.node_type.tolist()) == {"DiscreteControl", "FlowDemand"}:
            continue
        else:
            dup_control.append(group.from_node_id.iat[0])
    else:
        raise ValueError(
            f"found {len(group)} incoming control links for {to_node_id=} from {set(group.from_node_id.tolist())}"
        )

for duplicate in dup_control:
    ribasim_model.remove_node(cast(int, duplicate), True)
    print(f"Removed duplicate control node {duplicate}")


assign_pump_and_basin_names(ribasim_model, paths)

# manually assign better names (as suggested by JWV in FF)
ribasim_model.node.df.loc[19, "name"] = "Kromme Mijdrecht"
ribasim_model.node.df.loc[97, "name"] = "Amstel-Drecht kanaal"
ribasim_model.node.df.loc[20, "name"] = "Amstel"
ribasim_model.node.df.loc[85, "name"] = "Amstel"
ribasim_model.node.df.loc[86, "name"] = "Amstel"

increase_flow_rate_pumps = [412, 146]

# presumably wrong conversion of flow capacity in the data
assert ribasim_model.pump.static.df is not None
for pump_id in increase_flow_rate_pumps:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_id, "flow_rate"] *= 60
set_flow_rate(ribasim_model.pump.static.df, [475], 0.60)  # increase flow_rate of specific pump

# lower the difference in waterlevel for each manning node, except those of storing basins
set_manning_resistance_outside_bergend(ribasim_model, length=100.0, manning_n=0.01)

# The unused locks between the Amsterdam-Rijnkanaal and the Vecht are always open, see
# https://github.com/Deltares/Ribasim-NL/issues/916. They are Outlets without control or level limits, which keep both
# at the same level. The LevelBoundaries lie on the ARK, to which they are coupled in 09_koppelen_modellen.py.
# Vechtsluis and Nieuwersluis have a capacity of 5 m3/s, see https://github.com/Deltares/Ribasim-NL/pull/928#issuecomment-6437301792
# (name, Outlet, LevelBoundary on the ARK, Vecht Basin, whether the Outlet flows from the ARK to the Vecht, capacity [m3/s])
ARK_VECHT_LOCKS = (
    ("Vechtsluis", Point(132189, 459981), Point(132143, 459931), 23, True, 5.0),  # below ManningResistance 525
    ("Nieuwersluis", Point(129006, 467646), Point(128445, 467756), 23, True, 5.0),
    ("Sluis Nigtevecht", Point(130130, 476375), Point(129950, 476424), 206, False, 10.0),
)
ARK_LEVEL = -0.4  # m NAP
ark_vecht_outlet_ids, ark_boundary_ids = [], []
for name, outlet_point, ark_point, basin_id, to_vecht, flow_rate in ARK_VECHT_LOCKS:
    boundary_node = ribasim_model.level_boundary.add(
        Node(geometry=ark_point), [level_boundary.Static(level=[ARK_LEVEL])]
    )
    outlet_node = ribasim_model.outlet.add(
        Node(geometry=outlet_point, name=name),
        [outlet.Static(flow_rate=[flow_rate], max_flow_rate=[flow_rate])],
    )
    # `clean_tables` keeps the tables of the nodes by their meta_node_id
    ribasim_model.node.df.loc[[boundary_node.node_id, outlet_node.node_id], "meta_node_id"] = [
        boundary_node.node_id,
        outlet_node.node_id,
    ]
    ark_vecht_outlet_ids.append(outlet_node.node_id)
    ark_boundary_ids.append(boundary_node.node_id)
    basin_node = ribasim_model.basin[basin_id]
    if to_vecht:
        ribasim_model.link.add(boundary_node, outlet_node)
        ribasim_model.link.add(outlet_node, basin_node)
    else:
        ribasim_model.link.add(basin_node, outlet_node)
        ribasim_model.link.add(outlet_node, boundary_node)

ribasim_model = complete_parametrization(
    ribasim_model,
    paths,
    custom_nodes={
        907: "HollandsNoorderkwartier",
        1050: "HollandsNoorderkwartier",
        908: "HollandsNoorderkwartier",
        905: "Rijkswaterstaat",
        909: "Rijkswaterstaat",
        931: "Rijkswaterstaat",
        966: "Rijkswaterstaat",
        2956: "Rijkswaterstaat",
        994: "Rijnland",
        995: "Rijkswaterstaat",
    },
)

# set the unknown capacities of Pumps and Outlets from the area they drain or supply
set_design_capacities(ribasim_model, from_to_node_function_table)

# the locks between the ARK and the Vecht are coupled to the ARK, and keep their capacity and no level limits
ark_boundaries = ribasim_model.level_boundary.node.df.loc[ark_boundary_ids]
assert (ark_boundaries["meta_couple_authority"] == "Rijkswaterstaat").all(), ark_boundaries["meta_couple_authority"]
assert ribasim_model.outlet.static.df is not None
ark_vecht_static = ribasim_model.outlet.static.df[ribasim_model.outlet.static.df.node_id.isin(ark_vecht_outlet_ids)]
assert len(ark_vecht_static) == len(ark_vecht_outlet_ids), "Expected one static row per lock"
capacities = ark_vecht_static["node_id"].map(
    dict(zip(ark_vecht_outlet_ids, [lock[-1] for lock in ARK_VECHT_LOCKS], strict=True))
)
assert ark_vecht_static["flow_rate"].eq(capacities).all() and ark_vecht_static["max_flow_rate"].eq(capacities).all()
assert ark_vecht_static["min_upstream_level"].fillna(-math.inf).eq(-math.inf).all()
assert ark_vecht_static["max_downstream_level"].fillna(math.inf).eq(math.inf).all()

# manually assign max_flow_rates (as suggested by JWV in FF)
assert ribasim_model.outlet.static.df is not None
ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df.node_id == 826, "max_flow_rate"] = 0.05

# manually assign max_flow_rates to keep the lakes on target level, maybe due to large groundwater fluxes
ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df.node_id == 393, "max_flow_rate"] = 2
ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df.node_id == 676, "max_flow_rate"] = 2
ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df.node_id == 886, "max_flow_rate"] = 2
ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df.node_id == 1051, "max_flow_rate"] = 10

write_and_run_forcing_model(ribasim_model, paths)
