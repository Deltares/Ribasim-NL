"""Parameterisation of water board: Scheldestromen."""

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.workflow import UNKNOWN_STREEFPEIL, WaterBoardPaths, process_feedback_form
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import Point

from ribasim_nl import CloudStorage

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "Scheldestromen"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)

default_level = 0.42 if AANVOER_CONDITIONS else -0.42  # default LevelBoundary level

# process the feedback form and load the model
ribasim_model = process_feedback_form(paths)

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check target levels at both sides of the Manning Nodes
ribasim_param.validate_manning_basins(ribasim_model)

# model specific tweaks
# the vrij-afwaterende basins are a multipolygon, in a single basin (189). Only retain the largest value
exploded_basins = ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["node_id"] == 189].explode(
    index_parts=False
)
exploded_basins["area"] = exploded_basins.area
largest_polygon = exploded_basins.sort_values(by="area", ascending=False).iloc[0]
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df.node_id == 189, "geometry"] = largest_polygon["geometry"]

# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(UNKNOWN_STREEFPEIL)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    UNKNOWN_STREEFPEIL
)

inlaat_structures = []
# add an TRC and links to the newly created level boundary
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(74861, 382484)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(geometry=Point(74504, 382443)), [pump.Static(flow_rate=[0.1])])
ribasim_model.node.df.loc[pump_node.node_id, "meta_node_id"] = pump_node.node_id
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[133])

# add a pump and links to a newly created level boundary
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(65450, 374986)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(65429, 374945)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[148], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add a TRC and LB from Belgium
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(43290, 356428)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(43486, 357740)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[1])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# connection with Belgium
ribasim_model.remove_node(29, True)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(35147, 362794)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(level_boundary_node, ribasim_model.tabulated_rating_curve[491])
ribasim_model.link.add(level_boundary_node, ribasim_model.tabulated_rating_curve[547])
ribasim_model.link.add(level_boundary_node, ribasim_model.tabulated_rating_curve[334])
ribasim_model.link.add(level_boundary_node, ribasim_model.tabulated_rating_curve[554])
ribasim_model.link.add(ribasim_model.tabulated_rating_curve[227], level_boundary_node)
ribasim_model.link.add(ribasim_model.tabulated_rating_curve[381], level_boundary_node)
inlaat_structures.extend([491, 547, 334, 554])
inlaat_structures.append(309)

# (re) set 'meta_node_id'
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

for node in inlaat_structures:
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_aanvoer"] = 1
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_afvoer"] = 0

# add Gemaal Postweg, Kapelle, draining GPG1336 (basin 138) to GPG1335 (basin 142), see #844
# added last, so the node IDs of the nodes added above do not change
# its name and capacity are assigned from the 'gemaal' layer in the forcing stage
pump_node = ribasim_model.pump.add(Node(geometry=Point(57403, 390161)), [pump.Static(flow_rate=[0.1])])
ribasim_model.node.df.loc[pump_node.node_id, "meta_node_id"] = pump_node.node_id
ribasim_model.link.add(ribasim_model.basin[138], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[142])

ribasim_param.clean_tables(ribasim_model, waterschap)
ribasim_model.write(paths.model_dir("feedback") / "ribasim.toml")
