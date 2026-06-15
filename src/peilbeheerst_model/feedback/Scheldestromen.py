"""Parameterisation of water board: Scheldestromen."""

import warnings

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import Point

from ribasim_nl import CloudStorage, Model

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "Scheldestromen"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect data from the base model, feedback form, waterauthority & RWS border
ribasim_base_model_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_boezemmodel_{base_model_versie}")
FeedbackFormulier_path = cloud.joinpath(
    waterschap, "verwerkt/Feedback Formulier", f"feedback_formulier_{waterschap}.xlsx"
)
FeedbackFormulier_LOG_path = cloud.joinpath(
    waterschap, "verwerkt/Feedback Formulier", f"feedback_formulier_{waterschap}_LOG.xlsx"
)

cloud.synchronize(
    filepaths=[
        ribasim_base_model_dir,
        FeedbackFormulier_path,
    ]
)

# refresh only the feedback form from cloud
# cloud.download_file(cloud.file_url(FeedbackFormulier_path))

work_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_feedback")
work_dir.mkdir(parents=True, exist_ok=True)

ribasim_work_dir_model_toml = work_dir.joinpath("ribasim.toml")

# set path to base model toml
ribasim_base_model_toml = ribasim_base_model_dir.joinpath("ribasim.toml")

unknown_streefpeil = (
    0.00012345  # we need a streefpeil to create the profiles, Q(h)-relations, and af- and aanslag peil for pumps
)

# forcing settings
delta_crest_level = 0.1  # delta waterlevel of boezem compared to streefpeil till no water can flow through an outlet
default_level = 0.42 if AANVOER_CONDITIONS else -0.42  # default LevelBoundary level

# process the feedback form
name = "HKV"
processor = RibasimFeedbackProcessor(
    name,
    waterschap,
    base_model_versie,
    FeedbackFormulier_path,
    ribasim_base_model_toml,
    work_dir,
    FeedbackFormulier_LOG_path,
    use_validation=True,
)
processor.run()

# load model
with warnings.catch_warnings():
    warnings.simplefilter(action="ignore", category=FutureWarning)
    ribasim_model = Model.read(ribasim_work_dir_model_toml)
    ribasim_model.set_crs("EPSG:28992")

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
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
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

ribasim_param.clean_tables(ribasim_model, waterschap)
ribasim_model.write(ribasim_work_dir_model_toml)
