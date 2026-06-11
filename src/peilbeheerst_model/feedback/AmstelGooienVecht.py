"""Parameterisation of water board: Amstel, Gooi en Vecht."""

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
waterschap = "AmstelGooienVecht"
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

# # download the Feedback Formulieren, overwrite the old ones
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
default_level = -0.42  # default LevelBoundary level

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

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

# merge basins
ribasim_model.merge_basins(node_id=162, to_node_id=177, are_connected=False)
ribasim_model.merge_basins(node_id=178, to_node_id=177, are_connected=True)
ribasim_model.merge_basins(node_id=184, to_node_id=205, are_connected=True)  # convergence test
ribasim_model.merge_basins(node_id=159, to_node_id=54, are_connected=True)  # convergence test
ribasim_model.merge_basins(node_id=1, to_node_id=196, are_connected=True)  # convergence test
ribasim_model.merge_basins(node_id=62, to_node_id=129, are_connected=True)  # convergence test
ribasim_model.merge_basins(node_id=131, to_node_id=63, are_connected=True)  # convergence test
ribasim_model.merge_basins(node_id=222, to_node_id=88, are_connected=True)  # convergence test
ribasim_model.merge_basins(node_id=227, to_node_id=42, are_connected=True)  # convergence test
ribasim_model.merge_basins(node_id=133, to_node_id=15, are_connected=False)  # convergence test
ribasim_model.merge_basins(node_id=13, to_node_id=15, are_connected=False)  # convergence test
ribasim_model.merge_basins(node_id=196, to_node_id=56, are_connected=True)  # convergence test

# model specific tweaks
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(125905, 486750)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(125958, 486838)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[225])

# add additional pump and LB to ARK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(129850, 480894)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(129829, 480893)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[229], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add outlet and LB from ARK-NZK to Loosdrechtse Plassen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(129097, 468241)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(129097, 468241)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[59])


# add outlet and LB (Zeesluis)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(133355, 482943)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(133336, 482556)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[59])
zeesluis_node_id = tabulated_rating_curve_node.node_id

# add outlet to Gooimeer
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(146641, 479856)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(146592, 479749)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[215], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# add additional pump and LB to ARK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(129677, 482929)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(129674, 482974)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[228], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(119234, 492570)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(119234, 492587)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[9])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(123115, 489284)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(123112, 489351)), [pump.Static(flow_rate=[20])])
# TODO: Verify setting to 'aan- & afvoergemaal'
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[12])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(124470, 489070)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(124465, 489070)), [tabulated_rating_curve.Static(level=[0, 0.1234], flow_rate=[0, 0.1234])]
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[182])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(127630, 485961)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(127621, 485961)), [tabulated_rating_curve.Static(level=[0, 0.1234], flow_rate=[0, 0.1234])]
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[174])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(148516, 478866)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(148397, 478753)), [pump.Static(flow_rate=[20])])
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[168])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(148550, 478832)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(148437, 478733)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(ribasim_model.basin[168], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

ribasim_model.remove_node(350, True)
ribasim_model.remove_node(673, True)
ribasim_model.remove_node(1000, False)
ribasim_param.change_pump_func(ribasim_model, 430, "aanvoer", 1)
ribasim_param.change_pump_func(ribasim_model, 430, "afvoer", 0)

#  a multipolygon occurs in some basins (88, 62). Only retain the largest value. Update: 62 has been merged
multipolygon_basins = [88]
for basin_to_explode in multipolygon_basins:
    exploded_basins = ribasim_model.basin.area.df.loc[
        ribasim_model.basin.area.df["node_id"] == basin_to_explode
    ].explode(index_parts=False)
    exploded_basins["area"] = exploded_basins.area
    largest_polygon = exploded_basins.sort_values(by="area", ascending=False).iloc[0]
    ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df.node_id == basin_to_explode, "geometry"] = (
        largest_polygon["geometry"]
    )

# set all 'meta_node_id'-values
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

ribasim_param.clean_tables(ribasim_model, waterschap)
ribasim_model.write(ribasim_work_dir_model_toml)
