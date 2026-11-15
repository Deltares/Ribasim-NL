"""Parameterisation of water board: Rijnland."""

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim import Node
from ribasim.nodes import level_boundary, pump
from ribasim_nl.crossings.basin_references import References2024, rating_curves_between_levels
from ribasim_nl.split_basins import NodeMetaCache, SplitBasins
from ribasim_nl.workflow import UNKNOWN_STREEFPEIL, WaterBoardPaths, process_feedback_form
from shapely import Point

from ribasim_nl import CloudStorage

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "Rijnland"
base_model_versie = "2024_12_3"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)
splitted_basin_1_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_1.gpkg")
splitted_basin_15_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_15.gpkg")
splitted_basin_22_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_22.gpkg")

default_level = 2 if AANVOER_CONDITIONS else -2  # default LevelBoundary level

# process the feedback form and load the model
ribasim_model = process_feedback_form(paths, use_validation=False)
# the Basins and added nodes below are those of the models of 2024, see `References2024`
refs = References2024(ribasim_model, paths.boezemmodel_ids, paths.base_model_toml, paths.feedback_form)

inlaat_pump = []

# add levelboundary to avoid incorrect coupling of water authorities
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(110865, 446289)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(110884, 446307)), [pump.Static(flow_rate=[1.5])])
ribasim_model.link.add(refs.basin(338, pump_node), pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

assert ribasim_model.pump.static.df is not None
for n in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_aanvoer"] = 1

# (re)set 'meta_node_id'-values
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# model specific tweaks
# merge basins
refs.merge_basins(node_id=106, to_node_id=93, are_connected=True)  # (too) small area
refs.merge_basins(node_id=235, to_node_id=151, are_connected=True)  # (too) small area
refs.merge_basins(node_id=166, to_node_id=22, are_connected=True)  # (too) small area
refs.merge_basins(node_id=79, to_node_id=22, are_connected=True)  # (too) small area
# unconnected basins
refs.merge_basins(node_id=308, to_node_id=22, are_connected=False)
refs.merge_basins(node_id=332, to_node_id=138, are_connected=False)

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(88284, 469447)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(refs.basin(22, pump_node), pump_node)
ribasim_model.link.add(pump_node, refs.basin(27, pump_node))
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1

# re-define LevelBoundary-nodes connecting to HDSR, which are closer to the connector-nodes,
#  and thereby result in better coupling of the sub-models
ribasim_param.reassign_level_boundaries(ribasim_model, {141, 145})

# (re)set 'meta_node_id'-values
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# change unknown streefpeilen to a default streefpeil
assert ribasim_model.basin.area.df is not None
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(UNKNOWN_STREEFPEIL)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    UNKNOWN_STREEFPEIL
)

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

# a Basin that is merged with one of another target level keeps its ManningResistances
print(f"ManningResistances between target levels: {rating_curves_between_levels(ribasim_model, UNKNOWN_STREEFPEIL)}")

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)
ribasim_param.clean_tables(ribasim_model, waterschap)

# loop through all splitted basins
node_cache = NodeMetaCache(ribasim_model)
for splitted_basin_path, basin_id in zip(
    [splitted_basin_1_path, splitted_basin_15_path, splitted_basin_22_path], [1, 15, 22], strict=True
):
    # split basins to improve model convergence
    splitter = SplitBasins(
        model=ribasim_model, splitted_basin_path=splitted_basin_path, basin_node_id_to_split=basin_id
    )
    ribasim_model = splitter.run()

node_cache.set_meta_category(ribasim_model)
ribasim_model.write(paths.model_dir("feedback") / "ribasim.toml")
del node_cache
