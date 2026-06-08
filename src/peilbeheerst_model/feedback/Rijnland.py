"""Parameterisation of water board: Rijnland."""

import warnings

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import Node
from ribasim.nodes import level_boundary, pump
from ribasim_nl.split_basins import NodeMetaCache, SplitBasins
from shapely import Point

from ribasim_nl import CloudStorage, Model

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True
DYNAMIC_CONDITIONS: bool = True
RESCALE_FLOW_CAPACITIES: bool = True
ADD_LHM_FRACTIONS: bool = True
ADD_RWZI: bool = True
ADD_JUNCTIONS: bool = False

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

MIXED_CONDITIONS_DESIGN_P = 12
MIXED_CONDITIONS_DESIGN_E = 2

# model settings
waterschap = "Rijnland"
base_model_versie = "2024_12_3"

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
splitted_basin_1_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_1.gpkg")
splitted_basin_15_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_15.gpkg")
splitted_basin_22_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_22.gpkg")

cloud.synchronize(
    filepaths=[
        ribasim_base_model_dir,
        FeedbackFormulier_path,
        splitted_basin_1_path,
        splitted_basin_15_path,
        splitted_basin_22_path,
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
default_level = 2 if AANVOER_CONDITIONS else -2  # default LevelBoundary level

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
    use_validation=False,
)
processor.run()

# load model
with warnings.catch_warnings():
    warnings.simplefilter(action="ignore", category=FutureWarning)
    ribasim_model = Model.read(ribasim_work_dir_model_toml)
    ribasim_model.set_crs("EPSG:28992")

inlaat_pump = []

# add levelboundary to avoid incorrect coupling of water authorities
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(110865, 446289)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(110884, 446307)), [pump.Static(flow_rate=[1.5])])
ribasim_model.link.add(ribasim_model.basin[338], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

for n in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_aanvoer"] = 1

# (re)set 'meta_node_id'-values
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# model specific tweaks
# merge basins
ribasim_model.merge_basins(node_id=106, to_node_id=93, are_connected=True)  # (too) small area
ribasim_model.merge_basins(node_id=235, to_node_id=151, are_connected=True)  # (too) small area
ribasim_model.merge_basins(node_id=166, to_node_id=22, are_connected=True)  # (too) small area
ribasim_model.merge_basins(node_id=79, to_node_id=22, are_connected=True)  # (too) small area
# unconnected basins
ribasim_model.merge_basins(node_id=308, to_node_id=22, are_connected=False)
ribasim_model.merge_basins(node_id=332, to_node_id=138, are_connected=False)

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(88284, 469447)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[22], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[27])
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1

# re-define LevelBoundary-nodes connecting to HDSR, which are closer to the connector-nodes,
#  and thereby result in better coupling of the sub-models
ribasim_param.reassign_level_boundaries(ribasim_model, {141, 145})

# (re)set 'meta_node_id'-values
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

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

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
ribasim_model.write(ribasim_work_dir_model_toml)
del node_cache
