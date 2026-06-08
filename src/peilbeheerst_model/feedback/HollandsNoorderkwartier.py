"""Parameterisation of water board: Hollands Noorderkwartier."""

import warnings

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
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
waterschap = "HollandsNoorderkwartier"
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
splitted_basin_3_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_3.gpkg")

cloud.synchronize(
    filepaths=[
        ribasim_base_model_dir,
        FeedbackFormulier_path,
        splitted_basin_3_path,
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

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

# model specific tweaks
# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

# add spui Schermerboezem Den Helder (near gemaal Helsdeur)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(114728, 551405)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(114698, 551327)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[3], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# add hevel near Andijk
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(141549, 529097)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(141591, 529071)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[66])

# add hevel near stedelijk gebied ARK-NZK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(113959, 493654)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(113933, 493655)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[160])

# add hevel near stedelijk gebied ARK-NZK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(109989, 494265)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(110059, 494258)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[221])

# add gemaal and LB at boezemgemaal Monnickendam. Zowel af- als aanvoer. Dit blok: aanvoer
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(131027, 497603)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(130877, 497615)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[3])

# add gemaal and LB at boezemgemaal Monnickendam. Zowel af- als aanvoer. Dit blok: afvoer
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(131027, 497613)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(130877, 497625)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(ribasim_model.basin[3], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add gemaal and LB at Waterlandseboezem .
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(132115, 495290)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(132040, 495282)), [pump.Static(flow_rate=[10])])
ribasim_model.link.add(ribasim_model.basin[2], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add hevelat Schermersluis (ARK-NZK --> boezem)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(111930, 494827)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(111934, 494910)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[3])

ribasim_model.merge_basins(node_id=201, to_node_id=79, are_connected=False)  # klein snippertje vlakbij IJsselmeer
ribasim_model.merge_basins(node_id=192, to_node_id=17)  # klein snippertje vlakbij Markermeer
ribasim_model.merge_basins(node_id=182, to_node_id=2)  # klein snippertje
ribasim_model.merge_basins(node_id=214, to_node_id=3)  # klein snippertje in boezem
ribasim_model.merge_basins(node_id=117, to_node_id=6)  # klein gebiedje in enclave peilgebied
ribasim_model.merge_basins(node_id=218, to_node_id=3)  # klein gebiedje vlakbij boezem
ribasim_model.merge_basins(node_id=113, to_node_id=3)  # klein gebiedje in boezem
ribasim_model.merge_basins(node_id=203, to_node_id=21)  # klein gebiedje in duinen
ribasim_model.merge_basins(node_id=228, to_node_id=193)  # convergence
ribasim_model.merge_basins(node_id=110, to_node_id=3)  # convergence
ribasim_model.merge_basins(node_id=148, to_node_id=2)  # convergence


# (re)set 'meta_node_id'-values
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

ribasim_param.clean_tables(ribasim_model, waterschap)

# split basins to improve model convergence
node_cache = NodeMetaCache(ribasim_model)
splitter = SplitBasins(model=ribasim_model, splitted_basin_path=splitted_basin_3_path, basin_node_id_to_split=3)
ribasim_model = splitter.run()
node_cache.set_meta_category(ribasim_model)
ribasim_model.write(ribasim_work_dir_model_toml)
del node_cache
