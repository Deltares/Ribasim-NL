"""Parametrisation of water board: Delfland."""

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

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "Delfland"
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

splitted_basin_2_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_2.gpkg")
splitted_basin_9_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_9.gpkg")
splitted_basin_10_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_10.gpkg")

cloud.synchronize(
    filepaths=[
        ribasim_base_model_dir,
        FeedbackFormulier_path,
        splitted_basin_2_path,
        splitted_basin_9_path,
        splitted_basin_10_path,
    ]
)

# refresh only the feedback form from cloud (instead of all "verwerkt" files)
# cloud.download_file(cloud.file_url(FeedbackFormulier_path))

work_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_feedback")
work_dir.mkdir(parents=True, exist_ok=True)

ribasim_work_dir_model_toml = work_dir.joinpath("ribasim.toml")

# set path to base model toml
ribasim_base_model_toml = ribasim_base_model_dir.joinpath("ribasim.toml")

unknown_streefpeil = (
    0.00012345  # we need a streefpeil to create the profiles, Q(h)-relations, and af- and aanslag peil for pumps
)

delta_crest_level = 0.1  # delta waterlevel of boezem compared to streefpeil till no water can flow through an outlet
default_level = 0.42 if AANVOER_CONDITIONS else -0.42

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

# model specific tweaks

# should have been boezem
ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df.node_id == 3, "meta_categorie"] = "hoofdwater"

# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

# change high initial states to 0
ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df["level"] == 9.999, "level"] = 0.0
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == 9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

inlaat_pump = []

# change gemaal function Brielse Meer to aanvoer. Find node_id first, as it is added in the feedback form
BrielseMeerNodes = ribasim_model.link.df.loc[
    ribasim_model.link.df.from_node_id == 98, "to_node_id"
]  # array of all values from basin 98
BrielseMeerAanvoerPump = ribasim_model.link.df.loc[
    (ribasim_model.link.df.from_node_id.isin(BrielseMeerNodes)) & (ribasim_model.link.df.to_node_id == 10),
    "from_node_id",
]

# convert Brielse Meer from afvoer to aanvoer
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id.isin(BrielseMeerAanvoerPump), "meta_func_afvoer"
] = 0
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id.isin(BrielseMeerAanvoerPump), "meta_func_aanvoer"
] = 1

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(81619, 439852)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[2], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[113])
inlaat_pump.append(pump_node.node_id)

# add gemaal near Rijnland (Dolk). Dont use FF as it is an aanvoergemaal + boundary
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(87256, 455139)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(87082, 455089)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[9])
inlaat_pump.append(pump_node.node_id)

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(70197, 444207)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[41], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[53])
inlaat_pump.append(pump_node.node_id)

# move node for improved recognition of the system
ribasim_model.move_node(node_id=564, geometry=Point(79320, 437067))

# add riool gemaal Vlaardingen West
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(81712, 435370), name="Rioolgemaal Vlaardingen West"),
    [pump.Static(flow_rate=[192 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(81737, 435104)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[82], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add another riool gemaal Vettenoord
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(80831, 435239), name="Rioolgemaal Vettenoord"),
    [pump.Static(flow_rate=[29 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(80824, 434959)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[82], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Vlaardingen Oost
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(84440, 436203), name="Rioolgemaal Vlaardingen Oost"),
    [pump.Static(flow_rate=[32 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(84531, 435937)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[31], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Schiedam Oost
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(88064, 436633), name="Rioolgemaal Schiedam Oost"),
    [pump.Static(flow_rate=[62 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(88008, 436382)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[114], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Spangen
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(88312, 437389), name="Rioolgemaal Spangen"),
    [pump.Static(flow_rate=[34 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(88309, 436358)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[102], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Oud-Mathenesse
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(88657, 436591), name="Rioolgemaal Oud-Mathenesse"),
    [pump.Static(flow_rate=[7.2 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(88617, 436314)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[27], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Poldervaartpolder
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(85395, 436393), name="Rioolgemaal Poldervaartpolder"),
    [pump.Static(flow_rate=[1.0])],  # unknown, geusstimate
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(85578, 435846)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[59], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

inlaat_structures = []
# add inlaat Bergsluis. Do not add inlaat Schiegemaal, as there is already an inlaat
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(91595, 439326)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(91506, 439278), name="Inlaat Bergsluis"),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[9])
inlaat_structures.append(tabulated_rating_curve_node.node_id)


for n in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_aanvoer"] = 1

ribasim_model.update_used_ids()
ribasim_model.merge_basins(node_id=115, to_node_id=9, are_connected=False)
ribasim_model.merge_basins(node_id=116, to_node_id=13, are_connected=False)
ribasim_model.merge_basins(node_id=73, to_node_id=10, are_connected=True)
ribasim_model.merge_basins(node_id=67, to_node_id=2, are_connected=True)
ribasim_model.merge_basins(node_id=62, to_node_id=2, are_connected=True)
ribasim_model.merge_basins(node_id=61, to_node_id=2, are_connected=True)
ribasim_model.merge_basins(node_id=63, to_node_id=2, are_connected=True)
ribasim_model.merge_basins(node_id=89, to_node_id=16, are_connected=False)
ribasim_model.merge_basins(node_id=71, to_node_id=9, are_connected=True)
ribasim_model.merge_basins(node_id=88, to_node_id=2, are_connected=True)
ribasim_model.merge_basins(node_id=32, to_node_id=50, are_connected=True)
ribasim_model.merge_basins(node_id=54, to_node_id=1, are_connected=True)

ribasim_param.change_pump_func(ribasim_model, 158, "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, 300, "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, 300, "afvoer", 1)
ribasim_model.remove_node(160, True)

# remove Brielse Meer as `Basin` and add as `LevelBoundary`-`Pump`-combination
ribasim_model.remove_node(98, True)
ribasim_model.remove_node(565, True)
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == 460, "meta_func_aanvoer"] = 1
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == 460, "meta_func_afvoer"] = 0
ribasim_model.link.add(ribasim_model.pump[460], ribasim_model.basin[10])

# (re)set 'meta_node_id'-values
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

for node in inlaat_structures:
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_aanvoer"] = 1

ribasim_param.clean_tables(ribasim_model, waterschap)

# split large basins: "boezems"
node_cache = NodeMetaCache(ribasim_model)
for splitted_basin_path, basin_id in zip(
    [splitted_basin_2_path, splitted_basin_9_path, splitted_basin_10_path], [2, 9, 10], strict=True
):
    # split basins to improve model convergence
    splitter = SplitBasins(
        model=ribasim_model, splitted_basin_path=splitted_basin_path, basin_node_id_to_split=basin_id
    )
    ribasim_model = splitter.run()

node_cache.set_meta_category(ribasim_model)
ribasim_model.write(ribasim_work_dir_model_toml)
