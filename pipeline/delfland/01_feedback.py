"""Parametrisation of water board: Delfland."""

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from ribasim_nl.split_basins import NodeMetaCache, SplitBasins
from ribasim_nl.workflow import UNKNOWN_STREEFPEIL, WaterBoardPaths, process_feedback_form
from shapely import Point

from ribasim_nl import CloudStorage

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "Delfland"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)

splitted_basin_2_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_2.gpkg")
splitted_basin_9_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_9.gpkg")
splitted_basin_10_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_10.gpkg")

default_level = 0.42 if AANVOER_CONDITIONS else -0.42

# process the feedback form and load the model
ribasim_model = process_feedback_form(paths)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

# model specific tweaks

# should have been boezem
assert ribasim_model.basin.state.df is not None
ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df.node_id == n("BIB"), "meta_categorie"] = "hoofdwater"

# change unknown streefpeilen to a default streefpeil
assert ribasim_model.basin.area.df is not None
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(UNKNOWN_STREEFPEIL)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    UNKNOWN_STREEFPEIL
)

# change high initial states to 0
ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df["level"] == 9.999, "level"] = 0.0
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == 9.999, "meta_streefpeil"] = str(
    UNKNOWN_STREEFPEIL
)

inlaat_pump = []

# change gemaal function Brielse Meer to aanvoer. Find node_id first, as it is added in the feedback form
BrielseMeerNodes = ribasim_model.link.df.loc[
    ribasim_model.link.df.from_node_id == n("AHD 1"), "to_node_id"
]  # array of all values from basin 98
BrielseMeerAanvoerPump = ribasim_model.link.df.loc[
    (ribasim_model.link.df.from_node_id.isin(BrielseMeerNodes))
    & (ribasim_model.link.df.to_node_id == n("dummy_code_BZM 1_10")),
    "from_node_id",
]

# convert Brielse Meer from afvoer to aanvoer
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id.isin(BrielseMeerAanvoerPump), "meta_func_afvoer"
] = 0
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df.node_id.isin(BrielseMeerAanvoerPump), "meta_func_aanvoer"
] = 1

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(81619, 439852)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("dummy_code_BZM 1_6")], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("BRZ")])
inlaat_pump.append(pump_node.node_id)

# add gemaal near Rijnland (Dolk). Dont use FF as it is an aanvoergemaal + boundary
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(87256, 455139)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(87082, 455089)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("dummy_code_BZM 1_4")])
inlaat_pump.append(pump_node.node_id)

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(70197, 444207)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("NOO")], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("NIE")])
inlaat_pump.append(pump_node.node_id)

# move node for improved recognition of the system
ribasim_model.move_node(node_id=n("loc:79300,437000@2"), geometry=Point(79320, 437067))

# add riool gemaal Vlaardingen West
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(81712, 435370), name="Rioolgemaal Vlaardingen West"),
    [pump.Static(flow_rate=[192 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(81737, 435104)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[n("VWV")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add another riool gemaal Vettenoord
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(80831, 435239), name="Rioolgemaal Vettenoord"),
    [pump.Static(flow_rate=[29 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(80824, 434959)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[n("VWV")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Vlaardingen Oost
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(84440, 436203), name="Rioolgemaal Vlaardingen Oost"),
    [pump.Static(flow_rate=[32 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(84531, 435937)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[n("BVO")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Schiedam Oost
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(88064, 436633), name="Rioolgemaal Schiedam Oost"),
    [pump.Static(flow_rate=[62 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(88008, 436382)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[n("SDO")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Spangen
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(88312, 437389), name="Rioolgemaal Spangen"),
    [pump.Static(flow_rate=[34 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(88309, 436358)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[n("PSN")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Oud-Mathenesse
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(88657, 436591), name="Rioolgemaal Oud-Mathenesse"),
    [pump.Static(flow_rate=[7.2 / 60])],
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(88617, 436314)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[n("OME")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add riool gemaal Poldervaartpolder
pump_node = ribasim_model.pump.add(
    Node(geometry=Point(85395, 436393), name="Rioolgemaal Poldervaartpolder"),
    [pump.Static(flow_rate=[1.0])],  # unknown, geusstimate
)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(85578, 435846)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[n("PVP")], pump_node)
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
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("dummy_code_BZM 1_4")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)


for node_id in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == node_id, "meta_func_aanvoer"] = 1

ribasim_model.update_used_ids()
ribasim_model.merge_basins(node_id=n("dummy_code_BZM 1_9"), to_node_id=n("dummy_code_BZM 1_4"), are_connected=False)
ribasim_model.merge_basins(node_id=n("dummy_code_BZM 1_3"), to_node_id=n("dummy_code_BZM 1_11"), are_connected=False)
ribasim_model.merge_basins(node_id=n("BSH"), to_node_id=n("dummy_code_BZM 1_10"), are_connected=True)
ribasim_model.merge_basins(node_id=n("HVV 2"), to_node_id=n("dummy_code_BZM 1_6"), are_connected=True)
ribasim_model.merge_basins(node_id=n("HVV 1"), to_node_id=n("dummy_code_BZM 1_6"), are_connected=True)
ribasim_model.merge_basins(node_id=n("HVM 2"), to_node_id=n("dummy_code_BZM 1_6"), are_connected=True)
ribasim_model.merge_basins(node_id=n("HVM 1"), to_node_id=n("dummy_code_BZM 1_6"), are_connected=True)
ribasim_model.merge_basins(node_id=n("TKN 1"), to_node_id=n("KRI"), are_connected=False)
ribasim_model.merge_basins(node_id=n("TKC 1"), to_node_id=n("dummy_code_BZM 1_4"), are_connected=True)
ribasim_model.merge_basins(node_id=n("TKZ 1"), to_node_id=n("dummy_code_BZM 1_6"), are_connected=True)
ribasim_model.merge_basins(node_id=n("dummy_code_BZM 1_1"), to_node_id=n("NPR"), are_connected=True)
ribasim_model.merge_basins(node_id=n("TKS 1"), to_node_id=n("dummy_code_BZM 1_0"), are_connected=True)

ribasim_param.change_pump_func(ribasim_model, n("EEC009DA-E0E4-4705-A359-3CDF246D55D8"), "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, n("AA63DDF1-D7E2-49E5-B89C-D984CA469074|OME>"), "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, n("AA63DDF1-D7E2-49E5-B89C-D984CA469074|OME>"), "afvoer", 1)
ribasim_model.remove_node(n("467DFF38-DC03-4C4F-9BB6-5CDBD291D884"), True)

# remove Brielse Meer as `Basin` and add as `LevelBoundary`-`Pump`-combination
ribasim_model.remove_node(n("AHD 1"), True)
ribasim_model.remove_node(n("loc:69773,438573"), True)
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"] == n("6E55DFD8-873B-47E5-9DB2-84DE58B82F9B|>AHD 1"),
    "meta_func_aanvoer",
] = 1
ribasim_model.pump.static.df.loc[
    ribasim_model.pump.static.df["node_id"] == n("6E55DFD8-873B-47E5-9DB2-84DE58B82F9B|>AHD 1"),
    "meta_func_afvoer",
] = 0
ribasim_model.link.add(
    ribasim_model.pump[n("6E55DFD8-873B-47E5-9DB2-84DE58B82F9B|>AHD 1")],
    ribasim_model.basin[n("dummy_code_BZM 1_10")],
)

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

assert ribasim_model.outlet.static.df is not None
for node in inlaat_structures:
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_aanvoer"] = 1

ribasim_param.clean_tables(ribasim_model, waterschap)

# split large basins: "boezems"
node_cache = NodeMetaCache(ribasim_model)
for splitted_basin_path, basin_id in zip(
    [splitted_basin_2_path, splitted_basin_9_path, splitted_basin_10_path],
    [n("dummy_code_BZM 1_6"), n("dummy_code_BZM 1_4"), n("dummy_code_BZM 1_10")],
    strict=True,
):
    # split basins to improve model convergence
    splitter = SplitBasins(
        model=ribasim_model, splitted_basin_path=splitted_basin_path, basin_node_id_to_split=basin_id
    )
    ribasim_model = splitter.run()

node_cache.set_meta_category(ribasim_model)
ribasim_model.write(paths.model_dir("feedback") / "ribasim.toml")
