"""Parameterisation of water board: Zuiderzeeland."""

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
waterschap = "Zuiderzeeland"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)

splitted_basin_16_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_16.gpkg")
splitted_basin_31_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_31.gpkg")
splitted_basin_73_path = cloud.joinpath(waterschap, "verwerkt/Splitting_basins/Opgeknipte_basin_73.gpkg")

default_level = (
    0.40 if AANVOER_CONDITIONS else -0.40
)  # default LevelBoundary level, similar to surrounding IJsselmeer, Markermeer and Randmeren

# process the feedback form and load the model
ribasim_model = process_feedback_form(paths, use_validation=False)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

# merge the smallest basins together
ribasim_model.merge_basins(node_id=n("LS 1A_dummy_id_154"), to_node_id=n("LS 1B_dummy_id_206"))  # 4363 m2
ribasim_model.merge_basins(node_id=n("2.03a_dummy_id_163"), to_node_id=n("2.04_dummy_id_159"))  # 4745 m2
ribasim_model.merge_basins(node_id=n("HT 3_dummy_id_284"), to_node_id=n("HT 2_dummy_id_283"))  # 4964 m2
ribasim_model.merge_basins(node_id=n("HT 2_dummy_id_283"), to_node_id=n("HT 7_dummy_id_23"))
ribasim_model.merge_basins(node_id=n("HT 4_dummy_id_235"), to_node_id=n("HT 5_dummy_id_131"))
ribasim_model.merge_basins(node_id=n("HT 5_dummy_id_131"), to_node_id=n("HT 7_dummy_id_23"))
ribasim_model.merge_basins(node_id=n("OR 7_dummy_id_153"), to_node_id=n("OR 6_dummy_id_208"))  # 7831 m2
ribasim_model.merge_basins(node_id=n("LS 1B_dummy_id_206"), to_node_id=n("LS 1_dummy_id_65"))  # 8398 m2

ribasim_model.merge_basins(node_id=n("URK 14_dummy_id_46"), to_node_id=n("URK 5_dummy_id_24"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("TOPPAD 2_dummy_id_162"), to_node_id=n("TOPPAD 1_dummy_id_17"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 5_dummy_id_24"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 4_dummy_id_136"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 3_dummy_id_35"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 2_dummy_id_291"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 1a_dummy_id_109"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 1_dummy_id_72"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 9_dummy_id_209"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 10_dummy_id_174"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 11_dummy_id_73"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 12_dummy_id_219"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 13_dummy_id_183"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 7_dummy_id_38"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("ZW01_dummy_id_36"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("ZW02_dummy_id_132"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("ZW03_dummy_id_222"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("POLDERWIJK_dummy_id_201"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied
ribasim_model.merge_basins(node_id=n("URK 8_dummy_id_223"), to_node_id=n("URK 6_dummy_id_175"))  # stedelijk gebied

ribasim_model.merge_basins(node_id=n("NOP 6_dummy_id_31"), to_node_id=n("NOP 5_dummy_id_119"))  # stedelijk gebied

ribasim_model.merge_basins(
    node_id=n("NOP 3_dummy_id_220"), to_node_id=n("LAGE AFDELING_dummy_id_121")
)  # los snippertje
ribasim_model.merge_basins(
    node_id=n("ROTTERDAMSEHOEK_dummy_id_64"), to_node_id=n("LAGE AFDELING_dummy_id_121")
)  # los snippertje

ribasim_model.merge_basins(
    node_id=n("LV 4_dummy_id_29"), to_node_id=n("LV 5_dummy_id_10")
)  # Noordelijkst puntje veel kleine gebieden
ribasim_model.merge_basins(
    node_id=n("LV 3_dummy_id_185"), to_node_id=n("LV 5_dummy_id_10")
)  # Noordelijkst puntje veel kleine gebieden
ribasim_model.merge_basins(
    node_id=n("LV 2_dummy_id_28"), to_node_id=n("LV 5_dummy_id_10")
)  # Noordelijkst puntje veel kleine gebieden
ribasim_model.merge_basins(
    node_id=n("LV 1_dummy_id_260"), to_node_id=n("LV 5_dummy_id_10")
)  # Noordelijkst puntje veel kleine gebieden
ribasim_model.merge_basins(
    node_id=n("LV 9_dummy_id_69"), to_node_id=n("LV 7_dummy_id_188")
)  # Noordelijkst puntje veel kleine gebieden
ribasim_model.merge_basins(
    node_id=n("LV 6_dummy_id_34"), to_node_id=n("LV 7_dummy_id_188")
)  # Noordelijkst puntje veel kleine gebieden

ribasim_model.merge_basins(node_id=n("EW3_dummy_id_238"), to_node_id=n("EW2_dummy_id_62"))  # oostelijke NOP
ribasim_model.merge_basins(node_id=n("EW1_dummy_id_239"), to_node_id=n("EW2_dummy_id_62"))  # oostelijke NOP

ribasim_model.merge_basins(
    node_id=n("NOP 6A_dummy_id_58"), to_node_id=n("LAGE AFDELING_dummy_id_121")
)  # klein gebiedje in lage afdeling
ribasim_model.merge_basins(
    node_id=n("WELLERWAARD_dummy_id_21"), to_node_id=n("LAGE AFDELING_dummy_id_121")
)  # klein gebiedje in lage afdeling
ribasim_model.merge_basins(
    node_id=n("ECOPARK 2_dummy_id_40"), to_node_id=n("ECOPARK 1_dummy_id_186")
)  # klein gebiedjes met twee MR

ribasim_model.merge_basins(node_id=n("EH 2_dummy_id_217"), to_node_id=n("EH 1_dummy_id_234"))  # klein gebied NOP
ribasim_model.merge_basins(node_id=n("NOP 9_dummy_id_6"), to_node_id=n("OT.02_dummy_id_168"))  # klein gebied NOP
ribasim_model.merge_basins(
    node_id=n("ECOPARK 1_dummy_id_186"), to_node_id=n("MU_dummy_id_56"), are_connected=False
)  # klein gebied NOP

ribasim_model.merge_basins(
    node_id=n("NOP 7_dummy_id_182"), to_node_id=n("LAGE AFDELING_dummy_id_121")
)  # klein gebied NOP

ribasim_model.merge_basins(
    node_id=n("SCHOKLAND D1_dummy_id_248"), to_node_id=n("SCHOKLAND F1 - AFGEDAMD_dummy_id_133")
)  # natuurgebiedje NOP
ribasim_model.merge_basins(
    node_id=n("SCHOKLAND D2_dummy_id_16"), to_node_id=n("SCHOKLAND F1 - AFGEDAMD_dummy_id_133")
)  # natuurgebiedje NOP
ribasim_model.merge_basins(
    node_id=n("SCHOKLAND E_dummy_id_268"), to_node_id=n("SCHOKLAND F1 - AFGEDAMD_dummy_id_133")
)  # natuurgebiedje NOP
ribasim_model.merge_basins(
    node_id=n("SCHOKLAND F5 - AFGEDAMD_dummy_id_210"), to_node_id=n("SCHOKLAND F1 - AFGEDAMD_dummy_id_133")
)  # natuurgebiedje NOP
ribasim_model.merge_basins(
    node_id=n("SCHOKLAND C1_dummy_id_204"), to_node_id=n("SCHOKLAND F1 - AFGEDAMD_dummy_id_133")
)  # natuurgebiedje NOP
ribasim_model.merge_basins(
    node_id=n("SCHOKLAND A_dummy_id_246"), to_node_id=n("SCHOKLAND F1 - AFGEDAMD_dummy_id_133")
)  # natuurgebiedje NOP
ribasim_model.merge_basins(
    node_id=n("SCHOKLAND B_dummy_id_247"), to_node_id=n("SCHOKLAND F1 - AFGEDAMD_dummy_id_133")
)  # natuurgebiedje NOP

ribasim_model.merge_basins(
    node_id=n("SCHOKLAND G_dummy_id_115"), to_node_id=n("LAGE AFDELING_dummy_id_121")
)  # klein gebied NOP

ribasim_model.merge_basins(
    node_id=n("SCHOKLAND F2 - AFGEDAMD_dummy_id_110"), to_node_id=n("SCHOKLAND C2_dummy_id_229")
)  # natuurgebiedje NOP

ribasim_model.merge_basins(
    node_id=n("SCHOKLAND F3 - AFGEDAMD_dummy_id_245"), to_node_id=n("SCHOKLAND C3_dummy_id_142")
)  # natuurgebiedje NOP
ribasim_model.merge_basins(
    node_id=n("SCHOKLAND F4 - AFGEDAMD_dummy_id_200"), to_node_id=n("SCHOKLAND C3_dummy_id_142")
)  # natuurgebiedje NOP

ribasim_model.merge_basins(node_id=n("OR 35_dummy_id_49"), to_node_id=n("OR 34_dummy_id_129"))
ribasim_model.merge_basins(node_id=n("OR 36_dummy_id_27"), to_node_id=n("OR 34_dummy_id_129"))

ribasim_model.merge_basins(node_id=n("OR 49_dummy_id_63"), to_node_id=n("OR 43_dummy_id_250"))  # vakantieparkje(?)
ribasim_model.merge_basins(node_id=n("OR 48_dummy_id_54"), to_node_id=n("OR 43_dummy_id_250"))  # vakantieparkje(?)

ribasim_model.merge_basins(node_id=n("OR 54_dummy_id_177"), to_node_id=n("OR 55_dummy_id_289"))  # klein gebiedje
ribasim_model.merge_basins(node_id=n("2.03_dummy_id_228"), to_node_id=n("2.04_dummy_id_159"))  # klein gebiedje
ribasim_model.merge_basins(node_id=n("2.06_dummy_id_230"), to_node_id=n("2.05_dummy_id_71"))  # klein gebiedje

ribasim_model.merge_basins(node_id=n("NOP 2_dummy_id_32"), to_node_id=n("LAGE AFDELING_dummy_id_121"))  # klein gebiedje

ribasim_model.merge_basins(
    node_id=n("BK_dummy_id_88"), to_node_id=n("ALM 1_dummy_id_101")
)  # klein gebiedje in stedelijk gebied

ribasim_model.merge_basins(
    node_id=n("3.09_dummy_id_227"), to_node_id=n("3.01_dummy_id_92")
)  # klein gebiedje in hoge vaart
ribasim_model.merge_basins(
    node_id=n("3.08_dummy_id_190"), to_node_id=n("3.01_dummy_id_92")
)  # klein gebiedje in hoge vaart
ribasim_model.merge_basins(
    node_id=n("3.10_dummy_id_231"), to_node_id=n("3.01_dummy_id_92")
)  # klein gebiedje in hoge vaart
ribasim_model.merge_basins(
    node_id=n("3.07_dummy_id_75"), to_node_id=n("3.01_dummy_id_92")
)  # klein gebiedje in hoge vaart
ribasim_model.merge_basins(
    node_id=n("3.06_dummy_id_8"), to_node_id=n("3.01_dummy_id_92")
)  # klein gebiedje in hoge vaart

ribasim_model.merge_basins(
    node_id=n("LS 5_dummy_id_203"), to_node_id=n("LVA.01_dummy_id_94")
)  # klein gebiedje in lage vaart
ribasim_model.merge_basins(
    node_id=n("LVA.20 AFGEDAMD_dummy_id_60"), to_node_id=n("LVA.01_dummy_id_94")
)  # klein gebiedje in lage vaart
ribasim_model.merge_basins(
    node_id=n("LVA.21 AFGEDAMD_dummy_id_280"), to_node_id=n("LVA.01_dummy_id_94")
)  # klein gebiedje in lage vaart

ribasim_model.merge_basins(
    node_id=n("LVA.29_dummy_id_256"), to_node_id=n("LVA.28_dummy_id_257")
)  # klein gebiedjes in polder, noordelijk
ribasim_model.merge_basins(
    node_id=n("LVA.26_dummy_id_20"), to_node_id=n("LVA.28_dummy_id_257")
)  # klein gebiedjes in polder, noordelijk
ribasim_model.merge_basins(
    node_id=n("LVA.27_dummy_id_139"), to_node_id=n("LVA.28_dummy_id_257")
)  # klein gebiedjes in polder, noordelijk

ribasim_model.merge_basins(
    node_id=n("OR 18_dummy_id_179"), to_node_id=n("OR 17_dummy_id_267")
)  # ivm wateraanvoer. Klein onlogisch gebied.

ribasim_model.merge_basins(node_id=n("AV 2_dummy_id_89"), to_node_id=n("AV 1_dummy_id_103"))  # industriegebied Almere,
ribasim_model.merge_basins(
    node_id=n("AV 1_dummy_id_103"), to_node_id=n("LVA.01_dummy_id_94")
)  # industriegebied Almere,

ribasim_model.merge_basins(node_id=n("HT 7_dummy_id_23"), to_node_id=n("LS 2_dummy_id_150"))  # klein gebiedje Lelystad,
ribasim_model.merge_basins(
    node_id=n("LVA.15 AFGEDAMD_dummy_id_68"), to_node_id=n("LVA.16_dummy_id_15"), are_connected=False
)  # klein gebiedje zonder streefpeil

# There are +- 30 locations where a pump has been added in the FF,
# these pumps are generally not in the delivered data but are placed by i.e. farmers
# Convert these pumps to inlaat_pumps
inlaat_structures = []
inlaat_pump = []

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

# model specific tweaks
# 1 Add gemaal at blocq van kuffeler
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(143912, 492256)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(143959, 492198)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("LVA.01_dummy_id_94")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 1 Gemaal near Almere
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(143895, 492250)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(143927, 492229)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("3.01_dummy_id_92")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 1 Add gemaal Wortman
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(157201, 501796)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(157251, 501708)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("LVA.01_dummy_id_94")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# Inlaat (hevel) toevoegen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(193502, 526518)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(193491, 526526)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("TA.11_dummy_id_264")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(185725, 533120)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(185725, 533098)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("KUINDERVAART 2_dummy_id_37")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen near Lelystad 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(161109, 507280)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(161113, 507190)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("LVA.01_dummy_id_94")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen near Lelystad 2
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(155928, 500745)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(155977, 500692)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("LS 9_dummy_id_125")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen near Urk: 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(168944, 520125)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(168957, 520126)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("URK 6_dummy_id_175")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen near Urk: 2
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(168920, 520306)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(168956, 520306)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("LAGE AFDELING_dummy_id_121")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at Ketelhaven
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(180145, 510253)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(180131, 510204)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("3.01_dummy_id_92")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: east 1. According data it should go to basin 199, but 45 seems more logical
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(195210, 519077)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(195198, 519068)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("KADOELERTOCHT 2_dummy_id_189")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: east 2
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(193142, 525102)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(193131, 525102)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("TA.10_dummy_id_167")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: east 3
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(193293, 522692)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(193259, 522698)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("EW2_dummy_id_62")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: south
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(189485, 515337)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(189473, 515427)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("HERTENTOCHT_dummy_id_261")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: south east
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(192223, 521469)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(192219, 521474)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("VOORSTERTOCHT_dummy_id_143")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: north east
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(185723, 533122)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(185719, 533122)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("SCHOTERTOCHT 2_dummy_id_259")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on


# Inlaat (hevel) toevoegen at NOP: north 1 (not in data, but mentioned by RV)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(176393, 538604)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(176400, 538569)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("GEMAALTOCHT_dummy_id_5")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at NOP: north 2 (not in data, but mentioned by RV)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(176389, 538605)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(176392, 538568)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("NOORDERMEERTOCHT 1_dummy_id_67")])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(163582, 482871)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("3.01_dummy_id_92")], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("2.05_dummy_id_71")])
inlaat_pump.append(pump_node.node_id)

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(181525, 517580)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("HOGE AFDELING_dummy_id_45")], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("SCHOKLAND F1 - AFGEDAMD_dummy_id_133")])
inlaat_pump.append(pump_node.node_id)

# add gemaal in middle of beheergebied. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(156428, 476695)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("3.01_dummy_id_92")], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("2.08_dummy_id_164")])
inlaat_pump.append(pump_node.node_id)

# switching direction prohibited due to "old" terminal-node
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(169590, 518954)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(169592, 518954)), [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])]
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("LAGE AFDELING_dummy_id_121")])

ribasim_model.remove_node(n("ow:110347141"), remove_links=True)

# [from_node, to_node, x_coordinate, y_coordinate]
farmer_pumps = [
    [n("3.01_dummy_id_92"), n("LVA.13_dummy_id_93"), 163266, 487745],
    [n("LVA.01_dummy_id_94"), n("LVA.16_dummy_id_15"), 162476, 493070],
    [n("LVA.01_dummy_id_94"), n("LVA.18_dummy_id_279"), 160482, 495540],
    [n("LVA.01_dummy_id_94"), n("LVA.19_dummy_id_124"), 163155, 496109],
    [n("LVA.01_dummy_id_94"), n("MT01_dummy_id_151"), 167346, 493712],
    [n("LVA.01_dummy_id_94"), n("LVA.10_dummy_id_152"), 167553, 493696],
    [n("LVA.01_dummy_id_94"), n("LS 8_dummy_id_128"), 161132, 497790],
    [n("LVA.01_dummy_id_94"), n("LS 7_dummy_id_249"), 161413, 498612],
    [n("LVA.01_dummy_id_94"), n("WA01_dummy_id_158"), 159725, 499946],
    [n("LVA.01_dummy_id_94"), n("LS10_dummy_id_165"), 157082, 501506],
    [n("LVA.01_dummy_id_94"), n("LVA.24_dummy_id_66"), 163603, 498905],
    [n("LVA.01_dummy_id_94"), n("LVA.23_dummy_id_192"), 164112, 499846],
    [n("LVA.01_dummy_id_94"), n("GC 01_dummy_id_213"), 164702, 501743],
    [n("3.01_dummy_id_92"), n("OR 24_dummy_id_258"), 180860, 499866],
    [n("3.01_dummy_id_92"), n("OR 22_dummy_id_61"), 180899, 499914],
    [n("3.01_dummy_id_92"), n("OR 23_dummy_id_233"), 182273, 498826],
    [n("3.01_dummy_id_92"), n("OR 17_dummy_id_267"), 184918, 504045],
    [n("3.01_dummy_id_92"), n("OR 16_dummy_id_22"), 184013, 504227],
    [n("3.01_dummy_id_92"), n("OR 19_dummy_id_39"), 182081, 504226],
    [n("LVA.01_dummy_id_94"), n("DW 01_dummy_id_232"), 174932, 504574],
    [n("LVA.05 A_dummy_id_274"), n("RW_dummy_id_172"), 175637, 507063],
    [n("LVA.01_dummy_id_94"), n("LVA.05 A_dummy_id_274"), 176616, 508796],
    [n("LVA.01_dummy_id_94"), n("LVA.04_dummy_id_113"), 172251, 506176],
    [n("LVA.01_dummy_id_94"), n("LVA.03_dummy_id_288"), 175874, 509405],
    [n("LVA.01_dummy_id_94"), n("LVA.02_dummy_id_194"), 174223, 510437],
    [n("LVA.01_dummy_id_94"), n("LVA.25_dummy_id_57"), 167508, 510888],
    [n("HOGE AFDELING_dummy_id_45"), n("RAMSTOCHT 2_dummy_id_202"), 184856, 515922],
    [n("ENSERTOCHT 2_dummy_id_14"), n("ZWIJNSTOCHT 2_dummy_id_130"), 186402, 515281],
    [n("KRAGGENBURGERTOCHT_dummy_id_218"), n("KADOELEN_dummy_id_269"), 192906, 519400],
    [n("SCHOTERTOCHT 2_dummy_id_259"), n("SCHANSVELD_dummy_id_160"), 183948, 532356],
    [n("LAGE AFDELING_dummy_id_121"), n("WELLERZANDTOCHT_dummy_id_196"), 180285, 531935],
    [n("LAGE AFDELING_dummy_id_121"), n("VENETOCHT_dummy_id_285"), 177791, 532821],
    [n("3.01_dummy_id_92"), n("OR 13_dummy_id_1"), 180975, 509873],
    [n("OR 13_dummy_id_1"), n("OR 14_dummy_id_252"), 183547, 508147],
]

for farmer_pump in farmer_pumps:
    pump_node = ribasim_model.pump.add(
        Node(geometry=Point(farmer_pump[2], farmer_pump[3])), [pump.Static(flow_rate=[0.1])]
    )  # create node
    ribasim_model.link.add(ribasim_model.basin[farmer_pump[0]], pump_node)  # create link
    ribasim_model.link.add(pump_node, ribasim_model.basin[farmer_pump[1]])  # create link
    inlaat_pump.append(pump_node.node_id)  # add inlaat category

# change afvoergemaal to aanvoergemaal
inlaat_pump += [n("OP3120"), n("OP2503"), n("OP2007"), n("OP4165"), n("OP2008")]

assert ribasim_model.pump.static.df is not None
for node_id in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == node_id, "meta_func_aanvoer"] = 1
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == node_id, "meta_func_afvoer"] = 0

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

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

assert ribasim_model.outlet.static.df is not None
for node in inlaat_structures:
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_aanvoer"] = 1

ribasim_param.clean_tables(ribasim_model, waterschap)

# # loop through all splitted basins
node_cache = NodeMetaCache(ribasim_model)
for splitted_basin_path, basin_id in zip(
    [splitted_basin_16_path, splitted_basin_31_path, splitted_basin_73_path],
    [n("3.01_dummy_id_92"), n("LVA.01_dummy_id_94"), n("LAGE AFDELING_dummy_id_121")],
    strict=True,
):
    # split basins to improve model convergence
    splitter = SplitBasins(
        model=ribasim_model, splitted_basin_path=splitted_basin_path, basin_node_id_to_split=basin_id
    )
    ribasim_model = splitter.run()

node_cache.set_meta_category(ribasim_model)
ribasim_model.write(paths.model_dir("feedback") / "ribasim.toml")
del node_cache
