"""Parameterisation of water board: Amstel, Gooi en Vecht."""

import ribasim_nl.ribasim_parametrization as ribasim_param
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from ribasim_nl.workflow import UNKNOWN_STREEFPEIL, WaterBoardPaths, process_feedback_form
from shapely import Point

from ribasim_nl import CloudStorage

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "AmstelGooienVecht"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

paths = WaterBoardPaths(cloud, waterschap, base_model_versie)

default_level = -0.42  # default LevelBoundary level

# process the feedback form and load the model
ribasim_model = process_feedback_form(paths)
# nodes and links are referred to by their key, see docs/dev/node-keys.qmd
n = ribasim_model.node_id

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check streefpeilen at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

# merge basins
ribasim_model.merge_basins(
    node_id=n("Noorder- of Rietpolder (De Krijgsman)_dummy_id_86"),
    to_node_id=n("Noorder- of Rietpolder (zuid) (De Krijgsman)_dummy_id_87"),
    are_connected=False,
)
ribasim_model.merge_basins(
    node_id=n("Sportcombinatie Muiden_dummy_id_133"),
    to_node_id=n("Noorder- of Rietpolder (zuid) (De Krijgsman)_dummy_id_87"),
    are_connected=True,
)
ribasim_model.merge_basins(
    node_id=n("Polder Groot Wilnis Vinkeveen, Kleine plas_dummy_id_209"),
    to_node_id=n("Polder Groot Wilnis Vinkeveen, Zuidplas_dummy_id_213"),
    are_connected=True,
)  # convergence test
ribasim_model.merge_basins(
    node_id=n("Muyeveld, Kromme Rade_dummy_id_183"), to_node_id=n("Muyeveld, Vuntus_dummy_id_182"), are_connected=True
)  # convergence test
ribasim_model.merge_basins(
    node_id=n("Muyeveld, De Ster zuid_dummy_id_177"),
    to_node_id=n("Muyeveld, Oostelijke Drecht zuid_dummy_id_175"),
    are_connected=True,
)  # convergence test
ribasim_model.merge_basins(
    node_id=n("Polder Maarsseveen-Westbroek_dummy_id_40"),
    to_node_id=n("Polder Achtienhoven_dummy_id_32"),
    are_connected=True,
)  # convergence test
ribasim_model.merge_basins(
    node_id=n("Polder Maarsseveen-Westbroek, Kassen_dummy_id_200"),
    to_node_id=n("Polder Maarsseveen-Westbroek, Agrarisch Molenpolder_dummy_id_186"),
    are_connected=True,
)  # convergence test
ribasim_model.merge_basins(
    node_id=n("Begraafplaats Vredenhof_dummy_id_39"), to_node_id=n("Rijnlands Boezem_dummy_id_22"), are_connected=True
)  # convergence test
ribasim_model.merge_basins(
    node_id=n("Eilinzon_dummy_id_129"),
    to_node_id=n("Polder Groot Wilnis Vinkeveen, Noordplas_dummy_id_210"),
    are_connected=True,
)  # convergence test
ribasim_model.merge_basins(
    node_id=n("Polder Bernard_dummy_id_26"),
    to_node_id=n("Noordzeekanaal/IJ/Amsterdamrijnkanaalboezem_dummy_id_235"),
    are_connected=False,
)  # convergence test
ribasim_model.merge_basins(
    node_id=n("Buiksloterweg_dummy_id_65"),
    to_node_id=n("Noordzeekanaal/IJ/Amsterdamrijnkanaalboezem_dummy_id_235"),
    are_connected=False,
)  # convergence test
ribasim_model.merge_basins(
    node_id=n("Muyeveld, Oostelijke Drecht zuid_dummy_id_175"),
    to_node_id=n("Muyeveld, Weersloot oost_dummy_id_216"),
    are_connected=True,
)  # convergence test

# model specific tweaks
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(125905, 486750)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(125958, 486838)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("Eiland Zeeburg (oost)_dummy_id_236")])

# add additional pump and LB to ARK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(129850, 480894)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(129829, 480893)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("Gemeenschapspolder West (Tuincomplex Linnaeus)_dummy_id_112")], pump_node)
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
ribasim_model.link.add(
    tabulated_rating_curve_node, ribasim_model.basin[n("Muyeveld, Loosdrechtsche Plassen_dummy_id_173")]
)


# add outlet and LB (Zeesluis Muiden), which supplies the Vecht from the IJmeer, see
# https://github.com/Deltares/Ribasim-NL/issues/916
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(133355, 482943)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(133336, 482556)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(
    tabulated_rating_curve_node, ribasim_model.basin[n("Vechtboezem_dummy_id_219")]
)  # the Vechtboezem at Muiden
zeesluis_node_id = tabulated_rating_curve_node.node_id

# add outlet to Gooimeer
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(146641, 479856)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(146592, 479749)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(
    ribasim_model.basin[n("IJmeer, Markermeer, Gooimeer en Eemmeer_dummy_id_136")], tabulated_rating_curve_node
)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# add additional pump and LB to ARK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(129677, 482929)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(129674, 482974)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[n("Over-Diemen_dummy_id_118")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(119234, 492570)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(119234, 492587)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("Noorder IJ plas_dummy_id_64")])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(123115, 489284)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(123112, 489351)), [pump.Static(flow_rate=[20])])
# TODO: Verify setting to 'aan- & afvoergemaal'
assert ribasim_model.pump.static.df is not None
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("Florapark (zuid)_dummy_id_27")])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(124470, 489070)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(124465, 489070)), [tabulated_rating_curve.Static(level=[0, 0.1234], flow_rate=[0, 0.1234])]
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("W.H. Vliegenbos_dummy_id_121")])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(127630, 485961)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(127621, 485961)), [tabulated_rating_curve.Static(level=[0, 0.1234], flow_rate=[0, 0.1234])]
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[n("Steigereiland_dummy_id_89")])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(148516, 478866)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(148397, 478753)), [pump.Static(flow_rate=[20])])
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[n("De Gooise Zomerkade, Blaricummer Meent_dummy_id_205")])

level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(148550, 478832)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(148437, 478733)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(ribasim_model.basin[n("De Gooise Zomerkade, Blaricummer Meent_dummy_id_205")], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

ribasim_model.remove_node(n("ow:dummy_globalid_hydroobject_3834"), True)
ribasim_model.remove_node(n("KGM04218"), True)
ribasim_model.remove_node(n("bnd:KGM04218"), False)
# Outlets between two Basins that do not touch, see https://github.com/Deltares/Ribasim-NL/issues/1058. They cross into
# a part of a peilgebied that lies kilometres from the rest of it: 345 is a copy of 344, at a sliver of the peilgebied
# of Basin 80 that lies 3.5 km from it, and 877 enters a strip of 0.3 ha of the peilgebied of Basin 217, 4 km from it
ribasim_model.remove_node(n("KST00696"), True)
ribasim_model.remove_node(n("ow:KDU29425"), True)

#  a multipolygon occurs in some basins (88, 62). Only retain the largest value. Update: 62 has been merged
multipolygon_basins = [n("Rijnlands Boezem_dummy_id_22")]
assert ribasim_model.basin.area.df is not None
for basin_to_explode in multipolygon_basins:
    exploded_basins = ribasim_model.basin.area.df.loc[
        ribasim_model.basin.area.df["node_id"] == basin_to_explode
    ].explode(index_parts=False)
    exploded_basins["area"] = exploded_basins.area
    largest_polygon = exploded_basins.sort_values(by="area", ascending=False).iloc[0]
    ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df.node_id == basin_to_explode, "geometry"] = (
        largest_polygon["geometry"]
    )

# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(UNKNOWN_STREEFPEIL)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    UNKNOWN_STREEFPEIL
)

ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

ribasim_param.clean_tables(ribasim_model, waterschap)
ribasim_model.write(paths.model_dir("feedback") / "ribasim.toml")
