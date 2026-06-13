"""Parameterisation of water board: Rivierenland."""

import warnings

import geopandas as gpd
import peilbeheerst_model.ribasim_parametrization as ribasim_param
import shapely
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import LineString, Point

from ribasim_nl import CloudStorage, Model, geometry

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model settings
waterschap = "Rivierenland"
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
gaten_path = cloud.joinpath(waterschap, "aangeleverd/Na_levering/gaten.gpkg")

cloud.synchronize(
    filepaths=[
        ribasim_base_model_dir,
        FeedbackFormulier_path,
        gaten_path,
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
default_level = 12.4 if AANVOER_CONDITIONS else -0.60  # default LevelBoundary level, +- level at Kinderdijk

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

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check target levels at both sides of the Manning Nodes
ribasim_param.validate_manning_basins(ribasim_model)

afvoer_pumps = []
aanvoer_pumps = []

# model specific tweaks
# merge basins
ribasim_model.merge_basins(node_id=3, to_node_id=21, are_connected=True)
ribasim_model.merge_basins(node_id=63, to_node_id=97, are_connected=True)
ribasim_model.merge_basins(node_id=131, to_node_id=119, are_connected=True)
ribasim_model.merge_basins(node_id=212, to_node_id=210, are_connected=True)
ribasim_model.merge_basins(node_id=33, to_node_id=1, are_connected=True)
ribasim_model.merge_basins(node_id=181, to_node_id=162, are_connected=True)

# minor basins in south west of WSRL, which FW and HvdG said to merge
ribasim_model.merge_basins(node_id=50, to_node_id=56, are_connected=True)
ribasim_model.merge_basins(node_id=54, to_node_id=56, are_connected=True)

# (too) small basins connected via a Manning-node --> merge basins
ribasim_model.merge_basins(node_id=226, to_node_id=1, are_connected=True)
ribasim_model.merge_basins(node_id=220, to_node_id=219, are_connected=True)

# retrieve the two polygons where still holes occur so it can be merged with existing basins
gaten = gpd.read_file(gaten_path)

# fix hole Wijchen
wijchen = gaten.loc[gaten.meta_name == "Wijchen"]
basin_195 = ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["node_id"] == 195]
merged_geom = basin_195.geometry.iloc[0].union(wijchen.geometry.union_all())
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["node_id"] == 195, "geometry"] = merged_geom

# fix hole Vennen
vennen = gaten.loc[gaten.meta_name == "Vennen"]
basin_241 = ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["node_id"] == 241]
merged_geom = basin_241.geometry.iloc[0].union(vennen.geometry.union_all())
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["node_id"] == 241, "geometry"] = merged_geom

# redefine basins #66 & #69
split_line_string = LineString(
    [
        (179226.37281748792, 433425.4196105504),
        (179348.13125653792, 433771.9628601542),
        (179483.93874624756, 434001.43068759463),
        (179652.52735416294, 434146.6042110773),
        (179778.9688100995, 434240.2645488081),
        (180027.16870508605, 434376.07203851774),
        (180162.9761947957, 434455.68332558894),
        (180364.34592091685, 434614.9058997312),
        (180874.7947615496, 435172.18490922934),
        (181104.26258898998, 435340.7735171448),
        (181329.04739954387, 435345.4565340313),
        (181488.26997368617, 435607.7054796775),
        (181446.12282170734, 435832.49029023136),
        (181427.39075416117, 436061.9581176717),
        (181450.80583859386, 436249.2787931333),
    ]
)
basin_ids = 66, 69
basin = shapely.union_all(
    [
        ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["node_id"] == basin_id, "geometry"].values[0].geoms
        for basin_id in basin_ids
    ]
)
polygons = geometry.split_basin(basin, split_line_string)
for basin_id, polygon in zip(basin_ids, polygons.geoms, strict=True):
    ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["node_id"] == basin_id, "geometry"] = polygon

# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

inlaten = []

# add levelboundary and a pump
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(136538, 422962)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(136574, 422965)), [pump.Static(flow_rate=[6])])
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "aanvoer", 1)
ribasim_model.link.add(ribasim_model.basin[154], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)
afvoer_pumps.append(pump_node.node_id)

# add an inlaat and LB near Randwijk (gemaal Kuijk which can also water inlaten)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(174570, 440194)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(174615, 440126)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[94])

inlaat_Kuijk = tabulated_rating_curve_node.node_id
inlaten.append(inlaat_Kuijk)

# add an inlaat and LB at the Nederwaard
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(103305, 433637)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(103334, 433570)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[8])

inlaten.append(tabulated_rating_curve_node.node_id)

# add an inlaat and LB at the Overwaard
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(103401, 433650)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(103446, 433601)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[5])
inlaten.append(tabulated_rating_curve_node.node_id)

# add gemaal and LB at Pannerlingen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(198612, 434208)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(198568, 434184)), [pump.Static(flow_rate=[6])])
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "aanvoer", 1)
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[115])
inlaten.append(pump_node.node_id)

# add a TRC and LB near Groesbeek to Germany
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(196158, 421051)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(196147, 421056)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[237], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# # add gemaal and LB at downstream Linge (Hardinxveld)
# pump_node = ribasim_model.pump.add(Node(geometry=Point(118539.25, 425972.46)), [pump.Static(flow_rate=[20])])
# level_boundary_node = ribasim_model.level_boundary.add(
#     Node(geometry=Point(118530.65, 425964)), [level_boundary.Static(level=[default_level])]
# )
# ribasim_model.link.add(ribasim_model.basin[1], pump_node)
# ribasim_model.link.add(pump_node, level_boundary_node)

# add gemaal and LB at downstream Linge (Hardinxveld)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(118548, 425951)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(118561.5, 425973.0)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(ribasim_model.basin[1], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add outlet (TRC) and LB from Beneden Merwerde to Sliedrecht
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(113249, 425400)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(113249, 425499)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[30])

# Van Dam van Brakel is both afvoergemaal as aanvoer-outlet
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(132313, 422551)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(132313, 422591)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[158])

# Add Schanspolder
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(103050, 433650)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(103050, 433620)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[32])

# Add aanvoer-route from ARK to Rivierenland
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(153091, 440460)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(153091, 440360)), [pump.Static(flow_rate=[20])])
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "aanvoer", 1)
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[91])

# Add aanvoer-component of Kuijkgemaal
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(174629, 440132)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(174630, 440132)), [pump.Static(flow_rate=[20])])
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "aanvoer", 1)
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "afvoer", 0)
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[94])

# Add Inlaatgemaal van Beuningen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(158270, 436942)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(158276, 436942)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[86])
aanvoer_pumps.append(pump_node.node_id)

# Add Inlaatgemaal Bontemorgen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(164450, 441800)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(164412, 441741)), [pump.Static(flow_rate=[20])])
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "aanvoer", 1)
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "afvoer", 0)
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[77])

# set 'aanvoer'-function on
pump_ids: tuple[int, ...] = 989, 1000, 1001
for i in pump_ids:
    ribasim_param.change_pump_func(ribasim_model, i, "aanvoer", 1)

# force 'afvoergemalen'
pump_ids = 272, 280
for i in pump_ids:
    ribasim_param.change_pump_func(ribasim_model, i, "afvoer", 1)
    ribasim_param.change_pump_func(ribasim_model, i, "aanvoer", 0)

# TODO: Temporary fixes
ribasim_model.remove_node(788, True)
ribasim_model.remove_node(960, False)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(157934, 425855)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(157940, 425856)), [pump.Static(flow_rate=[20])])
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "afvoer", 0)
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "aanvoer", 1)
ribasim_model.link.add(ribasim_model.basin[194], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)
afvoer_pumps.append(pump_node.node_id)

ribasim_param.change_pump_func(ribasim_model, 414, "aanvoer", 1)
ribasim_param.change_pump_func(ribasim_model, 414, "afvoer", 0)
ribasim_param.change_pump_func(ribasim_model, 668, "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, 722, "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, 1005, "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, 1005, "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, 1153, "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, 1025, "afvoer", 0)
ribasim_param.change_pump_func(ribasim_model, 1025, "aanvoer", 1)

for afvoer_pump in afvoer_pumps:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == afvoer_pump, "meta_func_afvoer"] = 1
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == afvoer_pump, "meta_func_aanvoer"] = 0

for aanvoer_pump in aanvoer_pumps:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == aanvoer_pump, "meta_func_aanvoer"] = 1
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == aanvoer_pump, "meta_func_afvoer"] = 0

# (re)set meta_node_id
ribasim_model.node.df.loc[ribasim_model.level_boundary.node.df.index, "meta_node_id"] = (
    ribasim_model.level_boundary.node.df.index
)
ribasim_model.node.df.loc[ribasim_model.tabulated_rating_curve.node.df.index, "meta_node_id"] = (
    ribasim_model.tabulated_rating_curve.node.df.index
)
ribasim_model.node.df.loc[ribasim_model.pump.node.df.index, "meta_node_id"] = ribasim_model.pump.node.df.index

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

ribasim_param.clean_tables(ribasim_model, waterschap)
ribasim_model.write(ribasim_work_dir_model_toml)
