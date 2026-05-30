"""Parameterisation of water board: Schieland en de Krimpenerwaard."""

import warnings

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
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

MIXED_CONDITIONS_DESIGN_P = 18
MIXED_CONDITIONS_DESIGN_E = 5

# model settings
waterschap = "SchielandendeKrimpenerwaard"
base_model_versie = "2024_12_1"

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

default_level = 0.75 if AANVOER_CONDITIONS else -0.75  # default LevelBoundary level, similar to surrounding Maas

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
# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

inlaat_structures = []
inlaat_pump = []
# add the LevelBoundaries and Pumps
# 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(89792, 437301)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(89992, 437341)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[116], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 2
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(92237, 435452)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(92144, 435533)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[110])

# 3
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(95111, 436428)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(95431, 436488)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[157], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 4
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(95111, 436418)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(95736, 436211)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[143], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 5 not sure whether this one is correct. FF was not clear
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(94966, 437011)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(94966, 437011)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[25], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 6
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(92476, 435889)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(92334, 436428)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[142], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 7
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(104547, 443432)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(104485, 443506)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[19], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 7
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(104555, 443434)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(104527, 443455)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[2], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 8
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(102079, 438209)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(102230, 438082)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[29], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# 9, added by RB as it is quiet clear a levelboundary is missing.
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(91451, 439245)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(91514, 439286)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(ribasim_model.basin[28], tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, level_boundary_node)

# 10. cooperation day 12 dec 2024: Erik suggested adding an additional pump which already exists, but this would prevent two entire basins interacting with eachother if only one basin would get a pump, while this pump discharges both basins
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(89786, 437309)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(89985, 437345)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[115], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)


# Inlaat (hevel) toevoegen at Delfshaven
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(91122, 436003)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(91107, 436042)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[116])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at north of HHSK
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(105536, 448858)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(105485, 448858)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[27])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at Leuvehaven
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(92798, 436794)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(92801, 437070)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[139])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# Inlaat (hevel) toevoegen at Schilthuis
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(93880, 437435)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(93776, 437680)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[97])
inlaat_structures.append(tabulated_rating_curve_node.node_id)  # convert the node to aanvoer later on

# # # add gemaal between two basins in Rotterdam. Dont use FF as it is an aanvoergemaal
pump_node = ribasim_model.pump.add(Node(geometry=Point(95653, 436055)), [pump.Static(flow_rate=[2.5 / 60])])
ribasim_model.link.add(ribasim_model.basin[143], pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[157])
inlaat_pump.append(pump_node.node_id)

# Inlaat (hevel) toevoegen at Lek
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(110776, 435609)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(geometry=Point(110748, 435630)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.link.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.link.add(tabulated_rating_curve_node, ribasim_model.basin[26])
inlaat_structures.append(tabulated_rating_curve_node.node_id)

# also add a pump here (afvoer)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(110764, 435587)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(110747, 435628)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[26], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add pump to Schie (afvoer)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(89300, 439523)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(89429, 439574)), [pump.Static(flow_rate=[0.1])])
ribasim_model.link.add(ribasim_model.basin[45], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)


afvoer_pumps = [230, 387, 579, 366]
for afvoer_pump in afvoer_pumps:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == afvoer_pump, "meta_func_afvoer"] = 1
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == afvoer_pump, "meta_func_aanvoer"] = 0

for n in inlaat_pump:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == n, "meta_func_aanvoer"] = 1

ribasim_model.merge_basins(node_id=16, to_node_id=8)  # small (boezem)
ribasim_model.merge_basins(node_id=145, to_node_id=2)  # small (boezem)

# Flow directions have been changed: See feedback form
ribasim_model.remove_node(478, True)
ribasim_model.remove_node(641, False)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(93810, 437547)),
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(93804, 437547)), [pump.Static(flow_rate=[20])])
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "afvoer", 1)
ribasim_model.link.add(ribasim_model.basin[97], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check target levels at manning nodes
ribasim_param.validate_manning_basins(ribasim_model)

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

# (re)set 'meta_node_id'-values
for node_type in ["LevelBoundary", "TabulatedRatingCurve", "Pump", "Outlet"]:
    mask = ribasim_model.node.df["node_type"] == node_type
    ribasim_model.node.df.loc[mask, "meta_node_id"] = ribasim_model.node.df.loc[mask].index

for node in inlaat_structures:
    ribasim_model.outlet.static.df.loc[ribasim_model.outlet.static.df["node_id"] == node, "meta_func_aanvoer"] = 1

ribasim_param.clean_tables(ribasim_model, waterschap)
ribasim_model.write(ribasim_work_dir_model_toml)
