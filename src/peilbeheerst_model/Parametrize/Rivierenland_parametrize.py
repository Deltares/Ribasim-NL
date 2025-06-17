"""Parameterisation of water board: Rivierenland."""

import datetime
import os
import warnings

import shapely
from ribasim import Node
from ribasim.config import Solver
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import LineString, Point

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.add_storage_basins import AddStorageBasins
from peilbeheerst_model.assign_authorities import AssignAuthorities
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim_nl import CloudStorage, Model, geometry

AANVOER_CONDITIONS: bool = True
MIXED_CONDITIONS: bool = True

if MIXED_CONDITIONS and not AANVOER_CONDITIONS:
    AANVOER_CONDITIONS = True

# model tolerances
solver = Solver(abstol=1e-9, reltol=1e-9)

# model settings
waterschap = "Rivierenland"
base_model_versie = "2024_12_0"

# connect with the GoodCloud
cloud = CloudStorage()

# collect data from the base model, feedback form, waterauthority & RWS border
ribasim_base_model_dir = cloud.joinpath(waterschap, "modellen", f"{waterschap}_boezemmodel_{base_model_versie}")
FeedbackFormulier_path = cloud.joinpath(
    waterschap, "verwerkt", "Feedback Formulier", f"feedback_formulier_{waterschap}.xlsx"
)
FeedbackFormulier_LOG_path = cloud.joinpath(
    waterschap, "verwerkt", "Feedback Formulier", f"feedback_formulier_{waterschap}_LOG.xlsx"
)
ws_grenzen_path = cloud.joinpath("Basisgegevens", "RWS_waterschaps_grenzen", "waterschap.gpkg")
RWS_grenzen_path = cloud.joinpath("Basisgegevens", "RWS_waterschaps_grenzen", "Rijkswaterstaat.gpkg")
qlr_path = cloud.joinpath(
    "Basisgegevens", "QGIS_qlr", "output_controle_cc.qlr" if MIXED_CONDITIONS else "output_controle_202502.qlr"
)
aanvoer_path = cloud.joinpath(waterschap, "aangeleverd", "Na_levering", "Wateraanvoer", "Aanvoergebieden_detail.shp")

cloud.synchronize(
    filepaths=[
        ribasim_base_model_dir,
        FeedbackFormulier_path,
        ws_grenzen_path,
        RWS_grenzen_path,
        qlr_path,
        aanvoer_path,
    ]
)

# download the feedback forms, overwrite the old ones
cloud.download_verwerkt(authority=waterschap, overwrite=True)

# set paths to the TEMP working directory
work_dir = cloud.joinpath(waterschap, "verwerkt", "Work_dir", f"{waterschap}_parameterized")
ribasim_gpkg = work_dir.joinpath("database.gpkg")
ribasim_work_dir_model_toml = work_dir.joinpath("ribasim.toml")

# set path to base model toml
ribasim_base_model_toml = ribasim_base_model_dir.joinpath("ribasim.toml")

# create work_dir/parameterized
parameterized = os.path.join(work_dir, f"{waterschap}_parameterized/")
os.makedirs(parameterized, exist_ok=True)

# define variables and model
# basin area percentage
regular_percentage = 10
boezem_percentage = 90
unknown_streefpeil = (
    0.00012345  # we need a streefpeil to create the profiles, Q(h)-relations, and af- and aanslag peil for pumps
)

# forcing settings
starttime = datetime.datetime(2024, 1, 1)
endtime = datetime.datetime(2025, 1, 1)
saveat = 3600 * 24
timestep_size = "d"
timesteps = 2
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
    use_validation=True,
)
processor.run()

# load model
with warnings.catch_warnings():
    warnings.simplefilter(action="ignore", category=FutureWarning)
    ribasim_model = Model(filepath=ribasim_work_dir_model_toml, solver=solver)

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# check target levels at both sides of the Manning Nodes
ribasim_param.validate_manning_basins(ribasim_model)

# model specific tweaks
# merge basins
ribasim_model.merge_basins(node_id=3, to_node_id=21, are_connected=True)
ribasim_model.merge_basins(node_id=63, to_node_id=97, are_connected=True)
ribasim_model.merge_basins(node_id=131, to_node_id=119, are_connected=True)
ribasim_model.merge_basins(node_id=212, to_node_id=210, are_connected=True)

# (too) small basins connected via a Manning-node --> merge basins
ribasim_model.merge_basins(node_id=226, to_node_id=1, are_connected=True)
ribasim_model.merge_basins(node_id=220, to_node_id=219, are_connected=True)

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
for basin_id, polygon in zip(basin_ids, polygons.geoms):
    ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["node_id"] == basin_id, "geometry"] = polygon

# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

# add levelboundary and a pump
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(136538, 422962)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(136574, 422965)), [pump.Static(flow_rate=[6])])
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.link.add(ribasim_model.basin[154], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

# add gemaal and LB at Pannerlingen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(198612, 434208)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(198568, 434184)), [pump.Static(flow_rate=[6])])
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[115])

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

# add gemaal and LB at downstream Linge (Hardinxveld)
pump_node = ribasim_model.pump.add(Node(geometry=Point(118539.25, 425972.46)), [pump.Static(flow_rate=[20])])
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(118530.65, 425964)), [level_boundary.Static(level=[default_level])]
)
ribasim_model.link.add(ribasim_model.basin[1], pump_node)
ribasim_model.link.add(pump_node, level_boundary_node)

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
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[91])

# Add aanvoer-component of Kuijkgemaal
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(174629, 440132)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(174630, 440132)), [pump.Static(flow_rate=[20])])
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_afvoer"] = 0
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[94])

# Add Inlaatgemaal van Beuningen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(158270, 436942)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(158276, 436942)), [pump.Static(flow_rate=[20])])
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[86])

# Add Inlaatgemaal Bontemorgen
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(164450, 441800)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(164412, 441741)), [pump.Static(flow_rate=[20])])
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_aanvoer"] = 1
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == pump_node.node_id, "meta_func_afvoer"] = 0
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[77])

# set 'aanvoer'-function on
pump_ids = 989, 1000, 1001
for i in pump_ids:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == i, "meta_func_aanvoer"] = 1

# force 'afvoergemalen'
pump_ids = 272, 280
for i in pump_ids:
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == i, "meta_func_afvoer"] = 1
    ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df["node_id"] == i, "meta_func_aanvoer"] = 0

# TODO: Temporary fixes
ribasim_model.remove_node(788, True)
ribasim_model.remove_node(960, False)
level_boundary_node = ribasim_model.level_boundary.add(
    Node(geometry=Point(157934, 425855)), [level_boundary.Static(level=[default_level])]
)
pump_node = ribasim_model.pump.add(Node(geometry=Point(157940, 425856)), [pump.Static(flow_rate=[20])])
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "afvoer", 0)
ribasim_param.change_pump_func(ribasim_model, pump_node.node_id, "aanvoer", 1)
ribasim_model.link.add(level_boundary_node, pump_node)
ribasim_model.link.add(pump_node, ribasim_model.basin[194])

ribasim_param.change_pump_func(ribasim_model, 414, "aanvoer", 1)
ribasim_param.change_pump_func(ribasim_model, 414, "afvoer", 0)
ribasim_param.change_pump_func(ribasim_model, 668, "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, 722, "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, 1005, "aanvoer", 0)
ribasim_param.change_pump_func(ribasim_model, 1005, "afvoer", 1)
ribasim_param.change_pump_func(ribasim_model, 1153, "aanvoer", 0)

# (re)set meta_node_id
ribasim_model.level_boundary.node.df["meta_node_id"] = ribasim_model.level_boundary.node.df.index
ribasim_model.tabulated_rating_curve.node.df["meta_node_id"] = ribasim_model.tabulated_rating_curve.node.df.index
ribasim_model.pump.node.df["meta_node_id"] = ribasim_model.pump.node.df.index

# insert standard profiles to each basin: these are [depth_profiles] meter deep, defined from the streefpeil
ribasim_param.insert_standard_profile(
    ribasim_model,
    unknown_streefpeil=unknown_streefpeil,
    regular_percentage=regular_percentage,
    boezem_percentage=boezem_percentage,
    depth_profile=2,
)

add_storage_basins = AddStorageBasins(
    ribasim_model=ribasim_model, exclude_hoofdwater=True, additional_basins_to_exclude=[]
)

add_storage_basins.create_bergende_basins()

# set static forcing
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_forcing(ribasim_model, starttime, endtime, 10)
else:
    forcing_dict = {
        "precipitation": ribasim_param.convert_mm_day_to_m_sec(0 if AANVOER_CONDITIONS else 10),
        "potential_evaporation": ribasim_param.convert_mm_day_to_m_sec(10 if AANVOER_CONDITIONS else 0),
        "drainage": ribasim_param.convert_mm_day_to_m_sec(0),
        "infiltration": ribasim_param.convert_mm_day_to_m_sec(0),
    }
    ribasim_param.set_static_forcing(timesteps, timestep_size, starttime, forcing_dict, ribasim_model)

# set pump capacity for each pump
ribasim_model.pump.static.df["flow_rate"] = 0.16667  # 10 kuub per minuut

# convert all boundary nodes to LevelBoundaries
ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add the default levels
if MIXED_CONDITIONS:
    ribasim_param.set_hypothetical_dynamic_level_boundaries(ribasim_model, starttime, endtime, -0.6, 12.4)
else:
    ribasim_model.level_boundary.static.df["level"] = default_level

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

# add control, based on the meta_categorie
ribasim_param.identify_node_meta_categorie(ribasim_model, aanvoer_enabled=AANVOER_CONDITIONS)
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
ribasim_param.set_aanvoer_flags(
    ribasim_model, str(aanvoer_path), processor, aanvoer_enabled=AANVOER_CONDITIONS, basin_aanvoer_off=(204)
)

# change the control of the outlet at Kinderdijk
ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.node_id == 280, "meta_categorie"] = (
    "Inlaat boezem, afvoer gemaal"
)

# ribasim_param.add_discrete_control(ribasim_model, waterschap, default_level)
ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)
ribasim_param.add_continuous_control(ribasim_model, dy=-50)

# Manning resistance
# there is a MR without geometry and without links for some reason
ribasim_model.manning_resistance.node.df = ribasim_model.manning_resistance.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 10
ribasim_model.manning_resistance.static.df.manning_n = 0.01

# last formatting of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)
if MIXED_CONDITIONS:
    ribasim_model.basin.static.df = None
    ribasim_param.set_dynamic_min_upstream_max_downstream(ribasim_model)

# add the water authority column to couple the model with
assign = AssignAuthorities(
    ribasim_model=ribasim_model,
    waterschap=waterschap,
    ws_grenzen_path=ws_grenzen_path,
    RWS_grenzen_path=RWS_grenzen_path,
    custom_nodes={
        973: "Rijkswaterstaat",
        940: "Rijkswaterstaat",
        939: "Rijkswaterstaat",
        1004: "Rijkswaterstaat",
    },
)
ribasim_model = assign.assign_authorities()
# if MIXED_CONDITIONS:
#     assign.from_static_to_time_df(ribasim_model, clear_static=True)

# set numerical settings
# write model output
ribasim_model.use_validation = True
ribasim_model.starttime = starttime
ribasim_model.endtime = endtime
ribasim_model.solver.saveat = saveat
ribasim_model.write(ribasim_work_dir_model_toml)

# run model
ribasim_param.tqdm_subprocess(["ribasim", ribasim_work_dir_model_toml], print_other=False, suffix="init")

# model performance
controle_output = Control(work_dir=work_dir, qlr_path=qlr_path)
indicators = controle_output.run_dynamic_forcing() if MIXED_CONDITIONS else controle_output.run_all()

# write model
ribasim_param.write_ribasim_model_GoodCloud(
    ribasim_model=ribasim_model,
    work_dir=work_dir,
    waterschap=waterschap,
    include_results=True,
)
