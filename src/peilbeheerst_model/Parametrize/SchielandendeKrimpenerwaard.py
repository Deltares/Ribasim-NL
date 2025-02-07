"""Parameterisation of water board: Schieland en de Krimpenerwaard."""

import datetime
import os
import warnings

import ribasim
import ribasim.nodes
from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import Point

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.add_storage_basins import AddStorageBasins
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim_nl import CloudStorage

# model settings
waterschap = "SchielandendeKrimpenerwaard"
base_model_versie = "2024_12_1"

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
qlr_path = cloud.joinpath("Basisgegevens", "QGIS_qlr", "output_controle.qlr")

cloud.synchronize(
    filepaths=[ribasim_base_model_dir, FeedbackFormulier_path, ws_grenzen_path, RWS_grenzen_path, qlr_path]
)

# download the Feedback Formulieren, overwrite the old ones
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

default_level = 0.75  # default LevelBoundary level, similar to surrounding Maas

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
    ribasim_model = ribasim.Model(filepath=ribasim_work_dir_model_toml)

# check basin area
ribasim_param.validate_basin_area(ribasim_model)

# model specific tweaks
# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

# add the LevelBoundaries and Pumps
# 1
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(89792, 437301)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(new_node_id + 1, Point(89992, 437341)), [pump.Static(flow_rate=[0.1])])

ribasim_model.edge.add(ribasim_model.basin[116], pump_node)
ribasim_model.edge.add(pump_node, level_boundary_node)

# 2
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(92237, 435452)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(92144, 435533)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.edge.add(level_boundary_node, tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, ribasim_model.basin[110])

# 3
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(95111, 436428)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(new_node_id + 1, Point(95431, 436488)), [pump.Static(flow_rate=[0.1])])

ribasim_model.edge.add(ribasim_model.basin[157], pump_node)
ribasim_model.edge.add(pump_node, level_boundary_node)

# 4
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(95111, 436418)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(new_node_id + 1, Point(95736, 436211)), [pump.Static(flow_rate=[0.1])])

ribasim_model.edge.add(ribasim_model.basin[143], pump_node)
ribasim_model.edge.add(pump_node, level_boundary_node)

# 5 not sure whether this one is correct. FF was not clear
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(94966, 437011)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(new_node_id + 1, Point(94966, 437011)), [pump.Static(flow_rate=[0.1])])

ribasim_model.edge.add(ribasim_model.basin[25], pump_node)
ribasim_model.edge.add(pump_node, level_boundary_node)

# 6
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(92476, 435889)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(new_node_id + 1, Point(92334, 436428)), [pump.Static(flow_rate=[0.1])])

ribasim_model.edge.add(ribasim_model.basin[142], pump_node)
ribasim_model.edge.add(pump_node, level_boundary_node)

# 7
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(104547, 443432)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(new_node_id + 1, Point(104485, 443506)), [pump.Static(flow_rate=[0.1])])

ribasim_model.edge.add(ribasim_model.basin[19], pump_node)
ribasim_model.edge.add(pump_node, level_boundary_node)

# 7
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(104555, 443434)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(new_node_id + 1, Point(104527, 443455)), [pump.Static(flow_rate=[0.1])])

ribasim_model.edge.add(ribasim_model.basin[2], pump_node)
ribasim_model.edge.add(pump_node, level_boundary_node)

# 8
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(102079, 438209)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(new_node_id + 1, Point(102230, 438082)), [pump.Static(flow_rate=[0.1])])

ribasim_model.edge.add(ribasim_model.basin[29], pump_node)
ribasim_model.edge.add(pump_node, level_boundary_node)

# 9, added by RB as it is quiet clear a levelboundary is missing.
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(91451, 439245)), [level_boundary.Static(level=[default_level])]
)
tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(91514, 439286)),
    [tabulated_rating_curve.Static(level=[0.0, 0.1234], flow_rate=[0.0, 0.1234])],
)
ribasim_model.edge.add(ribasim_model.basin[28], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)

# 10. cooperation day 12 dec 2024: Erik suggested adding an additional pump which already exists, but this would prevent two entire basins interacting with eachother if only one basin would get a pump, while this pump discharges both basins
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1
level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(89786, 437309)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(new_node_id + 1, Point(89985, 437345)), [pump.Static(flow_rate=[0.1])])

ribasim_model.edge.add(ribasim_model.basin[115], pump_node)
ribasim_model.edge.add(pump_node, level_boundary_node)

ribasim_model.level_boundary.node.df.meta_node_id = ribasim_model.level_boundary.node.df.index
ribasim_model.tabulated_rating_curve.node.df.meta_node_id = ribasim_model.tabulated_rating_curve.node.df.index
ribasim_model.pump.node.df.meta_node_id = ribasim_model.pump.node.df.index

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
forcing_dict = {
    "precipitation": ribasim_param.convert_mm_day_to_m_sec(10),
    "potential_evaporation": ribasim_param.convert_mm_day_to_m_sec(0),
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
ribasim_model.level_boundary.static.df.level = default_level

# test for better afwatering
ribasim_model.level_boundary.static.df.loc[ribasim_model.level_boundary.static.df.node_id == 728, "level"] = -1.3
ribasim_model.edge.df.loc[ribasim_model.edge.df.to_node_id == 728, "meta_categorie"] = "hoofdwater"
ribasim_model.edge.df.loc[ribasim_model.edge.df.to_node_id == 728, "meta_to_node_type"] = "LevelBoundary"

# test for better afwatering
ribasim_model.level_boundary.static.df.loc[ribasim_model.level_boundary.static.df.node_id == 638, "level"] = -0.1

# add outlet
ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)

# add control, based on the meta_categorie
ribasim_param.identify_node_meta_categorie(ribasim_model)
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")
# ribasim_param.add_discrete_control(ribasim_model, waterschap, default_level)
ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)

# Manning resistance
# there is a MR without geometry and without edges for some reason
ribasim_model.manning_resistance.node.df = ribasim_model.manning_resistance.node.df.dropna(subset="geometry")

# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 10
ribasim_model.manning_resistance.static.df.manning_n = 0.01

# last formatting of the tables
# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)

# set numerical settings
# write model output
ribasim_model.use_validation = True
ribasim_model.starttime = starttime
ribasim_model.endtime = endtime
ribasim_model.solver.saveat = saveat
ribasim_model.write(ribasim_work_dir_model_toml)

# run model
ribasim_param.tqdm_subprocess(
    ["C:/ribasim_windows/ribasim/ribasim.exe", ribasim_work_dir_model_toml], print_other=False, suffix="init"
)

# model performance
controle_output = Control(work_dir=work_dir, qlr_path=qlr_path)
indicators = controle_output.run_all()

# write model
ribasim_param.write_ribasim_model_GoodCloud(
    ribasim_model=ribasim_model,
    work_dir=work_dir,
    waterschap=waterschap,
    include_results=True,
)
