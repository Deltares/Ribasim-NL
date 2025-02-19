# %%
import datetime
import os
import warnings

from ribasim import Node
from ribasim.nodes import level_boundary, pump, tabulated_rating_curve
from shapely import Point

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.add_storage_basins import AddStorageBasins
from peilbeheerst_model.controle_output import Control
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim_nl import CloudStorage, Model

waterschap = "WetterskipFryslan"
base_model_versie = "2024_12_8"


# # Connect with the GoodCloud


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


# %% Define variables and model

# %%## Set Config


# Basin area percentage
regular_percentage = 10
boezem_percentage = 90
unknown_streefpeil = (
    0.00012345  # we need a streefpeil to create the profiles, Q(h)-relations, and af- and aanslag peil for pumps
)

# Forcing settings
starttime = datetime.datetime(2024, 1, 1)
endtime = datetime.datetime(2024, 3, 1)
saveat = 3600 * 24
timestep_size = "d"
timesteps = 2
delta_crest_level = 0.1  # delta waterlevel of boezem compared to streefpeil till no water can flow through an outlet

default_level = -2.3456  # default LevelBoundary level


# %% Process the feedback form


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


# %%## Load model


# Load Ribasim model
with warnings.catch_warnings():
    warnings.simplefilter(action="ignore", category=FutureWarning)
    ribasim_model = Model(filepath=ribasim_work_dir_model_toml)

# # Parameterization

# %% Nodes

# %%# Basin (characteristics)

# %% Model specific tweaks
ribasim_model.merge_basins(
    node_id=1101, to_node_id=342, are_connected=False
)  # too small basin, caused numerical issues
ribasim_model.merge_basins(
    node_id=342, to_node_id=173
)  # basin 1101 is an enclave-like area, merge it with the surrounding area
ribasim_model.merge_basins(node_id=1022, to_node_id=297)  # 518 m2
ribasim_model.merge_basins(node_id=379, to_node_id=262)  # 803 m2
ribasim_model.merge_basins(node_id=913, to_node_id=95)  # 1420 m2
ribasim_model.merge_basins(node_id=1162, to_node_id=142, are_connected=False)  # 1921 m2
ribasim_model.merge_basins(node_id=673, to_node_id=89)  # 2050 m2
ribasim_model.merge_basins(node_id=1115, to_node_id=96)  # 2112 m2

# small basins at polder in the South with water level deviations
ribasim_model.merge_basins(node_id=796, to_node_id=153)  # Willem Jongma
ribasim_model.merge_basins(node_id=867, to_node_id=869, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=869, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=925, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=918, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=950, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=951, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=1159, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=1109, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=805, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=1113, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=863, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=1160, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=853, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=792, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=924, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=872, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=955, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=917, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=956, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=963, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=1112, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=923, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=1075, to_node_id=153, are_connected=False)  # Willem Jongma
ribasim_model.merge_basins(node_id=1146, to_node_id=153, are_connected=False)  # Willem Jongma

# remove undeleted nodes from the Willem Jongma polder
nodes_WJ = [
    2426,
    2605,
    2995,
    3091,
    3462,
    3534,
    3597,
    3868,
    3874,
    4050,
    4054,
    4142,
    4143,
    4144,
    4145,
    4146,
    4147,
    4148,
    4149,
    4150,
    4151,
    4152,
    4153,
    4154,
    4155,
    4156,
    4158,
    4159,
    4160,
    4161,
    4162,
]  # 13122, 13301, 13787, 14293, 14570

for node_id_WJ in nodes_WJ:
    ribasim_model.remove_node(node_id=node_id_WJ, remove_edges=True)

ribasim_model.merge_basins(node_id=1074, to_node_id=254)  # 3753 m2
ribasim_model.merge_basins(node_id=1128, to_node_id=458, are_connected=False)  # 4315 m2
ribasim_model.merge_basins(node_id=1108, to_node_id=87)  # 4564 m2
ribasim_model.merge_basins(node_id=961, to_node_id=463)  # 5032 m2
ribasim_model.merge_basins(node_id=674, to_node_id=750)  # 5555 m2
ribasim_model.merge_basins(node_id=1087, to_node_id=89)  # 5715 m2
ribasim_model.merge_basins(node_id=902, to_node_id=639)  # 5867 m2

ribasim_model.merge_basins(node_id=945, to_node_id=589)  # MR 10114 numerically unstable, increase size
ribasim_model.merge_basins(node_id=1050, to_node_id=561)  # MR 10575 numerically unstable, increase size
ribasim_model.merge_basins(node_id=1004, to_node_id=261)  # MR 10529 numerically unstable, increase size
ribasim_model.merge_basins(node_id=49, to_node_id=1015)  # MR 9574 numerically unstable, increase size

ribasim_param.validate_basin_area(ribasim_model)


# %% Model specific tweaks


new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1


# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)

# change high initial states to 0
ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df["level"] == 9.999, "level"] = 0
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == 9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)


new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(158166, 553915)), [level_boundary.Static(level=[default_level])]
)

tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(158166, 553916)),
    [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
)

ribasim_model.edge.add(ribasim_model.basin[89], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)

# %%
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(156824, 552856)), [level_boundary.Static(level=[default_level])]
)

tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(156831, 552967)),
    [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
)

ribasim_model.edge.add(ribasim_model.basin[861], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)


new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(155989, 563067)), [level_boundary.Static(level=[default_level])]
)

tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(156038, 563080)),
    [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
)

ribasim_model.edge.add(ribasim_model.basin[149], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)


new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(154647, 567409)), [level_boundary.Static(level=[default_level])]
)

tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(154644, 567399)),
    [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
)

ribasim_model.edge.add(ribasim_model.basin[149], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)


new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(156173, 576551)), [level_boundary.Static(level=[default_level])]
)

tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(157109, 576125)),
    [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
)

ribasim_model.edge.add(ribasim_model.basin[254], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)


# #add a zeegemaal
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(170894, 591637)), [level_boundary.Static(level=[default_level])]
)

pump_node = ribasim_model.pump.add(Node(new_node_id + 1, Point(170902, 591432)), [pump.Static(flow_rate=[700 / 60])])

ribasim_model.edge.add(ribasim_model.basin[4], pump_node)
ribasim_model.edge.add(pump_node, level_boundary_node)


ribasim_model.level_boundary.node.df.meta_node_id = ribasim_model.level_boundary.node.df.index
ribasim_model.tabulated_rating_curve.node.df.meta_node_id = ribasim_model.tabulated_rating_curve.node.df.index
ribasim_model.pump.node.df.meta_node_id = ribasim_model.pump.node.df.index

# %% #Add LB at Vlieland
new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1

level_boundary_node = ribasim_model.level_boundary.add(
    Node(new_node_id, Point(127410, 585808)), [level_boundary.Static(level=[default_level])]
)

tabulated_rating_curve_node = ribasim_model.tabulated_rating_curve.add(
    Node(new_node_id + 1, Point(127030, 585934)),
    [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
)

ribasim_model.edge.add(ribasim_model.basin[1148], tabulated_rating_curve_node)
ribasim_model.edge.add(tabulated_rating_curve_node, level_boundary_node)


ribasim_model.level_boundary.node.df.meta_node_id = ribasim_model.level_boundary.node.df.index
ribasim_model.tabulated_rating_curve.node.df.meta_node_id = ribasim_model.tabulated_rating_curve.node.df.index
ribasim_model.pump.node.df.meta_node_id = ribasim_model.pump.node.df.index

# %% Implement standard profile and a storage basin


# Insert standard profiles to each basin. These are [depth_profiles] meter deep, defined from the streefpeil
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


# %%# Basin (forcing)


# Set static forcing
forcing_dict = {
    "precipitation": ribasim_param.convert_mm_day_to_m_sec(10),
    "potential_evaporation": ribasim_param.convert_mm_day_to_m_sec(0),
    "drainage": ribasim_param.convert_mm_day_to_m_sec(0),
    "infiltration": ribasim_param.convert_mm_day_to_m_sec(0),
}

ribasim_param.set_static_forcing(timesteps, timestep_size, starttime, forcing_dict, ribasim_model)


# %%# Pumps


# Set pump capacity for each pump
ribasim_model.pump.static.df["flow_rate"] = 0.16667  # 10 kuub per minuut


# %%# Convert all boundary nodes to LevelBoundaries


ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)

# add the default levels
ribasim_model.level_boundary.static.df.level = default_level


# %%# Add Outlet


ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)


# %% Add control, based on the meta_categorie


ribasim_param.identify_node_meta_categorie(ribasim_model)


ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")


# ribasim_param.add_discrete_control(ribasim_model, waterschap, default_level)


ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)


# %%# Manning Resistance


# there is a MR without geometry and without edges for some reason
ribasim_model.manning_resistance.node.df = ribasim_model.manning_resistance.node.df.dropna(subset="geometry")


# lower the difference in waterlevel for each manning node
ribasim_model.manning_resistance.static.df.length = 25
ribasim_model.manning_resistance.static.df.manning_n = 0.01


# %% Last formating of the tables


# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model, waterschap)


# # Set numerical settings


# Write model output
ribasim_model.use_validation = True
ribasim_model.starttime = starttime
ribasim_model.endtime = endtime
ribasim_model.solver.saveat = saveat

ribasim_model.write(ribasim_work_dir_model_toml)


# %% Run Model


ribasim_param.tqdm_subprocess(
    ["C:/ribasim_windows/ribasim/ribasim.exe", ribasim_work_dir_model_toml], print_other=False, suffix="init"
)


controle_output = Control(work_dir=work_dir, qlr_path=qlr_path)
indicators = controle_output.run_all()


# # Write model


ribasim_param.write_ribasim_model_GoodCloud(
    ribasim_model=ribasim_model,
    work_dir=work_dir,
    waterschap=waterschap,
    include_results=True,
)

# %%
