import datetime
import os
import pathlib
import warnings

import pandas as pd
import ribasim
import ribasim.nodes

import peilbeheerst_model.ribasim_parametrization as ribasim_param
from peilbeheerst_model.add_storage_basins import AddStorageBasins
from peilbeheerst_model.controle_output import *
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor

get_ipython().run_line_magic("reload_ext", "autoreload")

warnings.filterwarnings("ignore")


# ## Define variables and model

# #### Set Config


waterschap = "AmstelGooienVecht"
work_dir = pathlib.Path(f"../../../../../Ribasim_updated_models/{waterschap}/modellen/{waterschap}_parametrized")
ribasim_gpkg = work_dir.joinpath("database.gpkg")
path_ribasim_toml = work_dir.joinpath("ribasim.toml")
output_dir = work_dir.joinpath("results")

# Basin area percentage
regular_percentage = 10
boezem_percentage = 90
unknown_streefpeil = (
    0.00012345  # we need a streefpeil to create the profiles, Q(h)-relations, and af- and aanslag peil for pumps
)

# Forcing settings
start_time = "2024-01-01"
timestep_size = "d"
timesteps = 2
delta_crest_level = 0.1  # delta waterlevel of boezem compared to streefpeil till no water can flow through an outlet

default_level = -0.42  # default LevelBoundary level


# ## Process the feedback form


name = "Ron Bruijns (HKV)"
versie = "2024_10_5"

feedback_excel = pathlib.Path(f"../../../../../Ribasim_feedback/V1_formulieren/feedback_formulier_{waterschap}.xlsx")
feedback_excel_processed = (
    f"../../../../..//Ribasim_feedback/V1_formulieren_verwerkt/feedback_formulier_{waterschap}_JA_processed.xlsx"
)

ribasim_toml = f"../../../../../Ribasim_base_models/{waterschap}_boezemmodel_{versie}/ribasim.toml"
output_folder = work_dir  # f"../../../../../Ribasim_updated_models/{waterschap}"

processor = RibasimFeedbackProcessor(
    name, waterschap, versie, feedback_excel, ribasim_toml, output_folder, feedback_excel_processed
)
processor.run()


# #### Load model


# Load Ribasim model
with warnings.catch_warnings():
    warnings.simplefilter(action="ignore", category=FutureWarning)
    ribasim_model = ribasim.Model(filepath=path_ribasim_toml)


# # Parameterization

# ## Nodes

# ### Basin (characteristics)


ribasim_param.validate_basin_area(ribasim_model)


# remove the basins of above in the feedback form


# ## Model specific tweaks


new_node_id = max(ribasim_model.edge.df.from_node_id.max(), ribasim_model.edge.df.to_node_id.max()) + 1


# change unknown streefpeilen to a default streefpeil
ribasim_model.basin.area.df.loc[
    ribasim_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil", "meta_streefpeil"
] = str(unknown_streefpeil)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df["meta_streefpeil"] == -9.999, "meta_streefpeil"] = str(
    unknown_streefpeil
)


ribasim_model.basin.area.df["meta_streefpeil"] = ribasim_model.basin.area.df["meta_streefpeil"].astype(float)
ribasim_model.basin.area.df.loc[ribasim_model.basin.area.df.index == 195, "meta_streefpeil"] = -2.45
ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df.index == 195, "level"] = -2.45


assert not pd.isnull(ribasim_model.basin.area.df.meta_streefpeil).any()


# ## Implement standard profile and a storage basin


# Insert standard profiles to each basin. These are [depth_profiles] meter deep, defined from the streefpeil
ribasim_param.insert_standard_profile(
    ribasim_model,
    unknown_streefpeil=unknown_streefpeil,
    regular_percentage=regular_percentage,
    boezem_percentage=boezem_percentage,
    depth_profile=2,
)


# remove after the feedback forms have been fixed
ribasim_model.basin.profile.df.loc[ribasim_model.basin.profile.df.index == 559, "area"] = 1
ribasim_model.basin.profile.df.loc[ribasim_model.basin.profile.df.index == 560, "area"] = 2


add_storage_basins = AddStorageBasins(
    ribasim_model=ribasim_model, exclude_hoofdwater=True, additional_basins_to_exclude=[]
)

add_storage_basins.create_bergende_basins()


# ### Basin (forcing)


# Set static forcing
forcing_dict = {
    "precipitation": ribasim_param.convert_mm_day_to_m_sec(10),
    "potential_evaporation": ribasim_param.convert_mm_day_to_m_sec(0),
    "drainage": ribasim_param.convert_mm_day_to_m_sec(0),
    "infiltration": ribasim_param.convert_mm_day_to_m_sec(0),
    # 'urban_runoff':          ribasim_param.convert_mm_day_to_m_sec(0),
}

ribasim_param.set_static_forcing(timesteps, timestep_size, start_time, forcing_dict, ribasim_model)


# ### Pumps


# Set pump capacity for each pump
ribasim_model.pump.static.df["flow_rate"] = 0.16667  # 10 kuub per minuut


# ### Convert all boundary nodes to LevelBoundaries


ribasim_param.Terminals_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)  # clean
ribasim_param.FlowBoundaries_to_LevelBoundaries(ribasim_model=ribasim_model, default_level=default_level)


# ### Add Outlet


ribasim_param.add_outlets(ribasim_model, delta_crest_level=0.10)


# ## Add control, based on the meta_categorie


ribasim_param.identify_node_meta_categorie(ribasim_model)


ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="outlet")
ribasim_param.find_upstream_downstream_target_levels(ribasim_model, node="pump")


# ribasim_param.add_discrete_control(ribasim_model, waterschap, default_level)


ribasim_param.determine_min_upstream_max_downstream_levels(ribasim_model, waterschap)


# ### Manning Resistance


# there is a MR without geometry and without edges for some reason
ribasim_model.manning_resistance.node.df = ribasim_model.manning_resistance.node.df.dropna(subset="geometry")


# ## Last formating of the tables


# only retain node_id's which are present in the .node table
ribasim_param.clean_tables(ribasim_model)


ribasim_model.edge.df["fid"] = ribasim_model.edge.df.index.copy()


# # Set numerical settings


ribasim_model.use_validation = True


# Write model output
# ribasim_param.index_reset(ribasim_model)
ribasim_model.starttime = datetime.datetime(2024, 1, 1)
ribasim_model.endtime = datetime.datetime(2025, 1, 1)
ribasim_model.solver.saveat = 3600
ribasim_param.write_ribasim_model_Zdrive(ribasim_model, path_ribasim_toml)


# ## Run Model

# ## Iterate over tabulated rating curves


# try:
#     ribasim_param.iterate_TRC(
#         ribasim_param=ribasim_param,
#         allowed_tolerance=0.02,
#         max_iter=1,
#         expected_difference=0.1,
#         max_adjustment=0.25,
#         cmd=["ribasim", path_ribasim_toml],
#         output_dir=output_dir,
#         path_ribasim_toml=path_ribasim_toml,
#     )

# except Exception:
#     logging.error("The model was not able to run. Log file:")
#     log_file_path = os.path.join(output_dir, "ribasim.log")  # Update with the correct path to your log file
#     try:
#         with open(log_file_path) as log_file:
#             log_content = log_file.read()
#             print(log_content)
#     except Exception as log_exception:
#         logging.error(f"Could not read the log file: {log_exception}")


# # Write model


# control_dict = Control(work_dir = work_dir).run_all()
ribasim_param.write_ribasim_model_GoodCloud(
    ribasim_model=ribasim_model,
    path_ribasim_toml=path_ribasim_toml,
    waterschap=waterschap,
    modeltype="boezemmodel",
    include_results=True,
)


# ## Open Output


df_basin = pd.read_feather(os.path.join(output_dir, "basin.arrow"))
df_basin


# ### Add discrete control nodes


# Add discrete control nodes and control edges
# ribasim_param.add_discrete_control_nodes(ribasim_model)
