# %%
import os

from ribasim import Model

# Importeren van de update koppeltabel
# from UpdateKoppeltabel import update_koppeltabel_with_feedback
from Update_Final_Transformed_koppeltabel import update_koppeltabel_with_feedback

from ribasim_nl import CloudStorage

# %%

cloud = CloudStorage()

locatie_koppeltabellen = cloud.joinpath("Landelijk", "resultaatvergelijking", "koppeltabel")

# paths:

#!TODO: Nog niet mogelijk om lhm-coupled model op GC in te lezen

# met huidige ribasim dev versie
# rws_model_versions = cloud.uploaded_models(authority="Rijkswaterstaat")
# latest_lhm_version = sorted([i for i in rws_model_versions if i.model == "lhm_coupled"], key=lambda x: getattr(x, "sorter", ""))[-1]
# model_folder = cloud.joinpath("Rijkswaterstaat", "modellen", latest_lhm_version.path_string)


model_folder_temporary = os.path.join(
    r"C:\Users\micha.veenendaal\Data\Ribasim LHM validatie\LHM_model_werkend\lhm_coupled", "lhm-coupled.toml"
)

lhm_model = Model.read(filepath=model_folder_temporary)

#########################################################
# Welke kolommen houden vanuit de feedback koppeltabel
#########################################################
columns_to_keep = [
    "Waterschap",
    "MeetreeksC",
    "Aan/Af",
    "geometry",
    "previous_from_node_geometry",
    "previous_to_node_geometry",
    "previous_from_node_types",
    "previous_to_node_types",
    "previous_link_id",
    "new_from_node_geometry",
    "new_to_node_geometry",
    "new_from_node_types",
    "new_to_node_types",
    "new_link_id",
    "Specifiek",
]


# synchronize paths
# cloud.synchronize([loc_ref_koppeltabel, model_folder])

cloud.synchronize([locatie_koppeltabellen])


# %%

# Script met de orginele getransformeerde koppeltabel
input_koppeltabel_path = cloud.joinpath(locatie_koppeltabellen, "Transformed_koppeltabel_versie1.xlsx")

# Script waarin de feedback is verwerkt
feedback_koppeltabel_path = cloud.joinpath(locatie_koppeltabellen, "Transformed_koppeltabel_versie1_Feedback.xlsx")

output_path = locatie_koppeltabellen


updated_koppeltabel = update_koppeltabel_with_feedback(
    input_koppeltabel_path,
    feedback_koppeltabel_path,
    lhm_model,
    output_path,
    cloud_sync=cloud,
    keep_all_columns=False,
    columns_to_keep=columns_to_keep,
    remove_meetreeksc=None,
    partij="HydroLogic",
    add_new_data=False,
)


# %%
