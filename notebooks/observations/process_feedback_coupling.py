# %%
from pathlib import Path

import geopandas as gpd
import pandas as pd

# Importeren van de update koppeltabel
from update_coupling import convert_to_point, update_koppeltabel_with_feedback

from ribasim_nl import CloudStorage, Model

# from ribasim import Model

# %%

cloud = CloudStorage()

locatie_koppeltabellen = cloud.joinpath("Basisgegevens/resultaatvergelijking/koppeltabel_2026")

# paths:
# model_folder_temporary = (
#     Path(r"C:\Users\micha.veenendaal\Data\Ribasim LHM validatie\LHM_model_werkend\lhm_coupled") / "lhm-coupled.toml"
# )

# model_folder_temporary = (
#     Path(r"C:\Users\micha.veenendaal\Data\Ribasim LHM validatie\LHM_model_2017\lhm_ctwq_compat") / "lhm_ctwq.toml"
# )

waterboard = "Limburg"
waterboard_model_versions = cloud.uploaded_models(authority=waterboard)

latest_model_version = sorted(
    [i for i in waterboard_model_versions if i.model == waterboard], key=lambda x: getattr(x, "sorter", "")
)[-1]

versie = f"{latest_model_version.path_string}"

model_folder = cloud.joinpath(f"{waterboard}/modellen", latest_model_version.path_string)

# Filteren of gebruiken we een gekoppeld model:
filter_waterschappen = True
waterschapsnaam = ["Limburg"]

# toml_naam = "lhm-coupled.toml"
toml_naam = "limburg.toml"

# synchronize paths
# cloud.synchronize([locatie_koppeltabellen, model_folder])
cloud.synchronize([locatie_koppeltabellen])

model = Model.read(Path(model_folder) / toml_naam)  # Aangepast van lhm.toml
links = model.link.df


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
    "previous_meta_link_id_waterbeheerder",
    "new_from_node_geometry",
    "new_to_node_geometry",
    "new_from_node_types",
    "new_to_node_types",
    "new_link_id",
    "new_meta_link_id_waterbeheerder",
    "status",
]

# Partij die feedback geeft
partij = "HydroLogic"

# synchronize paths
# cloud.synchronize([loc_ref_koppeltabel, model_folder])
# %%

# # Script met de orginele getransformeerde koppeltabel
# input_koppeltabel_path = cloud.joinpath(locatie_koppeltabellen, "Transformed_koppeltabel_versie1.xlsx")

# # Script waarin de feedback is verwerkt
# feedback_koppeltabel_path = cloud.joinpath(locatie_koppeltabellen, "Transformed_koppeltabel_versie1_Feedback.xlsx")

# Script met de orginele getransformeerde koppeltabel
input_koppeltabel_path = cloud.joinpath(locatie_koppeltabellen, "Transformed_koppeltabel_versie_Limburg_2026_4_0.xlsx")

# Script waarin de feedback is verwerkt
feedback_koppeltabel_path = cloud.joinpath(
    locatie_koppeltabellen, "Transformed_koppeltabel_versie_Limburg_2026_4_0_check.xlsx"
)


output_path = locatie_koppeltabellen

# # Reeksen die of niet goed gekoppeld kunnen worden of waarvan de meetreeks onrealistisch lijkt
# remove_meetreeksc = [
#     "Polder Oldebroek ValleiEnVeluwe",
#     "Hoogland wetterskip",
#     "Grafelijkheidssluis",
#     "Zuidersluis te Schardam_uit",
#     "Noordersluis te Schardam_in",
#     "Bunde",
#     "Loozen (kilometer 50)",
#     "Inlaat Schiegemaal",
#     "ADM Balladelaan",
#     "De Wenden (Gelderse Gracht)",
#     "De Wenden (Noordermerkkanaal)",
# ]

# remove_meetreeksc_specifiek_2017 = [
#     "Eijsden grens",  # geen goeie reeks voor 2017
#     "Borgharen dorp",  # Borgharen dorp
#     "Lobith Hoofdkranen",  # zit er dubbel in
#     "Driel boven Hoofdkranen",  # zit er dubbel in
#     "IJmuiden Hoofdkranen",  # zit er dubbel in
#     "Megen dorp",  # geen goeie reeks
#     "Olst Hoofdkranen",  # zit er dubbel in
#     "Sint Pieter noord",  # geen goeie reeks
#     "Smeermaas",  # geen goeie reeks
#     "Stevinsluizen Hoofdkranen",  # zit er dubbel in
#     "Lorentzsluizen Hoofdkranen",  # zit er dubbel in
#     "Barneveldse Beek ValleiEnVeluwe",  # zit er dubbel in
#     "Loobeek WL",  # zit er dubbel in
# ]

# remove_meetreeksc = remove_meetreeksc + remove_meetreeksc_specifiek_2017

# updated_koppeltabel = update_koppeltabel_with_feedback(
#     input_koppeltabel_path,
#     feedback_koppeltabel_path,
#     lhm_model,
#     output_path,
#     versie = versie,
#     cloud_sync=cloud,
#     keep_all_columns=False,
#     columns_to_keep=columns_to_keep,
#     remove_meetreeksc=None,
#     partij="HydroLogic",
#     add_new_data=False,
# )

remove_meetreeksc = None

updated_koppeltabel = update_koppeltabel_with_feedback(
    input_koppeltabel_path,
    feedback_koppeltabel_path,
    model,
    output_path,
    versie=versie,
    cloud_sync=cloud,  # cloud_sync is of CloudStorage() object of False bool
    keep_all_columns=False,
    columns_to_keep=columns_to_keep,
    remove_meetreeksc=remove_meetreeksc,
    partij=partij,
    add_new_data=False,
    transformed_koppeltabel=True,
)


# %%
####################
# AAN HET EINDE VAN HET VERWERKEN EVENTUEEL NOG SORTEN OP WATERSCHAP
# EN DUS WEGSCHRIJVEN ALS VOLLEDIGE NIEUWE .GPKG OM GECOMBINEERDE
# DEFINITIEVE SET, RUIMTELIJK IN TE ZIEN.
####################

# path
output_path = locatie_koppeltabellen
input_path = Path(input_koppeltabel_path)
base_without_feedback = input_path.stem.split("_Feedback")[0]
new_filename = f"{base_without_feedback}_Feedback_Verwerkt_{partij}{input_path.suffix}"
path_recente_koppeltabel = cloud.joinpath(output_path, new_filename)
path_recente_koppeltabel_gpkg = cloud.joinpath(output_path, f"{base_without_feedback}_Feedback_Verwerkt_{partij}.gpkg")

Laatst_verwerkte_koppeltabel = pd.read_excel(path_recente_koppeltabel)

# Apply the conversion to the geometry column
Laatst_verwerkte_koppeltabel["geometry"] = Laatst_verwerkte_koppeltabel["geometry"].apply(convert_to_point)

# Convert the dataframe to a GeoDataFrame
gdf = gpd.GeoDataFrame(Laatst_verwerkte_koppeltabel, geometry=Laatst_verwerkte_koppeltabel["geometry"])

# Set the CRS to Amersfoort (EPSG:28992)
gdf.set_crs(epsg=28992, inplace=True)

# Save the GeoDataFrame to a GeoPackage
gdf.to_file(path_recente_koppeltabel_gpkg, driver="GPKG")
cloud.upload_file(path_recente_koppeltabel_gpkg)

# %%
