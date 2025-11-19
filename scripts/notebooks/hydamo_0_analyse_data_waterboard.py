from pathlib import Path

import pandas as pd
from hydamo.datamodel import HyDAMO
from pandas_xlsx_tables import xlsx_tables_to_dfs
from ribasim_lumping_tools.LHM_data_bewerking_analyse_utils import (
    check_ids_hydamo_data,
    check_if_object_on_hydroobject,
    read_original_data,
    translate_data_to_hydamo_format,
)

# Vertaal originele data naar Hydamo data zoals gedefinieerd in de tabel hydamo_data_format.xlsx


base_dir = "..\\"

waterboard = "AAenMaas"
waterboard_code = 1


waterboard_dir = Path(base_dir, waterboard, "verwerkt")
path_hydamo_format = Path(waterboard_dir, "HyDAMO_format_AAenMaas.xlsx")
hydamo_format = xlsx_tables_to_dfs(path_hydamo_format)


# eerst inlezen hydroobject, vertalen naar hydamo
hydamo_object = "hydroobject"
hydamo_translate_table, data_original = read_original_data(waterboard_dir, hydamo_format, hydamo_object, waterboard)
hydroobject = translate_data_to_hydamo_format(hydamo_translate_table, data_original)

# maak een created_date aan indien nodig
if "created_date" not in data_original.columns:
    hydroobject["created_date"] = pd.NaT
# transformeer created_date waardes indien nodig
hydroobject["created_date"] = hydroobject["created_date"].replace("", pd.NaT)

# hydroobject.loc[hydroobject['code'].duplicated(keep=False), 'data_issue'] = 'duplicate_id'
data_hydamo_dict = {"hydroobject": hydroobject.set_crs(28992)}

# geometry hydroobject bufferen met 10 cm voor de spatial join
hydroobject["buffer"] = hydroobject.copy().buffer(5)  # 5 meter buffer omdat anders relevante gemalen wegvallen
hydroobject_buffered = hydroobject.set_geometry("buffer").set_crs(28992)


# Specificeer welke HyDAMO data je wilt omzetten


hydamo_objects = [
    "stuw",
    "gemaal",
    "afvoergebiedaanvoergebied",
    "pomp",
    ##'peilgebiedvigerend',
    ##'peilgebiedpraktijk',
    ##'streefpeil',
    "duikersifonhevel",
    ##'afsluiter',
    ##'sluis',
]


for hydamo_object in hydamo_objects:
    # lees aangeleverde data en hydamo tabel voor gegeven kunstwerk en waterschap
    table_hydamo, data_original = read_original_data(waterboard_dir, hydamo_format, hydamo_object, waterboard)
    if data_original is None:
        data_hydamo_dict[hydamo_object] = None
    else:
        # vertaal data naar hydamo-ribasim format
        data_hydamo = translate_data_to_hydamo_format(table_hydamo, data_original)

        # maak een created_date aan indien nodig
        if "created_date" not in data_original.columns and hydamo_object != "sluis":
            hydroobject["created_date"] = pd.NaT
        if "last_edited_date" not in data_original.columns and hydamo_object == "afsluiter":
            hydroobject["last_edited_date"] = pd.NaT
        if "lvpublicatiedatum" not in data_original.columns and hydamo_object == "afsluiter":
            hydroobject["lvpublicatiedatum"] = pd.NaT

        # transformeer created_date waardes indien nodig
        if hydamo_object != "sluis":
            data_hydamo["created_date"] = data_hydamo["created_date"].replace("", pd.NaT)
        if hydamo_object == "afsluiter":
            data_hydamo["last_edited_date"] = data_hydamo["last_edited_date"].replace("", pd.NaT)
            data_hydamo["lvpublicatiedatum"] = data_hydamo["lvpublicatiedatum"].replace("", pd.NaT)

        # check dubbele id's
        if hydamo_object not in ["streefpeil"]:  # streefpeil heeft geen code, alleen globalid etc
            data_hydamo.loc[data_hydamo["code"].duplicated(keep=False), "data_issue"] = "duplicate_id"
            # TODO check op 'code' lijkt met logischer want die kolom wordt vaker gebruikt. Maar bij WDOD bijv. is die niet ingevuld. Toch op globalid?
        # check of kuntstwerk op hydroobject ligt
        if hydamo_object in ["stuw", "gemaal", "duikersifonhevel", "sluis"]:
            data_hydamo = check_if_object_on_hydroobject(
                data_hydamo=data_hydamo, hydroobject_buffered=hydroobject_buffered
            )
            # verwijder kunstwerken die niet op hydroobject liggen
            data_hydamo = data_hydamo[data_hydamo["code_hydroobject"] != "niet op hydroobject"]
            data_hydamo = data_hydamo.reset_index()
        # voeg toe aan de hydamo dataset
        data_hydamo_dict[hydamo_object] = data_hydamo


# Waterschap specifieke acties

# Export normal


# for hydamo_object in ['hydroobject'] + hydamo_objects:
#     # export to geopackage
#     export_to_geopackage(
#         data_hydamo=data_hydamo_dict[hydamo_object],
#         hydamo_format=hydamo_format,
#         waterboard=waterboard,
#         hydamo_object=hydamo_object
#     )


# ### ribasim-nl hydamo


hydamo = HyDAMO(version="2.2.1_sweco")


for hydamo_object in ["hydroobject", *hydamo_objects]:
    data_hydamo = data_hydamo_dict[hydamo_object]
    if hydamo_object == "stuw":
        data_hydamo = data_hydamo.drop(columns=["code_hydroobject", "data_issue"])  # ,'index_right'
    data_hydamo = check_ids_hydamo_data(data_hydamo, waterboard_code, hydamo_object)
    setattr(hydamo, hydamo_object, data_hydamo)


hydamo.to_geopackage("..\\hydamo.gpkg")
