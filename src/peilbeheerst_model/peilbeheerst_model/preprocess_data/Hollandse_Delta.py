# import packages and functions
import os

import geopandas as gpd
import numpy as np
import pandas as pd

from peilbeheerst_model.general_functions import *

# # HD


# define relative paths
waterschap = "Hollandse_Delta"
data_path = "../../Data_preprocessed/Waterschappen/Hollandse_Delta/Hollandse_Delta_aangepast.gpkg"
output_gpkg_path = "../../Data_postprocessed/Waterschappen/Hollandse_Delta"


HD = read_gpkg_layers(
    gpkg_path=data_path,
    variables=[
        "stuwen",
        "gemalen",
        "afsluitmiddelen",
        "sluizen",
        "HydroObjectWatergangtype",  # = hydroobject
        "HydroObjectKunstwerkvakken",
    ],
)  # = duikersifonhevel

# change names
HD["stuw"] = HD.pop("stuwen")
HD["gemaal"] = HD.pop("gemalen")
HD["afsluitmiddel"] = HD.pop("afsluitmiddelen")
HD["sluis"] = HD.pop("sluizen")
HD["hydroobject"] = HD.pop("HydroObjectWatergangtype")
HD["duikersifonhevel"] = HD.pop("HydroObjectKunstwerkvakken")


# see the readme: peilgebieden have not be delivered properly. This data is retrieved from an external online database.
# peilgebieden_path = '../../Data_preprocessed/Waterschappen/Hollandse_Delta/WSHD_Peilgebieden_(Vigerend).shp'
peilgebieden_path_all = (
    "../../Data_preprocessed/Waterschappen/Hollandse_Delta/Nalvering_tweede/Shapefiles/PeilgebiedenPraktijk.shp"
)
peilgebieden_path_geen_AP = (
    "../../Data_preprocessed/Waterschappen/Hollandse_Delta/Nalvering_tweede/Shapefiles/PeilgebiedenGeenAP.shp"
)

# load in both the old ones and the new ones, as the new ones do not have the peilgebiedafwijkingen, but they dont have the streefpeilen. These are stored in the old ones.
peilgebieden_all = gpd.read_file(peilgebieden_path_all)
peilgebieden_geen_AP = gpd.read_file(peilgebieden_path_geen_AP)

peilgebieden = pd.merge(left=peilgebieden_all, right=peilgebieden_geen_AP, on="Dis", suffixes=("_all", ""))

# add the shapefile to the dictionary
HD["peilgebied"] = peilgebieden


# ### Adjust column names


# determine aanvoer en afvoer gemalen
HD["gemaal"]["func_aanvoer"], HD["gemaal"]["func_afvoer"], HD["gemaal"]["func_circulatie"] = (
    False,
    False,
    False,
)  # default is False
HD["gemaal"]["functiegemaal"] = HD["gemaal"]["FUNCTIEGEM"].astype(str)
HD["gemaal"].loc[HD["gemaal"]["functiegemaal"] == "Onbekend", "functiegemaal"] = (
    np.nan
)  # replace onbekend with nan, will be filled up later see one line below
HD["gemaal"]["functiegemaal"].fillna(
    HD["gemaal"]["NAAM"], inplace=True
)  # some additional information may given in this column
HD["gemaal"]["functiegemaal"] = HD["gemaal"]["functiegemaal"].astype(str)  # again, otherise issue True False

HD["gemaal"].loc[HD["gemaal"].functiegemaal.str.contains("f-|fvoer|nderbemaling"), "func_afvoer"] = True
HD["gemaal"].loc[HD["gemaal"].functiegemaal.str.contains("anvoergemaal|pmaling"), "func_aanvoer"] = True
HD["gemaal"].loc[HD["gemaal"].functiegemaal.str.contains("Doorspoel"), "func_circulatie"] = True
HD["gemaal"].loc[
    (HD["gemaal"].func_afvoer is False)
    & (HD["gemaal"].func_aanvoer is False)
    & (HD["gemaal"].func_circulatie is False),
    "func_afvoer",
] = True  # set to afvoergemaal is there the function is unknown


# discard irrelevant data of HD HD, and create a uniform dataset compared to the other waterschappen
# Stuw
HD["stuw"] = HD["stuw"][["CODE", "GLOBALID", "geometry"]]
HD["stuw"] = HD["stuw"].rename(columns={"CODE": "code", "GLOBALID": "globalid"})
HD["stuw"]["nen3610id"] = "dummy_nen3610id_stuw_" + HD["stuw"].index.astype(str)

# Gemaal
HD["gemaal"] = HD["gemaal"][["CODE", "GLOBALID", "func_afvoer", "func_aanvoer", "func_circulatie", "geometry"]]
HD["gemaal"] = HD["gemaal"].rename(columns={"GLOBALID": "globalid", "CODE": "code"})
HD["gemaal"]["nen3610id"] = "dummy_nen3610id_gemaal_" + HD["gemaal"].index.astype(str)

# Hydroobject
HD["hydroobject"] = HD["hydroobject"][["CODE", "GLOBALID", "geometry"]]
HD["hydroobject"] = HD["hydroobject"].rename(columns={"GLOBALID": "globalid", "CODE": "code"})
HD["hydroobject"]["nen3610id"] = "dummy_nen3610id_hydroobject_" + HD["hydroobject"].index.astype(str)

# sluis
HD["sluis"] = HD["sluis"][["CODE", "GLOBALID", "geometry"]]
HD["sluis"] = HD["sluis"].rename(columns={"GLOBALID": "globalid"})
HD["sluis"]["nen3610id"] = "dummy_nen3610id_sluis_" + HD["sluis"].index.astype(str)

# duikersifonhevel
HD["duikersifonhevel"] = HD["duikersifonhevel"][["CODE", "GLOBALID", "geometry"]]
HD["duikersifonhevel"] = HD["duikersifonhevel"].rename(columns={"CODE": "code", "GLOBALID": "globalid"})
HD["duikersifonhevel"]["nen3610id"] = "dummy_nen3610id_duikersifonhevel_" + HD["duikersifonhevel"].index.astype(str)

# afsluitmiddel
HD["afsluitmiddel"] = HD["afsluitmiddel"][["CODE", "geometry"]]
HD["afsluitmiddel"] = HD["afsluitmiddel"].rename(columns={"CODE": "code"})
HD["afsluitmiddel"]["nen3610id"] = "dummy_nen3610id_afsluitmiddel_" + HD["afsluitmiddel"].index.astype(str)
HD["afsluitmiddel"]["globalid"] = "dummy_globalid_afsluitmiddel_" + HD["afsluitmiddel"].index.astype(str)

# Peilgebied
HD["peilgebied"] = HD["peilgebied"][["REKENPEIL_", "NAAM_BEMAL", "geometry"]]
HD["peilgebied"]["nen3610id"] = "dummy_nen3610id_peilgebied_" + HD["peilgebied"].index.astype(str)
HD["peilgebied"]["globalid"] = "dummy_globalid_peilgebied_" + HD["peilgebied"].index.astype(str)
HD["peilgebied"] = HD["peilgebied"].rename(columns={"REKENPEIL_": "streefpeil", "NAAM_BEMAL": "code"})

# Streefpeil
HD["streefpeil"] = pd.DataFrame()
HD["streefpeil"]["waterhoogte"] = HD["peilgebied"]["streefpeil"]
HD["streefpeil"]["globalid"] = HD["peilgebied"]["globalid"]
HD["streefpeil"]["geometry"] = None
HD["streefpeil"] = gpd.GeoDataFrame(HD["streefpeil"], geometry="geometry")

# Remove the streefpeil from the peilgebied
HD["peilgebied"] = HD["peilgebied"][["code", "geometry", "nen3610id", "globalid"]]


# delete irrelvant data
variables = ["peilgebiedpraktijk"]

for variable in variables:
    if str(variable) in HD:
        del HD[variable]


# ### Check for the correct keys and columns


show_layers_and_columns(waterschap=HD)


peilgebieden.plot()


# ### Store data


# Check if the directory exists
if not os.path.exists(output_gpkg_path):
    # If it doesn't exist, create it
    os.makedirs(output_gpkg_path)

store_data(waterschap=HD, output_gpkg_path=output_gpkg_path + "/HD")
