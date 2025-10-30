# import packages and functions
import os

import geopandas as gpd
import numpy as np
import pandas as pd
from peilbeheerst_model.general_functions import *

pd.set_option("display.max_columns", None)


# define relative paths
waterschap = "Scheldestromen"
path_Scheldestromen = "..\..\Data_preprocessed\Waterschappen\Scheldestromen\Scheldestromen.gpkg"
output_gpkg_path = "../../Data_postprocessed/Waterschappen/Scheldestromen"


Scheldestromen = read_gpkg_layers(
    gpkg_path=path_Scheldestromen,
    engine="pyogrio",
)

# the peilgebieden and streefpeilen do not contain overlapping values. Scheldestromen has delivered additional files as shapes
Scheldestromen["peilgebied"] = gpd.read_file(
    "..\..\Data_preprocessed\Waterschappen\Scheldestromen\Praktijkpeilgebieden_20231204.shp"
)


Scheldestromen["peilgebied"]["code"] = Scheldestromen["peilgebied"]["GPGIDENT"]
Scheldestromen["peilgebied"]["globalid"] = Scheldestromen["peilgebied"]["GLOBALID"]
Scheldestromen["peilgebied"]["waterhoogte"] = Scheldestromen["peilgebied"]["GPGZP"]
Scheldestromen["peilgebied"]["nen3610id"] = "dummy_nen3610id_peilgebied_" + Scheldestromen["peilgebied"].index.astype(
    str
)

Scheldestromen["streefpeil"] = gpd.GeoDataFrame()
Scheldestromen["streefpeil"]["waterhoogte"] = Scheldestromen["peilgebied"]["waterhoogte"]
Scheldestromen["streefpeil"]["globalid"] = Scheldestromen["peilgebied"]["globalid"]
Scheldestromen["streefpeil"]["geometry"] = np.nan


Scheldestromen["peilgebied"] = Scheldestromen["peilgebied"][["code", "nen3610id", "globalid", "geometry"]]


# convert multiz points to points
Scheldestromen["stuw"].geometry = Scheldestromen["stuw"].centroid
Scheldestromen["gemaal"].geometry = Scheldestromen["gemaal"].centroid


# # Scheldestromen


# stuw
Scheldestromen["stuw"] = Scheldestromen["stuw"][["code", "nen3610id", "globalid", "geometry"]]

# gemaal
Scheldestromen["gemaal"] = Scheldestromen["gemaal"][["code", "nen3610id", "globalid", "geometry"]]
Scheldestromen["gemaal"]["code"] = "dummy_code_gemaal_" + Scheldestromen["gemaal"].index.astype(str)

# hydroobject
Scheldestromen["hydroobject"]["code"] = Scheldestromen["hydroobject"]["naam"]
Scheldestromen["hydroobject"] = Scheldestromen["hydroobject"][["code", "nen3610id", "globalid", "geometry"]]

# duikersifonhevel
Scheldestromen["duikersifonhevel"] = Scheldestromen["duikersifonhevel"][["code", "nen3610id", "globalid", "geometry"]]


# pd.merge(left = Scheldestromen['peilgebiedpraktijk'],
#          right = Scheldestromen['streefpeil'],
#          left_on = 'globalid',
#          right_on = 'peilgebiedpraktijkid')


Scheldestromen["stuw"] = Scheldestromen["stuw"][["code", "nen3610id", "globalid", "geometry"]]
Scheldestromen["gemaal"] = Scheldestromen["gemaal"][["code", "nen3610id", "globalid", "geometry"]]
Scheldestromen["hydroobject"] = Scheldestromen["hydroobject"][["code", "nen3610id", "globalid", "geometry"]]
Scheldestromen["duikersifonhevel"] = Scheldestromen["duikersifonhevel"][["code", "nen3610id", "globalid", "geometry"]]


# Scheldestromen['peilgebiedpraktijk']['geometry'] = Scheldestromen['peilgebiedpraktijk'].buffer(distance = 0)
# Scheldestromen['peilafwijkinggebied']['geometry'] = Scheldestromen['peilafwijkinggebied'].buffer(distance = 0)


# peilgebied = pd.merge(left = Scheldestromen['streefpeil'],
#                       right = Scheldestromen['peilgebiedpraktijk'],
#                       left_on = 'peilgebiedpraktijkid',
#                       right_on = 'globalid')


# streefpeil = gpd.GeoDataFrame()
# streefpeil['waterhoogte'] = peilgebied['waterhoogte']
# streefpeil['globalid'] = peilgebied['peilgebiedpraktijkid']
# streefpeil['geometry'] = np.nan
# Scheldestromen['streefpeil'] = gpd.GeoDataFrame(streefpeil)


# Scheldestromen['peilgebied'] = gpd.GeoDataFrame()
# Scheldestromen['peilgebied'][['code', 'nen3610id', 'globalid', 'geometry']] = peilgebied[['code', 'nen3610id_y', 'globalid_y', 'geometry_y']]


# delete irrelvant data
variables = ["peilgebiedpraktijk", "peilgebiedvigerend", "peilafwijkinggebied"]

for variable in variables:
    if str(variable) in Scheldestromen:
        del Scheldestromen[variable]


# ### Check for the correct keys and columns


show_layers_and_columns(waterschap=Scheldestromen)


for i in range(len(Scheldestromen["peilgebied"])):
    if Scheldestromen["peilgebied"]["geometry"].at[i].geom_type == "Polygon":
        Scheldestromen["peilgebied"].loc[i, "geometry"].plot()


merged = pd.merge(left=Scheldestromen["peilgebied"], right=Scheldestromen["streefpeil"], on="globalid")

merged[merged.waterhoogte.isna()]


# ### Store data


# Check if the directory exists
if not os.path.exists(output_gpkg_path):
    # If it doesn't exist, create it
    os.makedirs(output_gpkg_path)

store_data(waterschap=Scheldestromen, output_gpkg_path=output_gpkg_path + "/Scheldestromen")


Scheldestromen["hydroobject"]
