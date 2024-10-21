# import packages and functions
import os

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from peilbeheerst_model.general_functions import *

pd.set_option("display.max_columns", None)


# define relative paths
waterschap = "Rijnland"
path_Rijnland = "..\..\Data_preprocessed\Waterschappen\Rijnland\DataRijnland\DataRijnland.gpkg"
output_gpkg_path = "../../Data_postprocessed/Waterschappen/Rijnland"


Rijnland = read_gpkg_layers(gpkg_path=path_Rijnland)


# # Rijnland


fig, ax = plt.subplots()
Rijnland["peilgebiedvigerend"].plot(ax=ax, color="blue")
Rijnland["peilgebiedpraktijk"].plot(ax=ax, color="orange")
Rijnland["peilafwijkinggebied"].plot(ax=ax, color="green")


Rijnland["stuw"] = Rijnland["stuw"][["code", "nen3610id", "globalid", "geometry"]]
Rijnland["gemaal"] = Rijnland["gemaal"][["code", "nen3610id", "globalid", "geometry"]]
Rijnland["afsluitmiddel"] = Rijnland["afsluitmiddel"][["code", "nen3610id", "globalid", "geometry"]]
Rijnland["hydroobject"] = Rijnland["hydroobject"][["code", "nen3610id", "globalid", "geometry"]]
Rijnland["duikersifonhevel"] = Rijnland["duikersifonhevel"][["code", "nen3610id", "globalid", "geometry"]]
Rijnland["peilgebiedpraktijk"] = Rijnland["peilgebiedpraktijk"][["code", "nen3610id", "globalid", "geometry"]]
Rijnland["peilafwijkinggebied"] = Rijnland["peilafwijkinggebied"][["code", "nen3610id", "globalid", "geometry"]]
Rijnland["peilgebiedvigerend"] = Rijnland["peilgebiedvigerend"][["code", "nen3610id", "globalid", "geometry"]]


# fix geometries
Rijnland["peilgebiedvigerend"]["geometry"] = Rijnland["peilgebiedvigerend"].buffer(distance=0)
Rijnland["peilgebiedpraktijk"]["geometry"] = Rijnland["peilgebiedpraktijk"].buffer(distance=0)
Rijnland["peilafwijkinggebied"]["geometry"] = Rijnland["peilafwijkinggebied"].buffer(distance=0)


# peilgebied = burn_in_peilgebieden(base_layer = Rijnland['peilgebiedpraktijk'],
#                                   overlay_layer = Rijnland['peilafwijkinggebied'],
#                                   plot = True)
# Rijnland['peilgebied'] = gpd.GeoDataFrame(peilgebied)

peilgebied = burn_in_peilgebieden(
    base_layer=Rijnland["peilgebiedvigerend"], overlay_layer=Rijnland["peilgebiedpraktijk"], plot=True
)


peilgebied = gpd.GeoDataFrame(peilgebied)
peilgebied = peilgebied[peilgebied.geometry.type.isin(["Polygon", "MultiPolygon"])]  # only select polygons

Rijnland["peilgebied"] = gpd.GeoDataFrame(peilgebied)


gpd.GeoDataFrame(peilgebied).to_file("Rijnland_test_kan_weg.shp")
# peilgebied


# # Explode the multipolygons into separate parts
# exploded_peilgebied = Rijnland['peilgebied'].explode('geometry')

# # Check if each part is a single polygon
# is_simple_polygon = exploded_peilgebied['geometry'].apply(lambda geom: geom.type == 'Polygon')

# # Select only the simple polygons from the exploded GeoDataFrame
# simple_peilgebied = exploded_peilgebied[is_simple_polygon]
# simple_peilgebied


# #convert multi polygon to single polygon
# Rijnland['peilgebied'] = Rijnland['peilgebied'].explode()
# Rijnland['peilgebied']['nen3610id'] = 'dummy_nen3610id_duikersifonhevel_' + Rijnland['peilgebied'].index.astype(str)


Rijnland["streefpeil"].peilgebiedpraktijkid.fillna(value=Rijnland["streefpeil"]["peilgebiedvigerendid"], inplace=True)
# Rijnland['streefpeil'].drop_duplicates(subset=['peilgebiedpraktijkid'], inplace=True)


# get rid of irrelevant streefpeilen, which otherwise results in too many overlapped peilgebieden
filter_condition = Rijnland["streefpeil"]["soortstreefpeil"].isin(
    ["omer", "ondergrens"]
)  #'omer' for all rows where something of zomer, Zomer, dynamische zomer, etc, is used
kept_rows = Rijnland["streefpeil"][filter_condition]

other_rows = Rijnland["streefpeil"][~filter_condition].drop_duplicates(subset=["peilgebiedpraktijkid"])
Rijnland["streefpeil"] = pd.concat([kept_rows, other_rows])


# Rijnland['streefpeil'].peilafwijkinggebiedid.fillna(value=Rijnland['streefpeil']['peilgebiedpraktijkid'], inplace=True)
# Rijnland['streefpeil'].peilgebiedpraktijkid.fillna(value=Rijnland['streefpeil']['peilgebiedvigerendid'], inplace=True)

pg_sp = pd.merge(
    left=peilgebied,
    right=Rijnland["streefpeil"],
    left_on="globalid",
    right_on="peilgebiedpraktijkid",
    suffixes=("", "_streefpeil"),
)

pg_sp["geometry"] = gpd.GeoDataFrame(geometry=pg_sp["geometry"]).reset_index(drop=True)
# pg_sp = pg_sp.explode('geometry',ignore_index=True)


# gpd.GeoDataFrame(pg_sp.loc[pg_sp.code != 'PBS_WW-25AS'], geometry='geometry').set_crs('EPSG:28992').to_file('peilgebieden_Rijnland.gpkg', driver='GPKG')


# there are duplicate codes, nen3610ids and globalids due to the exploded function. Rename these.
pg_sp["nen3610id"] = "dummy_nen3610id_peilgebied_" + pg_sp.index.astype(str)
pg_sp["code"] = "dummy_code_peilgebied_" + pg_sp.index.astype(str)
pg_sp["globalid"] = "dummy_globalid_peilgebied_" + pg_sp.index.astype(str)

Rijnland["peilgebied"] = pg_sp
Rijnland["peilgebied"] = Rijnland["peilgebied"][["code", "nen3610id", "globalid", "geometry"]]
Rijnland["peilgebied"] = gpd.GeoDataFrame(Rijnland["peilgebied"], geometry="geometry")
Rijnland["peilgebied"] = Rijnland["peilgebied"].set_crs("EPSG:28992")


streefpeil = pg_sp[["waterhoogte", "globalid"]]

streefpeil["geometry"] = np.nan
streefpeil = gpd.GeoDataFrame(streefpeil)
Rijnland["streefpeil"] = streefpeil


# delete irrelvant data
variables = ["peilgebiedpraktijk", "peilgebiedvigerend", "peilafwijkinggebied"]

for variable in variables:
    if str(variable) in Rijnland:
        del Rijnland[variable]


Rijnland["peilgebied"].plot()


# ### Check for the correct keys and columns


show_layers_and_columns(waterschap=Rijnland)


# ### Store data


# Check if the directory exists
if not os.path.exists(output_gpkg_path):
    # If it doesn't exist, create it
    os.makedirs(output_gpkg_path)

store_data(waterschap=Rijnland, output_gpkg_path=output_gpkg_path + "/Rijnland")
