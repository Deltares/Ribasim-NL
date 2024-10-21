# # Delfland

# This script adds a new column "peilgebied_cat" and makes sure the peilgebieden allign with the HWS layer (Daniel):
# - peilgebied_cat = 0 -> peilgebied
# - peigelbied_cat = 1 -> boezem
# - peilgebied_cat = 2 -> HWSNotes:
#


import geopandas as gpd
import numpy as np
from general_functions import *

remove_cat_2 = True


# ## Set Paths


# define relative paths
waterschap = "Delfland"

data_path = f"/DATAFOLDER/projects/4750_20/Data_postprocessed/Waterschappen/{waterschap}/{waterschap}.gpkg"

# Waterschaps boundaries
grens_path = "/DATAFOLDER/projects/4750_30/Data_overig/Waterschapsgrenzen/Waterschapsgrenzen.geojson"
# Hoofdwatersysteem boundaries
hws_path = "/DATAFOLDER/projects/4750_30/Data_overig/HWS/krw_basins_vlakken.gpkg"
# Buffer boundaries
buffer_path = "/DATAFOLDER/projects/4750_30/Data_overig/HWS/hws_buffer_delfland.gpkg"
# Output folder
output_folder = f"/DATAFOLDER/projects/4750_30/Data_postprocessed/Waterschappen/{waterschap}/"


# ## Load files


# Load HHNK files
delfland = read_gpkg_layers(
    gpkg_path=data_path,
    variables=[
        "stuw",
        "gemaal",
        "hydroobject",
        "duikersifonhevel",
        "peilgebied",
        "streefpeil",
        "aggregation_area",
    ],
)

delfland["peilgebied"] = delfland["peilgebied"].to_crs("EPSG:28992")

# Load waterschap boundaries
gdf_grens = gpd.read_file(grens_path)
gdf_grens = gdf_grens.to_crs("EPSG:28992")
gdf_grens = gdf_grens.set_index("waterschap")

# Load hws
gdf_hws = gpd.read_file(hws_path)

# Load buffer
gdf_buffer = gpd.read_file(buffer_path)


# ## Select waterschap boundaries and clip hws layer


# Select boundaries HH Amstel, Gooi en Vecht
gdf_grens = gdf_grens.loc[["HHS van Delfland"]]

# Use waterschap boudnaries to clip HWS layer
gdf_hws = gpd.overlay(gdf_grens, gdf_hws, how="intersection")


# ## Peilgebied and HWS layer overlap:
# 1. Identify the overlapping areas
# 2. Clip
# 3. Calculate overlapping area percentage
# 4. Filter


# Step 1: Identify the Overlapping Areas and clip
overlaps = gpd.overlay(delfland["peilgebied"], gdf_hws, how="intersection", keep_geom_type=True)

# Step 2: Subtract Overlapping Areas from the original polygons in each DataFrame
non_overlapping_peilgebied = gpd.overlay(delfland["peilgebied"], overlaps, how="difference", keep_geom_type=True)
overlaps = gpd.overlay(non_overlapping_peilgebied, gdf_hws, how="intersection", keep_geom_type=False)

# Step 3: Calculate Area Percentages
# Calculate the area of overlaps
overlaps["overlap_area"] = overlaps.area

# Step 4: Filter based on area Area Percentages
minimum_area = 200
print(f"Number of overlapping shapes without filter: {len(overlaps)}")
overlap_ids = overlaps.loc[overlaps["overlap_area"] > minimum_area]
overlap_ids = overlap_ids.globalid.to_list()
print(f"Number of overlapping shapes with filter: {len(overlap_ids)}")


# ## Create peilgebied_cat column


# Add occurence to geodataframe
peilgebieden_cat = []
ids = []

for index, row in delfland["peilgebied"].iterrows():
    if row.code.startswith("BZM") or row.HWS_BZM:
        print("yes")
        peilgebieden_cat.append(1)

    # Check if the row's globalid is in overlap_ids
    elif row.globalid in overlap_ids:
        peilgebieden_cat.append(2)

    # If none of the above conditions are met, append 0
    else:
        peilgebieden_cat.append(0)

# Add new column and drop old HWS_BZM column
delfland["peilgebied"]["peilgebied_cat"] = peilgebieden_cat
delfland["peilgebied"] = delfland["peilgebied"].drop(columns=["HWS_BZM"])


# ## Add HWS to ['peilgebied', 'streefpeil']


# update peilgebied dict key
gdf_hws["globalid"] = "dummy_globalid__hws_" + gdf_hws.index.astype(str)
gdf_hws["code"] = "dummy_code_hws_" + gdf_hws.index.astype(str)
gdf_hws["nen3610id"] = "dummy_nen3610id_hws_" + gdf_hws.index.astype(str)
gdf_hws["peilgebied_cat"] = 2

gdf_hws = gdf_hws[["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]

delfland["peilgebied"] = pd.concat([gdf_hws, delfland["peilgebied"]])


# Create boezem streefpeil layer
streefpeil_hws = pd.DataFrame()
streefpeil_hws["waterhoogte"] = [np.nan] * len(gdf_hws)
streefpeil_hws["globalid"] = "dummy_globalid_hws_" + gdf_hws.index.astype(str)
streefpeil_hws["geometry"] = [None] * len(gdf_hws)

delfland["streefpeil"] = pd.concat([streefpeil_hws, delfland["streefpeil"]])
delfland["streefpeil"] = gpd.GeoDataFrame(delfland["streefpeil"])


# ### Create buffer layer that ensures spatial match between peilgebied and hws layers based on the buffer layer


# # Create buffer polygon
# buffer_polygon = gpd.overlay(gdf_buffer, gdf_grens, how='intersection', keep_geom_type=True)
# buffer_polygon = gpd.overlay(buffer_polygon, gdf_hws, how='difference', keep_geom_type=True)
# buffer_polygon = gpd.overlay(buffer_polygon, delfland['peilgebied'], how='difference', keep_geom_type=True)


# ## Add buffer to ['peilgebied','streefpeil']


# # update peilgebied dict key
# buffer_polygon['globalid'] = 'dummy_globalid_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['code'] = 'dummy_code_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['nen3610id'] = 'dummy_nen3610id_buffer_1' + buffer_polygon.index.astype(str)
# buffer_polygon['peilgebied_cat'] = 2

# buffer_polygon = buffer_polygon[['globalid', 'code', 'nen3610id', 'peilgebied_cat', 'geometry']]

# delfland['peilgebied'] = pd.concat([buffer_polygon, delfland['peilgebied']])


# # Create boezem streefpeil layer
# streefpeil_buffer = pd.DataFrame()
# streefpeil_buffer['waterhoogte'] = [np.nan]
# streefpeil_buffer['globalid'] = ['dummy_globalid_buffer_1']
# streefpeil_buffer['geometry'] = [None]


# delfland['streefpeil'] = pd.concat([streefpeil_buffer, delfland['streefpeil']])
# delfland['streefpeil'] = gpd.GeoDataFrame(delfland['streefpeil'])

# # Fix
# delfland['streefpeil']['waterhoogte'] = delfland['streefpeil']['waterhoogte'].replace('N/A', np.nan)
# delfland['streefpeil']['waterhoogte'] = pd.to_numeric(delfland['streefpeil']['waterhoogte'])


delfland["peilgebied"].peilgebied_cat.unique()


delfland["peilgebied"] = delfland["peilgebied"][["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]


if remove_cat_2:
    delfland["peilgebied"] = delfland["peilgebied"].loc[delfland["peilgebied"].peilgebied_cat != 2]


# ## Write output


for key in delfland.keys():
    print(key)
    delfland[str(key)].to_file(f"{output_folder}/{waterschap}.gpkg", layer=str(key), driver="GPKG")
