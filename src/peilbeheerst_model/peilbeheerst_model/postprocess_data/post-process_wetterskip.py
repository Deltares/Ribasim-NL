# # Wetterskip

# This script adds a new column "peilgebied_cat" and make sure the peilgebieden neatly match the HWS layer (Daniel):
# - peilgebied_cat = 0 -> peilgebied
# - peigelbied_cat = 1 -> boezem
# - peilgebied_cat = 2 -> HWS


import geopandas as gpd
import numpy as np

from peilbeheerst_model.general_functions import *

remove_cat_2 = True


# ## Set Paths


# define relative paths
waterschap = "Wetterskip"
data_path = f"/DATAFOLDER/projects/4750_20/Data_postprocessed/Waterschappen/{waterschap}/{waterschap}.gpkg"

# Waterschaps boundaries
grens_path = "/DATAFOLDER/projects/4750_30/Data_overig/Waterschapsgrenzen/Waterschapsgrenzen.geojson"
# Hoofdwatersysteem boundaries
hws_path = "/DATAFOLDER/projects/4750_30/Data_overig/HWS/krw_basins_vlakken.gpkg"
# Buffer boundaries
buffer_path = "/DATAFOLDER/projects/4750_30/Data_overig/HWS/hws_buffer_wetterskip.gpkg"
# Output folder
output_folder = f"/DATAFOLDER/projects/4750_30/Data_postprocessed/Waterschappen/{waterschap}/"
# Dm netwerk
boezem_path = "/DATAFOLDER/projects/4750_30/Data_overig/DM_netwerk/waterschapsgrenzen_boezem_netwerk.shp"


# ## Load Files


# Load HHNK files
Wetterskip = read_gpkg_layers(gpkg_path=data_path)
Wetterskip["peilgebied"] = Wetterskip["peilgebied"].to_crs("EPSG:28992")

# Load waterschap boundaries
gdf_grens = gpd.read_file(grens_path)
gdf_grens = gdf_grens.to_crs("EPSG:28992")
gdf_grens = gdf_grens.set_index("waterschap")

# Load hws
gdf_hws = gpd.read_file(hws_path)

# Load buffer
gdf_buffer = gpd.read_file(buffer_path)

gdf_rhws = gpd.read_file(boezem_path)


# ## Select waterschap boundaries and clip hws layer


# # Select boundaries HH Amstel, Gooi en Vecht
# gdf_grens = gdf_grens.loc[['Wetterskip Fryslân']]

# # Use waterschap boudnaries to clip HWS layer
# gdf_hws = gpd.overlay(gdf_grens, gdf_hws, how='intersection')


# ## Check Peilgebied and HWS layer overlap:
# 1. Identify the overlapping areas
# 2. Clip
# 3. Calculate overlapping area percentage
# 4. Filter


# # Step 1: Identify the Overlapping Areas and clip
# overlaps = gpd.overlay(Wetterskip['peilgebied'], gdf_hws, how='intersection', keep_geom_type=True)

# # # Step 2: Subtract Overlapping Areas from the original polygons in each DataFrame
# non_overlapping_peilgebied = gpd.overlay(Wetterskip['peilgebied'], overlaps, how='difference', keep_geom_type=True)
# overlaps = gpd.overlay(non_overlapping_peilgebied, gdf_hws, how='intersection', keep_geom_type=False)

# # Step 3: Calculate Area Percentages
# # Calculate the area of overlaps
# overlaps['overlap_area'] = overlaps.area

# # Step 4: Filter based on area Area Percentages
# minimum_area = 500
# print(f'Number of overlapping shapes without filter: {len(overlaps)}')
# overlap_ids = overlaps.loc[overlaps['overlap_area'] > minimum_area]
# overlap_ids = overlap_ids.globalid.to_list()
# print(f'Number of overlapping shapes with filter: {len(overlap_ids)}')


# ## Create peilgebied_cat column


# Add to geodataframe
peilgebieden_cat = []

for index, row in Wetterskip["streefpeil"].iterrows():
    if round(row.waterhoogte, 2) == -0.52:
        peilgebieden_cat.append(1)
    else:
        peilgebieden_cat.append(0)

Wetterskip["peilgebied"]["peilgebied_cat"] = peilgebieden_cat


# ## Add nhws to ['peilgebied','streefpeil']


# update peilgebied dict key
gdf_hws["globalid"] = "dummy_globalid_nhws_" + gdf_hws.index.astype(str)
gdf_hws["code"] = "dummy_code_nhws_" + gdf_hws.index.astype(str)
gdf_hws["nen3610id"] = "dummy_nen3610id_nhws_" + gdf_hws.index.astype(str)
gdf_hws["peilgebied_cat"] = 2

gdf_hws = gdf_hws[["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]

Wetterskip["peilgebied"] = pd.concat([gdf_hws, Wetterskip["peilgebied"]])


# update streefpeil dict key
streefpeil_hws = pd.DataFrame()
streefpeil_hws["waterhoogte"] = [np.nan] * len(gdf_hws)
streefpeil_hws["globalid"] = "dummy_globalid_nhws_" + gdf_hws.index.astype(str)
streefpeil_hws["geometry"] = [None] * len(gdf_hws)

Wetterskip["streefpeil"] = pd.concat([streefpeil_hws, Wetterskip["streefpeil"]])
Wetterskip["streefpeil"] = gpd.GeoDataFrame(Wetterskip["streefpeil"])


# ### Create buffer polygon between NHWS and peilgebied/RHWS


# buffer_polygon = gdf_buffer.geometry.iat[0].intersection(gdf_grens.geometry.iat[0])
# buffer_polygon = buffer_polygon.difference(shapely.geometry.MultiPolygon(gdf_hws.geometry.tolist()))
# buffer_polygon = buffer_polygon.difference(shapely.ops.unary_union(Wetterskip['peilgebied'].geometry.tolist()))

# buffer_polygon = gpd.GeoDataFrame(buffer_polygon)
# buffer_polygon = buffer_polygon.set_geometry(0)
# buffer_polygon = buffer_polygon.dissolve()
# buffer_polygon = buffer_polygon.rename(columns={0:'geometry'})
# buffer_polygon = buffer_polygon.set_geometry('geometry')
# buffer_polygon = buffer_polygon.set_crs('EPSG:28992')


# ### Add buffer to ['peilgebied','streefpeil']


# # update peilgebied dict key
# buffer_polygon = gpd.GeoDataFrame(buffer_polygon)
# buffer_polygon['globalid'] = 'dummy_globalid_nhws_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['code'] = 'dummy_code_nhws_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['nen3610id'] = 'dummy_nen3610id_nhws_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['peilgebied_cat'] = 2
# buffer_polygon = buffer_polygon.rename(columns={0:'geometry'})
# buffer_polygon = buffer_polygon[['globalid', 'code', 'nen3610id', 'peilgebied_cat', 'geometry']]

# Wetterskip['peilgebied'] = pd.concat([buffer_polygon, Wetterskip['peilgebied']])
# Wetterskip['peilgebied'] = gpd.GeoDataFrame(Wetterskip['peilgebied'])


# # Create boezem streefpeil layer
# streefpeil_buffer = pd.DataFrame()
# streefpeil_buffer['waterhoogte'] = [np.nan]
# streefpeil_buffer['globalid'] = 'dummy_globalid_nhws_buffer_' + buffer_polygon.index.astype(str)
# streefpeil_buffer['geometry'] = [None]


# Wetterskip['streefpeil'] = pd.concat([streefpeil_buffer, Wetterskip['streefpeil']])
# Wetterskip['streefpeil'] = gpd.GeoDataFrame(Wetterskip['streefpeil'])


# ## Wetterskip data contains many duplicate peilgebieden

# ### Calculate polygons that overlap with more than 90 % of their area


# from itertools import combinations
# gdf = Wetterskip['peilgebied']
# # gdf = test
# gdf = gdf[6:]

# # Initialize a list to store index pairs with more than 90% overlap
# overlapping_pairs = []

# # Iterate through each unique pair of geometries
# for idx1, idx2 in combinations(gdf.index, 2):
#     print(f'Processing {idx1} out of {len(gdf)}...', end='\r')
#     geom1 = gdf.at[idx1, 'geometry']
#     geom2 = gdf.at[idx2, 'geometry']

#     # Calculate intersection
#     intersection = geom1.intersection(geom2)
#     intersection_area = intersection.area

#     # Calculate original areas
#     area1 = geom1.area
#     area2 = geom2.area

#     # Calculate intersection percentage for each geometry
#     intersection_percentage1 = (intersection_area / area1) * 100
#     intersection_percentage2 = (intersection_area / area2) * 100

#     # Check if both geometries overlap more than 90%
#     if intersection_percentage1 > 90 and intersection_percentage2 > 90:
#         # print((idx1, idx2))
#         overlapping_pairs.append((idx1, idx2))

# idx1s = []
# idx2s = []

# glob_1s = []
# glob_2s = []


# for idx1, idx2 in overlapping_pairs:

#     idx1s.append(idx1)
#     idx2s.append(idx2)

#     glob_1s.append(gdf.iloc[idx1].globalid)
#     glob_2s.append(gdf.iloc[idx2].globalid)

# df = pd.DataFrame()
# df['idx1'] = idx1s
# df['idx2'] = idx2s
# df['globalid_1'] = glob_1s
# df['globalid_2'] = glob_2s

# df.to_csv('./overlapping_wetterskip.csv')


# df = pd.read_csv('./overlapping_wetterskip.csv')


# ### Create list of duplicates for removal


# numbers_to_remove = []

# # Go loop unique index values
# for number in df['idx1'].unique():
#     if number in numbers_to_remove:
#         continue

#     # Find all combinations
#     associated_idx2 = df[df['idx1'] == number]['idx2'].tolist()
#     # Append combinations
#     numbers_to_remove.extend(associated_idx2)

# # Remove duplicates using set operation
# numbers_to_remove = list(set(numbers_to_remove))


# ### Remove duplicates


# Wetterskip['peilgebied'] = Wetterskip['peilgebied'][~Wetterskip['peilgebied'].index.isin(numbers_to_remove)]
# Wetterskip['streefpeil'] = Wetterskip['streefpeil'][~Wetterskip['streefpeil'].index.isin(numbers_to_remove)]


Wetterskip["streefpeil"]["globalid"].is_unique


if remove_cat_2:
    Wetterskip["peilgebied"] = Wetterskip["peilgebied"].loc[Wetterskip["peilgebied"].peilgebied_cat != 2]


# ## Store data


for key in Wetterskip.keys():
    print(key)
    Wetterskip[str(key)].to_file(f"{output_folder}/{waterschap}.gpkg", layer=str(key), driver="GPKG")
