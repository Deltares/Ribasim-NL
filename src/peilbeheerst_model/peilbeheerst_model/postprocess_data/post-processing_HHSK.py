# # HHSK

# This script adds a new column "peilgebied_cat" and make sure the peilgebieden allign witgh the HWS layer (Daniel):
# - peilgebied_cat = 0 -> peilgebied
# - peigelbied_cat = 1 -> RHWS (boezem)
# - peilgebied_cat = 2 -> NHWS Notes:
#


import geopandas as gpd
import shapely

from peilbeheerst_model.general_functions import *

remove_cat_2 = True


# ## HHSK


# define relative paths
waterschap = "HHSK"
data_path = f"/DATAFOLDER/projects/4750_20/Data_postprocessed/Waterschappen/{waterschap}/{waterschap}.gpkg"

# Waterschaps boundaries
grens_path = "/DATAFOLDER/projects/4750_30/Data_overig/Waterschapsgrenzen/Waterschapsgrenzen.geojson"
# Hoofdwatersysteem boundaries
hws_path = "/DATAFOLDER/projects/4750_30/Data_overig/HWS/krw_basins_vlakken.gpkg"
# Buffer boundaries
buffer_path = r"/DATAFOLDER/projects/4750_30/Data_overig/HWS/hws_buffer_HHSK.gpkg"
# Output folder
output_folder = f"/DATAFOLDER/projects/4750_30/Data_postprocessed/Waterschappen/{waterschap}"


# ## Load Files


# Load HHNK files
HHSK = read_gpkg_layers(gpkg_path=data_path)
HHSK["peilgebied"] = HHSK["peilgebied"].to_crs("EPSG:28992")

# Load waterschap boundaries
gdf_grens = gpd.read_file(grens_path)
gdf_grens = gdf_grens.to_crs("EPSG:28992")
gdf_grens = gdf_grens.set_index("waterschap")

# Load hws
gdf_hws = gpd.read_file(hws_path)

# Load buffer
gdf_buffer = gpd.read_file(buffer_path)
gdf_buffer = gdf_buffer.to_crs("EPSG:28992")
gdf_buffer = gdf_buffer.dissolve()


print(len(HHSK["duikersifonhevel"].globalid.unique()))
print(len(HHSK["duikersifonhevel"].globalid))


HHSK["peilgebied"].globalid.is_unique


len(HHSK["hydroobject"])


# HHSK['hydroobject'] = HHSK['hydroobject'].explode(ignore_index=False, index_parts=True)
HHSK["hydroobject"]["geometry"] = HHSK["hydroobject"].make_valid()
HHSK["hydroobject"]["geometry"] = HHSK["hydroobject"].geometry.apply(shapely.force_2d)
HHSK["hydroobject"] = HHSK["hydroobject"][~HHSK["hydroobject"].is_empty].copy()


HHSK["hydroobject"] = HHSK["hydroobject"].drop_duplicates(subset="geometry", keep="first")


len(HHSK["hydroobject"])


# ## Select waterschap boundaries and clip hws layer


# Select boundaries HH Amstel, Gooi en Vecht
gdf_grens = gdf_grens.loc[["Schieland en de Krimpenerwaard"]]

# Use waterschap boudnaries to clip HWS layer
gdf_hws = gpd.overlay(gdf_grens, gdf_hws, how="intersection")


# ## Check Peilgebied and HWS layer overlap:
# 1. Identify the overlapping areas
# 2. Clip
# 3. Calculate overlapping area percentage
# 4. Filter


# Step 1: Identify the Overlapping Areas and clip
overlaps = gpd.overlay(HHSK["peilgebied"], gdf_hws, how="intersection", keep_geom_type=True)
gdf_hws = gpd.overlay(gdf_hws, HHSK["peilgebied"], how="difference")

# # Step 2: Subtract Overlapping Areas from the original polygons in each DataFrame
non_overlapping_peilgebied = gpd.overlay(HHSK["peilgebied"], overlaps, how="difference", keep_geom_type=True)
overlaps = gpd.overlay(non_overlapping_peilgebied, gdf_hws, how="intersection", keep_geom_type=False)

# Step 3: Calculate Area Percentages
# Calculate the area of overlaps
overlaps["overlap_area"] = overlaps.area

# Step 4: Filter based on area Area Percentages
minimum_area = 50
print(f"Number of overlapping shapes without filter: {len(overlaps)}")
overlap_ids = overlaps.loc[overlaps["overlap_area"] > minimum_area]
overlap_ids = overlap_ids.globalid.to_list()
print(f"Number of overlapping shapes with filter: {len(overlap_ids)}")

# gdf_hws = gdf_hws_clipped


# ## Create peilgebied_cat column


# list(HHSK['peilgebied'][HHSK['peilgebied'].code.str.contains('boezem')].code.unique())


# # Add to geodataframe
# peilgebieden_cat = []


# # code_list = ["dummy_id_78_dummy_id_78","PPG-48_dummy_id_196_dummy_id_196","PPG-49_dummy_id_85_dummy_id_85","PPG-237_dummy_id_148_dummy_id_148","PPG-1040_dummy_id_125_dummy_id_125"]
# # code_list = ["dummy_code_peilgebied_486","dummy_code_peilgebied_450","dummy_code_peilgebied_906","dummy_code_peilgebied_1060","dummy_code_peilgebied_552","dummy_code_peilgebied_953",
# #              "dummy_code_peilgebied_216","dummy_code_peilgebied_544","dummy_code_peilgebied_5","dummy_code_peilgebied_480","dummy_code_peilgebied_308","dummy_code_peilgebied_677",
# #              "dummy_code_peilgebied_1053"]

# code_list = list(HHSK['peilgebied'][HHSK['peilgebied'].code.str.contains('boezem')].code.unique())

# for index, row in HHSK['peilgebied'].iterrows():
#     # print(row.code)
#     # if row.code in code_list:
#     if 'boezem' in row.code:
#         print('appending_boezem')
#         peilgebieden_cat.append(1)

#     else:
#         peilgebieden_cat.append(0)

# HHSK['peilgebied']['peilgebied_cat'] = peilgebieden_cat


HHSK["peilgebied"]["peilgebied_cat"] = 0


HHSK["peilgebied"].loc[HHSK["peilgebied"].code.str.contains("GPG-399"), "peilgebied_cat"] = 1
HHSK["peilgebied"].loc[HHSK["peilgebied"].code.str.contains("GPG-1005"), "peilgebied_cat"] = 1
HHSK["peilgebied"].loc[HHSK["peilgebied"].code.str.contains("GPG-1360"), "peilgebied_cat"] = 1
HHSK["peilgebied"].loc[HHSK["peilgebied"].code.str.contains("GPG-1012"), "peilgebied_cat"] = 1


# ## Add nhws to ['peilgebied','streefpeil']


# # update peilgebied dict key
# gdf_hws['globalid'] = 'dummy_globalid_nhws_' + gdf_hws.index.astype(str)
# gdf_hws['code'] = 'dummy_code_nhws_' + gdf_hws.index.astype(str)
# gdf_hws['nen3610id'] = 'dummy_nen3610id_nhws_' + gdf_hws.index.astype(str)
# gdf_hws['peilgebied_cat'] = 2

# gdf_hws = gdf_hws[['globalid', 'code', 'nen3610id', 'peilgebied_cat', 'geometry']]

# HHSK['peilgebied'] = pd.concat([gdf_hws, HHSK['peilgebied']])


# # update streefpeil dict key
# streefpeil_hws = pd.DataFrame()
# streefpeil_hws['waterhoogte'] = [np.nan] * len(gdf_hws)
# streefpeil_hws['globalid'] = 'dummy_globalid_nhws_' + gdf_hws.index.astype(str)
# streefpeil_hws['geometry'] = [None]* len(gdf_hws)

# HHSK['streefpeil'] = pd.concat([streefpeil_hws, HHSK['streefpeil']])
# HHSK['streefpeil'] = gpd.GeoDataFrame(HHSK['streefpeil'])


HHSK["peilgebied"]["peilgebied_cat"].unique()


# ### Create buffer polygon between NHWS and peilgebied/RHWS


# buffer_polygon = gdf_buffer.geometry.iat[0].intersection(gdf_grens.geometry.iat[0])
# buffer_polygon = buffer_polygon.difference(shapely.geometry.MultiPolygon(gdf_hws.geometry.tolist()))
# buffer_polygon = buffer_polygon.difference(shapely.ops.unary_union(HHSK['peilgebied'].geometry.tolist()))

# buffer_polygon = gpd.GeoDataFrame(buffer_polygon)
# buffer_polygon = buffer_polygon.set_geometry(0)
# buffer_polygon = buffer_polygon.dissolve()
# buffer_polygon = buffer_polygon.rename(columns={0:'geometry'})
# buffer_polygon = buffer_polygon.set_geometry('geometry')
# buffer_polygon = buffer_polygon.set_crs('EPSG:28992')


# ### Add buffer to ['peilgebied','streefpeil']


# # update peilgebied dict key
# buffer_polygon = gpd.GeoDataFrame(buffer_polygon)
# buffer_polygon['globalid'] = 'dummy_globalid_nhws_buffer_' + '1'
# buffer_polygon['code'] = 'dummy_code_nhws_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['nen3610id'] = 'dummy_nen3610id_nhws_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['peilgebied_cat'] = 2
# buffer_polygon = buffer_polygon.rename(columns={0:'geometry'})
# buffer_polygon = buffer_polygon[['globalid', 'code', 'nen3610id', 'peilgebied_cat', 'geometry']]

# HHSK['peilgebied'] = pd.concat([buffer_polygon, HHSK['peilgebied']])
# HHSK['peilgebied'] = gpd.GeoDataFrame(HHSK['peilgebied'])


# # Create boezem streefpeil layer
# streefpeil_buffer = pd.DataFrame()
# streefpeil_buffer['waterhoogte'] = [np.nan]
# streefpeil_buffer['globalid'] = ['dummy_globalid_nhws_buffer_1']
# streefpeil_buffer['geometry'] = [None]


# HHSK['streefpeil'] = pd.concat([streefpeil_buffer, HHSK['streefpeil']])
# HHSK['streefpeil'] = gpd.GeoDataFrame(HHSK['streefpeil'])


if remove_cat_2:
    HHSK["peilgebied"] = HHSK["peilgebied"].loc[HHSK["peilgebied"].peilgebied_cat != 2]


# ### Store post-processed data


for key in HHSK.keys():
    print(key)
    HHSK[str(key)].to_file(f"{output_folder}/{waterschap}.gpkg", layer=str(key), driver="GPKG")
