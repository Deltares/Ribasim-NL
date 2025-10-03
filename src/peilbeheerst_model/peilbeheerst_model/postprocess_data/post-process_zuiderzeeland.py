# # Zuiderzeeland

# This script adds a new column "peilgebied_cat" and makes sure the peilgebieden allign with the HWS layer:
# - peilgebied_cat = 0 -> peilgebied
# - peigelbied_cat = 1 -> RHWS (boezem)
# - peilgebied_cat = 2 -> NHWS


import geopandas as gpd
import numpy as np
from peilbeheerst_model.general_functions import *

remove_cat_2 = True


# ## Zuiderzeeland


# define relative paths
waterschap = "Zuiderzeeland"
waterschap2 = "Zuiderzeeland"
data_path = f"/DATAFOLDER/projects/4750_20/Data_postprocessed/Waterschappen/{waterschap}/{waterschap}.gpkg"

# Waterschaps boundaries
grens_path = "/DATAFOLDER/projects/4750_30/Data_overig/Waterschapsgrenzen/Waterschapsgrenzen.geojson"
# Hoofdwatersysteem boundaries
hws_path = "/DATAFOLDER/projects/4750_30/Data_overig/HWS/krw_basins_vlakken.gpkg"
# Buffer boundaries
buffer_path = "/DATAFOLDER/projects/4750_30/Data_overig/HWS/hws_buffer_zuiderzeeland.gpkg"
# Output folder
output_folder = f"/DATAFOLDER/projects/4750_30/Data_postprocessed/Waterschappen/{waterschap}"


# ### Load Files


# Load HHNK files
Zuiderzeeland = read_gpkg_layers(gpkg_path=data_path)
Zuiderzeeland["peilgebied"] = Zuiderzeeland["peilgebied"].to_crs("EPSG:28992")

# Load waterschap boundaries
gdf_grens = gpd.read_file(grens_path)
gdf_grens = gdf_grens.to_crs("EPSG:28992")
gdf_grens = gdf_grens.set_index("waterschap")

# Load hws
gdf_hws = gpd.read_file(hws_path)

# Load buffer
gdf_buffer = gpd.read_file(buffer_path)


Zuiderzeeland["peilgebied"].globalid.is_unique


# ## Select waterschap boundaries and clip hws layer


# Select boundaries HH Amstel, Gooi en Vecht
gdf_grens = gdf_grens.loc[["Waterschap Zuiderzeeland"]]

# Use waterschap boudnaries to clip HWS layer
gdf_hws = gpd.overlay(gdf_grens, gdf_hws, how="intersection")


Zuiderzeeland["peilgebied"]


# ## Peilgebied and HWS layer overlap:
# 1. Identify the overlapping areas
# 2. Clip
# 3. Calculate overlapping area percentage
# 4. Filter


# Step 1: Identify the Overlapping Areas and clip
overlaps = gpd.overlay(Zuiderzeeland["peilgebied"], gdf_hws, how="intersection", keep_geom_type=True)

# # Step 2: Subtract Overlapping Areas from the original polygons in each DataFrame
non_overlapping_peilgebied = gpd.overlay(Zuiderzeeland["peilgebied"], overlaps, how="difference", keep_geom_type=True)
overlaps = gpd.overlay(non_overlapping_peilgebied, gdf_hws, how="intersection", keep_geom_type=False)

# Step 3: Calculate Area Percentages
# Calculate the area of overlaps
overlaps["overlap_area"] = overlaps.area

# Step 4: Filter based on area Area Percentages
minimum_area = 100
print(f"Number of overlapping shapes without filter: {len(overlaps)}")
overlap_ids = overlaps.loc[overlaps["overlap_area"] > minimum_area]
overlap_ids = overlap_ids.globalid.to_list()
print(f"Number of overlapping shapes with filter: {len(overlap_ids)}")


Zuiderzeeland["peilgebied"]


# Add occurence to geodataframe
peilgebieden_cat = []

for index, row in Zuiderzeeland["peilgebied"].iterrows():
    if "LVA.01" in row.code:
        peilgebieden_cat.append(1)
        print("yes")
    elif "3.01" in row.code:
        peilgebieden_cat.append(1)
        print("yes")
    elif "LAGE AFDELING" in row.code:
        print("yes")
        peilgebieden_cat.append(1)

    elif "HOGE AFDELING" in row.code:
        print("yes")
        peilgebieden_cat.append(1)

    else:
        peilgebieden_cat.append(0)


# Add new column and drop old HWS_BZM column
Zuiderzeeland["peilgebied"]["peilgebied_cat"] = peilgebieden_cat
# Zuiderzeeland['peilgebied'] = Zuiderzeeland['peilgebied'].drop(columns=['HWS_BZM'])


# ## Add nhws to ['peilgebied']


# update peilgebied dict key
gdf_hws["globalid"] = "dummy_globalid_nhws_" + gdf_hws.index.astype(str)
gdf_hws["code"] = "dummy_code_nhws_" + gdf_hws.index.astype(str)
gdf_hws["nen3610id"] = "dummy_nen3610id_nhws_" + gdf_hws.index.astype(str)
gdf_hws["peilgebied_cat"] = 2

gdf_hws = gdf_hws[["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]

Zuiderzeeland["peilgebied"] = pd.concat([gdf_hws, Zuiderzeeland["peilgebied"]])


# ## Add HWS to ['streefpeil']


# Create boezem streefpeil layer
streefpeil_hws = pd.DataFrame()
streefpeil_hws["waterhoogte"] = [np.nan] * len(gdf_hws)
streefpeil_hws["globalid"] = "dummy_globalid_nhws_" + gdf_hws.index.astype(str)
streefpeil_hws["geometry"] = [None] * len(gdf_hws)

Zuiderzeeland["streefpeil"] = pd.concat([streefpeil_hws, Zuiderzeeland["streefpeil"]])
Zuiderzeeland["streefpeil"] = gpd.GeoDataFrame(Zuiderzeeland["streefpeil"])


# ### Create buffer layer that ensures spatial match between peilgebied and hws layers based on the buffer layer


# buffer_polygon = gdf_buffer.geometry.iat[0].intersection(gdf_grens.geometry.iat[0])
# buffer_polygon = buffer_polygon.difference(shapely.geometry.MultiPolygon(gdf_hws.geometry.tolist()))
# buffer_polygon = buffer_polygon.difference(shapely.ops.union_all()(Zuiderzeeland['peilgebied'].geometry.tolist()))

# buffer_polygon = gpd.GeoDataFrame(buffer_polygon)
# buffer_polygon = buffer_polygon.set_geometry(0)
# buffer_polygon = buffer_polygon.dissolve()
# buffer_polygon = buffer_polygon.rename(columns={0:'geometry'})
# buffer_polygon = buffer_polygon.set_geometry('geometry')
# buffer_polygon = buffer_polygon.set_crs('EPSG:28992')


# ## Add buffer to ['peilgebied','streefpeil']


# # update peilgebied dict key
# buffer_polygon['globalid'] = 'dummy_globalid_nhws_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['code'] = 'dummy_code_nhws_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['nen3610id'] = 'dummy_nen3610id_nhws_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['peilgebied_cat'] = 2

# buffer_polygon = buffer_polygon[['globalid', 'code', 'nen3610id', 'peilgebied_cat', 'geometry']]

# Zuiderzeeland['peilgebied'] = pd.concat([buffer_polygon, Zuiderzeeland['peilgebied']])


# # Create boezem streefpeil layer
# streefpeil_buffer = pd.DataFrame()
# streefpeil_buffer['waterhoogte'] = [np.nan]
# streefpeil_buffer['globalid'] = ['dummy_globalid_nhws_buffer_1']
# streefpeil_buffer['geometry'] = [None]


# Zuiderzeeland['streefpeil'] = pd.concat([streefpeil_buffer, Zuiderzeeland['streefpeil']])
# Zuiderzeeland['streefpeil'] = gpd.GeoDataFrame(Zuiderzeeland['streefpeil'])


if remove_cat_2:
    Zuiderzeeland["peilgebied"] = Zuiderzeeland["peilgebied"].loc[Zuiderzeeland["peilgebied"].peilgebied_cat != 2]


# ## Store output


Zuiderzeeland["peilgebied"].globalid.is_unique


for key in Zuiderzeeland.keys():
    print(key)
    Zuiderzeeland[str(key)].to_file(f"{output_folder}/{waterschap2}.gpkg", layer=str(key), driver="GPKG")
