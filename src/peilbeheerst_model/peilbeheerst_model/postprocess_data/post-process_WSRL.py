# # WSRL

# This script adds a new column "peilgebied_cat" and makes sure the peilgebieden allign with the HWS layer (Daniel):
# - peilgebied_cat = 0 -> peilgebied
# - peigelbied_cat = 1 -> RHWS (boezem)
# - peilgebied_cat = 2 -> NHWS


import geopandas as gpd
import numpy as np

from peilbeheerst_model.general_functions import *

remove_cat_2 = True


# ## WSRL


# define relative paths
waterschap = "WSRL"

data_path = f"/DATAFOLDER/projects/4750_20/Data_postprocessed/Waterschappen/{waterschap}/{waterschap}.gpkg"

# Waterschaps boundaries
grens_path = "/DATAFOLDER/projects/4750_30/Data_overig/Waterschapsgrenzen/Waterschapsgrenzen.geojson"
# Hoofdwatersysteem boundaries
hws_path = "/DATAFOLDER/projects/4750_30/Data_overig/HWS/krw_basins_vlakken.gpkg"
# Buffer boundaries
buffer_path = "/DATAFOLDER/projects/4750_30/Data_overig/HWS/hws_buffer_wsrl.gpkg"
# Output folder
output_folder = f"/DATAFOLDER/projects/4750_30/Data_postprocessed/Waterschappen/{waterschap}"


# ### Load Files


# Load HHNK files
WSRL = read_gpkg_layers(
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
WSRL["peilgebied"] = WSRL["peilgebied"].to_crs("EPSG:28992")

# Load waterschap boundaries
gdf_grens = gpd.read_file(grens_path)
gdf_grens = gdf_grens.to_crs("EPSG:28992")
gdf_grens = gdf_grens.set_index("waterschap")

# Load hws
gdf_hws = gpd.read_file(hws_path)

# Load buffer
gdf_buffer = gpd.read_file(buffer_path)


# check primary key
WSRL["peilgebied"]["globalid"].is_unique


# ## Select waterschap boundaries and clip hws layer


# Select boundaries HH Amstel, Gooi en Vecht
gdf_grens = gdf_grens.loc[["Waterschap Rivierenland"]]

# Use waterschap boudnaries to clip HWS layer
gdf_hws = gpd.overlay(gdf_grens, gdf_hws, how="intersection")


# Step 1: Identify the Overlapping Areas and clip
overlaps = gpd.overlay(WSRL["peilgebied"], gdf_hws, how="intersection", keep_geom_type=True)

# # # Step 2: Subtract Overlapping Areas from the original polygons in each DataFrame
# non_overlapping_peilgebied = gpd.overlay(WSRL['peilgebied'], overlaps, how='difference', keep_geom_type=True)
# overlaps = gpd.overlay(non_overlapping_peilgebied, gdf_hws, how='intersection', keep_geom_type=False)

# Step 3: Calculate Area Percentages
# Calculate the area of overlaps
overlaps["overlap_area"] = overlaps.area

# Step 4: Filter based on area Area Percentages
minimum_area = 20000
print(f"Number of overlapping shapes without filter: {len(overlaps)}")
overlap_ids = overlaps.loc[overlaps["overlap_area"] > minimum_area]
overlap_ids = overlap_ids.globalid.to_list()
print(f"Number of overlapping shapes with filter: {len(overlap_ids)}")


# Add occurence to geodataframe
peilgebieden_cat = []

for index, row in WSRL["peilgebied"].iterrows():
    if row.CODE == "LNG014-P":
        print("yes")
        peilgebieden_cat.append(1)

    else:
        peilgebieden_cat.append(0)

# Add new column and drop old HWS_BZM column
WSRL["peilgebied"]["peilgebied_cat"] = peilgebieden_cat
# WSRL['peilgebied'] = WSRL['peilgebied'].drop(columns=['HWS_BZM'])
WSRL["peilgebied"] = WSRL["peilgebied"].rename(columns={"CODE": "code"})


# list(WSRL['peilgebied'].code.unique())


# add boezems
codes_to_update = [
    "NDB004-P",
    "LNG013-P",
    "LNG012-P",
    "LNG011-P",
    "LNG010-P",
    "LNG009-P",
    "LNG008-P",
    "LNG007-P",
    "LNG006-P",
    "LNG005-P",
    "LNG304-P",
    "LNG002-P",
    "LNG001-P",
    "LNG014-P_extra",
    "NDW100-P",
    "OVW200-P",
]
WSRL["peilgebied"].loc[WSRL["peilgebied"]["code"].isin(codes_to_update), "peilgebied_cat"] = 1


# ## Adjust globalid, code, nen3610id ['streefpeil'], ['peilgebied']


codes = []
globalids = []
nen3610ids = []

for index, row in WSRL["peilgebied"].iterrows():
    codes.append(f"dummy_code_peilgebied_{row.globalid}")
    globalids.append(f"dummy_globalid_peilgebied_{row.globalid}")
    nen3610ids.append(f"dummy_nen3610id_peilgebied_{row.globalid}")

WSRL["peilgebied"]["code"] = codes
WSRL["peilgebied"]["globalid"] = globalids
WSRL["peilgebied"]["nen3610id"] = nen3610ids

WSRL["streefpeil"]["globalid"] = globalids


WSRL["peilgebied"]["globalid"].is_unique


# ## Add nhws to ['peilgebied','streefpeil']


# update peilgebied dict key
gdf_hws["globalid"] = "dummy_globalid_nhws_" + gdf_hws.index.astype(str)
gdf_hws["code"] = "dummy_code_nhws_" + gdf_hws.index.astype(str)
gdf_hws["nen3610id"] = "dummy_nen3610id_nhws_" + gdf_hws.index.astype(str)
gdf_hws["peilgebied_cat"] = 2

gdf_hws = gdf_hws[["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]

WSRL["peilgebied"] = pd.concat([gdf_hws, WSRL["peilgebied"]])


# Create boezem streefpeil layer
streefpeil_hws = pd.DataFrame()
streefpeil_hws["waterhoogte"] = [np.nan] * len(gdf_hws)
streefpeil_hws["globalid"] = "dummy_globalid_nhws_" + gdf_hws.index.astype(str)
streefpeil_hws["geometry"] = [None] * len(gdf_hws)

WSRL["streefpeil"] = pd.concat([streefpeil_hws, WSRL["streefpeil"]])
WSRL["streefpeil"] = gpd.GeoDataFrame(WSRL["streefpeil"])


WSRL["peilgebied"]["globalid"].is_unique


# ### Create buffer layer that ensures spatial match between peilgebied and hws layers based on the buffer layer


# buffer_polygon = gdf_buffer.geometry.iat[0].intersection(gdf_grens.geometry.iat[0])
# buffer_polygon = buffer_polygon.difference(shapely.geometry.MultiPolygon(gdf_hws.geometry.tolist()))
# buffer_polygon = buffer_polygon.difference(shapely.ops.unary_union(WSRL['peilgebied'].geometry.tolist()))

# buffer_polygon = gpd.GeoDataFrame(buffer_polygon)
# buffer_polygon = buffer_polygon.set_geometry(0)
# buffer_polygon = buffer_polygon.dissolve()
# buffer_polygon = buffer_polygon.rename(columns={0:'geometry'})
# buffer_polygon = buffer_polygon.set_geometry('geometry')
# buffer_polygon = buffer_polygon.set_crs('EPSG:28992')


# ## Add buffer to ['peilgebied']

# ## Add buffer to ['peilgebied','streefpeil']


# # Create boezem streefpeil layer
# streefpeil_buffer = pd.DataFrame()
# streefpeil_buffer['waterhoogte'] = [np.nan]
# streefpeil_buffer['globalid'] = ['dummy_globalid_nhws_buffer_1']
# streefpeil_buffer['geometry'] = [None]

# WSRL['streefpeil'] = pd.concat([streefpeil_buffer, WSRL['streefpeil']])
# WSRL['streefpeil'] = gpd.GeoDataFrame(WSRL['streefpeil'])


if remove_cat_2:
    WSRL["peilgebied"] = WSRL["peilgebied"].loc[WSRL["peilgebied"].peilgebied_cat != 2]


WSRL["peilgebied"]["peilgebied_cat"].unique()


# ## Store output


for key in WSRL.keys():
    print(key)
    WSRL[str(key)].to_file(f"{output_folder}/{waterschap}.gpkg", layer=str(key), driver="GPKG")
