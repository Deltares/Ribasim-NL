# Amstel Gooi en Vecht

# This script adds a new column "peilgebied_cat" and makes sure the peilgebieden allign with the HWS layer (Daniel):
# - peilgebied_cat = 0 -> peilgebied
# - peigelbied_cat = 1 -> RHWS (boezem)
# - peilgebied_cat = 2 -> NHWS Notes:

# %%
import geopandas as gpd
import numpy as np
import pandas as pd
from general_functions import read_gpkg_layers

from ribasim_nl import CloudStorage

# %%

remove_cat_2 = True

waterschap = "AmstelGooienVecht"
waterschap2 = "AGV"

# %%
cloud = CloudStorage()
cloud.download_verwerkt(waterschap)

# cloud.download_basisgegevens()
# cloud.download_aangeleverd("Rijkswaterstaat")

# %%
verwerkt_dir = cloud.joinpath(waterschap, "verwerkt")
data_path = verwerkt_dir / "preprocessed.gpkg"

# Waterschaps boundaries
grens_path = cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")

# Hoofdwatersysteem boundaries
hws_path = cloud.joinpath("Rijkswaterstaat/verwerkt/krw_basins_vlakken.gpkg")

# Buffer boundaries
buffer_path = cloud.joinpath("Rijkswaterstaat/verwerkt/hws_buffer_agv.gpkg")

# Buffer RWHS
rhws_path = cloud.joinpath("Rijkswaterstaat/verwerkt/agv_rhws_buffer.gpkg")


# %% Load Files
# Load HHNK files
AVG = read_gpkg_layers(
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
AVG["peilgebied"] = AVG["peilgebied"].to_crs("EPSG:28992")

# Load waterschap boundaries
gdf_grens = gpd.read_file(grens_path)

# Load hws
gdf_hws = gpd.read_file(hws_path)

# Load buffer
gdf_buffer = gpd.read_file(buffer_path)
gdf_buffer = gdf_buffer.to_crs("EPSG:28992")
gdf_buffer = gdf_buffer.dissolve()

# Load rhws
# gdf_rhws = gpd.read_file(rhws_path)
# gdf_rhws = gdf_rhws.to_crs('EPSG:28992')
# gdf_rhws = gdf_rhws.dissolve()

# %%
AVG["peilgebied"].globalid.is_unique

# Select waterschap boundaries and clip hws layer

# %%
# Select boundaries HH Amstel, Gooi en Vecht
gdf_grens = gdf_grens.loc[gdf_grens["naam"].str.contains("Amstel, Gooi en Vecht")]
assert len(gdf_grens) == 1

# Use waterschap boundaries to clip HWS layer
gdf_hws = gpd.overlay(gdf_grens, gdf_hws, how="intersection")

# Use waterschap boundaries to clip HWS layer
# gdf_rhws = gpd.overlay(gdf_grens, gdf_rhws, how='intersection')

# Peilgebied and HWS layer overlap:
# 1. Identify the overlapping areas
# 2. Clip
# 3. Calculate overlapping area percentage
# 4. Filter

# %%
# Step 1: Identify the Overlapping Areas and clip
overlaps = gpd.overlay(AVG["peilgebied"], gdf_hws, how="intersection", keep_geom_type=True)

# # Step 2: Subtract Overlapping Areas from the original polygons in each DataFrame
non_overlapping_peilgebied = gpd.overlay(AVG["peilgebied"], overlaps, how="difference", keep_geom_type=True)
overlaps = gpd.overlay(non_overlapping_peilgebied, gdf_hws, how="intersection", keep_geom_type=False)

# Step 3: Calculate Area Percentages
# Calculate the area of overlaps
overlaps["overlap_area"] = overlaps.area

# Step 4: Filter based on area Area Percentages
minimum_area = 500
print(f"Number of overlapping shapes without filter: {len(overlaps)}")
overlap_ids = overlaps.loc[overlaps["overlap_area"] > minimum_area]
overlap_ids = overlap_ids.globalid.to_list()
print(f"Number of overlapping shapes with filter: {len(overlap_ids)}")

# %%
# Add occurence to geodataframe
peilgebieden_cat = []

for index, row in AVG["peilgebied"].iterrows():
    # if row.code == "Oosterpark" or row.code == "Vechtboezem":
    if "Oosterpark" in row.code or "Vechtboezem" in row.code or "Stadsboezem Amsterdam" in row.code:
        print("true")

        peilgebieden_cat.append(1)
    else:
        peilgebieden_cat.append(0)

# Add new column and drop old HWS_BZM column
AVG["peilgebied"]["peilgebied_cat"] = peilgebieden_cat

# %% Add rhws to ['peilgebied','streefpeil']
# update peilgebied dict key
# gdf_rhws['globalid'] = 'dummy_globalid_rhws_' + gdf_rhws.index.astype(str)
# gdf_rhws['code'] = 'dummy_code_nhws_' + gdf_rhws.index.astype(str)
# gdf_rhws['nen3610id'] = 'dummy_nen3610id_rhws_' + gdf_rhws.index.astype(str)
# gdf_rhws['peilgebied_cat'] = 1

# gdf_rhws = gdf_rhws[['globalid', 'code', 'nen3610id', 'peilgebied_cat', 'geometry']]

# AVG['peilgebied'] = pd.concat([gdf_rhws, AVG['peilgebied']])

# %%
# # Create boezem streefpeil layer
# streefpeil_hws = pd.DataFrame()
# streefpeil_hws['waterhoogte'] = [np.nan] * len(gdf_rhws)
# streefpeil_hws['globalid'] = 'dummy_globalid_rhws_' + gdf_rhws.index.astype(str)
# streefpeil_hws['geometry'] = [None]* len(gdf_rhws)

# AVG['streefpeil'] = pd.concat([streefpeil_hws, AVG['streefpeil']])
# AVG['streefpeil'] = gpd.GeoDataFrame(AVG['streefpeil'])

# Add nhws to ['peilgebied','streefpeil']

# %%
# update peilgebied dict key
gdf_hws["globalid"] = "dummy_globalid_nhws_" + gdf_hws.index.astype(str)
gdf_hws["code"] = "dummy_code_nhws_" + gdf_hws.index.astype(str)
gdf_hws["nen3610id"] = "dummy_nen3610id_nhws_" + gdf_hws.index.astype(str)
gdf_hws["peilgebied_cat"] = 2

gdf_hws = gdf_hws[["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]

AVG["peilgebied"] = pd.concat([gdf_hws, AVG["peilgebied"]])

# %%
# Create boezem streefpeil layer
streefpeil_hws = pd.DataFrame()
streefpeil_hws["waterhoogte"] = [np.nan] * len(gdf_hws)
streefpeil_hws["globalid"] = "dummy_globalid_nhws_" + gdf_hws.index.astype(str)
streefpeil_hws["geometry"] = [None] * len(gdf_hws)

AVG["streefpeil"] = pd.concat([streefpeil_hws, AVG["streefpeil"]])
AVG["streefpeil"] = gpd.GeoDataFrame(AVG["streefpeil"])

# %% Create buffer polygon between NHWS and peilgebied/RHWS
# buffer_polygon = gdf_buffer.geometry.iat[0].intersection(gdf_grens.geometry.iat[0])
# buffer_polygon = buffer_polygon.difference(shapely.geometry.MultiPolygon(gdf_hws.geometry.tolist()))
# buffer_polygon = buffer_polygon.difference(shapely.ops.unary_union(AVG['peilgebied'].geometry.tolist()))

# buffer_polygon = gpd.GeoDataFrame(buffer_polygon)
# buffer_polygon = buffer_polygon.set_geometry(0)
# buffer_polygon = buffer_polygon.dissolve()
# buffer_polygon = buffer_polygon.rename(columns={0:'geometry'})
# buffer_polygon = buffer_polygon.set_geometry('geometry')
# buffer_polygon = buffer_polygon.set_crs('EPSG:28992')


# %% Add buffer to ['peilgebied','streefpeil']

# update peilgebied dict key
# buffer_polygon = gpd.GeoDataFrame(buffer_polygon)
# buffer_polygon['globalid'] = 'dummy_globalid_nhws_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['code'] = 'dummy_code_nhws_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['nen3610id'] = 'dummy_nen3610id_nhws_buffer_' + buffer_polygon.index.astype(str)
# buffer_polygon['peilgebied_cat'] = 2
# buffer_polygon = buffer_polygon.rename(columns={0:'geometry'})
# buffer_polygon = buffer_polygon[['globalid', 'code', 'nen3610id', 'peilgebied_cat', 'geometry']]

# AVG['peilgebied'] = pd.concat([buffer_polygon, AVG['peilgebied']])
# AVG['peilgebied'] = gpd.GeoDataFrame(AVG['peilgebied'])

# %%
# # Create boezem streefpeil layer
# streefpeil_buffer = pd.DataFrame()
# streefpeil_buffer['waterhoogte'] = [np.nan]
# streefpeil_buffer['globalid'] = 'dummy_globalid_nhws_buffer_' + buffer_polygon.index.astype(str)
# streefpeil_buffer['geometry'] = [None]


# AVG['streefpeil'] = pd.concat([streefpeil_buffer, AVG['streefpeil']])
# AVG['streefpeil'] = gpd.GeoDataFrame(AVG['streefpeil'])

# %%
if remove_cat_2:
    AVG["peilgebied"] = AVG["peilgebied"].loc[AVG["peilgebied"].peilgebied_cat != 2]

# %% Store output

output_gpkg_path = verwerkt_dir / "postprocessed.gpkg"

for key in AVG.keys():
    print(key)
    AVG[str(key)].to_file(output_gpkg_path, layer=str(key), driver="GPKG")

cloud.upload_verwerkt(output_gpkg_path)
# %%
AVG["peilgebied"]["peilgebied_cat"].unique()
