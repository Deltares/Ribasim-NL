# %%
import os
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
from ribasim_nl.utils.geometry import cut_basin, drop_z
from ribasim_nl.utils.geoseries import basins_to_points

DATA_DIR = Path(os.getenv("RIBASIM_NL_DATA_DIR"))
MODEL_DIR = Path(os.getenv("RIBASIM_NL_MODEL_DIR")) / "ijsselmeer"


def add_basin(**kwargs):
    global basins
    kwargs["geometry"] = drop_z(kwargs["geometry"])
    basins += [kwargs]


krw_ids = [
    "NL92_IJSSELMEER",
    "NL92_MARKERMEER",
    "NL92_RANDMEREN_ZUID",
    "NL92_RANDMEREN_OOST",
    "NL92_KETELMEER_VOSSEMEER",
    "NL92_ZWARTEMEER",
]

rws_krw_gpkg = DATA_DIR / r"KRW/krw-oppervlaktewaterlichamen-nederland-vlakken.gpkg"
rws_krw_gdf = gpd.read_file(rws_krw_gpkg).set_index("owmident")

basins = []

# rws_krw_gdf.loc[krw_ids].explore()

krw_cutlines_gdf = gpd.read_file(MODEL_DIR / "model_data.gpkg", layer="krw_cutlines")


for row in rws_krw_gdf.loc[krw_ids].itertuples():
    # row = next(rws_krw_gdf.loc[krw_ids].itertuples())
    krw_id = row.Index
    basin_polygon = row.geometry
    if krw_id in krw_cutlines_gdf.owmident.to_numpy():
        if krw_id in krw_cutlines_gdf.owmident.to_numpy():
            if basin_polygon.geom_type == "Polygon":
                for cut_line in (
                    krw_cutlines_gdf[krw_cutlines_gdf.owmident == row.Index]
                    .sort_values("cut_order")
                    .geometry
                ):
                    # cut_line = krw_cutlines_gdf[krw_cutlines_gdf.owmident == row.Index].sort_values("cut_order").geometry[0]
                    basin_multi_polygon = cut_basin(basin_polygon, cut_line)
                    geometry = basin_multi_polygon.geoms[0]
                    add_basin(krw_id=krw_id, geometry=geometry)
                    basin_polygon = basin_multi_polygon.geoms[1]
                add_basin(krw_id=krw_id, geometry=basin_polygon)
            else:
                raise TypeError(
                    f"basin_polygon not of correct type {basin_polygon.geom_type}"
                )
    else:
        add_basin(krw_id=krw_id, geometry=basin_polygon)

gdf = gpd.read_file(DATA_DIR / r"Zuiderzeeland/Oplevering LHM/peilgebieden.gpkg")

# Define your selection criteria
mask = (
    (gdf["GPGIDENT"] == "LVA.01")
    | (gdf["GPGIDENT"] == "3.01")
    | (gdf["GPGIDENT"] == "LAGE AFDELING")
    | (gdf["GPGIDENT"] == "HOGE AFDELING")
)

# Process the selected polygons and add centroids and attributes
for index, row in gdf[mask].iterrows():
    add_basin(krw_id=row.GPGIDENT, geometry=row.geometry)

# Also, save the selected polygons to the new GeoPackage
# "sel_peilgebieden_gdf.to_file(output_geopackage, driver="GPKG")

basins_gdf = gpd.GeoDataFrame(basins, crs=28992)
basins_gdf.to_file(MODEL_DIR / "basins.gpkg", layer="basins_areas")
basins_gdf.loc[:, "geometry"] = basins_to_points(basins_gdf["geometry"])
basins_gdf.to_file(MODEL_DIR / "basins.gpkg", layer="basins")


# %% kunstwerken Zuiderzeeland


# Inlezen waterschapsgrenzen
shapefile = DATA_DIR / r"nederland/Waterschapsgrenzen.shp"
waterschapsgrenzen_gdf = gpd.read_file(shapefile)

# Filter the GeoDataFrame to select the feature with value 'Waterschap Zuiderzeeland'
selected_waterschap_gdf = waterschapsgrenzen_gdf[
    waterschapsgrenzen_gdf["waterschap"] == "Waterschap Zuiderzeeland"
]

output_gpkg = MODEL_DIR / "ZZL_grens.gpkg"
selected_waterschap_gdf.to_file(output_gpkg, driver="GPKG")

# Path to the GeoPackage file
gpkg_path = DATA_DIR / r"uitlaten_inlaten.gpkg"

# List available layers in the GeoPackage
layers = fiona.listlayers(gpkg_path)
print(layers)

# Select the desired layers
desired_layers = ["gemaal", "stuw", "sluis"]

# Read the selected layers into GeoDataFrames
uitlaten_inlaten_gdf = {}
with fiona.open(gpkg_path, "r") as gpkg:
    for layer_name in desired_layers:
        if layer_name in layers:
            gdf = gpd.read_file(gpkg_path, layer=layer_name)
            uitlaten_inlaten_gdf[layer_name] = gdf

# Spatial operations
selected_waterschap_gdf = selected_waterschap_gdf.to_crs(
    uitlaten_inlaten_gdf[desired_layers[0]].crs
)

# Perform the spatial join for the first layer
points_within_waterschap_gdf = gpd.sjoin(
    uitlaten_inlaten_gdf[desired_layers[0]], selected_waterschap_gdf, op="within"
)

# Combine the results from different layers (if needed)
dfs_to_concat = []
for layer_name in desired_layers[1:]:
    result = gpd.sjoin(
        uitlaten_inlaten_gdf[layer_name], selected_waterschap_gdf, op="within"
    )
    dfs_to_concat.append(result)

# Concatenate the DataFrames
points_within_waterschap_gdf = pd.concat(
    [points_within_waterschap_gdf] + dfs_to_concat, ignore_index=True
)

# Drop the 'OBJECTID' column if it exists
if "OBJECTID" in points_within_waterschap_gdf:
    points_within_waterschap_gdf = points_within_waterschap_gdf.drop(
        columns=["OBJECTID"]
    )

# Output to a GeoPackage
output_gpkg = MODEL_DIR / "inlaten_uitlaten_ZZL.gpkg"
points_within_waterschap_gdf.to_file(output_gpkg, driver="GPKG")

# %% waterlopen Zuiderzeeland

# Handmatig bewerkte waterlopen
gpkg_path = MODEL_DIR / "primaire_waterlopen.gpkg"


# %%
