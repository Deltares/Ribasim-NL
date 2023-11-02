# %%
import os
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd

DATA_DIR = Path(os.getenv("RIBASIM_NL_DATA_DIR"))
MODEL_DIR = Path(os.getenv("RIBASIM_NL_MODEL_DIR")) / "ijsselmeer"

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
