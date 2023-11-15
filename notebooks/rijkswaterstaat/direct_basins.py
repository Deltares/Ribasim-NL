# %%
import os
from pathlib import Path

import geopandas as gpd
from hydamo import HyDAMO
from shapely.geometry import LineString, Point

DATA_DIR = Path(os.getenv("RIBASIM_NL_DATA_DIR"))
MODEL_DIR = Path(os.getenv("RIBASIM_NL_MODEL_DIR")) / "ijsselmeer"
MODEL_DATA_GPKG = Path(MODEL_DIR) / "model_data.gpkg"

zuiderzeeland_hydamo_gpkg = DATA_DIR.joinpath("Zuiderzeeland", "overig", "hydamo.gpkg")

hydamo = HyDAMO.from_geopackage(file_path=zuiderzeeland_hydamo_gpkg, version="2.2.1")

# %% Create file with direction basins
# Load GeoPackage files with explicit geometry column name
polygons = gpd.read_file(
    DATA_DIR / "Rijkswaterstaat/verwerkt/krw_basins_vlakken.gpkg", geom_col="geometry"
)
lines = gpd.read_file(
    DATA_DIR / "nederland/lsm3-hydamo_withinKRW.gpkg", geom_col="geometry"
)

# Add a temporary column to polygons for later identification
polygons["temp_id"] = range(len(polygons))

# Initialize lists to store attributes
polygon_attributes = []
owmident_attributes = []

# Iterate through each line to check for intersection with the outer boundary of polygons
for line_index, line in lines.iterrows():
    line_geometry = line["geometry"]

    # Check for intersection with the outer boundary of polygons
    intersected_polygons = polygons[
        polygons["geometry"].boundary.intersects(line_geometry)
    ]

    # Extract attributes from intersected polygons
    for _, intersected_polygon in intersected_polygons.iterrows():
        polygon_id = intersected_polygon["temp_id"]

        # Check if start and end points are within the polygon
        start_point_inside = intersected_polygon["geometry"].contains(
            Point(line_geometry.xy[0][0], line_geometry.xy[1][0])
        )
        end_point_inside = intersected_polygon["geometry"].contains(
            Point(line_geometry.xy[0][-1], line_geometry.xy[1][-1])
        )

        # Store polygon owmident for both start and end points
        start_polygon_owmident = (
            polygons.loc[polygon_id, "owmident"] if start_point_inside else None
        )
        end_polygon_owmident = (
            polygons.loc[polygon_id, "owmident"] if end_point_inside else None
        )

        # Create a LineString object from the start and end points
        line_geometry = LineString(
            [
                (line_geometry.xy[0][0], line_geometry.xy[1][0]),
                (line_geometry.xy[0][-1], line_geometry.xy[1][-1]),
            ]
        )

        # Store attributes for both start and end points
        owmident_attributes.append(
            {
                "wl_naam": line["naam"],
                "geometry": line_geometry,
                "start_polygon_owmident": start_polygon_owmident,
                "end_polygon_owmident": end_polygon_owmident,
            }
        )

# Convert the lists to GeoDataFrames
owmident_gdf = gpd.GeoDataFrame(
    owmident_attributes, geometry="geometry", crs=polygons.crs
)

# Filter rows where start_polygon_owmident is not equal to end_polygon_owmident
filtered_gdf = owmident_gdf[
    owmident_gdf["start_polygon_owmident"] != owmident_gdf["end_polygon_owmident"]
]


# Define a custom aggregation function to fill missing values
def fill_missing_values(group):
    result = group.copy()
    result["start_polygon_owmident"] = group["start_polygon_owmident"].fillna(
        group["start_polygon_owmident"].shift(-1)
    )
    result["end_polygon_owmident"] = group["end_polygon_owmident"].fillna(
        group["end_polygon_owmident"].shift(-1)
    )
    return result.iloc[0]


# Group by 'wl_naam' and apply the custom aggregation function
result_gdf = (
    filtered_gdf.groupby("wl_naam").apply(fill_missing_values).reset_index(drop=True)
)
combined_gdf = result_gdf.dropna(
    subset=["start_polygon_owmident", "end_polygon_owmident"]
)

# Identify rows to drop based on matching 'start_polygon_owmident' and 'end_polygon_owmident'
rows_to_drop = combined_gdf[
    (
        combined_gdf.duplicated(
            subset=["start_polygon_owmident", "end_polygon_owmident"], keep="first"
        )
    )
].index

# Drop the identified rows
filtered_gdf_dropped = combined_gdf.drop(rows_to_drop)

# Save the result to a new GeoPackage
output_gpkg_path = DATA_DIR / "output_filtered_dropped.gpkg"
filtered_gdf_dropped.to_file(output_gpkg_path, driver="GPKG")

# %%
