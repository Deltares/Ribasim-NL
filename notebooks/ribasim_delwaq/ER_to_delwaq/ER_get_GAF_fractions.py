import geopandas as gpd

# 1. Load input layers
gaf_path = "P:/11210327-lwkm2/01_data/Emissieregistratie/gaf_90.shp"
basin_path = "C:/Users/leeuw_je/Projecten/LWKM_Ribasim/lhm_rwzi_delwaq_Dommel/database.gpkg"
basin_layer = "Basin / area"

gaf = gpd.read_file(gaf_path)
basin = gpd.read_file(basin_path, layer=basin_layer)

# 2. Filter out invalid geometries
gaf = gaf[gaf.is_valid]
basin = basin[basin.is_valid]

# 3. Intersection
intersected = gpd.overlay(gaf, basin, how="intersection")

# 4. Calculate overlap area in m²
intersected["overlap_area"] = intersected.geometry.area

# 5. Calculate fraction: overlap_area / SHAPE_AREA
# Make sure SHAPE_AREA exists
if "SHAPE_AREA" not in intersected.columns:
    raise ValueError("Missing 'SHAPE_AREA' field in intersected layer. Ensure it exists in GAF layer.")

intersected["frac"] = intersected["overlap_area"] / intersected["SHAPE_AREA"]

# 6. Keep only relevant fields
output = intersected[["AI_CODE", "node_id", "frac"]]

# 7. Export to CSV
output_path = "C:/Users/leeuw_je/Projecten/LWKM_Ribasim/GIS/GAF_fractions.csv"
output.to_csv(output_path, index=False)

print(f"Intersected {len(output)} features.")
print(f"Output saved to:\n{output_path}")
