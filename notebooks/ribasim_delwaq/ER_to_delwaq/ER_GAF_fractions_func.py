import geopandas as gpd


def compute_overlap_df(gaf_path, basin_path):
    # Load layers
    gaf = gpd.read_file(gaf_path)
    basin = gpd.read_file(basin_path, layer="Basin / area")

    # Filter invalid geometries
    gaf = gaf[gaf.is_valid]
    basin = basin[basin.is_valid]

    # Ensure same CRS
    if gaf.crs != basin.crs:
        gaf = gaf.to_crs(basin.crs)

    # Check if SHAPE_AREA exists, else compute it
    if "SHAPE_AREA" not in gaf.columns:
        gaf["SHAPE_AREA"] = gaf.geometry.area

    # Intersection
    intersected = gpd.overlay(gaf, basin, how="intersection")

    # Compute overlap area and fraction
    intersected["overlap_area"] = intersected.geometry.area
    intersected["frac"] = intersected["overlap_area"] / intersected["SHAPE_AREA"]

    # Retain desired fields
    df = intersected[["AI_CODE", "node_id", "frac"]].copy()
    df.columns = ["GAF-eenheid", "NodeId", "fractie"]  # Rename to match ER coupling script

    return df
