import geopandas as gpd


def compute_overlap_df(gaf_path, basin_path):
    # Load layers with polygons for intersection
    gaf = gpd.read_file(gaf_path)
    basin = gpd.read_file(basin_path, layer="Basin / area")

    # Obtain meta_categorie from Node layer
    nodes = gpd.read_file(basin_path, layer="Node", fid_as_index=True)
    nodes.index.rename("node_id", inplace=True)
    nodes = nodes.reset_index()
    basin = basin.merge(nodes[["node_id", "meta_categorie"]], on="node_id", how="left")

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
    df = intersected[["AI_CODE", "node_id", "meta_categorie", "frac"]].copy()
    df.columns = ["GAF-eenheid", "NodeId", "meta_categorie", "fractie"]  # Rename to match ER coupling script

    return df


if __name__ == "__main__":
    import os
    from pathlib import Path

    # Set paths
    model_name = "lhm_coupled_2025_9_0"
    model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "modellen" / model_name
    basin_path = model_path / "input/database.gpkg"
    gaf_path = "P:/11210327-lwkm2/01_data/Emissieregistratie/gaf_90.shp"

    # Compute overlap and create dataframe
    overlap_df = compute_overlap_df(gaf_path, basin_path)
    print(overlap_df)
