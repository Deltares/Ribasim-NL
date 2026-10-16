# %%
import logging

import geopandas as gpd
import pandas as pd
import xarray as xr

logger = logging.getLogger(__name__)
if not logging.getLogger().handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )

logger.info("Starting KRW waterbody fraction calculation script")


# %%
def compute_overlap_df(krw_wl_path, basin_path):
    """Compute overlap fractions between KRW waterbody polygons and Ribasim basin areas.

    Parameters
    ----------
    krw_wl_path : str | Path
        Path to the KRW waterbody shapefile.
    basin_path : str | Path
        Path to the Ribasim GeoPackage containing layers 'Basin / area' and 'Node'.

    Returns
    -------
    pandas.DataFrame
        DataFrame with columns ['GAF-eenheid', 'NodeId', 'meta_categorie', 'fractie'].
    """
    # Load layers with polygons for intersection
    krw_wl = gpd.read_file(krw_wl_path)
    # Check which geometry types are present
    print(krw_wl.geom_type.value_counts())

    # Split into polygon and line GeoDataFrames
    krw_wl_polygons = krw_wl[krw_wl.geom_type.isin(["Polygon", "MultiPolygon"])].copy()

    krw_wl_lines = krw_wl[krw_wl.geom_type.isin(["LineString", "MultiLineString"])].copy()

    # reset indices
    krw_wl_polygons.reset_index(drop=True, inplace=True)
    krw_wl_lines.reset_index(drop=True, inplace=True)

    print(f"Number of polygons: {len(krw_wl_polygons)}")
    print(f"Number of lines: {len(krw_wl_lines)}")

    basin = gpd.read_file(basin_path, layer="Basin / area")

    # Obtain meta_categorie from Node layer
    nodes = gpd.read_file(basin_path, layer="Node", fid_as_index=True)
    nodes.index.rename("node_id", inplace=True)
    nodes = nodes.reset_index()
    basin = basin.merge(nodes[["node_id", "meta_categorie", "meta_waterbeheerder"]], on="node_id", how="left")

    # Filter invalid geometries
    krw_wl_polygons = krw_wl_polygons[krw_wl_polygons.is_valid]
    krw_wl_lines = krw_wl_lines[krw_wl_lines.is_valid]
    basin = basin[basin.is_valid]  # currently, 8 basin geometries are invalid:
    # ids 1100155, 1102256, 1200025, 1202826, 3900043, 3900136, 3901357, 3901450
    # select basins with meta_categorie "doorgaand" or "hoofdwater"
    # basin_unique = basin
    basin_unique = basin[
        basin["meta_categorie"].isin(["doorgaand", "hoofdwater"])
        | basin["meta_waterbeheerder"].isin(["Rijkswaterstaat"])
    ]

    # Ensure same CRS
    if krw_wl_polygons.crs != basin.crs:
        krw_wl_polygons = krw_wl_polygons.to_crs(basin.crs)
        krw_wl_lines = krw_wl_lines.to_crs(basin.crs)

    # Check if SHAPE_AREA exists, else compute it
    if "SHAPE_AREA" not in krw_wl_polygons.columns:
        krw_wl_polygons["SHAPE_AREA"] = krw_wl_polygons.geometry.area

    # Check if LENGTH_AREA exists, else compute it
    if "LENGTH_AREA" not in krw_wl_lines.columns:
        krw_wl_lines["LENGTH_AREA"] = krw_wl_lines.geometry.length

    # Check imported gml file
    print(f"KRW waterbody polygons: {len(krw_wl_polygons)}")
    print(f"KRW waterbody line elements: {len(krw_wl_lines)}")
    print(f"Basin areas: {len(basin_unique)}")
    # print(f"Columns in KRW waterbody polygons: {krw_wl_polygons.columns}")
    # print(f"Columns in Basin areas: {basin_unique.columns}")
    # print(f"First few rows of KRW waterbody polygons:\n{krw_wl_polygons.head()}")
    # print(f"First few rows of Basin areas:\n{basin_unique.head()}")

    # Intersection
    intersected_polygons = gpd.overlay(krw_wl_polygons, basin_unique, how="intersection")
    intersected_lines = gpd.overlay(krw_wl_lines, basin_unique, how="intersection")

    print(f"Columns in intersected_polygons: {intersected_polygons.columns}")
    print(f"Columns in intersected_lines: {intersected_lines.columns}")

    # Compute overlap area and fraction
    intersected_polygons["overlap_area"] = intersected_polygons.geometry.area
    intersected_polygons["frac"] = intersected_polygons["overlap_area"] / intersected_polygons["SHAPE_AREA"]
    intersected_lines["overlap_length"] = intersected_lines.geometry.length
    intersected_lines["frac"] = intersected_lines["overlap_length"] / intersected_lines["LENGTH_AREA"]

    # sanity check
    if (intersected_polygons["frac"] > 1.0 + 1e-9).any():
        logger.info("Some intersection fractions are larger than 1 for KRW polygon features.")
        logger.info("Check for overlapping basin polygons or duplicate geometries.")

    if (intersected_lines["frac"] > 1.0 + 1e-9).any():
        logger.info("Some intersection fractions are larger than 1 for KRW line features.")
        logger.info("Check for overlapping basin polygons or duplicate geometries.")

    # Retain desired fields
    df_polygons = intersected_polygons[
        ["localId", "text", "CharacterString", "overlap_area", "node_id", "meta_categorie", "frac"]
    ].copy()
    df_polygons.columns = [
        "WL_Id",
        "WL_Name",
        "Waterschap",
        "overlap_area",
        "basin_id",
        "basin_type",
        "fractie",
    ]  # Rename
    df_polygons["type"] = "polygon"
    df_lines = intersected_lines[
        ["localId", "text", "CharacterString", "overlap_length", "node_id", "meta_categorie", "frac"]
    ].copy()
    df_lines.columns = [
        "WL_Id",
        "WL_Name",
        "Waterschap",
        "overlap_length",
        "basin_id",
        "basin_type",
        "fractie",
    ]  # Rename
    df_lines["type"] = "line"
    df = pd.concat([df_polygons, df_lines], ignore_index=True)
    # move "overlap_length" column to middle of dataframe, after "overlap_area" column, for better readability
    df = df.reindex(
        columns=[
            "WL_Id",
            "WL_Name",
            "Waterschap",
            "overlap_area",
            "overlap_length",
            "basin_id",
            "basin_type",
            "fractie",
        ]
    )

    # group by WL_Id, sum the basin fractions
    fraction_sum = df.groupby(["WL_Id"], as_index=False).agg(
        {
            "fractie": "sum",
        }
    )
    fraction_sum.to_csv("krw_wl_fractionsum.csv", index=False)
    logger.info(f"KRW waterbodies: {len(fraction_sum)}")

    # FOR DEBUGGING: Filter krw_wl for which calculated fractions do not sum to 1 within a specified tolerance
    tolerance = 1e-2  # select a tolerance value for filtering; remove range between 0.99 and 1.01
    filtered_fraction_sum_tolerance_option1 = fraction_sum[(fraction_sum["fractie"] - 1.0).abs() > tolerance]
    filtered_fraction_sum_tolerance_option1.to_csv("krw_wl_filtered_fractionsum_tolerance_option1.csv", index=False)
    tolerance = 5e-2  # select a tolerance value for filtering; remove range between 0.95 and 1.05
    filtered_fraction_sum_tolerance_option2 = fraction_sum[(fraction_sum["fractie"] - 1.0).abs() > tolerance]
    filtered_fraction_sum_tolerance_option2.to_csv("krw_wl_filtered_fractionsum_tolerance_option2.csv", index=False)

    # selecteer WLs die niet voldoen aan tolerance
    lower_tolerance = (
        25e-2  # <75%; select a tolerance value for filtering; remove WLs that do not meet min. 75% coverage
    )
    upper_tolerance = 1e-2  # > 101%
    WLs_outside_tolerance = fraction_sum[
        ((1.0 - fraction_sum["fractie"]) > lower_tolerance) | (fraction_sum["fractie"] > (1.0 + upper_tolerance))
    ]
    print(WLs_outside_tolerance)

    # Normalize fractions so they sum to 1
    df_copy = df.copy()
    df_normalized = df_copy[~df_copy["WL_Id"].isin(WLs_outside_tolerance["WL_Id"])]

    df_normalized["fractie_normalized"] = df_normalized["fractie"] / df_normalized.groupby("WL_Id")[
        "fractie"
    ].transform("sum")

    print(f"dimension of resulting dataframe: {len(df_normalized)}")

    return df_normalized


# %%
def add_volume(df, model_path, output_csv="krw_wl_fractions_normalized_with_volume.csv"):
    """Add volume information (time averaged) to the dataframe based on model output for each basin.

    Parameters
    ----------
    df : pandas.DataFrame
        DataFrame containing the overlap fractions.
    basin_path : str | Path
        Path to the Ribasim GeoPackage containing layers 'Basin / area' and 'Node'.
    output_csv : str, optional
        Path to save the resulting DataFrame with volume information, by default "krw_wl_fractions_normalized_with_volume.csv".

    Returns
    -------
    pandas.DataFrame
        DataFrame with added volume information.
    """
    # Load basin volumes
    lhm_basins = xr.open_dataset(model_path / "results" / "basin.nc")
    lhm_basins = lhm_basins.to_dataframe().reset_index()
    lhm_basins_timeaveraged = lhm_basins.groupby("node_id").mean().reset_index()

    # Merge to get volume for each basin_id
    df_with_volume = df.merge(
        lhm_basins_timeaveraged[["node_id", "storage"]], left_on="basin_id", right_on="node_id", how="left"
    )
    df_with_volume.rename(columns={"storage": "basin_volume"}, inplace=True)

    # Save to CSV
    df_with_volume.to_csv(output_csv, index=False)
    logger.info(f"Saved dataframe with volume information to {output_csv}")

    return df_with_volume


# %%

if __name__ == "__main__":
    import os
    from pathlib import Path

    # Set paths
    model_name = "lhm_coupled_full"
    model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "Rijkswaterstaat" / "modellen" / model_name
    basin_path = model_path / "input/database.gpkg"
    krw_wl_path = "p:\\11212767-lwkm2\\RibasimNL\\Validatie\\Waterkwaliteit\\INSPIRESurfaceWaterBody.gml"

    # Compute overlap and create dataframe
    overlap_df = compute_overlap_df(krw_wl_path, basin_path)
    print(overlap_df)
    overlap_df.to_csv("krw_wl_basin_fractions_normalized.csv", index=False)
    add_volume(overlap_df, model_path, output_csv="krw_wl_basin_fractions_normalized_with_volume.csv")

# %%
