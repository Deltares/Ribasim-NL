from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

from ribasim_nl import Model


def add_streefpeil(model: Model, peilgebieden_path: Path, layername: str, target_level: str, code: str):
    authority = model.filepath.parents[2].name
    print(authority)
    # synchronize files

    # Check if the layername is provided and the peilgebieden_path is a GPKG file
    if layername is None:
        layername = gpd.list_layers(peilgebieden_path).at[0, "name"]
    peilgebieden = gpd.read_file(peilgebieden_path, layer=layername, fid_as_index=True)

    # Explode Peilgebieden geometries to handle multiple parts of MultiPolygons
    peilgebieden = peilgebieden.explode()
    peilgebieden[target_level] = pd.to_numeric(peilgebieden[target_level], errors="coerce")
    peilgebieden = peilgebieden.dropna(subset=[target_level])

    # Ensure 'meta_code_waterbeheerder' exists in the dataframe as string
    if "meta_code_waterbeheerder" not in model.basin.area.df.columns:
        model.basin.area.df["meta_code_waterbeheerder"] = np.nan
    model.basin.area.df["meta_code_waterbeheerder"] = model.basin.area.df["meta_code_waterbeheerder"].astype(str)

    # Ensure 'meta_streefpeil' exists in the dataframe as float
    if "meta_streefpeil" not in model.basin.area.df.columns:
        model.basin.area.df["meta_streefpeil"] = np.nan
    model.basin.area.df["meta_streefpeil"] = model.basin.area.df["meta_streefpeil"].astype(float)

    # Prepare basin geometries
    basins_gdf = gpd.GeoDataFrame(model.basin.area.df, geometry=model.basin.area.df["geometry"], crs=peilgebieden.crs)

    # Iterate over basins to calculate meta_streefpeil
    for basin_row in basins_gdf.itertuples():
        node_id = basin_row.node_id  # Use node_id as the unique identifier for the basin
        basin_geometry = basin_row.geometry
        downstream_nodes = model.node_table().df.loc[model.downstream_node_id(node_id=node_id)]

        # Create buffer of 100m around structures
        buffer_100m_gdf = gpd.GeoDataFrame(
            {"node_id": downstream_nodes.index, "geometry": downstream_nodes.geometry.buffer(100)}, crs=peilgebieden.crs
        )
        buffer_100m_gdf = buffer_100m_gdf.reset_index(drop=True)
        buffer_union = buffer_100m_gdf.dissolve(by="node_id").geometry.union_all()

        # Filter peilgebieden that intersect with buffered structures
        overlapping_peilgebieden = peilgebieden[peilgebieden.geometry.intersects(buffer_union)]

        # Check if overlapping_peilgebieden are empty
        if overlapping_peilgebieden.empty:
            overlapping_peilgebieden = peilgebieden[peilgebieden.geometry.intersects(basin_geometry)]

        current_basin = basins_gdf[basins_gdf["node_id"] == node_id].copy()

        # Perform the overlay with the basin to get linked areas and calculate the intersection_area
        linked = gpd.overlay(overlapping_peilgebieden, current_basin, how="intersection", keep_geom_type=False)
        current_basin.loc[:, "area"] = current_basin.geometry.area
        # Now calculate the intersection area

        linked["intersection_area"] = linked.geometry.area
        linked["intersection_area_fraction"] = linked["intersection_area"] / current_basin["area"].iloc[0]
        # Ensure linked_filtered is initialized and contains valid data
        linked_filtered = None
        # Take the minimum GPGZMRPL from the peilgebied with a structure. The intersection_area should be at least 50m2
        if not linked.empty:
            linked_filtered = (
                linked.loc[linked["intersection_area_fraction"] > 0.025]
                .dropna(subset=[target_level])
                .sort_values(by=[target_level])
                .drop_duplicates(keep="first")
            )
        # Check if linked_filtered is empty or invalid
        if linked_filtered is None or linked_filtered.empty:
            # For now, take the first valid entry from linked
            linked_filtered = (
                linked.dropna(subset=[target_level]).sort_values(by=[target_level]).drop_duplicates(keep="first")
            )

        if not linked_filtered.empty:
            model.basin.area.df.loc[model.basin.area.df.node_id == node_id, ["meta_streefpeil"]] = linked_filtered.iloc[
                0
            ][target_level]
            model.basin.area.df.loc[model.basin.area.df.node_id == node_id, ["meta_code_waterbeheerder"]] = (
                linked_filtered.iloc[0][code]
            )
    return model
