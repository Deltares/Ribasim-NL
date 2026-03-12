# %%
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ribasim_nl.model import Model


def add_streefpeil(model: Model, peilgebieden_path: Path, layername: str, target_level: str, code: str):
    # Check if the layername is provided and the peilgebieden_path is a GPKG file
    if layername is None:
        layername = gpd.list_layers(peilgebieden_path).at[0, "name"]

    peilgebieden = gpd.read_file(peilgebieden_path, layer=layername, fid_as_index=True)

    # Explode Peilgebieden geometries to handle multiple parts of MultiPolygons
    peilgebieden = peilgebieden.explode()
    peilgebieden[target_level] = pd.to_numeric(peilgebieden[target_level], errors="coerce")
    peilgebieden = peilgebieden.dropna(subset=[target_level])
    peilgebieden = peilgebieden[(peilgebieden[target_level] != 0) & (peilgebieden[target_level] < 100)]

    # Ensure required columns exist in the dataframe
    if "meta_code_waterbeheerder" not in model.basin.area.df.columns:
        model.basin.area.df["meta_code_waterbeheerder"] = pd.Series(dtype=str)

    if "meta_streefpeil" in model.basin.area.df.columns:
        model.basin.area.df.drop(columns="meta_streefpeil", inplace=True)
    model.basin.area.df["meta_streefpeil"] = pd.Series(dtype=float)

    # Prepare basin geometries
    basins_gdf = gpd.GeoDataFrame(model.basin.area.df, geometry=model.basin.area.df["geometry"], crs=peilgebieden.crs)

    for basin_row in basins_gdf.itertuples():
        node_id = basin_row.node_id
        basin_geometry = basin_row.geometry

        downstream_nodes = model.node.df.loc[model.downstream_node_id(node_id=node_id)]

        buffer_100m_gdf = gpd.GeoDataFrame(
            {"node_id": downstream_nodes.index, "geometry": downstream_nodes.geometry.buffer(100)}, crs=peilgebieden.crs
        )
        buffer_100m_gdf = buffer_100m_gdf.reset_index(drop=True)
        buffer_union = buffer_100m_gdf.dissolve(by="node_id").geometry.union_all()

        overlapping_peilgebieden = peilgebieden[peilgebieden.geometry.intersects(buffer_union)]

        if overlapping_peilgebieden.empty:
            overlapping_peilgebieden = peilgebieden[peilgebieden.geometry.intersects(basin_geometry)]

        current_basin = basins_gdf[basins_gdf["node_id"] == node_id].copy()

        linked = gpd.overlay(overlapping_peilgebieden, current_basin, how="intersection", keep_geom_type=False)
        current_basin.loc[:, "area"] = current_basin.geometry.area
        linked["intersection_area"] = linked.geometry.area
        linked["intersection_area_fraction"] = linked["intersection_area"] / current_basin["area"].iloc[0]

        # Check if any peilgebied covers more than 50%
        linked_majority = linked[linked["intersection_area_fraction"] > 0.5]

        if not linked_majority.empty:
            selected_peil = linked_majority.iloc[0][target_level]
            selected_code = linked_majority.iloc[0][code]
        else:
            # Default behavior: pick the minimum streefpeil from overlapping peilgebieden
            linked_filtered = (
                linked.loc[linked["intersection_area_fraction"] > 0.025]
                .dropna(subset=[target_level])
                .sort_values(by=[target_level])
                .drop_duplicates(keep="first")
            )

            if linked_filtered.empty:
                linked_filtered = (
                    linked.dropna(subset=[target_level]).sort_values(by=[target_level]).drop_duplicates(keep="first")
                )

            if not linked_filtered.empty:
                selected_peil = linked_filtered.iloc[0][target_level]
                selected_code = linked_filtered.iloc[0][code]
            else:
                selected_peil = None
                selected_code = None

        # Apply selected values

        model.basin.area.df.loc[model.basin.area.df.node_id == node_id, ["meta_streefpeil"]] = selected_peil
        model.basin.area.df.loc[model.basin.area.df.node_id == node_id, ["meta_code_waterbeheerder"]] = selected_code
