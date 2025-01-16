# %%
# This script is to define the target level of the basin.
# It is taken from the lowest Gemiddeld zomerpeil (GPGZMRPL), located at the structure
import geopandas as gpd
import numpy as np
import pandas as pd

from ribasim_nl import CloudStorage, Model


# Function to run the model processing for each set of parameters
def process_basin_model(params):
    authority, short_name, peilgebieden_path, layername, target_level, code, model_network = params
    cloud = CloudStorage()
    print(authority)

    # Check if the layername is provided and the peilgebieden_path is a GPKG file
    if layername and peilgebieden_path.endswith(".gpkg"):
        peilgebieden = gpd.read_file(cloud.joinpath(authority, peilgebieden_path), layer=layername, fid_as_index=True)
    else:
        peilgebieden = gpd.read_file(cloud.joinpath(authority, peilgebieden_path), fid_as_index=True)

    # Explode Peilgebieden geometries to handle multiple parts of MultiPolygons
    peilgebieden = peilgebieden.explode()
    peilgebieden[target_level] = pd.to_numeric(peilgebieden[target_level], errors="coerce")
    peilgebieden = peilgebieden.dropna(subset=[target_level])

    # Dynamically set the path based on the model network
    ribasim_toml = cloud.joinpath(authority, "modellen", model_network, f"{short_name}.toml")
    model = Model.read(ribasim_toml)

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

        current_basin = basins_gdf[basins_gdf["node_id"] == node_id]

        # Perform the overlay with the basin to get linked areas and calculate the intersection_area
        linked = gpd.overlay(overlapping_peilgebieden, current_basin, how="intersection", keep_geom_type=False)
        linked["intersection_area"] = linked.geometry.area

        # Ensure linked_filtered is initialized and contains valid data
        linked_filtered = None

        # Take the minimum GPGZMRPL from the peilgebied with a structure. The intersection_area should be at least 50m2
        if not linked.empty:
            linked_filtered = (
                linked[linked["intersection_area"] > 50]
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

    # Write updated model
    model.use_validation = True
    model.write(ribasim_toml)


# List of different parameters for the models, including peilgebieden path for each authority
params_list = [
    (
        "HunzeenAas",
        "hea",
        "verwerkt/1_ontvangen_data/peilgebieden.gpkg",
        None,
        "gpgzmrpl",
        "gpgident",
        "HunzeenAas_fix_model_network",
    ),
    (
        "DrentsOverijsselseDelta",
        "dod",
        "verwerkt/1_ontvangen_data/extra data/Peilgebieden/Peilgebieden.shp",
        None,
        "GPGZMRPL",
        "GPGIDENT",
        "DrentsOverijsselseDelta_fix_model_network",
    ),
    ("AaenMaas", "aam", "downloads/WS_PEILGEBIEDPolygon.shp", None, "ZOMERPEIL", "CODE", "AaenMaas_fix_model_area"),
    (
        "BrabantseDelta",
        "wbd",
        "verwerkt/4_ribasim/hydamo.gpkg",
        "peilgebiedpraktijk",
        "WS_ZOMERPEIL",
        "CODE",
        "BrabantseDelta_fix_model_area",
    ),
    (
        "StichtseRijnlanden",
        "hdsr",
        "verwerkt/4_ribasim/peilgebieden.gpkg",
        None,
        "WS_ZP",
        "WS_PGID",
        "StichtseRijnlanden_fix_model_area",
    ),
    (
        "ValleienVeluwe",
        "venv",
        "verwerkt/1_ontvangen_data/Eerste_levering/vallei_en_veluwe.gpkg",
        "peilgebiedpraktijk",
        "ws_max_peil",
        "code",
        "ValleienVeluwe_fix_model_area",
    ),
    (
        "Vechtstromen",
        "vechtstromen",
        "downloads/peilgebieden_voormalig_velt_en_vecht.gpkg",
        None,
        "GPGZMRPL",
        "GPGIDENT",
        "Vechtstromen_fix_model_area",
    ),
]

# Main function
if __name__ == "__main__":
    for params in params_list:
        result = process_basin_model(params)
