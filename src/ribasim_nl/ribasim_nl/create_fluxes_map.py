import os

import geopandas as gpd
import pandas as pd
import ribasim

from ribasim_nl import CloudStorage

# connect to the cloud and synchronize
cloud = CloudStorage()
LHM_path = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm_coupled_2025_9_0")
comparison_path = cloud.joinpath("Basisgegevens", "resultaatvergelijking", "fluxen", "fluxen_LHM_stats.gpkg")
LHM_model_path = cloud.joinpath(LHM_path, "lhm.toml")
year_of_interest = 2017

cloud.synchronize(filepaths=[LHM_path])

# outdated model causes problems with the Manning Resistance nodes? Read gpkg in manually if error occurs

try:
    LHM = ribasim.Model.read(LHM_model_path)
    LHM_basin_area_df = LHM.basin.area.df.copy()
    LHM_basin_time_df = LHM.basin.time.df.copy()
    LHM_links_df = LHM.links.df.copy()

except Exception:
    LHM_basin_area_path = os.path.join(LHM_path, "input", "database.gpkg")
    LHM_basin_time_path = os.path.join(LHM_path, "input", "basin_time.arrow")
    LHM_links_path = os.path.join(LHM_path, "results", "flow.arrow")

    LHM_basin_area_df = gpd.read_file(LHM_basin_area_path, layer="basin / area")
    LHM_basin_time_df = pd.read_feather(LHM_basin_time_path)
    LHM_links_df = pd.read_feather(LHM_links_path)

# add the surface area to the time series dataframe
LHM_basin_area_df["meta_area"] = LHM_basin_area_df["geometry"].area.astype(float)

# there are some duplicates in Hunze and Aa's. Remove the smallest polygons (quick fix, asked D2Hydro to fix this issue in SA model)
LHM_basin_area_df = LHM_basin_area_df.sort_values(by="meta_area", ascending=True).drop_duplicates(
    subset="node_id", keep="last"
)
LHM_basin_time_df = LHM_basin_time_df.merge(LHM_basin_area_df[["node_id", "meta_area"]], on="node_id", how="left")

# convert to floats, only select values in year_of_interest
LHM_basin_time_df["drainage"] = LHM_basin_time_df["drainage"].astype(float)
LHM_basin_time_df["infiltration"] = LHM_basin_time_df["infiltration"].astype(float)
LHM_basin_time_df = LHM_basin_time_df[LHM_basin_time_df["time"].dt.year == year_of_interest]

# calculate fluxes to mm/day
LHM_basin_time_df["meta_drainage_mm"] = (
    LHM_basin_time_df["drainage"] / LHM_basin_time_df["meta_area"] * 1000 * 24 * 3600
)
LHM_basin_time_df["meta_infiltration_mm"] = (
    LHM_basin_time_df["infiltration"] / LHM_basin_time_df["meta_area"] * 1000 * 24 * 3600
)
LHM_basin_time_df["meta_precipitation_mm"] = LHM_basin_time_df["precipitation"] * 1000 * 24 * 3600
LHM_basin_time_df["meta_potential_evaporation_mm"] = LHM_basin_time_df["potential_evaporation"] * 1000 * 24 * 3600

# take min, max, mean value per node_id per year based on the time column
LHM_basin_time_df["meta_year"] = LHM_basin_time_df["time"].dt.year

LHM_min = LHM_basin_time_df.groupby(["node_id", "meta_year"]).min(numeric_only=True).reset_index()
LHM_min["time"] = pd.to_datetime(LHM_min["meta_year"], format="%Y")

LHM_max = LHM_basin_time_df.groupby(["node_id", "meta_year"]).max(numeric_only=True).reset_index()
LHM_max["time"] = pd.to_datetime(LHM_max["meta_year"], format="%Y")

LHM_mean = LHM_basin_time_df.groupby(["node_id", "meta_year"]).mean(numeric_only=True).reset_index()
LHM_mean["time"] = pd.to_datetime(LHM_mean["meta_year"], format="%Y")

# add geometries to the min, max, mean dataframes
LHM_min = LHM_min.merge(LHM_basin_area_df, on="node_id", how="left")
LHM_max = LHM_max.merge(LHM_basin_area_df, on="node_id", how="left")
LHM_mean = LHM_mean.merge(LHM_basin_area_df, on="node_id", how="left")


def change_names(df, min_max_mean):
    df = df.rename(
        columns={
            "meta_drainage_mm": f"{min_max_mean}_drainage_mm",
            "meta_infiltration_mm": f"{min_max_mean}_infiltration_mm",
            "meta_precipitation_mm": f"{min_max_mean}_precipitation_mm",
            "meta_potential_evaporation_mm": f"{min_max_mean}_potential_evaporation_mm",
            "geometry": "geometry",
        }
    )

    # only retain the columns of above
    df = df[
        [
            "node_id",
            f"{min_max_mean}_drainage_mm",
            f"{min_max_mean}_infiltration_mm",
            f"{min_max_mean}_precipitation_mm",
            f"{min_max_mean}_potential_evaporation_mm",
            "geometry",
        ]
    ]
    return df


LHM_min = change_names(LHM_min, "min")
LHM_max = change_names(LHM_max, "max")
LHM_mean = change_names(LHM_mean, "mean")

# merge the min, max, mean dataframes
LHM_stats = LHM_mean.merge(LHM_min, on=["node_id", "geometry"], how="left")
LHM_stats = LHM_stats.merge(LHM_max, on=["node_id", "geometry"], how="left")
LHM_stats = gpd.GeoDataFrame(LHM_stats, geometry="geometry", crs="EPSG:28992")

# only focus on the bergende bakjes. Remove all duplicated geometries, first entry is removed as this is doorgaand
LHM_stats = LHM_stats.sort_values(by="node_id").drop_duplicates(subset="geometry", keep="last")

comparison_path.parent.mkdir(parents=True, exist_ok=True)
LHM_stats.to_file(comparison_path, driver="GPKG")
cloud.upload_file(comparison_path)
