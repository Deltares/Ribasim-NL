"""Create annual vertical basin flux statistics for the full model.

Compute min/mean/max values and write them to a local GeoPackage to avoid
cluttering the GoodCloud.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
import ribasim

from ribasim_nl import CloudStorage

# connect to the cloud and synchronize
cloud = CloudStorage()
lhm_path = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm_coupled_2025_9_0")
lhm_model_path = cloud.joinpath(lhm_path, "lhm.toml")
local_output_path = Path("../../../../compare_model_fluxes")
year_of_interest = 2017

cloud.synchronize(filepaths=[lhm_path])

# outdated model causes problems with the Manning Resistance nodes? Read gpkg in manually if error occurs
try:
    lhm = ribasim.Model.read(lhm_model_path)
    lhm_basin_area_df = lhm.basin.area.df.copy()
    lhm_basin_time_df = lhm.basin.time.df.copy()
    lhm_links_df = lhm.links.df.copy()

except Exception:
    lhm_basin_area_path = Path(lhm_path) / "input" / "database.gpkg"
    lhm_basin_time_path = Path(lhm_path) / "input" / "basin_time.arrow"
    lhm_links_path = Path(lhm_path) / "results" / "flow.arrow"

    lhm_basin_area_df = gpd.read_file(lhm_basin_area_path, layer="basin / area")
    lhm_basin_time_df = pd.read_feather(lhm_basin_time_path)
    lhm_links_df = pd.read_feather(lhm_links_path)

# add the surface area to the time series dataframe
lhm_basin_area_df["meta_area"] = lhm_basin_area_df["geometry"].area.astype(float)

# there are some duplicates in Hunze and Aa's. Remove the smallest polygons (quick fix, asked D2Hydro to fix this issue in SA model)
lhm_basin_area_df = lhm_basin_area_df.sort_values(by="meta_area", ascending=True).drop_duplicates(
    subset="node_id", keep="last"
)
lhm_basin_time_df = lhm_basin_time_df.merge(lhm_basin_area_df[["node_id", "meta_area"]], on="node_id", how="left")

# convert to floats, only select values in year_of_interest
lhm_basin_time_df["drainage"] = lhm_basin_time_df["drainage"].astype(float)
lhm_basin_time_df["infiltration"] = lhm_basin_time_df["infiltration"].astype(float)
lhm_basin_time_df = lhm_basin_time_df[lhm_basin_time_df["time"].dt.year == year_of_interest]

# calculate fluxes to mm/day
lhm_basin_time_df["meta_drainage_mm"] = (
    lhm_basin_time_df["drainage"] / lhm_basin_time_df["meta_area"] * 1000 * 24 * 3600
)
lhm_basin_time_df["meta_infiltration_mm"] = (
    lhm_basin_time_df["infiltration"] / lhm_basin_time_df["meta_area"] * 1000 * 24 * 3600
)
lhm_basin_time_df["meta_precipitation_mm"] = lhm_basin_time_df["precipitation"] * 1000 * 24 * 3600
lhm_basin_time_df["meta_potential_evaporation_mm"] = lhm_basin_time_df["potential_evaporation"] * 1000 * 24 * 3600

# take min, max, mean value per node_id per year based on the time column
lhm_basin_time_df["meta_year"] = lhm_basin_time_df["time"].dt.year

lhm_min = lhm_basin_time_df.groupby(["node_id", "meta_year"]).min(numeric_only=True).reset_index()
lhm_min["time"] = pd.to_datetime(lhm_min["meta_year"], format="%Y")

lhm_max = lhm_basin_time_df.groupby(["node_id", "meta_year"]).max(numeric_only=True).reset_index()
lhm_max["time"] = pd.to_datetime(lhm_max["meta_year"], format="%Y")

lhm_mean = lhm_basin_time_df.groupby(["node_id", "meta_year"]).mean(numeric_only=True).reset_index()
lhm_mean["time"] = pd.to_datetime(lhm_mean["meta_year"], format="%Y")

# add geometries to the min, max, mean dataframes
lhm_min = lhm_min.merge(lhm_basin_area_df, on="node_id", how="left")
lhm_max = lhm_max.merge(lhm_basin_area_df, on="node_id", how="left")
lhm_mean = lhm_mean.merge(lhm_basin_area_df, on="node_id", how="left")


def change_names(df, min_max_mean):
    """Rename flux columns with a min/max/mean prefix and retain key fields."""
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


lhm_min = change_names(lhm_min, "min")
lhm_max = change_names(lhm_max, "max")
lhm_mean = change_names(lhm_mean, "mean")

# merge the min, max, mean dataframes
lhm_stats = lhm_mean.merge(lhm_min, on=["node_id", "geometry"], how="left")
lhm_stats = lhm_stats.merge(lhm_max, on=["node_id", "geometry"], how="left")
lhm_stats = gpd.GeoDataFrame(lhm_stats, geometry="geometry", crs="EPSG:28992")

# only focus on the bergende bakjes. Remove all duplicated geometries, first entry is removed as this is doorgaand
lhm_stats = lhm_stats.sort_values(by="node_id").drop_duplicates(subset="geometry", keep="last")

gpkg_path = local_output_path / "fluxes_LHM_stats.gpkg"

local_output_path.mkdir(parents=True, exist_ok=True)
lhm_stats.to_file(gpkg_path, driver="GPKG")
