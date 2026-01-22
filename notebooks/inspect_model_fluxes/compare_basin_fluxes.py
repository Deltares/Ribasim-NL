"""Compare basin fluxes and plot time series for the selected basins."""

from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import ribasim

from ribasim_nl import CloudStorage

# connect to the cloud and synchronize
cloud = CloudStorage()
lhm_path = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm_coupled_2025_9_0")
lhm_model_path = cloud.joinpath(lhm_path, "lhm.toml")
basin_of_interest = [1200142, 1201578]  # doorgaand bakje first, bergend bakje second (optional)
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

subset = lhm_basin_time_df[lhm_basin_time_df.node_id.isin(basin_of_interest)]

# take the mean value multiple basins are selected
subset = subset.groupby("time").mean(numeric_only=True).reset_index()
subset["node_id"] = str(basin_of_interest)
subset = subset.set_index("time")

# retrieve Ribasim discharges from the links
subset_links = lhm_links_df[
    lhm_links_df["from_node_id"] == basin_of_interest[0]
]  # use the first as this is the doorgaande basin
subset_links = subset_links.set_index("time")

# sum the discharges if there are multiple links from the basin
subset_links = subset_links.groupby("time").sum(numeric_only=True).reset_index()
subset_links = subset_links.set_index("time")
subset_links = subset_links[["flow_rate", "convergence"]]

# plot each series in its own subplot
fig, axes = plt.subplots(5, 1, figsize=(12, 12), sharex=True)
axes[0].plot(subset.index, subset.meta_potential_evaporation_mm, label="Potential Evaporation")
axes[1].plot(subset.index, subset.meta_precipitation_mm, label="Precipitation")
axes[2].plot(subset.index, subset.meta_infiltration_mm, label="Infiltration")
axes[3].plot(subset.index, subset.meta_drainage_mm, label="Drainage")
axes[4].plot(subset_links.index, subset_links["flow_rate"], label="Flow Rate")

axes[0].set_title(f"Water balans basin {basin_of_interest}")
axes[4].set_xlabel("Tijd")
axes[0].set_ylabel("Evaporation [mm/day]")
axes[1].set_ylabel("Precipitation [mm/day]")
axes[2].set_ylabel("Infiltration [mm/day]")
axes[3].set_ylabel("Drainage [mm/day]")
axes[4].set_ylabel("Afvoer [m3/s]")
for ax in axes:
    ax.legend()
    ax.grid()
plt.show()
