"""
14-10-2026 Sibren Loos

Goal of this script:
- read observation locations for water quality from Good Cloud (source: WKP data processed for KRW-NUTrend by Steven Kelderman)
- visualize example locations
- read Ribasim basin nodes and select those which are relevant for comparison with observations (e.g. "doorgaand" | "hoofdwater" | "Rijkswaterstaat")
- create shapefile that contains the mapping between observation locations and ribasim basin nodes (using spatial join)
- OPTIONALLY: add extra mapping between observation locations and ribasim basin nodes after adding small buffer to the basin nodes (currently ~100 locations unmapped)
"""

# %% Import necessary libraries
import logging
import os
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from matplotlib import ticker
from ribasim import Model
from ribasim.delwaq import parse
from shapely.geometry import Point

from ribasim_nl import CloudStorage

# from notebooks.observations import spatial_coupling

logger = logging.getLogger(__name__)


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,
    )


setup_logging()

current_dir = Path(__file__).resolve().parent
print(f"Current directory: {current_dir}")
print("Check if working directory is the script directory.")

# auto-check
root_dir = ""
if current_dir.parts[-2:] == ("ribasim_delwaq", "postprocessing"):
    print("match")
    root_dir = "../../../"


# %% Load functions
def load_obs_data(path: str) -> pd.DataFrame:
    """Load observation data from a CSV file.

    Args:
        path (str): Path to the CSV file.

    Returns
    -------
        pd.DataFrame: DataFrame containing the observation data.
    """
    print(f"Loading observation data from: {path}")
    val_data = pd.read_parquet(path)
    return val_data


def mask(df, key, value):
    return df[df[key] == value]


pd.DataFrame.mask = mask


def sjoin_within(df_points, df_polygons):
    joined_within = gpd.sjoin(
        df_points,
        df_polygons,
        how="left",
        predicate="within",
    )
    # joined_unique = joined_within.loc[~joined_within.index.duplicated(keep="first")]
    return joined_within


# %% Define folder locations and synchronize with the Good Cloud
cloud = CloudStorage()
upload_results = False
logger.info("Synchronizing with file on the Good Cloud")
validatie_folder = cloud.joinpath(Path(os.environ["RIBASIM_NL_DATA_DIR"]), "Basisgegevens/Validatie/Waterkwaliteit")
WKP_metingen_path = cloud.joinpath(validatie_folder, "KRWMeetwaarden_1990_2025_20260615_1432.parquet")
WKP_metingen_coordinates = cloud.joinpath(
    validatie_folder,
    "WKP_KRW-monitoringlocaties-oppervlaktewater_Nederland_2025_20250915090837.csv",
)
cloud.synchronize(filepaths=[WKP_metingen_path, WKP_metingen_coordinates])

# %% Import observation data
data_filename = "KRWMeetwaarden_1990_2025_20260615_1432.parquet"
obs_data = Path(root_dir, "data/Basisgegevens/Validatie/Waterkwaliteit", data_filename)
df_obs = load_obs_data(obs_data)

# X, Y monitoring location from p:\krw-nutrend\01_jaarlijkse_update_KRW_NUtrend_data\2026_1990_2025\01_data_extern\03_ihw_monitoringsprogramma_NL\2025\WKP_KRW-monitoringlocaties-oppervlaktewater_Nederland_2025_20250915090837.csv
xy_filename = "WKP_KRW-monitoringlocaties-oppervlaktewater_Nederland_2025_20250915090837.csv"
obs_xy = Path(root_dir, "data/Basisgegevens/Validatie/Waterkwaliteit", xy_filename)
df_xy = pd.read_csv(obs_xy, sep=";")

df_xy = df_xy[["lokaleCode", "geometriePunt.X_RD", "geometriePunt.Y_RD"]]
df_xy.rename(
    {
        "lokaleCode": "loc_Code",
        "geometriePunt.X_RD": "loc_X",
        "geometriePunt.Y_RD": "loc_Y",
    },
    axis=1,
    inplace=True,
)

# %% convert xy df to shapefile
# combine lat and lon column to a shapely Point() object
df_xy["geometry"] = df_xy.apply(lambda x: Point((float(x.loc_X), float(x.loc_Y))), axis=1)
df_monitoringlocaties = gpd.GeoDataFrame(df_xy, geometry="geometry")
df_monitoringlocaties.to_file(
    Path(
        root_dir,
        "data/Basisgegevens/Validatie/Waterkwaliteit",
        "WKP_KRW-monitoringlocaties.shp",
    ),
    driver="ESRI Shapefile",
)

# %% READ MODEL OUTPUT
model_name = "lhm_coupled_full"
model_path = Path("../../../data/Rijkswaterstaat/modellen") / model_name
toml_name = "lhm_coupled.toml"
toml_path = model_path / toml_name
dmodel = Model.read(toml_path)
output_folder = "delwaq"
output_path = model_path / output_folder

# %% https://ribasim.org/guide/delwaq
# Parse Delwaq results and also populate Basin / concentration_external for plotting
nmodel = parse(dmodel, output_path, to_input=True)

# %% map the monitoring locations with the Ribasim model nodes

basins = nmodel.basin.area.df

# select basin nodes which will be compared with observations ("doorgaand" | "hoofdwater" | "Rijkswaterstaat")
nodes_doorgaand = nmodel.node.df.mask("meta_categorie", "doorgaand")
nodes_hoofdwater = nmodel.node.df.mask("meta_categorie", "hoofdwater")
nodes_Rijkswater = nmodel.node.df.mask("meta_waterbeheerder", "Rijkswaterstaat")
nodes_combined = pd.concat([nodes_doorgaand, nodes_hoofdwater, nodes_Rijkswater], ignore_index=False)
nodes_combined_selectedcols = nodes_combined[["node_type", "meta_categorie", "geometry"]]
basins_selected = basins.merge(nodes_combined_selectedcols, on="node_id", how="right")

basins_selected.rename(columns={"geometry_x": "geometry"}, inplace=True)
basins_selected.set_geometry("geometry", inplace=True)
# add attributes to nodes in nmodel.node.df: WKP_monitoring_code
basins_selected_joined_monlocs = sjoin_within(df_monitoringlocaties, basins_selected[["node_id", "geometry"]])

basins_selected_joined_monlocs.to_file(
    Path(
        root_dir,
        "data/Basisgegevens/Validatie/Waterkwaliteit",
        "basins_selected_joined_monlocs.shp",
    ),
    driver="ESRI Shapefile",
)

# %% VISUALIZE OBSERVATION DATA (purpose: quick inspection)
# Plot observation data for specific parameters and locations
parameters_of_interest = ["Ntot", "Ptot"]  # example parameters
locations_of_interest = ["NL02_0003", "NL94_KEIZVR"]  # example locations
filtered_df = df_obs[
    df_obs["parameter"].isin(parameters_of_interest) & df_obs["KRW_monitoringslocatie"].isin(locations_of_interest)
]
print(f"Filtered data shape: {filtered_df.shape}")
print(f"Unique parameters in filtered data: {filtered_df['parameter'].unique()}")
print(f"Unique locations in filtered data: {filtered_df['KRW_monitoringslocatie'].unique()}")

# %% Plot filtered observation data
selected_parameters_df = df_obs.isin(parameters_of_interest)
obs_locations = selected_parameters_df["KRW_monitoringslocatie"].unique()

# %% Plot filtered observation data
unit = filtered_df["eenheid"].unique()[0]  # only one unit present, mg/L
obs_locs = filtered_df["KRW_monitoringslocatie"].unique()
substances = filtered_df["parameter"].unique()

obs_summary = filtered_df.groupby(["KRW_monitoringslocatie", "parameter"])["meetwaarde"].mean().reset_index()
obs_timeseries = filtered_df
print(f"Observation summary shape: {obs_summary.shape}")

ax = sns.barplot(data=obs_summary, x="KRW_monitoringslocatie", y="meetwaarde", hue="parameter")
ax.tick_params(axis="x", rotation=90)
ax.set_ylabel(f"concentration ({unit})")
ax.set_title("Mean concentration of selected monitoring sites")
plt.show()

for loc in obs_locs:
    for par in substances:
        loc_data = obs_timeseries[obs_timeseries["KRW_monitoringslocatie"] == loc][obs_timeseries["parameter"] == par]
        ax = sns.lineplot(data=loc_data, x="datum", y="meetwaarde", label=f"{par} {loc}")
        max_xticks = 10
        ax.xaxis.set_major_locator(ticker.MaxNLocator(max_xticks))
        ax.tick_params(axis="x", rotation=90)
        ax.set_ylabel(f"concentration ({unit})")
        ax.set_title("Mean concentration of selected monitoring sites")
        plt.show()
