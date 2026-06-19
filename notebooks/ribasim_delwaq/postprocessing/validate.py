"""
5-2-2026 Jesse van Leeuwen

19-6-2026 Sibren Loos

Goal of this script:
- read observation data for water quality from Good Cloud (source: WKP data processed for KRW-NUTrend by Steven Kelderman)
- Visualize example locations
- Load water quality data from delwaq simulation for specific stations and parameters Ntot and Ptot
- aggregate both datasets to equivalent time steps (e.g. monthly / quarterly)
- Compare to output of delwaq simulation and calculate statistics (e.g. RMSE, NSE, bias)
- Plot results
"""

# %% Import necessary libraries
import logging
import os
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd
import seaborn as sns

from ribasim_nl import CloudStorage

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


# %% Define folder locations and synchronize with the Good Cloud
cloud = CloudStorage()
upload_results = False
logger.info("Synchronizing with file on the Good Cloud")
validatie_folder = cloud.joinpath(Path(os.environ["RIBASIM_NL_DATA_DIR"]), "Basisgegevens/Validatie/Waterkwaliteit")
WKP_metingen_path = cloud.joinpath(validatie_folder, "KRWMeetwaarden_1990_2025_20260615_1432.parquet")
cloud.synchronize(
    filepaths=[
        WKP_metingen_path,
    ]
)

# %% Import observation data
data_filename = "KRWMeetwaarden_1990_2025_20260615_1432.parquet"
obs_data = Path(root_dir, "data/Rijkswaterstaat/validatie/observations", data_filename)
df = load_obs_data(obs_data)


# %% Filter imported observation data
# filter for specific parameters and locations
parameters_of_interest = ["Ntot", "Ptot"]  # example parameters
locations_of_interest = ["NL02_0003", "NL94_KEIZVR"]  # example locations
filtered_df = df[
    df["parameter"].isin(parameters_of_interest) & df["KRW_monitoringslocatie"].isin(locations_of_interest)
]
print(f"Filtered data shape: {filtered_df.shape}")
print(f"Unique parameters in filtered data: {filtered_df['parameter'].unique()}")
print(f"Unique locations in filtered data: {filtered_df['KRW_monitoringslocatie'].unique()}")

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


########## CODE BELOW IS PREVIOUS VERSION BASED ON WATERINFO DATA AND FOR TESTING PURPOSES ONLY, DELETE LATER ############
"""
# %%

val_data_path = "c:\\Users\\leeuw_je\\projects\\LWKM\\data\\obs\\20260205_018.csv"

val_data = pd.read_csv(
    val_data_path, sep=";", decimal=",", usecols=[1, 2, 8, 10, 21, 24, 41, 42], parse_dates=[4], dayfirst=True
)

val_data.rename(columns={"PARAMETER_ CODE": "PARAMETER_CODE"}, inplace=True)

val_data = val_data[~(val_data["NUMERIEKEWAARDE"] > 1e6)]  # delete rows with no data (9999999999 as value)

# %% inspect validation data

# in val data numeriekwaarde column display the rows where numwaar exceeds a value

unit = val_data["EENHEID_CODE"].unique()[0]  # only one unit present, ml/L
obs_locs = val_data["LOCATIE_CODE"].unique()
substances = val_data["PARAMETER_CODE"].unique()

obs_summary = val_data.groupby(["LOCATIE_CODE", "PARAMETER_CODE"])["NUMERIEKEWAARDE"].mean().reset_index()

ax = sns.barplot(data=obs_summary, x="LOCATIE_CODE", y="NUMERIEKEWAARDE", hue="PARAMETER_CODE")
ax.set_yscale("log")
ax.tick_params(axis="x", rotation=90)
ax.set_ylabel(f"concentration ({unit})")
ax.set_title("Mean concentration of monitoring sites in the Hoofdwatersysteem")
plt.show()


# coupling logic - find the closest node with meta_waterbeheerder == "Rijkswaterstaat"

# for each station, plot timeseries together with delwaq output for this location
# then also calculate the rmse
# for cumulative flux, we may need the discharge, or extract mass flux directly from delwaq output
# for now, just plot concentration timeseries

# %%
# set path of Ribasim model
model_name = "lhm_coupled_2025_9_0"

model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / model_name
toml_path = model_path / "lhm.toml"
assert toml_path.is_file()

# %%
model = Model.read(toml_path)
# display(model.basin.concentration_external)
# %%

concentration_df = pd.DataFrame(model.basin.concentration_external)
concentration_df.loc[concentration_df["substance"] == "NO3"]
add_tracer(model, 700970, "Foo")


model.graph.nodes(data=True)

# somehow try to extract the XY of all nodes, filter for RWS nodes or something.
 """
