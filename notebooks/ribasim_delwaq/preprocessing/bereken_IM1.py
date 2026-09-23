"""
02-09-2026 Sibren Loos

Goal of this script:
- uit meetgegevens GR en OS per KRWtype een gemiddelde en range (min-max) berekenen (obv alle beschikbare jaren?)
- IM berekenen per watertype door vermenigvuldiging OS en GR
"""

# %% Import necessary libraries
import logging
from pathlib import Path

import pandas as pd

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
if current_dir.parts[-2:] == ("ribasim_delwaq", "preprocessing"):
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
    val_data = pd.read_csv(path, sep=";", decimal=".")
    return val_data


def filter_data(df):
    print("Filter data: remove outliers and NaN values")
    df = df[(df["Numeriekewaarde"] > 0)]  # delete all rows with negative values in column "Numeriekewaarde"
    df = df[df["KwaliteitsoordeelCode"] == 00]  # select only rows with KwaliteitsoordeelCode == 00
    df = df[df["Numeriekewaarde"].notna()]
    return df


def mask(df, key, value):
    return df[df[key] == value]


pd.DataFrame.mask = mask


# %% Import observation data
data_filename1 = "WKP_meetwaarden_20260902164422_GR.csv"
obs_data1 = Path(root_dir, "data/Basisgegevens/Preprocessing", data_filename1)
df_obs1 = load_obs_data(obs_data1)
df_obs1 = filter_data(df_obs1)

# %% Process observation data
GR_grouped = df_obs1.groupby("KRWWatertypeCode").agg(
    GR_mean=("Numeriekewaarde", "mean"),
    GR_min=("Numeriekewaarde", "min"),
    GR_max=("Numeriekewaarde", "max"),
    Record_Count=("Numeriekewaarde", "size"),
)

# %% Import observation data
data_filename2 = "WKP_meetwaarden_20260902165852_OS.csv"
obs_data2 = Path(root_dir, "data/Basisgegevens/Preprocessing", data_filename2)
df_obs2 = load_obs_data(obs_data2)
df_obs2 = filter_data(df_obs2)

# %% Process observation data
OS_grouped = df_obs2.groupby("KRWWatertypeCode").agg(
    OS_mean=("Numeriekewaarde", "mean"),
    OS_min=("Numeriekewaarde", "min"),
    OS_max=("Numeriekewaarde", "max"),
    Record_Count=("Numeriekewaarde", "size"),
    KRWWatertypeOmschrijving=("KRWWatertypeOmschrijving", "first"),
)

# %% Calculate IM1 from grouped data
IM1_grouped = OS_grouped.copy().join(GR_grouped, how="outer", lsuffix="_OS", rsuffix="_GR", on="KRWWatertypeCode")
IM1_grouped["IM1"] = IM1_grouped["OS_mean"] * (IM1_grouped["GR_mean"] * 0.01)

# %% Print grouped data
print("GR grouped data:")
print(GR_grouped)
print("\nOS grouped data:")
print(OS_grouped)
print("\nIM1 grouped data:")
print(IM1_grouped)

# %% Export grouped data to CSV
output_dir = Path(root_dir, "data/Basisgegevens/Preprocessing")
output_dir.mkdir(parents=True, exist_ok=True)
GR_grouped.to_csv(output_dir / "GR_grouped.csv", sep=";", decimal=".", index=True)
OS_grouped.to_csv(output_dir / "OS_grouped.csv", sep=";", decimal=".", index=True)
IM1_grouped.to_csv(output_dir / "IM1_grouped.csv", sep=";", decimal=".", index=True)

# %%
