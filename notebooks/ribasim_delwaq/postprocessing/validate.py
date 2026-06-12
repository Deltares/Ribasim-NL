"""
5-2-2026 Jesse van Leeuwen

Goal of this script:
- Visualize output
- Load water quality data from Waterinfo API for specific stations and parameters or from local files
- Compare to output of delwaq simulation
- Plot results

"""

# %% Import necessary libraries
import os
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from ribasim import Model
from ribasim.delwaq import add_tracer

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
