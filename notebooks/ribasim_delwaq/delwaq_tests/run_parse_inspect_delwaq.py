"""
10-11-2025 - Jesse van Leeuwen

This notebook runs delwaq model from python code and parses the results after running setup_delwaq.py

At this time, it still requires manual editing of the delwaq input files before running the model (see README.txt for detailed instructions)

To run delwaq from this script, add environment variable DIMR_PATH with the path to .env
"""

# %% Import necessary libraries
import os
import subprocess
from pathlib import Path

from ribasim.delwaq import parse, plot_fraction

# %% Set path of Ribasim model
model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "modellen/lhm_coupled_2025_9_0"
toml_path = model_path / "lhm.toml"
output_path = model_path / "delwaqq"  # changed folder name to delwaqq to avoid overwriting previous run

assert toml_path.is_file()

# %% run delwaq from python code
dimr_path = Path(os.environ["DIMR_PATH"])
dimr_config_path = output_path / "dimr_config.xml"

result = subprocess.run([dimr_path, dimr_config_path], cwd=output_path, capture_output=True, encoding="utf-8")
print(result.stdout)
print(result.stderr)
result.check_returncode()

# %% before parsing model: include manually added substance/load
# dummy substances and graph
# TODO don't rely on variables defined in ER_setup_delwaq.py
# commented out because these variables are not defined in this script
graph = []
substances = set()

# $ this is not possible right now because we still need to manually edit the delwaq input files before running model
# $ setting graph empty makes parsing fail and requires rerun of generate and delwaq model run

# %% Manually edit substances
# remove unwanted substances with 'substances.remove("PO3")'
# substances.add("NO3")
# substances.add("NH4")
# substances.add("OON")
# substances.add("PO4")
# substances.add("AAP")
# substances.add("OOP")


# %% parse delwaq results
nmodel = parse(toml_path, graph, substances, output_folder=output_path)

# %% check added loads in specified Ribasim nodes
plot_fraction(nmodel, 700970, ["NO3"])  # node downstream of BA 700008; see lhm.toml in QGIS

# %% plot fractions of all concentrations in a node of choice
plot_fraction(nmodel, 700970)
# $ for some reason this does not plot the added nutrients nor include them in the legend

# %% display data in tabular view
# display(nmodel.basin.concentration_external)
t = nmodel.basin.concentration_external.df  # display all concentrations
t[t.time == t.time.unique()[2]]  # check concentration at a specific time step
