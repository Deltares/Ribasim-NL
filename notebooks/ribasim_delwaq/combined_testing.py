"""
5-12-2025 Jesse van Leeuwen

- Test script to generate delwaq input files from Ribasim model, run delwaq simulation and check results
- At this moment includes manual steps to modify delwaq.inp and add input timeseries files
- For testing purposes, the delwaq simulation itself uses a different delwaq folder than the standard generated one
- Here the manual adjustments have already been made
"""

# %%
# Import necessary libraries
import os
import subprocess
from pathlib import Path

from ribasim.delwaq import generate, parse, plot_fraction  # are we not using parse anymore?
from ribasim_nl.model import Model

# %%
# set path of Ribasim model
model_name = "lhm_coupled_full"
toml_name = "lhm_coupled.toml"

# model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "Rijkswaterstaat" / "modellen" / model_name
model_path = Path("../../data/Rijkswaterstaat/modellen") / model_name
toml_path = model_path / toml_name
assert toml_path.is_file()

# %%
# couple emission data

# $ RUN EM_to_delwaq to get long table of loads (and save to csv/parquet)
# $ RUN ANIMO_to_delwaq to get long table of loads (and save to csv/parquet)
# $ (RUN RWZI_to_delwaq to get long table of concentrations) (and save to csv/parquet)
# $ (RUN BA_to_delwaq to get long table of concentrations) (and save to csv/parquet)

# model.basin.mass_load = ER_loads_df + ANIMO_loads_df
# model.basin.concentration = RWZI_concentrations_df + BA_concentrations_df

# $ intermediate saving of model?

# %%
# generate delwaq input files and graph variable
output_folder = "delwaq"

output_path = model_path / output_folder
graph, substances = generate(toml_path, output_path)

# %%
# run delwaq

output_path = model_path / output_folder
assert toml_path.is_file()

# %%
# run delwaq from python code
dimr_path = Path(os.environ["DIMR_PATH"])
dimr_config_path = output_path / "dimr_config.xml"

result = subprocess.run([dimr_path, dimr_config_path], cwd=output_path, capture_output=True, encoding="utf-8")
print(result.stdout)
print(result.stderr)
result.check_returncode()

# %%
# before parsing model: include manually added substance/load
# TODO: include in generate function
substances.add("NO3")
substances.add("NH4")
substances.add("OON")
substances.add("PO4")
substances.add("AAP")
substances.add("OOP")

# %%
# parse delwaq results
nmodel = parse(toml_path, graph, substances, output_folder=output_path, to_input=True)

# %% check added loads in specified Ribasim nodes
plot_fraction(nmodel, 700970, ["NO3"])  # node downstream of BA 700008; see lhm.toml in QGIS
# fraction exceeds 1, units unclear

# %% just to check if model can be read again
model = Model.read(toml_path)
# model.basin.concentration  # should include new substances
# model.basin.concentration_external

# %%
# save results for QGIS visualization #$ made redundant by parse(to_input=True)
# nmodel.basin.concentration_external._write_arrow("concentration.arrow", nmodel.filepath.parent, nmodel.results_dir)
# nmodel.write(model_path / "lhm_test.toml")

# %%
# additional inspection
# nmodel.basin.concentration_external.filepath = "results/concentration.arrow"
# display(nmodel.basin.concentration_external)
