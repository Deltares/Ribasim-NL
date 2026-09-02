"""
5-12-2025 Jesse van Leeuwen

- Test script to generate delwaq input files from Ribasim model, run delwaq simulation and check results

Updated doc will follow after ANIMO coupling is done from this script
"""

# %%
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
from ribasim.delwaq import generate, parse
from ribasim_nl.model import Model

# # %% configure GDAL/PROJ for this session (might be necessary to produce figures with emission sub-scripts, which inherit these variables thru subprocess)
# # The clean solution is to make the environment/kernel start with the activation layer already applied, so GDAL/PROJ are set by the env itself rather than by the script.
# env_root = Path(sys.executable).resolve().parent
# os.environ.setdefault("CONDA_PREFIX", str(env_root))
# os.environ.setdefault("GDAL_DATA", str
# (env_root / "Library" / "share" / "gdal"))
# os.environ.setdefault("PROJ_DATA", str(env_root / "Library" / "share" / "proj"))
# os.environ.setdefault("PROJ_LIB", os.environ["PROJ_DATA"])

# %% run ER data conversion script
# 1. Couple emission data to Ribasim model
ER_loads_df_script = Path(__file__).resolve().parent / "ER_to_delwaq" / "ER_data_conversion_delwaq.py"
ER_loads_df_path = ER_loads_df_script.parent / "output" / "ER_loads_df.parquet"

result = subprocess.run(
    [sys.executable, str(ER_loads_df_script)],
    check=False,
    capture_output=True,
    text=True,
)

if result.stdout:
    print(result.stdout)

if result.stderr:
    print(result.stderr)

if result.returncode != 0:
    raise RuntimeError(f"ER data coupling script failed with exit code {result.returncode}")

if ER_loads_df_path.exists():
    ER_loads_df = pd.read_parquet(ER_loads_df_path)
else:
    raise FileNotFoundError(f"Expected ER loads file not found: {ER_loads_df_path}")


# %% run ANIMO data conversion script

# ANIMO_loads_df_script = Path(__file__).resolve().parent / "ER_to_delwaq" / "ANIMO_data_conversion_delwaq.py"
# ANIMO_loads_df_path = ER_loads_df_script.parent / "output" / "ANIMO_loads_df.parquet"

# ANIMO_loads_df_script = r"p:\11212767-lwkm2\Koppeling_ANIMO_Delwaq\scripts\1-prepare\ANIMO2Delwaq.py"

# result = subprocess.run(
#     [sys.executable, str(ANIMO_loads_df_script)],
#     check=False,
#     capture_output=True,
#     text=True,
# )

# if result.stdout:
#     print(result.stdout)

# if result.stderr:
#     print(result.stderr)

# if result.returncode != 0:
#     raise RuntimeError(f"ANIMO data coupling script failed with exit code {result.returncode}")

# if ANIMO_loads_df_path.exists():
#     ANIMO_loads_df = pd.read_parquet(ANIMO_loads_df_path)
# else:
#     raise FileNotFoundError(f"Expected ANIMO loads file not found: {ANIMO_loads_df_path}")

# TODO: combine ER and ANIMO loads into one df (how, when the time resolution is not equal? convert ER to daily values or let delwaq handle this?)

# %%
# set path of Ribasim model
model_name = "lhm_coupled_full"
toml_name = "lhm_coupled.toml"

# model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "Rijkswaterstaat" / "modellen" / model_name
model_path = Path("../../data/Rijkswaterstaat/modellen") / model_name
toml_path = model_path / toml_name
assert toml_path.is_file()

# %% read model
model = Model.read(toml_path)

# %% add emission data to model
model = Model.read(toml_path)
model.basin.mass_load = ER_loads_df  # either the sum of ER and ANIMO or two separate dataframes (or another column specifying the data source, depending on what generate.py can handle easiest)
model.write(toml_path)

# %%
# 2. Set up DELWAQ simulation automatically using generate.py

output_folder = "delwaq_sep"

output_path = model_path / output_folder
generate(toml_path, output_path)

# %%
# 3. Run DELWAQ simulation

# Define path of Ribasim model again
output_folder = "delwaq"  # change folder name with delwaq.inp modifications and added files: boundwq_rwzi.dat, boundwq_ba.dat, loadswq.id, b6_loads.inc to prevent overwriting

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
# 4. Parse and save simulation results

# before parsing model: include manually added substance/load
# TODO: include in generate function

# %%
# parse delwaq results
nmodel = parse(toml_path, output_folder=output_path, to_input=True)

# # %% check added loads in specified Ribasim nodes
# plot_fraction(nmodel, 700970, ["NO3"])  # node downstream of BA 700008; see lhm.toml in QGIS
