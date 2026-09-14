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

# %%
# set path of Ribasim model
model_name = "lhm_coupled_full"
toml_name = "lhm_coupled.toml"

# model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "Rijkswaterstaat" / "modellen" / model_name
model_path = Path(__file__).parent.parent.parent / "data/Rijkswaterstaat/modellen" / model_name
toml_path = model_path / toml_name
assert toml_path.is_file()

model = Model.read(toml_path)

### READ & PROCESS MASS LOAD EMISSIONS ###

# %% run ER data conversion script
# units: g/s
# 1. Couple emission data to Ribasim model
ER_loads_df_script = Path(__file__).resolve().parent / "ER_to_delwaq" / "ER_data_conversion_delwaq.py"
ER_loads_df_path = ER_loads_df_script.parent / "output" / "ER_loads_g_s_df.parquet"

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
# units: g/s

ANIMO_loads_df_script = Path(__file__).resolve().parent / "ANIMO_to_delwaq" / "ANIMO_to_clean_csv.py"
ANIMO_loads_df_path = ANIMO_loads_df_script.parent / "output" / "ANIMO_loads_g_s_df.parquet"


# ANIMO_loads_df_script = r"p:\11212767-lwkm2\Koppeling_ANIMO_Delwaq\scripts\1-prepare\ANIMO2Delwaq.py"

result = subprocess.run(
    [sys.executable, str(ANIMO_loads_df_script)],
    check=False,
    capture_output=True,
    text=True,
)

if result.stdout:
    print(result.stdout)

if result.stderr:
    print(result.stderr)

if result.returncode != 0:
    raise RuntimeError(f"ANIMO data coupling script failed with exit code {result.returncode}")

if ANIMO_loads_df_path.exists():
    ANIMO_loads_df = pd.read_parquet(ANIMO_loads_df_path)
else:
    raise FileNotFoundError(f"Expected ANIMO loads file not found: {ANIMO_loads_df_path}")

ANIMO_loads_df.head()

### COMBINE LOADS AND COUPLE TO RIBASIM ###

# %%

# for every ANIMO load in a specific time, add the ER load that is defined for that year
# this assumes that the ER load, defined for the first day of that year, is representative for that whole year
# if we end up using more detailed ER values, we might use merge_asof + direction = backward
# --> this would take for every ANIMO load the most recent ER load defined either on that day or before that day
# but it is more likely that we change generate.py to handle multiple emission sources together

# ER_loads_df["year"] = ER_loads_df["time"].dt.year
# ANIMO_loads_df["year"] = ANIMO_loads_df["time"].dt.year

animo = ANIMO_loads_df.assign(year=ANIMO_loads_df["time"].dt.year)

er = ER_loads_df.assign(year=ER_loads_df["time"].dt.year)

result = animo.merge(
    er[["node_id", "substance", "year", "time", "load"]],
    on=["node_id", "substance", "year"],
    how="outer",
    suffixes=("_animo", "_er"),
)

result["time"] = result["time_animo"].fillna(result["time_er"])

result["load"] = result["load_animo"].fillna(0) + result["load_er"].fillna(0)

ANIMO_and_ER_loads_df = result[["node_id", "time", "substance", "load"]]

ANIMO_and_ER_loads_df.sort_values("load")

# %% run boundwq data conversion script
# units: mg/L

boundwq_df_script = Path(__file__).resolve().parent / "IM_Zinfo_to_delwaq" / "write_boundwq_files.py"
IM_boundaries_df_path = boundwq_df_script.parent / "output" / "IM_boundaries_mg_L_df.parquet"
Zinfo_boundaries_df_path = boundwq_df_script.parent / "output" / "Zinfo_boundaries_mg_L_df.parquet"

result = subprocess.run(
    [sys.executable, str(boundwq_df_script)],
    check=False,
    capture_output=True,
    text=True,
)

if result.stdout:
    print(result.stdout)

if result.stderr:
    print(result.stderr)

if result.returncode != 0:
    raise RuntimeError(f"Boundary data coupling script failed with exit code {result.returncode}")

# Load IM boundaries

if IM_boundaries_df_path.exists():
    IM_boundaries_df = pd.read_parquet(IM_boundaries_df_path)
else:
    raise FileNotFoundError(f"Expected Boundary loads file not found: {IM_boundaries_df_path}")

IM_boundaries_df.head()

# Load Zinfo boundaries

if Zinfo_boundaries_df_path.exists():
    Zinfo_boundaries_df = pd.read_parquet(Zinfo_boundaries_df_path)
else:
    raise FileNotFoundError(f"Expected Boundary loads file not found: {Zinfo_boundaries_df_path}")

Zinfo_boundaries_df.head()

IM_and_Zinfo_df = pd.concat([IM_boundaries_df, Zinfo_boundaries_df], ignore_index=True)

# %% inspect and remove nans & -999

IM_and_Zinfo_df["concentration"].describe()

conc = IM_and_Zinfo_df["concentration"]

print("NA/NaN:", conc.isna().sum())
print("-999:", (conc == -999).sum())

nodeid = IM_and_Zinfo_df["node_id"]

print("NA/NaN:", nodeid.isna().sum())

IM_and_Zinfo_df = IM_and_Zinfo_df[IM_and_Zinfo_df["concentration"].notna() & (IM_and_Zinfo_df["concentration"] != -999)]

IM_and_Zinfo_df = IM_and_Zinfo_df[IM_and_Zinfo_df["node_id"].notna()]

IM_and_Zinfo_df.sort_values("node_id")
# %% add emission data to model

# mass loads
mass_loads = ANIMO_and_ER_loads_df  # or ER_loads_df or ANIMO_loads_df
model.basin.mass_load = mass_loads  # either the sum of ER and ANIMO or two separate dataframes (or another column specifying the data source, depending on what generate.py can handle easiest)

# boundary concentrations
# replace current concentration object or simply append this data? Have not run generate with concentration data before
flow_boundary_concentration = IM_and_Zinfo_df
model.flow_boundary.concentration = pd.concat(
    [model.flow_boundary.concentration.df, flow_boundary_concentration],
    ignore_index=True,
)

model.write(toml_path)

# %%
# 2. Set up DELWAQ simulation automatically using generate.py

output_folder = "delwaq_all_sources"

output_path = model_path / output_folder
generate(toml_path, output_path)

# %%
# 3. Run DELWAQ simulation

# Define path of Ribasim model again
output_folder = "delwaq_all_sources"  # change folder name with delwaq.inp modifications and added files: boundwq_rwzi.dat, boundwq_ba.dat, loadswq.id, b6_loads.inc to prevent overwriting

output_path = model_path / output_folder
assert toml_path.is_file()

# %%
# run delwaq & stream output

dimr_path = Path(os.environ["D3D_HOME"])
dimr_config_path = output_path / "dimr_config.xml"

cmd = f'"{dimr_path}" "{dimr_config_path}"'

proc = subprocess.Popen(  # noqa: S602
    cmd,
    cwd=output_path,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    shell=True,
)

with proc:
    for line in proc.stdout:
        print(line, end="")

returncode = proc.wait()

if returncode != 0:
    raise subprocess.CalledProcessError(returncode, cmd)

# %%
# 4. Parse and save simulation results

# before parsing model: include manually added substance/load
# TODO: include in generate function

# %%
# parse delwaq results
nmodel = parse(toml_path, output_folder=output_path, to_input=True)

# # %% check added loads in specified Ribasim nodes
# plot_fraction(nmodel, 700970, ["NO3"])  # node downstream of BA 700008; see lhm.toml in QGIS
