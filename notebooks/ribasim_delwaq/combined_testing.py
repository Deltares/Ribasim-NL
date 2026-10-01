"""
5-12-2025 Jesse van Leeuwen

- Test script to generate delwaq input files from Ribasim model, run delwaq simulation and check results

"""

# %%
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd
from ribasim import Model
from ribasim.delwaq import generate, parse

# %%
ROOT = Path(__file__).resolve().parents[2]

model_name = "lhm_coupled_full"
toml_name = "lhm_coupled.toml"

model_path = ROOT / "data" / "Rijkswaterstaat" / "modellen" / model_name
toml_path = model_path / toml_name
assert toml_path.is_file()

# %% define data dirs
DELWAQ_DATA_DIR = ROOT / "data" / "Basisgegevens" / "Delwaq"

ANIMO_DATA_DIR = DELWAQ_DATA_DIR / "ANIMO"
ER_DATA_DIR = DELWAQ_DATA_DIR / "Emissieregistratie"
IM_DATA_DIR = DELWAQ_DATA_DIR / "IM"
ZINFO_DATA_DIR = DELWAQ_DATA_DIR / "Zinfo"

RUN_CONVERSION_SCRIPTS = False


def run_conversion_script(script_path: Path, description: str) -> None:
    result = subprocess.run(
        [sys.executable, str(script_path)],
        check=False,
        capture_output=True,
        text=True,
    )

    if result.stdout:
        print(result.stdout)

    if result.stderr:
        print(result.stderr)

    if result.returncode != 0:
        raise RuntimeError(f"{description} failed with exit code {result.returncode}")


### READ & PROCESS MASS LOAD EMISSIONS ###

# %% run ER data conversion script
# units: g/s

ER_loads_df_script = Path(__file__).resolve().parents[0] / "ER_to_delwaq" / "ER_coupling_delwaq.py"
ER_loads_df_path = ER_DATA_DIR / "output" / "ER_loads_g_s_df.parquet"

if RUN_CONVERSION_SCRIPTS:
    run_conversion_script(ER_loads_df_script, "ER data coupling script")

if ER_loads_df_path.exists():
    ER_loads_df = pd.read_parquet(ER_loads_df_path)
else:
    raise FileNotFoundError(f"Expected ER loads file not found: {ER_loads_df_path}")


# %% run ANIMO data conversion script
# units: g/s

ANIMO_loads_df_script = Path(__file__).resolve().parents[0] / "ANIMO_to_delwaq" / "ANIMO_coupling_delwaq.py"
ANIMO_loads_df_path = ANIMO_DATA_DIR / "output" / "ANIMO_loads_g_s_df.parquet"

if RUN_CONVERSION_SCRIPTS:
    run_conversion_script(ANIMO_loads_df_script, "ANIMO data coupling script")

if ANIMO_loads_df_path.exists():
    ANIMO_loads_df = pd.read_parquet(ANIMO_loads_df_path)
else:
    raise FileNotFoundError(f"Expected ANIMO loads file not found: {ANIMO_loads_df_path}")


# %% run boundwq data conversion script
# units: mg/L

boundwq_df_script = Path(__file__).resolve().parents[0] / "IM_Zinfo_to_delwaq" / "write_boundwq_files.py"
IM_boundaries_df_path = IM_DATA_DIR / "output" / "IM_boundaries_mg_L_df.parquet"
Zinfo_boundaries_df_path = ZINFO_DATA_DIR / "output" / "Zinfo_boundaries_mg_L_df.parquet"

if RUN_CONVERSION_SCRIPTS:
    run_conversion_script(boundwq_df_script, "Boundary data coupling script")

# Load IM boundaries

if IM_boundaries_df_path.exists():
    IM_boundaries_df = pd.read_parquet(IM_boundaries_df_path)
else:
    raise FileNotFoundError(f"Expected Boundary loads file not found: {IM_boundaries_df_path}")

# Load Zinfo boundaries

if Zinfo_boundaries_df_path.exists():
    Zinfo_boundaries_df = pd.read_parquet(Zinfo_boundaries_df_path)
else:
    raise FileNotFoundError(f"Expected Boundary loads file not found: {Zinfo_boundaries_df_path}")


# %%
"""
COMBINING LOADS FROM ANIMO AND ER

for every ANIMO load in a specific time, add the ER load that is defined for that year
this assumes that the ER load, defined for the first day of that year, is representative for that whole year
if we end up using more detailed ER values, we might use merge_asof + direction = backward
--> this would take for every ANIMO load the most recent ER load defined either on that day or before that day
but it is more likely that we change generate.py to handle multiple emission sources together
"""

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

# %%
"""
COMBINING BOUNDARY CONCENTRATIONS FROM IM AND ZINFO

This is less complex as both data sources are linked to different boundaries: transboundary inflows and WWTPs respecitively
--> here, NA/NaN/-999 entries have to be removed to enable coupling to ribasim model --> check whether this is a problem
"""

IM_and_Zinfo_df = pd.concat([IM_boundaries_df, Zinfo_boundaries_df], ignore_index=True)

conc = IM_and_Zinfo_df["concentration"]

print("Rows with concentration == NA/NaN:", conc.isna().sum(), "- excluding rows..")
print("Rows with concentration == -999:", (conc == -999).sum(), "- excluding rows..")

nodeid = IM_and_Zinfo_df["node_id"]

print("Rows with node_id == NA/NaN:", nodeid.isna().sum(), "- excluding rows..")

IM_and_Zinfo_df = IM_and_Zinfo_df[IM_and_Zinfo_df["concentration"].notna() & (IM_and_Zinfo_df["concentration"] != -999)]

IM_and_Zinfo_df = IM_and_Zinfo_df[IM_and_Zinfo_df["node_id"].notna()]

IM_and_Zinfo_df.sort_values("node_id")


# %%

"""
ADD EMISSION DATA TO MODEL AND WRITE FOR REPRODUCIBILITY

--> The 'new' written model may be added to dvc - simply running generate based on this model should result in a properly set up delwaq simulation
--> With DVC, it could then easily be tested whether changes in generate.py still result in the same delwaq input, given that the WQ forcing remains constant
"""

# read model to add emission data to
model = Model.read(toml_path)

# mass loads
mass_loads = ANIMO_and_ER_loads_df  # or ER_loads_df or ANIMO_loads_df
model.basin.mass_load = mass_loads  # either the sum of ER and ANIMO or two separate dataframes (or another column specifying the data source, depending on what generate.py can handle easiest)

# boundary concentrations
# Add new data to existing (basic) tracers
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
