"""Generate, run, and parse the DELWAQ model."""

# %%
import os
import subprocess
from pathlib import Path

from ribasim.delwaq import generate, parse

# %%
ROOT = Path(__file__).resolve().parents[2]
MODEL_NAME = "lhm_coupled_full"
TOML_NAME = "lhm_coupled.toml"
OUTPUT_FOLDER = "delwaq_all_sources"

model_path = ROOT / "data" / "Rijkswaterstaat" / "modellen" / MODEL_NAME
toml_path = model_path / TOML_NAME
output_path = model_path / OUTPUT_FOLDER
assert toml_path.is_file()

# %% Generate DELWAQ input
generate(toml_path, output_path)

# %% Run DELWAQ and stream output
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

# %% Parse DELWAQ results
nmodel = parse(toml_path, output_folder=output_path, to_input=True)
