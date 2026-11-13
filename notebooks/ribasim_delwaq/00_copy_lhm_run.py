"""Copy the run of the coupled LHM model, for the other DELWAQ scripts to work on.

The DELWAQ workflow adds mass loads and concentrations to the model, and writes its input and results next to it.
It does so in a copy, since the run itself is the output of the `run_lhm_coupled` DVC stage, which other stages read.
"""

# %%
import shutil
import tomllib
from pathlib import Path

# %%
CONFIG_PATH = Path(__file__).resolve().parent / "config.toml"
ROOT = CONFIG_PATH.parents[2]

# `load_delwaq_config` requires the copy to exist, so read the paths directly
with CONFIG_PATH.open("rb") as config_file:
    config = tomllib.load(config_file)
run_toml = ROOT / config["model"]["run_toml_path"]
model_toml = ROOT / config["model"]["toml_path"]
run_dir = run_toml.parent
model_dir = model_toml.parent

assert run_toml.is_file(), f"No run of the coupled model at {run_toml}"
assert model_toml.name == run_toml.name, f"The copy keeps the name of {run_toml.name}, not {model_toml.name}"
assert not run_dir.is_relative_to(model_dir), f"The copy {model_dir} would replace the run {run_dir}"
assert not model_dir.is_relative_to(run_dir), f"The copy {model_dir} is inside the run {run_dir}"

with run_toml.open("rb") as run_file:
    results_dir = run_dir / tomllib.load(run_file).get("results_dir", "results")
for filename in ("basin.nc", "flow.nc"):
    assert (results_dir / filename).is_file(), (
        f"DELWAQ needs the results of the run, {results_dir / filename} is missing"
    )

# %%
# DVC removes the outputs of a stage before running it; this also removes them when the script is run directly
shutil.rmtree(model_dir, ignore_errors=True)
shutil.copytree(run_dir, model_dir)
print(f"Copied {run_dir} to {model_dir}")
