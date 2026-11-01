"""Run a copy of the coupled LHM model from `koppelen` in a separate directory, so its results can be tracked by DVC.

The run directory holds the complete model, input and results together. The input is copied unchanged, so DVC stores
it only once, in the cache and on the remote, since it deduplicates files by their content.
The commits of the code and of the Ribasim core in `RIBASIM_HOME` are written to `runinfo.json`, for the model viewer, see `ribasim_nl.webmap.run_info`.
"""

import json
import os
import shutil
import subprocess
import tomllib
from pathlib import Path

import tomli_w
from ribasim_nl.run_model import core_info, run
from ribasim_nl.settings import settings
from ribasim_nl.webmap import RUN_INFO

# A looser tolerance, until a run with the default tolerance is feasible
SOLVER_OVERRIDES = {"reltol": 1e-3}
# Deflate the NetCDF results, which are pushed to DVC for every run. This makes them about
# 40% smaller, at the cost of about a minute of writing at the end of the run.
RESULTS_OVERRIDES = {"compression": True}
# Run the first 2 of the 3 years of the model, to save time on every run
RUN_YEARS = 2


def git(*args: str) -> str:
    """Run a git command in this repository and return its output."""
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True, cwd=Path(__file__).parent).stdout


model_dir = settings.ribasim_nl_data_dir / "Rijkswaterstaat/modellen/lhm_coupled"
run_dir = settings.ribasim_nl_data_dir / "Rijkswaterstaat/runs/lhm_coupled"
model_toml = model_dir / "lhm_coupled.toml"
run_toml = run_dir / model_toml.name

with model_toml.open("rb") as f:
    config = tomllib.load(f)
input_dir = model_dir / config.get("input_dir", ".")
assert input_dir.resolve().is_relative_to(model_dir.resolve()), f"The input {input_dir} is outside {model_dir}"
assert (input_dir / "database.gpkg").is_file(), f"No database in {input_dir}"
results_dir = "results"
assert not (model_dir / results_dir).exists(), f"The model already has results in {model_dir / results_dir}"

config["solver"] = config.get("solver", {}) | SOLVER_OVERRIDES
config["results"] = config.get("results", {}) | RESULTS_OVERRIDES
endtime = config["starttime"].replace(year=config["starttime"].year + RUN_YEARS)
assert endtime <= config["endtime"], f"The run ends at {endtime}, after the model ends at {config['endtime']}"
config["endtime"] = endtime
config["results_dir"] = results_dir

# During a reproduction `dvc.lock` is updated before it is committed, so it does not count as a change
commit = git("rev-parse", "HEAD").strip()
dirty = git("status", "--porcelain", "--untracked-files=no", "--", ":/", ":!/dvc.lock") != ""

# DVC removes the outputs of a stage before running it; this also removes them when the script is run directly
shutil.rmtree(run_dir, ignore_errors=True)
shutil.copytree(model_dir, run_dir)
core = core_info(Path(os.environ["RIBASIM_HOME"]))
(run_dir / RUN_INFO).write_text(json.dumps({"commit": commit, "dirty": dirty, "core": core}, indent=2))
with run_toml.open("wb") as f:
    tomli_w.dump(config, f)

specs = run(run_toml, threads=4)
if specs.exit_code != 0:
    raise RuntimeError(f"Ribasim failed with exit code {specs.exit_code}")
