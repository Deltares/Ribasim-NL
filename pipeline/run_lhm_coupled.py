"""Run the coupled LHM model from `koppelen` in a separate directory, so its results can be tracked by DVC.

The run TOML reads the input of `data/Rijkswaterstaat/modellen/lhm_coupled`, so the input is not copied.
The commits of the code and of the Ribasim core in `RIBASIM_HOME` are written to `runinfo.json`, for the model viewer, see `ribasim_nl.webmap.run_info`.
"""

import json
import os
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


model_toml = settings.ribasim_nl_data_dir / "Rijkswaterstaat/modellen/lhm_coupled/lhm_coupled.toml"
run_dir = settings.ribasim_nl_data_dir / "Rijkswaterstaat/runs/lhm_coupled"
run_toml = run_dir / model_toml.name

with model_toml.open("rb") as f:
    config = tomllib.load(f)
input_dir = model_toml.parent / config.get("input_dir", ".")
assert (input_dir / "database.gpkg").is_file(), f"No database in {input_dir}"

config["solver"] = config.get("solver", {}) | SOLVER_OVERRIDES
config["results"] = config.get("results", {}) | RESULTS_OVERRIDES
endtime = config["starttime"].replace(year=config["starttime"].year + RUN_YEARS)
assert endtime <= config["endtime"], f"The run ends at {endtime}, after the model ends at {config['endtime']}"
config["endtime"] = endtime
config["input_dir"] = os.path.relpath(input_dir, run_dir).replace("\\", "/")
config["results_dir"] = "results"

# During a reproduction `dvc.lock` is updated before it is committed, so it does not count as a change
commit = git("rev-parse", "HEAD").strip()
dirty = git("status", "--porcelain", "--untracked-files=no", "--", ":/", ":!/dvc.lock") != ""

run_dir.mkdir(parents=True, exist_ok=True)
core = core_info(Path(os.environ["RIBASIM_HOME"]))
(run_dir / RUN_INFO).write_text(json.dumps({"commit": commit, "dirty": dirty, "core": core}, indent=2))
with run_toml.open("wb") as f:
    tomli_w.dump(config, f)

specs = run(run_toml, threads=4)
if specs.exit_code != 0:
    raise RuntimeError(f"Ribasim failed with exit code {specs.exit_code}")
