"""Run the coupled LHM model from `koppelen` in a separate directory, so its results can be tracked by DVC.

The run TOML reads the input of `data/Rijkswaterstaat/modellen/lhm_coupled`, so the input is not copied.
The commit of the code is written to `runinfo.json`, for the model viewer, see `ribasim_nl.webmap.run_info`.
"""

import json
import os
import subprocess
import tomllib
from datetime import datetime
from pathlib import Path

import tomli_w
from ribasim_nl.run_model import run
from ribasim_nl.settings import settings
from ribasim_nl.webmap import RUN_INFO

# Temporary: 1 year with a looser tolerance, until a full run is feasible
OVERRIDES = {"endtime": datetime(2018, 1, 1)}
SOLVER_OVERRIDES = {"reltol": 1e-3}


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

config |= OVERRIDES
config["solver"] = config.get("solver", {}) | SOLVER_OVERRIDES
config["input_dir"] = os.path.relpath(input_dir, run_dir).replace("\\", "/")
config["results_dir"] = "results"

# During a reproduction `dvc.lock` is updated before it is committed, so it does not count as a change
commit = git("rev-parse", "HEAD").strip()
dirty = git("status", "--porcelain", "--untracked-files=no", "--", ":/", ":!/dvc.lock") != ""

run_dir.mkdir(parents=True, exist_ok=True)
(run_dir / RUN_INFO).write_text(json.dumps({"commit": commit, "dirty": dirty}, indent=2))
with run_toml.open("wb") as f:
    tomli_w.dump(config, f)

specs = run(run_toml, threads=4)
if specs.exit_code != 0:
    raise RuntimeError(f"Ribasim failed with exit code {specs.exit_code}")
