# %%
import re
import shutil
import subprocess
import sys
import tomllib
from collections import namedtuple
from datetime import datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast

from ribasim.cli import _find_cli

if TYPE_CHECKING:
    from ribasim_nl.model import Model

RunSpecs = namedtuple("RunSpecs", ["exit_code", "simulation_time"])

CORE_INFO_KEYS = ("version", "commit", "branch")


def core_info(ribasim_home: Path) -> dict[str, str]:
    """The version, commit and branch of a Ribasim core, from the TOML block in its `README.md`.

    The commit and branch are GitHub URLs. The version alone does not identify a build of a branch.
    """
    readme = ribasim_home / "README.md"
    assert readme.is_file(), f"No README.md in the Ribasim core at {ribasim_home}"
    match = re.search(r"^```toml\n(.*?)^```", readme.read_text(encoding="utf-8"), re.DOTALL | re.MULTILINE)
    assert match, f"No TOML block in {readme}"
    info = tomllib.loads(match.group(1))
    missing = [key for key in CORE_INFO_KEYS if not isinstance(info.get(key), str)]
    assert not missing, f"{readme} does not record {missing}"
    return {key: info[key] for key in CORE_INFO_KEYS}


def run_model_and_control(
    model: "Model",
    ribasim_toml: Path,
    *,
    model_exec: bool,
    qlr_path: Path,
    authority: str | None = None,
    scenario: str | None = None,
) -> "Model":
    """Write a model and optionally run, export, and validate its result metrics."""
    model.write(ribasim_toml)
    if not model_exec:
        return model

    from ribasim_nl.controle_output import STATIC_FORCING_METRICS, Control
    from ribasim_nl.model import Model
    from ribasim_nl.steady_state_regression import allowed_non_steady_state_node_ids

    model.run()
    control = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
    control.run(metrics=STATIC_FORCING_METRICS)
    control.validate_result_conditions(
        allowed_non_steady_state_node_ids=allowed_non_steady_state_node_ids(authority, scenario)
    )
    return Model.read(ribasim_toml)


def write_steady_state_scenarios(
    model: "Model",
    authority: str,
    short_name: str,
    *,
    dry_evaporation_mm_per_day: float,
    model_exec: bool,
    qlr_path: Path,
    dry_period: tuple[datetime, datetime] | None = None,
    wet_period: tuple[datetime, datetime] | None = None,
    fill_missing_level_boundaries: bool = False,
) -> "Model":
    """Write, and optionally run and check, the dry and wet steady state scenarios, and then the full control model.

    The scenarios get constant forcing, in this order: evaporation in the dry scenario, 2 mm/day precipitation in the
    wet scenario, and 1.5 mm/day precipitation in the full control model. The foreign inflows get their summer,
    winter and overall median flow rate, see `ribasim_nl.transboundary_inflow`. See `run_model_and_control`.

    :param model: model of the vrij afwaterend water authority
    :param authority: water authority, the folder of its models
    :param short_name: name of the TOML files
    :param dry_evaporation_mm_per_day: evaporation in the dry scenario [mm/day]
    :param model_exec: run the scenarios and check their results
    :param qlr_path: QGIS layer definition for `output_controle`
    :param dry_period: start and end time of the dry scenario, defaults to the time of the model
    :param wet_period: start and end time of the wet scenario and the full control model, defaults to the time of
        the model
    :param fill_missing_level_boundaries: set the missing levels of LevelBoundaries, see
        `topology.fill_missing_level_boundary_static_levels`
    :return: the full control model
    """
    from ribasim_nl.cloud import CloudStorage
    from ribasim_nl.parametrization.basin_tables import update_basin_static
    from ribasim_nl.topology import fill_missing_level_boundary_static_levels
    from ribasim_nl.transboundary_inflow import set_constant_inflow

    model_dir = CloudStorage().joinpath(authority, "modellen")
    scenarios = (
        (
            "dry",
            {"evaporation_mm_per_day": dry_evaporation_mm_per_day},
            "constant_summer",
            dry_period,
            "steady_state_dry",
        ),
        ("wet", {"precipitation_mm_per_day": 2}, "constant_winter", wet_period, "steady_state_wet"),
        (None, {"precipitation_mm_per_day": 1.5}, "constant_global", None, "full_control_model"),
    )
    for scenario, forcing, inflow, period, post_fix in scenarios:
        update_basin_static(model=model, **forcing)
        set_constant_inflow(model, authority, inflow)
        if period is not None:
            model.starttime, model.endtime = period
        if fill_missing_level_boundaries:
            fill_missing_level_boundary_static_levels(model)
        model = run_model_and_control(
            model,
            model_dir / f"{authority}_{post_fix}" / f"{short_name}.toml",
            model_exec=model_exec,
            qlr_path=qlr_path,
            authority=authority,
            scenario=scenario,
        )
    return model


def parse_computation_time(line: str) -> timedelta | None:
    match = re.search(
        r"Computation time:\s*"
        r"(?:(\d+)\s+hour?,\s*)?"
        r"(?:(\d+)\s+minutes?,\s*)?"
        r"(?:(\d+)\s+seconds?,\s*)?"
        r"(?:(\d+)\s+milliseconds?)?",
        line,
    )
    if match:
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0
        milliseconds = int(match.group(4)) if match.group(4) else 0
        return timedelta(
            hours=hours,
            minutes=minutes,
            seconds=seconds,
            milliseconds=milliseconds,
        )
    return None


def run(
    toml_path: Path,
    threads: int = 1,
) -> RunSpecs:
    """To run a Ribasim model

    Args:
        toml_path (Path): path to your ribasim toml-file
        threads (int): number of threads passed to the Ribasim CLI

    """
    toml_path = Path(toml_path)
    assert threads >= 1, f"threads must be at least 1, got {threads}"

    if not toml_path.exists():
        raise FileNotFoundError(f"{toml_path} does not exist!")

    cli = _find_cli()
    args = [cli.as_posix(), f"--threads={threads}", toml_path.absolute().as_posix()]

    proc = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
        encoding="utf-8",
    )

    with proc:
        assert proc.stdout is not None
        # Reconfigure stdout to replace unencodable characters (e.g. cp1252 on Windows)
        if hasattr(sys.stdout, "reconfigure"):
            cast(Any, sys.stdout).reconfigure(errors="replace")
        was_simulating = False
        computation_time = None
        term_width = shutil.get_terminal_size((80, 20)).columns
        for line in proc.stdout:
            if "Simulating" in line:
                print(end="\r")
                print("\r" + " " * term_width, end="\r")  # Clear current line
                print(line.rstrip(), end="\r")  # Allow progress bar to stay on one line
                was_simulating = True
            else:
                if was_simulating:
                    print()
                    was_simulating = False
                if "Computation time" in line:
                    computation_time = parse_computation_time(line)
                print(line, end="")  # Standard line
            sys.stdout.flush()  # Flush to Jupyter        outs = None

    return RunSpecs(proc.returncode, computation_time)
