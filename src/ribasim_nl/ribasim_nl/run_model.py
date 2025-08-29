# %%
import os
import re
import shutil
import subprocess
import sys
from collections import namedtuple
from datetime import timedelta
from pathlib import Path

# TODO: check if ribasim is in path, stop if not and ribasim_exe is not provided
# TODO: raise FileNotFoundError if toml_path does not exist. User
from ribasim_nl.settings import settings

RunSpecs = namedtuple("RunSpecs", ["exit_code", "simulation_time"])


def run(
    toml_path: Path,
    ribasim_exe: Path | None = settings.ribasim_exe,
):
    """To run a Ribasim model

    Args:
        toml_path (Path): path to your ribasim toml-file
        ribasim_exe (Path): path to ribasim exe-file

    """
    env = os.environ.copy()
    toml_path = Path(toml_path)

    if not toml_path.exists():
        raise FileNotFoundError(f"{toml_path} does not exist!")

    if ribasim_exe is not None:
        ribasim_exe = Path(ribasim_exe)
        # Check if ribasim_exe exists, or if it's in PATH by running '--version'
        if not ribasim_exe.exists():
            try:
                subprocess.run(
                    [ribasim_exe.as_posix(), "--version"],
                    capture_output=True,
                    check=True,
                )
            except Exception:
                raise FileNotFoundError(f"{ribasim_exe} does not exist and is not found in PATH!")
        args = [ribasim_exe.as_posix(), toml_path.absolute().as_posix()]
    else:
        args = ["ribasim", toml_path.absolute().as_posix()]

    proc = subprocess.Popen(
        args,
        cwd=toml_path.parent.as_posix(),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
        encoding="utf-8",
    )

    with proc:
        was_simulating = False
        computation_time = None
        term_width = shutil.get_terminal_size((80, 20)).columns
        for line in proc.stdout:
            if "Simulating" in line:
                print("", end="\r")
                print("\r" + " " * term_width, end="\r")  # Clear current line
                print(line.rstrip(), end="\r")  # Allow progress bar to stay on one line
                was_simulating = True
            else:
                if was_simulating:
                    print()
                    was_simulating = False
                if "Computation time" in line:
                    match = re.search(
                        r"Computation time:\s*"
                        r"(?:(\d+)\s+hour?,\s*)?"
                        r"(?:(\d+)\s+minutes?,\s*)?"
                        r"(?:(\d+)\s+seconds?,\s*)?"
                        r"(?:(\d+)\s+milliseconds?)?",
                        line,
                    )
                    hours = int(match.group(1)) if match.group(1) else 0
                    minutes = int(match.group(2)) if match.group(2) else 0
                    seconds = int(match.group(3)) if match.group(3) else 0
                    milliseconds = int(match.group(4)) if match.group(4) else 0
                    computation_time = timedelta(
                        hours=hours,
                        minutes=minutes,
                        seconds=seconds,
                        milliseconds=milliseconds,
                    )
                print(line, end="")  # Standard line
            sys.stdout.flush()  # Flush to Jupyter        outs = None

    return RunSpecs(proc.returncode, computation_time)
