# %%
import re
import shutil
import subprocess
import sys
from collections import namedtuple
from datetime import timedelta
from pathlib import Path

from ribasim.cli import _find_cli

from ribasim_nl.settings import settings

RunSpecs = namedtuple("RunSpecs", ["exit_code", "simulation_time"])


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
    ribasim_home: str | Path | None = settings.ribasim_home,
) -> RunSpecs:
    """To run a Ribasim model

    Args:
        toml_path (Path): path to your ribasim toml-file
        ribasim_home (str | Path | None): path to Ribasim installation directory

    """
    toml_path = Path(toml_path)

    if not toml_path.exists():
        raise FileNotFoundError(f"{toml_path} does not exist!")

    cli = _find_cli(ribasim_home=ribasim_home)
    args = [cli.as_posix(), toml_path.absolute().as_posix()]

    proc = subprocess.Popen(
        args,
        cwd=toml_path.parent.as_posix(),
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
            sys.stdout.reconfigure(errors="replace")
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
