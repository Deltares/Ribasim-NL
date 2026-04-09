# file taken from https://github.com/Deltares/Ribasim-NL/tree/main/src/ribasim_nl/ribasim_nl
# %%

import os
import subprocess
import sys
from collections import namedtuple
from pathlib import Path

RunSpecs = namedtuple("RunSpecs", ["exit_code"])


def run_delwaq(dimr_config: Path, run_dimr_bat: Path) -> RunSpecs:
    """To run a Delwaq model within Python

    Parameters
    ----------
    dimr_config : Path
        Path to dimr_config.xml
    run_dimr_bat : Path
        Path to run_dimr.bat part of DHydro installation

    Returns
    -------
    RunSpecs
        Specification of run, for now only exit_code
    """
    dimr_config = dimr_config.absolute().resolve()
    run_dimr_bat = run_dimr_bat.absolute().resolve()
    env = os.environ.copy()
    args = [run_dimr_bat.as_posix(), dimr_config.as_posix()]

    proc = subprocess.Popen(
        args,
        cwd=dimr_config.parent.as_posix(),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
        universal_newlines=True,
        encoding="utf-8",
    )

    with proc:
        for line in proc.stdout:
            print(line, end="")  # Standard line
            sys.stdout.flush()  # Flush to Jupyter        outs = None

    return RunSpecs(proc.returncode)
