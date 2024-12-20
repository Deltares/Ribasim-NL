import os
import subprocess
from pathlib import Path

# TODO: add ribasim_exe so it can be used if ribasim is not part of env path
# TODO: check if ribasim is in path, stop if not and ribasim_exe is not provided
# TODO: raise FileNotFoundError if toml_path does not exist. User


def run(
    toml_path: Path,
    stream_output: bool = True,
    returncode: bool = True,
):
    """To run a Ribasim model

    Args:
        toml_path (Path): path to your ribasim toml-file
        stream_output (bool, optional): stream output in IDE. Defaults to False.
        returncode (bool, optional): return return code after running model. Defaults to True.

    """
    env = os.environ.copy()

    input = ""
    proc = subprocess.Popen(
        ["ribasim", toml_path.as_posix()],
        cwd=toml_path.parent.as_posix(),
        env=env,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        encoding="ascii",
    )
    if stream_output:
        with proc:
            proc.stdin.write(input)
            proc.stdin.close()
            for line in proc.stdout:
                print(line, end="")
        outs = None
    else:
        outs, _ = proc.communicate(input)

    if returncode:
        return proc.returncode
    else:
        return outs
