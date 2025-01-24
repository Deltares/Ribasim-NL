import os
import subprocess
from pathlib import Path

# TODO: check if ribasim is in path, stop if not and ribasim_exe is not provided
# TODO: raise FileNotFoundError if toml_path does not exist. User


def run(
    toml_path: Path,
    ribasim_exe: Path | None = None,
    stream_output: bool = True,
    returncode: bool = True,
):
    """To run a Ribasim model

    Args:
        toml_path (Path): path to your ribasim toml-file
        ribasim_exe (Path): path to ribasim exe-file
        stream_output (bool, optional): stream output in IDE. Defaults to False.
        returncode (bool, optional): return return code after running model. Defaults to True.

    """
    env = os.environ.copy()

    toml_path = Path(toml_path)

    if not toml_path.exists():
        raise FileNotFoundError(f"{toml_path} does not exist!")

    # use exe_path if not None (and check if exists)
    if ribasim_exe is not None:
        ribasim_exe = Path(ribasim_exe)
        args = [ribasim_exe.as_posix(), toml_path.as_posix()]
        if not ribasim_exe.exists():
            raise FileNotFoundError(f"{ribasim_exe} does not exist!")
    else:
        args = ["ribasim", toml_path.as_posix()]

    input = ""
    proc = subprocess.Popen(
        args,
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
