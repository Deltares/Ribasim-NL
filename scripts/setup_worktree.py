"""Set up a git worktree of this repository for running DVC stages and tests, without copying data.

Run `pixi run setup-worktree` in a new worktree. This:

1. copies `.env`, with the credentials, from the main checkout if the worktree has none;
2. uses the DVC cache of the main checkout, so data is shared instead of downloaded again. DVC checks out
   data from the cache as reflinks where the filesystem supports them, and as copies otherwise;
3. runs `pixi run install-core`, which installs the Ribasim core in `bin/ribasim` with the credentials of step 1.
   Not `pixi run install`: its `prek install` would point the git hooks, shared by all worktrees, to this one.

No data is pulled here: `pixi run repro-local` and `scripts/clean.py` pull what the selected stages need.
In the main checkout this does nothing.

Usage: pixi run setup-worktree
"""

import shutil
import subprocess
from pathlib import Path


def git(*args: str) -> str:
    """Run a git command in this repository and return its stripped output."""
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True).stdout.strip()


def dvc_cache_dir(root: Path) -> Path:
    """Return the DVC cache directory that is configured in a checkout."""
    out = subprocess.run(["dvc", "cache", "dir"], check=True, capture_output=True, text=True, cwd=root).stdout
    return Path(out.strip())


def setup_worktree(root: Path, main: Path) -> None:
    """Share the credentials and DVC cache of the main checkout `main` with the worktree `root`."""
    env = root / ".env"
    if env.exists():
        print(f"Keeping {env}")
    elif (main / ".env").exists():
        shutil.copy2(main / ".env", env)
        print(f"Copied {main / '.env'} to {env}")
    else:
        print(f"No .env in {main}, add one with the credentials, see the docs on the cloud storage")

    cache = dvc_cache_dir(main)
    assert cache.is_dir(), f"DVC cache {cache} of the main checkout does not exist"
    if dvc_cache_dir(root).resolve() == cache.resolve():
        print(f"Already using DVC cache {cache}")
    else:
        subprocess.run(["dvc", "cache", "dir", "--local", str(cache)], check=True, cwd=root)
        print(f"Using DVC cache {cache}")


def main() -> None:
    """Set up the worktree this is run from."""
    root = Path(git("rev-parse", "--show-toplevel")).resolve()
    main = Path(git("rev-parse", "--path-format=absolute", "--git-common-dir")).resolve().parent
    if root == main:
        print(f"{root} is the main checkout, nothing to set up")
        return
    setup_worktree(root, main)
    # A new process, so the installer reads the copied .env
    subprocess.run(["pixi", "run", "install-core"], check=True, cwd=root)


if __name__ == "__main__":
    main()
