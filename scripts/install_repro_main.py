"""Install a systemd user timer that runs `pixi run repro-main` every night, see `scripts/repro_main.py`.

Run `pixi run install-repro-main` in the main checkout or any worktree. This:

1. creates a worktree dedicated to the catch-up runs, next to the main checkout, and marks it in its git directory,
   so the service refuses to run in any other checkout. It is set up with `pixi run setup-worktree`, and its
   copy of `.env` is made readable by its owner only;
2. writes the service, its timer, and a service that shows a desktop notification if it fails, to
   `~/.config/systemd/user`, and enables the timer.

The service runs as the user, without new privileges, and with private file permissions. It runs the code of
`origin/main`, which only changes through reviewed pull requests, with the credentials of the user: the MinIO key in
`.env`, `gh`, and `claude`, which `repro_main.py` only allows to read files. For a GitHub token that can do less than
the one of `gh`, put a fine-grained token for Deltares/Ribasim-NL with read and write access to contents and pull
requests in `~/.config/ribasim-nl/repro-main.env`, readable by the user only:

    GH_TOKEN=github_pat_...

The timer only fires while the user is logged in, unless lingering is enabled with `loginctl enable-linger`.
Then the `gh` credentials in the keyring are locked, so a token as above is needed.

Usage: pixi run install-repro-main [--worktree PATH] [--time HH:MM] [--uninstall]
"""

import argparse
import re
import shutil
import subprocess
from pathlib import Path

from repro_main import MARKER, TOKEN_FILE

UNIT = "ribasim-nl-repro-main"
UNIT_DIR = Path.home() / ".config/systemd/user"


def git(*args: str, cwd: Path) -> str:
    """Run a git command and return its stripped output."""
    return subprocess.run(["git", *args], check=True, capture_output=True, text=True, cwd=cwd).stdout.strip()


def which(name: str) -> Path:
    """The absolute path of a command, asserted to exist."""
    path = shutil.which(name)
    assert path is not None, f"{name} is not on the PATH"
    return Path(path)


def create_worktree(main: Path, worktree: Path) -> None:
    """Create and set up the worktree for the catch-up runs, detached at origin/main, if it does not exist."""
    if worktree.exists():
        git_dir = Path(git("rev-parse", "--absolute-git-dir", cwd=worktree))
        assert (git_dir / MARKER).is_file(), f"{worktree} exists, but is not a worktree for repro-main"
        print(f"Keeping worktree {worktree}")
        return
    git("fetch", "--quiet", "origin", "main", cwd=main)
    git("worktree", "add", "--detach", str(worktree), "origin/main", cwd=main)
    git_dir = Path(git("rev-parse", "--absolute-git-dir", cwd=worktree))
    (git_dir / MARKER).write_text("This worktree is reset to origin/main by the repro-main service.\n")
    subprocess.run(["pixi", "run", "setup-worktree"], check=True, cwd=worktree)
    print(f"Created worktree {worktree}")


def service_unit(worktree: Path, git_dir: Path, path: str) -> str:
    """The service, which resets the worktree to origin/main and runs `pixi run repro-main` in it."""
    git_exe = which("git")
    return f"""[Unit]
Description=Reproduce Ribasim-NL main and open a pull request with the run
OnFailure={UNIT}-failed.service

[Service]
Type=oneshot
WorkingDirectory={worktree}
Environment=PATH={path}
EnvironmentFile=-{TOKEN_FILE}
# Only reset a worktree that is marked for this
ExecStartPre=/usr/bin/test -f {git_dir / MARKER}
ExecStartPre={git_exe} fetch --quiet origin main
ExecStartPre={git_exe} checkout --quiet --force --detach origin/main
ExecStart={which("pixi")} run repro-main
NoNewPrivileges=yes
UMask=0077
Nice=10
TimeoutStartSec=12h
"""


def timer_unit(time: str) -> str:
    """The timer that starts the service every day at a time."""
    return f"""[Unit]
Description=Reproduce Ribasim-NL main every night

[Timer]
OnCalendar=*-*-* {time}:00
Persistent=false

[Install]
WantedBy=timers.target
"""


def failed_unit() -> str:
    """A service that shows a desktop notification that the run failed."""
    return f"""[Unit]
Description=Notify that reproducing Ribasim-NL main failed

[Service]
Type=oneshot
ExecStart={which("notify-send")} --urgency=critical "Reproducing Ribasim-NL main failed" "journalctl --user -u {UNIT}"
"""


def systemctl(*args: str) -> None:
    """Run a systemctl command for the user manager."""
    subprocess.run(["systemctl", "--user", *args], check=True)


def install(main: Path, worktree: Path, time: str) -> None:
    """Create the worktree, and install and enable the timer."""
    assert re.fullmatch(r"([01]\d|2[0-3]):[0-5]\d", time), f"Not a time HH:MM: {time}"
    create_worktree(main, worktree)
    env = worktree / ".env"
    assert env.is_file(), f"No credentials in {env}"
    env.chmod(0o600)
    if TOKEN_FILE.exists():
        TOKEN_FILE.chmod(0o600)
    git_dir = Path(git("rev-parse", "--absolute-git-dir", cwd=worktree))

    # The commands that the service runs, and the system directories
    dirs = [str(which(name).parent) for name in ("pixi", "gh", "claude", "git")]
    path = ":".join(dict.fromkeys([*dirs, "/usr/local/bin", "/usr/bin"]))
    UNIT_DIR.mkdir(parents=True, exist_ok=True)
    units = {
        f"{UNIT}.service": service_unit(worktree, git_dir, path),
        f"{UNIT}.timer": timer_unit(time),
        f"{UNIT}-failed.service": failed_unit(),
    }
    for name, text in units.items():
        (UNIT_DIR / name).write_text(text)
        print(f"Wrote {UNIT_DIR / name}")
    systemctl("daemon-reload")
    systemctl("enable", "--now", f"{UNIT}.timer")
    print(f"Enabled the timer, see `systemctl --user list-timers {UNIT}.timer`")
    print(f"Run it now with `systemctl --user start --no-block {UNIT}`, follow it with `journalctl --user -fu {UNIT}`")


def uninstall() -> None:
    """Disable and remove the timer and its services, keeping the worktree."""
    subprocess.run(["systemctl", "--user", "disable", "--now", f"{UNIT}.timer"], check=False)
    for name in (f"{UNIT}.service", f"{UNIT}.timer", f"{UNIT}-failed.service"):
        (UNIT_DIR / name).unlink(missing_ok=True)
    systemctl("daemon-reload")
    print("Removed the timer. Remove the worktree with `git worktree remove --force <path>`")


def main() -> None:
    """Install or uninstall the timer."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--worktree", type=Path, help="the worktree for the runs, next to the main checkout by default")
    parser.add_argument("--time", default="01:13", help="the time to start every night, HH:MM, 01:13 by default")
    parser.add_argument("--uninstall", action="store_true", help="disable and remove the timer")
    args = parser.parse_args()
    if args.uninstall:
        uninstall()
        return
    main_checkout = Path(git("rev-parse", "--path-format=absolute", "--git-common-dir", cwd=Path.cwd())).parent
    worktree = (args.worktree or main_checkout.parent / f"{main_checkout.name}-repro-main").resolve()
    install(main_checkout, worktree, args.time)


if __name__ == "__main__":
    main()
