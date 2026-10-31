"""Reproduce `main` and open a catch-up pull request with the run, see `docs/dev/pull-requests.qmd`.

This runs unattended from the systemd user timer that `pixi run install-repro-main` installs. Its service checks
out `origin/main` in a worktree dedicated to this, and then runs `pixi run repro-main` there, which:

1. Asserts that it runs in that worktree, at `origin/main`, without changes.
2. Handles the open catch-up pull request, if there is one. If `dvc.lock` on `main` changed since its run, it
   is closed, since pull requests keep the `dvc.lock` of `main`. Otherwise this stops until it is merged.
3. Stops if another run of `pixi run repro-main` holds the lock of the worktree, or if this commit of `main` was
   attempted before. Runs of `pixi run repro-local` for other pull requests do not stop it.
4. Removes the data of the previous run, and runs `pixi run repro-local --incremental --push --no-publish`.
   Like in a new worktree, data that is not in the workspace does not count as changed. With `--full` it runs
   all stages instead, for instance when the previous run used the wrong environment.
5. If `dvc.lock` changed, commits it with the metrics to a new branch, pushes it, and opens a draft pull request.
   Then it publishes the model viewer preview, and has Claude describe the effect of the run with the
   `describe-run` skill, using only tools that read files. The pull request is then marked as ready for review.

Merging the pull request is left to a person.

Usage: pixi run repro-main [--force] [--full] [--no-describe]
"""

import argparse
import fcntl
import json
import os
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import TextIO

LOCKED = ["dvc.lock", "data/Rijkswaterstaat/metrics/lhm_coupled.json"]
TITLE = "Reproduce the pipeline on top of main"
# Catch-up branches are named by the time of the run, and are the only branches this pushes to
BRANCH_PATTERN = re.compile(r"repro-main-\d{8}-\d{4}")
# Created by `install_repro_main.py` in the git directory of the worktree, which is not part of the work tree
MARKER = "repro-main"
# Held in the git directory of the worktree during a run, so that runs of main do not overlap
LOCK = "repro-main.lock"
STATE_FILE = Path(os.environ.get("XDG_STATE_HOME", Path.home() / ".local/state")) / "ribasim-nl/repro-main.json"
# The optional GitHub token of the service, see `install_repro_main.py`
TOKEN_FILE = Path.home() / ".config/ribasim-nl/repro-main.env"
# Environment variables with secrets, which Claude does not get, and which must not end up in the pull request
SECRETS = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "RIBASIM_NL_CLOUD_PASS", "GH_TOKEN", "GITHUB_TOKEN"]
# Claude can only read files, and not those with credentials
CLAUDE_SETTINGS = {
    "permissions": {
        "deny": [
            "Read(./.env)",
            "Read(./.dvc/config.local)",
            "Read(~/.config/**)",
            "Read(~/.aws/**)",
            "Read(~/.claude/**)",
            "Read(~/.ssh/**)",
        ]
    }
}
CLAUDE_TIMEOUT = 30 * 60


def log(message: str) -> None:
    """Print a timestamped message, which ends up in the journal of the service."""
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


def run(*args: str, cwd: Path, input: str | None = None) -> str:
    """Run a command and return its stripped output, failing if it fails."""
    return subprocess.run(args, check=True, capture_output=True, text=True, cwd=cwd, input=input).stdout.strip()


def git(*args: str, cwd: Path) -> str:
    """Run a git command and return its stripped output."""
    return run("git", *args, cwd=cwd)


def check_worktree(root: Path) -> str:
    """Assert this is the worktree made for this, at `origin/main` without changes; return its commit."""
    git_dir = Path(git("rev-parse", "--absolute-git-dir", cwd=root))
    assert (git_dir / MARKER).is_file(), f"{root} is not the worktree of `pixi run install-repro-main`"
    head = git("rev-parse", "HEAD", cwd=root)
    main = git("rev-parse", "origin/main", cwd=root)
    assert head == main, f"HEAD {head} is not origin/main {main}, the service checks that out first"
    status = git("status", "--porcelain", cwd=root)
    assert not status, f"The worktree has changes:\n{status}"
    return head


def check_private(path: Path) -> None:
    """Assert that a file with secrets, if it exists, is only accessible by its owner."""
    if path.exists():
        mode = path.stat().st_mode
        assert mode & 0o077 == 0, f"{path} is accessible by others, run `chmod 600 {path}`"


def read_state() -> dict:
    """The main commit of the previous attempt and its outcome, or an empty dict."""
    return json.loads(STATE_FILE.read_text()) if STATE_FILE.exists() else {}


def write_state(commit: str, status: str, pr: int | None = None) -> None:
    """Record the outcome of an attempt for a main commit."""
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    state = {"commit": commit, "status": status, "pr": pr, "time": datetime.now().isoformat(timespec="seconds")}
    STATE_FILE.write_text(json.dumps(state, indent=2) + "\n")


def branch_name(time: datetime) -> str:
    """The branch of a catch-up run started at a time."""
    name = f"repro-main-{time:%Y%m%d-%H%M}"
    assert BRANCH_PATTERN.fullmatch(name)
    return name


def open_catch_up_prs(root: Path) -> list[dict]:
    """The open pull requests of catch-up branches."""
    prs = json.loads(
        run("gh", "pr", "list", "--state", "open", "--base", "main", "--json", "number,headRefName", cwd=root)
    )
    return [pr for pr in prs if BRANCH_PATTERN.fullmatch(pr["headRefName"])]


def superseded(root: Path, base: str) -> bool:
    """Whether `dvc.lock` or the metrics on `origin/main` changed since a catch-up run on commit `base`."""
    diff = subprocess.run(["git", "diff", "--quiet", base, "origin/main", "--", *LOCKED], cwd=root, check=False)
    assert diff.returncode in (0, 1), f"git diff failed for base {base}"
    return diff.returncode == 1


def handle_open_pr(root: Path) -> bool:
    """Close the open catch-up pull request if `main` superseded it; return whether one remains open."""
    prs = open_catch_up_prs(root)
    assert len(prs) <= 1, f"Expected at most one open catch-up pull request, found {prs}"
    if not prs:
        return False
    number, branch = prs[0]["number"], prs[0]["headRefName"]
    git("fetch", "--quiet", "origin", branch, cwd=root)
    # The pull request has a single commit on top of the main commit that it reproduced
    base = git("rev-parse", "FETCH_HEAD^", cwd=root)
    if not superseded(root, base):
        log(f"Waiting until catch-up pull request #{number} is merged")
        return True
    comment = "`dvc.lock` on `main` changed since this run, so it is kept, see `docs/dev/pull-requests.qmd`."
    run("gh", "pr", "close", str(number), "--delete-branch", "--comment", comment, cwd=root)
    log(f"Closed catch-up pull request #{number}, since dvc.lock on main changed since its run")
    return False


def lock_worktree(root: Path) -> TextIO | None:
    """Lock the worktree until this process exits; return the lock file, or None if another run holds it."""
    git_dir = Path(git("rev-parse", "--absolute-git-dir", cwd=root))
    file = (git_dir / LOCK).open("w")
    try:
        fcntl.flock(file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        file.close()
        return None
    return file


def remove_data(root: Path) -> None:
    """Remove the ignored files in `data`, which hold the data of the previous run."""
    git("clean", "-d", "--force", "-X", "--quiet", "--", "data", cwd=root)


def changed_files(root: Path) -> list[str]:
    """The files that differ from HEAD, also untracked ones."""
    changed = git("diff", "--name-only", "HEAD", cwd=root).splitlines()
    untracked = git("ls-files", "--others", "--exclude-standard", cwd=root).splitlines()
    return changed + untracked


def repro_command(full: bool) -> list[str]:
    """The command of the run: only the changed stages and their downstream, or with `full` all stages."""
    return ["pixi", "run", "repro-local", *([] if full else ["--incremental"]), "--push", "--no-publish"]


def context(root: Path, base: str, branch: str, command: str) -> str:
    """The inputs for the description of the run: the pull requests it adds, and the metrics diff."""
    previous = git("log", "-1", "--format=%H", base, "--", "dvc.lock", cwd=root)
    added = git("log", "--format=- %s", f"{previous}..{base}", cwd=root) or "(none)"
    metrics = run("pixi", "run", "dvc", "metrics", "diff", "--md", "--all", base, cwd=root)
    return (
        f"# Catch-up run {branch}\n\n"
        f"Branch `{branch}` reproduces `main` at commit {base} with `{command}`.\n"
        f"The previous run on `main` was recorded in commit {previous}.\n\n"
        f"## Commits on `main` since the previous run\n\n{added}\n\n"
        f"## Metrics compared with the previous run\n\n"
        f"Output of `pixi run dvc metrics diff --md --all {base}`:\n\n{metrics}\n"
    )


def describe(root: Path, context_file: Path) -> str:
    """Have Claude describe the run with the `describe-run` skill, allowing it only to read files."""
    prompt = (
        "/describe-run This is an unattended catch-up run on main, without access to commands. "
        f"The pull requests it adds and the metrics are in {context_file}, the results in "
        "data/Rijkswaterstaat/runs/lhm_coupled. Print only the pull request description."
    )
    args = [
        "claude",
        "--print",
        "--tools",
        "Read,Grep,Glob",
        "--permission-mode",
        "default",
        "--permission-prompts",
        "none",
        "--setting-sources",
        "project",
        "--settings",
        json.dumps(CLAUDE_SETTINGS),
        "--strict-mcp-config",
        prompt,
    ]
    env = {key: value for key, value in os.environ.items() if key not in SECRETS}
    result = subprocess.run(args, cwd=root, env=env, capture_output=True, text=True, timeout=CLAUDE_TIMEOUT, check=True)
    return result.stdout.strip()


def check_no_secrets(text: str) -> None:
    """Assert that no secret from the environment is in a text that is published."""
    for name in SECRETS:
        value = os.environ.get(name, "")
        assert len(value) < 8 or value not in text, f"The text contains the value of {name}"


def publish(root: Path, base: str, branch: str, command: str, describe_run: bool) -> int:
    """Commit and push the run, open its pull request and describe it; return its number."""
    unexpected = set(changed_files(root)) - set(LOCKED)
    assert not unexpected, f"The run changed other files than {LOCKED}: {sorted(unexpected)}"
    assert BRANCH_PATTERN.fullmatch(branch), f"Not a catch-up branch: {branch}"

    git("switch", "--quiet", "--create", branch, cwd=root)
    git("add", "--", *LOCKED, cwd=root)
    git("commit", "--quiet", "--message", f"{TITLE}\n\nA catch-up run of main at {base}.", cwd=root)
    git("push", "--quiet", "--set-upstream", "origin", branch, cwd=root)

    context_text = context(root, base, branch, command)
    body = f"{context_text}\nThe description of the effect follows."
    url = run(
        "gh", "pr", "create", "--draft", "--base", "main", "--head", branch, "--title", TITLE,
        "--body-file", "-", cwd=root, input=body,
    )  # fmt: skip
    number = int(url.rstrip("/").rsplit("/", 1)[1])
    log(f"Opened {url}")

    if subprocess.run(["pixi", "run", "publish-webmap"], cwd=root, check=False).returncode != 0:
        log("Publishing the model viewer preview failed, run `pixi run publish-webmap` on the branch")

    if describe_run:
        context_file = root / "logs/repro-main" / f"{branch}.md"
        context_file.parent.mkdir(parents=True, exist_ok=True)
        context_file.write_text(f"{context_text}\nThe pull request of the run is #{number}.\n")
        try:
            description = describe(root, context_file)
            assert description, "Claude returned an empty description"
            check_no_secrets(description)
            run("gh", "pr", "edit", str(number), "--body-file", "-", cwd=root, input=description)
            log("Described the effect of the run")
        except (subprocess.SubprocessError, AssertionError) as e:
            log(f"Describing the run failed, the pull request lists the metrics only: {e!r}")

    run("gh", "pr", "ready", str(number), cwd=root)
    return number


def main() -> int:
    """Reproduce main if needed, and return the exit code."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--force", action="store_true", help="also run if this commit of main was attempted before")
    parser.add_argument("--full", action="store_true", help="run all stages, not only the changed ones")
    parser.add_argument("--no-describe", action="store_true", help="do not have Claude describe the run")
    args = parser.parse_args()

    root = Path(git("rev-parse", "--show-toplevel", cwd=Path.cwd()))
    base = check_worktree(root)
    check_private(TOKEN_FILE)
    check_private(root / ".env")
    # Kept open until the process exits, which releases the lock
    lock = lock_worktree(root)
    if lock is None:
        log("pixi run repro-main runs already, trying again next time")
        return 0

    if handle_open_pr(root):
        return 0
    state = read_state()
    if state.get("commit") == base and not args.force:
        log(f"main {base[:10]} was attempted before, with outcome {state.get('status')}; use --force to rerun")
        return 0

    log(f"Reproducing main {base[:10]}")
    branch = branch_name(datetime.now())
    remove_data(root)
    repro = repro_command(args.full)
    if subprocess.run(repro, cwd=root, check=False).returncode != 0:
        write_state(base, "failed")
        log(f"The run failed, see the logs in {root / 'logs/repro'}")
        return 1
    if not changed_files(root):
        write_state(base, "up to date")
        log("main is up to date")
        return 0

    try:
        # Without --no-publish, since this publishes the preview itself
        command = " ".join(arg for arg in repro if arg != "--no-publish")
        number = publish(root, base, branch, command, describe_run=not args.no_describe)
    except BaseException:
        # Not run again for this commit, since the outputs may already be pushed
        write_state(base, f"publishing {branch} failed")
        raise
    finally:
        # Leave the worktree detached, without the branch checked out; the service checks out main again
        subprocess.run(["git", "switch", "--quiet", "--detach", base], cwd=root, check=False)
    write_state(base, "opened", number)
    return 0


if __name__ == "__main__":
    sys.exit(main())
