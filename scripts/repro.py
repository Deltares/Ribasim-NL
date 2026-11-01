"""Reproduce DVC stages in parallel on a single machine, the local counterpart of `repro.sh` for SLURM.

Like `repro.sh`, this runs the commands of the stages directly instead of via `dvc repro`, since
concurrent `dvc repro` processes contend for the DVC lock. The stages and their commands are taken
from `dvc.yaml`:

1. Select the given target stages and all their upstream stages, skipping frozen stages and
   `.dvc` files. Without `--incremental` all of them are run, like `repro.sh`. With `--incremental`
   only the stages that changed compared with `dvc.lock`, and everything downstream of them, are run.
   Data that is not in the workspace does not count as a change, so this also works in a fresh git worktree.
2. Check that the Ribasim core and credentials are installed, see `pixi run setup-worktree` for worktrees.
   `pixi run repro-local` first installs the core of `install_ribasim_core.py`, unless it is installed already.
   Then prepare the workspace with `clean.py`: remove the outputs of the selected stages and pull
   everything upstream of them.
3. Run the stages as soon as their upstream stages have succeeded, with at most `--jobs` at a time.
   Each stage logs to `logs/repro/<timestamp>/<stage>.log`. If a stage fails, its downstream stages
   are skipped while the others continue.
4. `dvc commit` the stages that succeeded. If all succeeded, run `pixi run check`, and with
   `--push` also `dvc push` them.
5. If `export_webmap` ran, publish the model to the model viewer as a preview of the pull request of this
   branch, compared with main, see `ribasim_nl.webmap_publish`. Use `--no-publish` to skip this.

Usage: pixi run repro-local [TARGET ...] [--jobs N] [--stagger SECONDS] [--incremental] [--push] [--no-publish]
"""

import argparse
import os
import subprocess
import sys
import time
from collections.abc import Callable, Collection, Iterable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from pathlib import Path

import networkx as nx
from clean import clean
from dvc.repo import Repo
from dvc.stage import PipelineStage, Stage

# The final stages of the pipeline: the coupled model, its results, their regression checks and their export for the
# model viewer. `repro.sh` reproduces the pipeline up to `koppelen`.
DEFAULT_TARGETS = ["validate_lhm_coupled", "export_webmap"]
PUBLISHED_STAGE = "export_webmap"


class Status(StrEnum):
    """Outcome of a stage."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass(frozen=True)
class Result:
    """Outcome and wall-clock duration of a stage."""

    status: Status
    seconds: float = 0.0


def select_stages[T](graph: nx.DiGraph, targets: Iterable[T], runnable: Callable[[T], bool]) -> set[T]:
    """Return the targets and all their runnable upstream stages.

    Edges point from a stage to the stages it depends on, like `Repo.index.graph`.
    """
    targets = list(targets)
    assert targets, "No targets given"
    for target in targets:
        assert target in graph, f"Unknown target {target}"
        assert runnable(target), f"Target {target} cannot be run, it is frozen or has no command"
    upstream = set().union(*(nx.descendants(graph, target) for target in targets))
    return set(targets) | {stage for stage in upstream if runnable(stage)}


# Statuses of a dependency or output that is not in the workspace, which `clean.py` pulls at its locked version
MISSING_STATUSES = {"deleted", "not in cache"}


def changed(stage: Stage) -> bool:
    """Whether a stage changed compared with `dvc.lock`, ignoring data that is not in the workspace.

    Unlike `Stage.changed`, a fresh worktree without data does not make every stage changed. A changed
    upstream stage still selects this stage, see `with_downstream`.
    """
    if stage.always_changed or stage.changed_stage():
        return True
    entries = (*stage.deps, *stage.outs)
    return any(status not in MISSING_STATUSES for entry in entries for status in entry.status().values())


def check_environment(root: Path) -> None:
    """Fail before removing any outputs if the Ribasim core or the credentials are missing."""
    ribasim_home = Path(os.environ.get("RIBASIM_HOME", root / "bin/ribasim"))
    cli = ribasim_home / "bin" / ("ribasim.exe" if os.name == "nt" else "ribasim")
    hint = "In a git worktree run `pixi run setup-worktree`, otherwise"
    assert cli.is_file(), f"No Ribasim core at {cli}. {hint} `pixi run install-core`"
    assert (root / ".env").is_file(), f"No credentials in {root / '.env'}. {hint} add them"


def with_downstream[T](graph: nx.DiGraph, selected: Collection[T], changed: Iterable[T]) -> set[T]:
    """Return the changed stages and their downstream stages, limited to the selected stages."""
    changed = set(changed)
    assert changed <= set(selected), f"Changed stages are not selected: {changed - set(selected)}"
    downstream = set().union(*(nx.ancestors(graph, stage) for stage in changed))
    return changed | (downstream & set(selected))


def dependencies[T](graph: nx.DiGraph, selected: Collection[T]) -> dict[T, set[T]]:
    """Map each selected stage to the selected stages it depends on, also indirectly via other stages."""
    return {stage: nx.descendants(graph, stage) & set(selected) for stage in selected}


def run_dag[T](
    deps: Mapping[T, Collection[T]],
    run_stage: Callable[[T], bool],
    jobs: int,
    stagger: float = 0.0,
    key: Callable[[T], str] = str,
) -> dict[T, Result]:
    """Run stages in parallel, each once all its dependencies succeeded.

    A stage is skipped if any of its dependencies failed or was skipped. Stages are started at
    least `stagger` seconds apart, and ready stages are started in order of `key`.
    """
    assert jobs >= 1, f"jobs must be at least 1, got {jobs}"
    assert stagger >= 0, f"stagger must be non-negative, got {stagger}"
    for stage, stage_deps in deps.items():
        assert stage not in stage_deps, f"Stage {stage} depends on itself"
        assert set(stage_deps) <= deps.keys(), f"Dependencies of {stage} are not all scheduled"

    def timed(stage: T) -> Result:
        start = time.monotonic()
        try:
            ok = run_stage(stage)
        except Exception as e:
            print(f"Stage {key(stage)} raised {e!r}", flush=True)
            ok = False
        return Result(Status.SUCCEEDED if ok else Status.FAILED, time.monotonic() - start)

    results: dict[T, Result] = {}
    running: dict[Future[Result], T] = {}
    last_start = -float("inf")

    with ThreadPoolExecutor(max_workers=jobs) as executor:
        while len(results) < len(deps):
            for stage in sorted(deps.keys() - results.keys(), key=key):
                dep_status = {results[dep].status for dep in deps[stage] if dep in results}
                if dep_status - {Status.SUCCEEDED}:
                    results[stage] = Result(Status.SKIPPED)

            pending = deps.keys() - results.keys() - set(running.values())
            ready = sorted((stage for stage in pending if all(dep in results for dep in deps[stage])), key=key)
            if not ready and not running:
                assert len(results) == len(deps), "No stage is running or ready, the dependencies contain a cycle"
                break

            # Start one stage at a time, so finished stages are collected while waiting for the stagger
            next_start = last_start + stagger
            if ready and len(running) < jobs and time.monotonic() >= next_start:
                last_start = time.monotonic()
                running[executor.submit(timed, ready[0])] = ready[0]
                continue

            timeout = max(0.0, next_start - time.monotonic()) if ready and len(running) < jobs else None
            if running:
                done, _ = wait(running, timeout=timeout, return_when=FIRST_COMPLETED)
            else:
                assert timeout is not None
                time.sleep(timeout)
                done = set()
            for future in done:
                results[running.pop(future)] = future.result()

    return results


def stage_command(stage: Stage) -> str:
    """Return the shell command of a stage, joining multiple commands to stop at the first failure."""
    assert stage.cmd, f"Stage {stage.addressing} has no command"
    cmds = [stage.cmd] if isinstance(stage.cmd, str) else list(stage.cmd)
    return " && ".join(f"( {cmd} )" for cmd in cmds)


def run_command(cmd: str, root: Path, log_path: Path) -> bool:
    """Run a shell command from the repository root, writing its output to a log file."""
    env = os.environ | {"PYTHONUTF8": "1"}
    with log_path.open("w", encoding="utf-8") as log:
        log.write(f"$ {cmd}\n")
        log.flush()
        process = subprocess.run(cmd, shell=True, cwd=root, env=env, stdout=log, stderr=subprocess.STDOUT)  # noqa: S602
    return process.returncode == 0


def log(message: str) -> None:
    """Print a timestamped message."""
    print(f"[{datetime.now():%H:%M:%S}] {message}", flush=True)


def format_duration(seconds: float) -> str:
    """Format seconds as H:MM:SS."""
    minutes, seconds = divmod(round(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    return f"{hours}:{minutes:02}:{seconds:02}"


def main() -> int:
    """Reproduce the stages given on the command line, and return the exit code."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("targets", nargs="*", default=DEFAULT_TARGETS, help="stages to reproduce with their upstream")
    parser.add_argument("--jobs", "-j", type=int, default=12, help="maximum number of stages to run at once")
    parser.add_argument("--stagger", type=float, default=30.0, help="minimum seconds between stage starts")
    parser.add_argument("--incremental", action="store_true", help="only run changed stages and their downstream")
    parser.add_argument("--push", action="store_true", help="dvc push the stages if all succeeded")
    parser.add_argument("--no-publish", action="store_true", help=f"do not publish the model if {PUBLISHED_STAGE} ran")
    args = parser.parse_args()

    repo = Repo()
    root = Path(repo.root_dir)
    graph = repo.index.graph
    by_name = {stage.addressing: stage for stage in graph}
    missing = set(args.targets) - by_name.keys()
    assert not missing, f"Unknown stages {sorted(missing)}"

    def runnable(stage: Stage) -> bool:
        return isinstance(stage, PipelineStage) and bool(stage.cmd) and not stage.frozen

    selected = select_stages(graph, (by_name[name] for name in args.targets), runnable)
    if args.incremental:
        with repo.lock:
            changed_stages = [stage for stage in selected if changed(stage)]
        selected = with_downstream(graph, selected, changed_stages)
    if not selected:
        log("All stages are up to date")
        return 0
    names = sorted(stage.addressing for stage in selected)
    log(f"Reproducing {len(names)} stages: {' '.join(names)}")

    check_environment(root)
    clean(names)

    log_dir = root / "logs/repro" / f"{datetime.now():%Y%m%d-%H%M%S}"
    log_dir.mkdir(parents=True)
    log(f"Logs in {log_dir}")

    def run_stage(stage: Stage) -> bool:
        name = stage.addressing
        log_path = log_dir / f"{name}.log"
        log(f"Start {name}")
        ok = run_command(stage_command(stage), root, log_path)
        log(f"{'Done' if ok else 'FAILED'} {name}, see {log_path}")
        return ok

    results = run_dag(
        dependencies(graph, selected), run_stage, jobs=args.jobs, stagger=args.stagger, key=lambda s: s.addressing
    )

    print("\n=== SUMMARY ===")
    for stage in sorted(results, key=lambda s: s.addressing):
        result = results[stage]
        print(f"{result.status:<10} {format_duration(result.seconds):>9}  {stage.addressing}")

    succeeded = sorted(stage.addressing for stage, result in results.items() if result.status == Status.SUCCEEDED)
    if succeeded:
        log(f"Committing {len(succeeded)} stages")
        subprocess.run(["dvc", "commit", "--force", *succeeded], cwd=root, check=True)

    if len(succeeded) < len(results):
        log(f"{len(results) - len(succeeded)} stages failed or were skipped, not running checks or pushing")
        return 1

    # Hooks that fix files, like trimming trailing whitespace in dvc.lock, fail the first run
    if subprocess.run(["pixi", "run", "check"], cwd=root).returncode != 0:
        subprocess.run(["pixi", "run", "check"], cwd=root, check=True)
    if args.push:
        log("Pushing")
        subprocess.run(["dvc", "push", *succeeded], cwd=root, check=True)
    if PUBLISHED_STAGE in succeeded and not args.no_publish:
        log("Publishing the model viewer preview")
        if subprocess.run(["pixi", "run", "publish-webmap"], cwd=root).returncode != 0:
            log("Publishing failed, fix the cause and run `pixi run publish-webmap`")
            return 1
    log("Finished")
    return 0


if __name__ == "__main__":
    sys.exit(main())
