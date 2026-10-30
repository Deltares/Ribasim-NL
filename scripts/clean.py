"""Prepare the workspace for running DVC stages outside of `dvc repro`, as repro.sh and repro.py do.

0. Check that each dependency of the given stages is tracked by git or by DVC, so it can be
   pulled. For instance a .dvc file in a gitignored directory is not seen by DVC.
1. Remove the outputs of the given stages, like `dvc repro` does before running a stage.
   Otherwise outputs are modified rather than recreated: e.g. GeoPackage layers written with
   `to_file` keep the old file layout, so their hash changes on every run even if the data does
   not, and stale files in output directories get committed.
2. Pull everything upstream of the given stages with `--force`, so the inputs exactly match
   the committed `.dvc` files and `dvc.lock`. Outputs of the given stages are not pulled,
   since they are regenerated anyway.

Usage: pixi run python scripts/clean.py STAGE [STAGE ...]
"""

import os
import sys

import networkx as nx
from dvc.repo import Repo
from dvc.stage import Stage


def untracked_deps(repo: Repo, stages: list[Stage]) -> list[str]:
    """Return the dependencies of the stages that are neither tracked by git nor an output of a DVC stage or .dvc file."""
    outs = {out.fs_path for out in repo.index.outs}
    untracked = []
    for stage in stages:
        for dep in stage.deps:
            path = dep.fs_path
            if any(path == out or path.startswith(out + os.sep) for out in outs):
                continue
            relpath = os.path.relpath(path, repo.root_dir)
            if not repo.scm.is_tracked(relpath):
                untracked.append(f"{relpath} of stage {stage.addressing}")
    return untracked


def clean(stage_names: list[str]) -> None:
    """Remove the outputs of the given DVC stages, and pull all their upstream data."""
    assert stage_names, "No stages given"
    repo = Repo()

    stages: list[Stage] = []
    for name in stage_names:
        collected = list(repo.stage.collect(name))
        assert len(collected) == 1, f"Expected one stage named {name}, got {collected}"
        stages.extend(collected)

    untracked = untracked_deps(repo, stages)
    assert not untracked, "Dependencies not tracked by git or DVC:\n" + "\n".join(untracked)

    for stage in stages:
        for out in stage.outs:
            # `dvc repro` keeps persistent outputs, removing them here would diverge from that
            assert not out.persist, f"Output {out} of stage {stage} is persistent"
            print(f"Removing {out} of stage {stage}")
            out.remove()

    # Edges point from a stage to the stages it depends on
    graph = repo.index.graph
    upstream = set().union(*(nx.descendants(graph, stage) for stage in stages)) - set(stages)
    targets = sorted(stage.addressing for stage in upstream)
    print(f"Pulling {len(targets)} upstream targets")
    repo.pull(targets=targets, force=True)


if __name__ == "__main__":
    clean(sys.argv[1:])
