"""Prepare the workspace for running DVC stages outside of `dvc repro`, as repro.sh does.

1. Remove the outputs of the given stages, like `dvc repro` does before running a stage.
   Otherwise outputs are modified rather than recreated: e.g. GeoPackage layers written with
   `to_file` keep the old file layout, so their hash changes on every run even if the data does
   not, and stale files in output directories get committed.
2. Pull everything upstream of the given stages with `--force`, so the inputs exactly match
   the committed `.dvc` files and `dvc.lock`. Outputs of the given stages are not pulled,
   since they are regenerated anyway.

Usage: pixi run python scripts/clean.py STAGE [STAGE ...]
"""

import sys

import networkx as nx
from dvc.repo import Repo
from dvc.stage import Stage


def clean(stage_names: list[str]) -> None:
    """Remove the outputs of the given DVC stages, and pull all their upstream data."""
    assert stage_names, "No stages given"
    repo = Repo()

    stages: list[Stage] = []
    for name in stage_names:
        collected = repo.stage.collect(name)
        assert len(collected) == 1, f"Expected one stage named {name}, got {collected}"
        stages.extend(collected)

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
