"""Tests for the stage selection and scheduling of scripts/repro.py"""

import sys
import threading
from pathlib import Path

import networkx as nx
import pytest

sys.path.insert(0, str(Path(__file__).parent))
from repro import Result, Status, dependencies, run_dag, select_stages, with_downstream


def pipeline() -> nx.DiGraph:
    """Return a small pipeline, edges point from a stage to the stages it depends on.

    frozen <- a <- b <- merge <- final
         rwzi <- c <-/
    unrelated
    """
    graph = nx.DiGraph()
    graph.add_edges_from(
        [
            ("a", "frozen"),
            ("b", "a"),
            ("c", "rwzi"),
            ("merge", "b"),
            ("merge", "c"),
            ("final", "merge"),
        ]
    )
    graph.add_node("unrelated")
    return graph


def runnable(stage: str) -> bool:
    return stage != "frozen"


def test_select_stages():
    graph = pipeline()
    assert select_stages(graph, ["merge"], runnable) == {"a", "b", "c", "rwzi", "merge"}
    assert select_stages(graph, ["b", "c"], runnable) == {"a", "b", "c", "rwzi"}
    with pytest.raises(AssertionError, match="Unknown target"):
        select_stages(graph, ["missing"], runnable)
    with pytest.raises(AssertionError, match="cannot be run"):
        select_stages(graph, ["frozen"], runnable)


def test_with_downstream():
    graph = pipeline()
    selected = select_stages(graph, ["merge"], runnable)
    assert with_downstream(graph, selected, ["a"]) == {"a", "b", "merge"}
    assert with_downstream(graph, selected, ["merge"]) == {"merge"}
    assert with_downstream(graph, selected, []) == set()


def test_dependencies_through_unselected_stage():
    graph = pipeline()
    # b is not selected, but merge still has to wait for a
    assert dependencies(graph, {"a", "merge"}) == {"a": set(), "merge": {"a"}}


def test_run_dag_order_and_parallelism():
    graph = pipeline()
    selected = select_stages(graph, ["final"], runnable)
    order: list[str] = []
    lock = threading.Lock()
    running = 0
    max_running = 0

    def run_stage(stage: str) -> bool:
        nonlocal running, max_running
        with lock:
            running += 1
            max_running = max(max_running, running)
            order.append(stage)
        threading.Event().wait(0.01)
        with lock:
            running -= 1
        return True

    results = run_dag(dependencies(graph, selected), run_stage, jobs=2)
    assert {stage: result.status for stage, result in results.items()} == dict.fromkeys(selected, Status.SUCCEEDED)
    assert max_running <= 2
    for stage in selected:
        for dep in graph.successors(stage):
            if dep in selected:
                assert order.index(dep) < order.index(stage)


def test_run_dag_skips_downstream_of_failure():
    graph = pipeline()
    selected = select_stages(graph, ["final"], runnable)

    def run_stage(stage: str) -> bool:
        if stage == "a":
            return False
        if stage == "c":
            raise RuntimeError("boom")
        return True

    results = run_dag(dependencies(graph, selected), run_stage, jobs=3)
    assert {stage: result.status for stage, result in results.items()} == {
        "a": Status.FAILED,
        "rwzi": Status.SUCCEEDED,
        "c": Status.FAILED,
        "b": Status.SKIPPED,
        "merge": Status.SKIPPED,
        "final": Status.SKIPPED,
    }
    assert results["b"] == Result(Status.SKIPPED)


def test_run_dag_detects_cycle():
    with pytest.raises(AssertionError, match="cycle"):
        run_dag({"a": {"b"}, "b": {"a"}}, lambda _: True, jobs=1)


def test_run_dag_stagger_and_duration():
    def run_stage(stage: str) -> bool:
        threading.Event().wait(0.05)
        return True

    results = run_dag({"a": set(), "b": set(), "c": {"a"}}, run_stage, jobs=3, stagger=0.1)
    assert all(result.status == Status.SUCCEEDED for result in results.values())
    # Durations are measured in the worker, not including the stagger waits in the scheduler
    assert all(0.04 < result.seconds < 0.09 for result in results.values())
