"""Tests for the selection of what to delete from the MinIO bucket in scripts/prune_remote.py"""

import sys
from datetime import UTC, datetime
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent))
from prune_remote import (
    DELETE_ORDER,
    Deletion,
    Object,
    dvc_md5,
    export_md5,
    is_dvc_file,
    is_lock,
    md5s,
    plan_dvc,
    plan_webmap,
    with_dir_contents,
)

OLD = datetime(2026, 1, 1, tzinfo=UTC)
CUTOFF = datetime(2026, 6, 1, tzinfo=UTC)
NEW = datetime(2026, 7, 1, tzinfo=UTC)
WEBMAP = "doc-image/webmap/"
MODEL = f"{WEBMAP}lhm_coupled/"


def md5(char: str, suffix: str = "") -> str:
    """An md5 made of one character."""
    return char * 32 + suffix


def dvc_key(hash_: str) -> str:
    return f"dvc/files/md5/{hash_[:2]}/{hash_[2:]}"


def test_md5s():
    lock = {
        "schema": "2.0",
        "stages": {
            "rwzi": {
                "deps": [{"path": "a.py", "hash": "md5", "md5": md5("a"), "size": 1}],
                "outs": [{"path": "out", "hash": "md5", "md5": md5("b", ".dir"), "nfiles": 2}],
            }
        },
    }
    assert md5s(lock) == {md5("a"), md5("b", ".dir")}
    assert md5s({"outs": [{"path": "data.gpkg", "md5": md5("c")}]}) == {md5("c")}
    assert md5s(None) == set()
    with pytest.raises(AssertionError, match="Invalid md5"):
        md5s({"outs": [{"md5": "abc"}]})


def test_export_md5():
    out = {"path": "data/Rijkswaterstaat/webmap/lhm_coupled", "md5": md5("e", ".dir")}
    assert export_md5({"stages": {"export_webmap": {"outs": [out]}}}) == md5("e")
    assert export_md5({"stages": {"rwzi": {"outs": [out]}}}) is None
    assert export_md5({"outs": [out]}) is None


def test_paths():
    assert is_lock("dvc.lock") and is_lock("sub/dvc.lock") and not is_lock("dvc.lock.bak")
    assert is_dvc_file("data/a.gpkg.dvc") and not is_dvc_file(".dvcignore") and not is_dvc_file("dvc.lock")


def test_dvc_md5():
    assert dvc_md5(dvc_key(md5("a"))) == md5("a")
    assert dvc_md5(dvc_key(md5("b", ".dir"))) == md5("b", ".dir")
    with pytest.raises(AssertionError, match="Unexpected object"):
        dvc_md5("dvc/runs/abc")


def test_with_dir_contents():
    listings = {md5("d", ".dir"): [{"md5": md5("1"), "relpath": "a"}, {"md5": md5("2"), "relpath": "b"}]}
    read: list[str] = []

    def read_dir(hash_: str) -> list[dict]:
        read.append(hash_)
        return listings[hash_]

    hashes = {md5("d", ".dir"), md5("e", ".dir"), md5("3")}
    # A `.dir` that is not on the remote is not read
    on_remote = {md5("d", ".dir"), md5("3")}
    assert with_dir_contents(hashes, read_dir, on_remote) == hashes | {md5("1"), md5("2")}
    assert read == [md5("d", ".dir")]
    listings[md5("d", ".dir")].append({"md5": md5("f", ".dir"), "relpath": "c"})
    with pytest.raises(AssertionError, match="Invalid entry"):
        with_dir_contents(hashes, read_dir, on_remote)


def test_plan_dvc():
    objects = [
        Object(dvc_key(md5("a")), 1, OLD),
        Object(dvc_key(md5("b")), 2, OLD),
        Object(dvc_key(md5("c", ".dir")), 3, OLD),
        Object(dvc_key(md5("d")), 4, NEW),
    ]
    deletions = plan_dvc(objects, keep={md5("a")}, cutoff=CUTOFF)
    assert deletions == [Deletion("dvc file", objects[1]), Deletion("dvc directory", objects[2])]
    # A `.dir` goes before its files, so DVC never finds a `.dir` whose files are gone
    assert DELETE_ORDER.index("dvc directory") < DELETE_ORDER.index("dvc file")


def test_plan_webmap():
    kept, main, open_head, recent, old, base = (md5(c) for c in "abcdef")
    manifests = {
        kept: {"files": {"node": {"path": "1.parquet"}}, "tables": []},
        main: {"files": {"node": {"path": "2.parquet"}}, "tables": []},
        open_head: {"files": {"node": {"path": "3.parquet"}}, "tables": []},
        recent: {"files": {"node": {"path": "4.parquet"}}, "tables": []},
        old: {"files": {"node": {"path": "5.parquet"}}, "tables": [{"path": "1.parquet"}]},
        base: {"files": {"node": {"path": "6.parquet"}}, "tables": []},
    }
    records = {
        f"{MODEL}main.json": {"snapshot": main},
        f"{MODEL}prs/5.json": {"pr": 5, "head": {"snapshot": open_head}, "base": {"snapshot": base}},
        f"{MODEL}prs/3.json": {"pr": 3, "head": {"snapshot": old}, "base": {"snapshot": kept}},
        f"{MODEL}prs/4.json": {"pr": 4, "head": {"snapshot": kept}, "base": {"snapshot": main}},
    }
    records |= {f"{MODEL}snapshots/{md5_}/manifest.json": manifest for md5_, manifest in manifests.items()}
    modified = {recent: NEW}
    objects = [Object(key, 10, OLD) for key in records if "/snapshots/" not in key]
    objects += [Object(f"{MODEL}snapshots/{md5_}/manifest.json", 1, modified.get(md5_, OLD)) for md5_ in manifests]
    objects += [Object(f"{WEBMAP}files/{n}.parquet", 100, OLD) for n in range(1, 8)]
    objects += [Object(f"{WEBMAP}files/8.parquet", 100, NEW)]
    objects += [Object(f"{MODEL}diffs/{kept}-{old}.json", 10, OLD), Object(f"{MODEL}diffs/{main}-{kept}.json", 10, OLD)]

    deletions = plan_webmap(objects, {kept}, {5}, 0, records.__getitem__, CUTOFF)
    assert {(d.kind, d.obj.key) for d in deletions} == {
        ("snapshot", f"{MODEL}snapshots/{old}/manifest.json"),
        ("difference", f"{MODEL}diffs/{kept}-{old}.json"),
        ("pull request", f"{MODEL}prs/3.json"),
        ("viewer file", f"{WEBMAP}files/5.parquet"),
        # used by no snapshot, and not uploaded recently
        ("viewer file", f"{WEBMAP}files/7.parquet"),
    }

    # The last pull requests with a preview are kept with their snapshots and differences, also when closed
    assert plan_webmap(objects, {kept}, {5}, 2, records.__getitem__, CUTOFF) == deletions
    deletions = plan_webmap(objects, {kept}, set(), 3, records.__getitem__, CUTOFF)
    assert {d.obj.key for d in deletions} == {f"{WEBMAP}files/7.parquet"}

    with pytest.raises(AssertionError, match="Unknown object"):
        plan_webmap([*objects, Object(f"{MODEL}other.json", 1, OLD)], {kept}, {5}, 0, records.__getitem__, CUTOFF)
