import hashlib
import io
import json
from pathlib import Path

import pytest
from ribasim_nl.webmap_publish import (
    COMMENT_MARKER,
    EXPORT_PATH,
    PREFIX,
    Store,
    branch_name,
    checkout_export,
    comment_body,
    commit_hash,
    diff_counts,
    export_hash,
    manifest_entries,
    pr_number,
    snapshot_name,
    store_manifest,
)

MD5 = "9a4179107b7b8453ffbe531376149250"


def _lock(md5: str) -> dict:
    out = {"path": EXPORT_PATH, "hash": "md5", "md5": md5, "size": 1, "nfiles": 1}
    return {"stages": {"export_webmap": {"cmd": "x", "deps": [], "outs": [out]}}}


def test_export_hash():
    assert export_hash(_lock(f"{MD5}.dir")) == MD5
    with pytest.raises(AssertionError):
        export_hash(_lock("not-a-hash.dir"))


def _entry(path: str, content_hash: str) -> dict:
    return {"path": path, "hash": content_hash, "bytes": 1}


def test_store_manifest():
    manifest = {
        "files": {"nodes": _entry("nodes.parquet", "aa"), "basin_area": _entry("basin_area.pmtiles", "bb")},
        "tables": [{"name": "Basin / time", **_entry("tables/basin_time.parquet", "cc")}],
        "results": {
            "basin": {
                "by_id": _entry("results/basin_by_id.parquet", "dd"),
                "by_time": _entry("results/b.parquet", "ee"),
            },
            "flow": {"by_id": _entry("results/flow_by_id.parquet", "ff"), "by_time": _entry("results/f.parquet", "ee")},
        },
    }
    stored, sources = store_manifest(manifest)
    assert stored["files"]["nodes"]["path"] == "aa.parquet"
    assert stored["tables"][0]["path"] == "cc.parquet"
    assert stored["tables"][0]["name"] == "Basin / time"
    # Files with the same content are stored once
    assert sources == {
        "aa.parquet": "nodes.parquet",
        "bb.pmtiles": "basin_area.pmtiles",
        "cc.parquet": "tables/basin_time.parquet",
        "dd.parquet": "results/basin_by_id.parquet",
        "ee.parquet": "results/b.parquet",
        "ff.parquet": "results/flow_by_id.parquet",
    }
    assert len(manifest_entries(stored)) == 7
    # The original is not changed
    assert manifest["files"]["nodes"]["path"] == "nodes.parquet"


def test_comment_body():
    diff = {
        "config": [{"key": "solver.reltol", "base": "1e-3", "head": "1e-4"}],
        "nodes": {"1": {"status": "added", "changes": []}, "2": {"status": "changed", "changes": ["name"]}},
        "links": {"-3": {"status": "removed", "changes": []}},
        "results": {"basin": {"differences": {"1": 0.1}}, "flow": {"differences": {}}},
        "results_note": None,
    }
    counts = diff_counts(diff)
    assert counts["nodes"] == {"added": 1, "changed": 1}
    assert counts["results"] == {"basin": 1, "flow": 0}
    body = comment_body(884, diff, "a" * 40, "b" * 40)
    assert body.startswith(COMMENT_MARKER)
    assert "https://ribasim.nl/viewer?pr=884" in body
    assert "| nodes | 1 | 0 | 1 |" in body
    assert "| links | 0 | 1 | 0 |" in body
    assert "1 Basins and 0 flow links" in body
    assert "`solver.reltol`" in body
    assert "`bbbbbbb` with `main` at `aaaaaaa`" in body

    stacked = comment_body(888, diff, "a" * 40, "b" * 40, base_pr=892)
    assert "with the differences from #892." in stacked
    assert "`bbbbbbb` with #892 at `aaaaaaa`" in stacked


def test_command_inputs():
    assert commit_hash("a" * 40) == "a" * 40
    assert pr_number(886) == 886
    for commit in ["--output=x", "main", "a" * 39, "A" * 40]:
        with pytest.raises(AssertionError):
            commit_hash(commit)
    for number in [0, -1]:
        with pytest.raises(AssertionError):
            pr_number(number)
    assert branch_name("pin-ribasim-core") == "pin-ribasim-core"
    assert branch_name("user/fix_1.2") == "user/fix_1.2"
    for branch in ["--upload-pack=x", "-x", "a..b", "a b", ""]:
        with pytest.raises(AssertionError):
            branch_name(branch)


class FakeResponse(io.BytesIO):
    def release_conn(self) -> None:
        pass


class FakeClient:
    """The objects of a MinIO bucket, by name."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        self.objects = objects
        self.downloads: list[str] = []

    def get_object(self, bucket: str, name: str) -> FakeResponse:
        return FakeResponse(self.objects[name])

    def fget_object(self, bucket: str, name: str, path: str) -> None:
        self.downloads.append(name)
        Path(path).write_bytes(self.objects[name])


def _short_hash(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()[:16]


def _fake_store(manifest: dict, files: dict[str, bytes]) -> tuple[Store, FakeClient]:
    """A store with one published snapshot `MD5`, without connecting to MinIO."""
    store = Store.__new__(Store)
    objects = {PREFIX + snapshot_name(MD5): json.dumps(manifest).encode()}
    objects |= {PREFIX + f"files/{name}": content for name, content in files.items()}
    client = FakeClient(objects)
    store.client = client  # ty: ignore[invalid-assignment]
    store.is_published = lambda md5: md5 == MD5  # ty: ignore[invalid-assignment]
    return store, client


def test_checkout_export(tmp_path: Path):
    nodes, results = b"nodes", b"results"
    nodes_name, results_name = f"{_short_hash(nodes)}.parquet", f"{_short_hash(results)}.parquet"
    manifest = {
        "files": {"nodes": _entry(nodes_name, _short_hash(nodes))},
        "tables": [],
        "results": {
            # The same content is stored once
            kind: {
                "by_id": _entry(results_name, _short_hash(results)),
                "by_time": _entry(nodes_name, _short_hash(nodes)),
            }
            for kind in ("basin", "flow")
        },
    }
    store, client = _fake_store(manifest, {nodes_name: nodes, results_name: results})
    export_dir = checkout_export(store, "a" * 40, MD5, cache_dir=tmp_path)
    assert json.loads((export_dir / "manifest.json").read_text()) == manifest
    assert (export_dir / nodes_name).read_bytes() == nodes
    assert (export_dir / results_name).read_bytes() == results
    assert sorted(client.downloads) == sorted(PREFIX + f"files/{name}" for name in (nodes_name, results_name))
    # The second time the cached export is used
    assert checkout_export(store, "a" * 40, MD5, cache_dir=tmp_path) == export_dir
    assert len(client.downloads) == 2


@pytest.mark.parametrize("name", ["../nodes.parquet", "nodes.parquet"])
def test_download_snapshot_checks(tmp_path: Path, name: str):
    content = b"nodes"
    manifest = {"files": {"nodes": _entry(name, _short_hash(content))}, "tables": []}
    store, _ = _fake_store(manifest, {name: content})
    with pytest.raises(AssertionError):
        store.download_snapshot(MD5, tmp_path)
    assert not (tmp_path / "manifest.json").exists()


def test_download_snapshot_hash(tmp_path: Path):
    name = f"{_short_hash(b'nodes')}.parquet"
    store, _ = _fake_store({"files": {"nodes": _entry(name, _short_hash(b"nodes"))}, "tables": []}, {name: b"other"})
    with pytest.raises(AssertionError, match="does not match its hash"):
        store.download_snapshot(MD5, tmp_path)
    assert not (tmp_path / "manifest.json").exists()
