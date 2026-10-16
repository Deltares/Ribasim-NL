import pytest
from ribasim_nl.webmap_publish import (
    COMMENT_MARKER,
    EXPORT_PATH,
    comment_body,
    diff_counts,
    export_hash,
    manifest_entries,
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
