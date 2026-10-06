import json
import threading
import urllib.request
from http.server import ThreadingHTTPServer

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import ribasim_nl.webmap_diff
import ribasim_nl.webmap_local
from ribasim import Model, Node
from ribasim.nodes import basin, level_boundary, tabulated_rating_curve
from ribasim_nl.webmap import export_webmap
from ribasim_nl.webmap_diff import compare_config, compare_features, diff_models, node_digests
from ribasim_nl.webmap_local import export_cached, make_handler, parse_range
from shapely.geometry import LineString, MultiPolygon, Point, box


def _write_model(path, changed: bool):
    """A small model; the changed version differs in several ways from the base version."""
    model = Model(
        starttime="2020-01-01", endtime="2020-01-03", crs="EPSG:28992", solver={"abstol": 1e-5 if changed else 1e-6}
    )
    basin_node = model.basin.add(
        Node(1, Point(150_000, 450_000)),
        [
            basin.Profile(area=[10.0, 100.0], level=[0.0, 2.0 if changed else 1.0]),
            basin.State(level=[0.5]),
            basin.Area(geometry=[MultiPolygon([box(149_900, 449_900, 150_050 if changed else 150_000, 450_100)])]),
        ],
    )
    rating = model.tabulated_rating_curve.add(
        Node(2, Point(150_100, 450_000), name="weir" if changed else ""),
        [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])],
    )
    boundary_id = 4 if changed else 3
    boundary = model.level_boundary.add(
        Node(boundary_id, Point(150_200, 450_050)), [level_boundary.Static(level=[0.0])]
    )
    model.link.add(basin_node, rating, link_id=20 if changed else 10)
    model.link.add(rating, boundary, link_id=12 if changed else 11)
    toml = path / "model.toml"
    model.write(toml)
    return toml


def test_diff_models(tmp_path):
    base_toml = _write_model(tmp_path / "base", changed=False)
    head_toml = _write_model(tmp_path / "head", changed=True)
    base_dir, head_dir = tmp_path / "base_webmap", tmp_path / "head_webmap"
    export_webmap(base_toml, None, base_dir)
    export_webmap(head_toml, None, head_dir)
    output = tmp_path / "diff" / "diff.json"
    diff = diff_models(base_toml, head_toml, base_dir, head_dir, output)

    assert json.loads(output.read_text()) == diff
    nodes = diff["nodes"]
    assert nodes["1"] == {"status": "changed", "changes": ["Basin / profile", "Basin / area"]}
    assert nodes["2"] == {"status": "changed", "changes": ["name"]}
    assert nodes["3"]["status"] == "removed"
    assert nodes["4"]["status"] == "added"
    # Links are matched on their nodes, so the link to the replaced boundary is replaced as well,
    # and the renumbered link is unchanged
    assert diff["links"] == {"12": {"status": "added", "changes": []}, "-11": {"status": "removed", "changes": []}}
    assert {"key": "solver.abstol", "base": "1e-06", "head": "1e-05"} in diff["config"]

    # A model compared with itself has no differences
    same = diff_models(base_toml, base_toml, base_dir, base_dir, tmp_path / "same.json")
    assert same["nodes"] == {} and same["links"] == {} and same["config"] == []


def test_compare_features_renumbered():
    def links(ids, names):
        return gpd.GeoDataFrame(
            {"link_id": ids, "from_node_id": [1, 2, 3], "to_node_id": [2, 3, 4], "name": names},
            geometry=[LineString([(0, 0), (1, 0)])] * 3,
        )

    diff, removed = compare_features(
        links([1, 2, 3], ["a", "b", "c"]), links([7, 8, 3], ["a", "x", "c"]), "link_id", ["from_node_id", "to_node_id"]
    )
    assert diff == {8: {"status": "changed", "changes": ["name"], "base_id": 2}}
    assert removed == {}


def test_node_digests_across_batches(tmp_path, monkeypatch):
    df = pd.DataFrame({"node_id": np.repeat([1, 2, 3], [5, 1, 6]), "value": np.arange(12.0)})
    path = tmp_path / "table.parquet"
    df.to_parquet(path)
    expected = node_digests(path, ["value"])
    monkeypatch.setattr(ribasim_nl.webmap_diff, "TABLE_BATCH_ROWS", 2)
    batched = node_digests(path, ["value"])
    pd.testing.assert_frame_equal(batched, expected)
    assert batched["rows"].tolist() == [5, 1, 6]

    # Row order within a node matters
    swapped = df.copy()
    swapped.loc[[0, 1], "value"] = swapped.loc[[1, 0], "value"].to_numpy()
    swapped.to_parquet(path)
    changed = node_digests(path, ["value"])
    assert (changed["digest"] != expected["digest"]).tolist() == [True, False, False]


def test_compare_config():
    assert compare_config({"solver": {"abstol": 1}, "a": 1}, {"solver": {"abstol": 2}, "b": 1}) == [
        {"key": "a", "base": "1", "head": None},
        {"key": "b", "base": None, "head": "1"},
        {"key": "solver.abstol", "base": "1", "head": "2"},
    ]


def test_parse_range():
    assert parse_range("bytes=0-9", 100) == (0, 9)
    assert parse_range("bytes=90-", 100) == (90, 99)
    assert parse_range("bytes=95-200", 100) == (95, 99)
    assert parse_range("bytes=-10", 100) == (90, 99)
    assert parse_range("bytes=100-", 100) is None
    assert parse_range("bytes=0-1,5-6", 100) is None


def test_export_cached(tmp_path, monkeypatch):
    monkeypatch.setattr(ribasim_nl.webmap_local.settings, "ribasim_nl_data_dir", tmp_path / "missing")
    toml = _write_model(tmp_path / "model", changed=False)
    output = export_cached(toml, tmp_path / "cache")
    manifest = output / "manifest.json"
    assert "waterboards" not in json.loads(manifest.read_text())["files"]
    mtime = manifest.stat().st_mtime_ns
    assert export_cached(toml, tmp_path / "cache") == output
    assert manifest.stat().st_mtime_ns == mtime


def _get(url: str, headers: dict[str, str] | None = None):
    """GET a URL of the local test server."""
    assert url.startswith("http://127.0.0.1:"), url
    return urllib.request.urlopen(urllib.request.Request(url, headers=headers or {}))  # noqa: S310


def test_server_range_requests(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "file.parquet").write_bytes(bytes(range(100)))
    (tmp_path / "secret.txt").write_text("secret")
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler({"data": tmp_path / "data"}, "<html></html>"))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_address[1]}"
    try:
        with _get(f"{url}/") as response:
            assert response.read() == b"<html></html>"
        with _get(f"{url}/data/file.parquet?v=abc", {"Range": "bytes=10-19"}) as response:
            assert response.status == 206
            assert response.headers["Content-Range"] == "bytes 10-19/100"
            assert response.read() == bytes(range(10, 20))
        with _get(f"{url}/data/file.parquet") as response:
            assert response.read() == bytes(range(100))
        for path in ["/data/../secret.txt", "/data/%2e%2e/secret.txt", "/other/file.parquet"]:
            with pytest.raises(urllib.error.HTTPError, match="404"):
                _get(url + path)
    finally:
        server.shutdown()
        server.server_close()
