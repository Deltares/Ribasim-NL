import json
import shutil
import subprocess
import threading
import urllib.request
from http.server import ThreadingHTTPServer

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import ribasim_nl.webmap_diff
import ribasim_nl.webmap_local
import xarray as xr
from ribasim import Model, Node
from ribasim.nodes import basin, level_boundary, tabulated_rating_curve
from ribasim_nl.webmap import export_webmap
from ribasim_nl.webmap_diff import (
    compare_config,
    compare_features,
    compare_results,
    diff_models,
    matched_ids,
    node_digests,
)
from ribasim_nl.webmap_local import checkout_model, export_cached, make_handler, parse_range, resolve_commit
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


def _write_results(toml, link_ids: list[int], changed: bool, level_offset: float, flow_scale: float) -> None:
    """Results of the small model: Basin #1, and links with the given ids from the Basin and to the boundary."""
    results_dir = toml.parent / "results"
    results_dir.mkdir()
    time = pd.date_range("2020-01-01", periods=3, freq="D")
    level = np.array([[0.5], [0.6], [0.7]]) + level_offset
    xr.Dataset(
        {"level": (("time", "node_id"), level, {"units": "m"}), "storage": (("time", "node_id"), 10.0 * level)},
        coords={"time": time, "node_id": [1]},
    ).to_netcdf(results_dir / "basin.nc")
    boundary_id = 4 if changed else 3
    xr.Dataset(
        {
            "flow_rate": (("time", "link_id"), np.full((3, len(link_ids)), 10.0) * flow_scale, {"units": "m3 s-1"}),
            "from_node_id": (("link_id",), [1, 2]),
            "to_node_id": (("link_id",), [2, boundary_id]),
        },
        coords={"time": time, "link_id": link_ids},
    ).to_netcdf(results_dir / "flow.nc")


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

    assert diff["results"] is None
    assert diff["base"]["label"] == diff["head"]["label"] == "model"

    # A model compared with itself has no differences
    same = diff_models(base_toml, base_toml, base_dir, base_dir, tmp_path / "same.json", ("main", "head"))
    assert same["nodes"] == {} and same["links"] == {} and same["config"] == []
    assert same["base"]["label"] == "main" and same["head"]["label"] == "head"


def test_diff_models_results(tmp_path):
    base_toml = _write_model(tmp_path / "base", changed=False)
    head_toml = _write_model(tmp_path / "head", changed=True)
    # The Basin to weir link is renumbered from 10 to 20, and its flow changes by 0.1 %, within the tolerance
    _write_results(base_toml, [10, 11], changed=False, level_offset=0.0, flow_scale=1.0)
    _write_results(head_toml, [20, 12], changed=True, level_offset=0.25, flow_scale=1.001)
    base_dir, head_dir = tmp_path / "base_webmap", tmp_path / "head_webmap"
    export_webmap(base_toml, None, base_dir)
    export_webmap(head_toml, None, head_dir)
    results = diff_models(base_toml, head_toml, base_dir, head_dir, tmp_path / "diff.json")["results"]

    assert results["basin"]["variable"] == "level"
    assert results["basin"]["units"] == "m"
    assert results["basin"]["compared"] == 1
    assert results["basin"]["differences"] == {1: pytest.approx(0.25)}
    # Link 12 is added, so only the renumbered link is compared
    assert results["flow"]["compared"] == 1
    assert results["flow"]["differences"] == {}

    # Results of another network, here with swapped link ids, are outdated and not compared
    shutil.rmtree(head_toml.parent / "results")
    _write_results(head_toml, [12, 20], changed=True, level_offset=0.25, flow_scale=1.0)
    diff = diff_models(base_toml, head_toml, base_dir, head_dir, tmp_path / "diff.json")
    assert diff["results"] is None
    assert diff["results_note"] == "The results of the head model are outdated: their links differ from the model"


def test_compare_results():
    time = pd.date_range("2020-01-01", periods=3, freq="D")
    base = pd.DataFrame({1: [1.0, 1.0, 1.0], 2: [5.0, 5.0, 5.0], 3: [2.0, 2.0, np.nan], 4: [0.0, 0.0, 0.0]}, time)
    head = pd.DataFrame(
        {1: [1.0, 1.0005, 1.0], 2: [5.0, 4.5, 5.2], 7: [2.0, np.nan, np.nan], 4: [0.0, 0.0, 0.0]},
        # A later time step is not compared
        time[:2].append(pd.DatetimeIndex(["2021-01-01"])),
    )
    # Head feature 7 is base feature 3, head feature 4 is not in the base model
    base_ids = pd.Series([1, 2, 3], index=[1, 2, 7])
    result = compare_results(base, head, base_ids, atol=1e-3, rtol=0.0)
    assert result == {"compared": 3, "differences": {2: -0.5, 7: None}}
    # A relative tolerance relative to the largest magnitude of the feature
    assert compare_results(base, head, base_ids, atol=0.0, rtol=0.2)["differences"] == {7: None}


def test_matched_ids():
    base = pd.DataFrame({"link_id": [1, 2, 3], "from_node_id": [1, 2, 3], "to_node_id": [2, 3, 4]})
    head = pd.DataFrame({"link_id": [9, 1], "from_node_id": [2, 1], "to_node_id": [3, 5]})
    key = ["from_node_id", "to_node_id"]
    assert matched_ids(base, head, "link_id", key).to_dict() == {9: 2}
    assert matched_ids(base, base, "link_id", ["link_id"]).to_dict() == {1: 1, 2: 2, 3: 3}


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


def _git(repo, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def test_checkout_model(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "--quiet")
    subprocess.run(["dvc", "init", "--quiet"], cwd=repo, check=True)
    toml = _write_model(repo / "model", changed=False)
    _write_results(toml, [10, 11], changed=False, level_offset=0.0, flow_scale=1.0)
    subprocess.run(["dvc", "add", "--quiet", "model"], cwd=repo, check=True)
    _git(repo, "add", ".")
    _git(repo, "-c", "user.name=test", "-c", "user.email=test@example.com", "commit", "--quiet", "-m", "model")
    commit = resolve_commit("HEAD", repo)
    assert commit is not None and len(commit) == 40
    assert resolve_commit("no-such-branch", repo) is None
    assert resolve_commit("--help", repo) is None
    original = toml.read_text()
    toml.write_text(f"{original}\n# changed in the working tree\n")

    cache = tmp_path / "cache"
    target = checkout_model(toml, commit, repo, cache)
    assert target == cache / "revisions" / commit / "model" / "model.toml"
    assert target.read_text() == original
    assert (target.parent / "input" / "database.gpkg").is_file()
    assert sorted(path.name for path in (target.parent / "results").iterdir()) == ["basin.nc", "flow.nc"]
    # A second time the cached files are used
    (target.parent / "input" / "database.gpkg").unlink()
    assert checkout_model(toml, commit, repo, cache) == target
    assert not (target.parent / "input" / "database.gpkg").exists()
    with pytest.raises(AssertionError, match="not in the repository"):
        checkout_model(tmp_path / "other.toml", commit, repo, cache)


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
