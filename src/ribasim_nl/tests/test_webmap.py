import json

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import xarray as xr
from ribasim import Model, Node
from ribasim.nodes import basin, level_boundary, tabulated_rating_curve
from ribasim_nl.webmap import (
    NETCDF_NODES_PER_ROW_GROUP,
    downcast_int64,
    export_netcdf_table,
    export_webmap,
    file_hash,
    netcdf_tables,
    table_slug,
)
from shapely.geometry import MultiPolygon, Point, box


def _write_model(tmp_path):
    model = Model(starttime="2020-01-01", endtime="2020-01-03", crs="EPSG:28992")
    basin_node = model.basin.add(
        Node(1, Point(150_000, 450_000)),
        [
            basin.Profile(area=[10.0, 100.0], level=[0.0, 1.0]),
            basin.State(level=[0.5]),
            basin.Time(time=["2020-01-01", "2020-01-02"], precipitation=[0.0, 1e-3]),
            basin.Area(geometry=[MultiPolygon([box(149_900, 449_900, 150_050, 450_100)])]),
        ],
    )
    rating = model.tabulated_rating_curve.add(
        Node(2, Point(150_100, 450_000)), [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])]
    )
    boundary = model.level_boundary.add(Node(3, Point(150_200, 450_050)), [level_boundary.Static(level=[0.0])])
    model.link.add(basin_node, rating)
    model.link.add(rating, boundary)
    toml = tmp_path / "model" / "model.toml"
    model.write(toml)

    waterboards = tmp_path / "waterschap.gpkg"
    gpd.GeoDataFrame(
        {"naam": ["\n  Waterschap Test\n"], "code": ["99"]},
        geometry=[box(140_000, 440_000, 160_000, 460_000)],
        crs="EPSG:28992",
    ).to_file(waterboards)
    return toml, waterboards


def test_export_webmap(tmp_path):
    toml, waterboards = _write_model(tmp_path)
    output = tmp_path / "webmap"
    manifest = export_webmap(toml, waterboards, output)

    assert json.loads((output / "manifest.json").read_text()) == manifest
    assert manifest["node_types"] == {"Basin": 1, "LevelBoundary": 1, "TabulatedRatingCurve": 1}
    for entry in list(manifest["files"].values()) + manifest["tables"]:
        path = output / entry["path"]
        assert entry["hash"] == file_hash(path)
        assert entry["bytes"] == path.stat().st_size

    nodes = pq.read_table(output / "nodes.parquet")
    assert nodes["node_id"].to_pylist() == [1, 2, 3]
    assert nodes.schema.field("node_id").type == pa.int32()
    assert 3 < nodes["x"][0].as_py() < 8 and 50 < nodes["y"][0].as_py() < 54

    links = pq.read_table(output / "links.parquet")
    assert links["from_node_id"].to_pylist() == [1, 2]
    assert all(len(coords) == 4 for coords in links["coords"].to_pylist())

    tables = {table["name"]: table for table in manifest["tables"]}
    assert {"Basin / profile", "Basin / state", "Basin / time", "TabulatedRatingCurve / static"} <= set(tables)
    basin_time = pq.read_table(output / tables["Basin / time"]["path"])
    assert pa.types.is_timestamp(basin_time.schema.field("time").type)

    boards = gpd.read_file(output / "waterboards.geojson")
    assert boards["naam"].tolist() == ["Waterschap Test"]


def test_export_netcdf_table(tmp_path):
    node_ids = np.arange(20, 0, -1, dtype="int32")
    time = pd.date_range("2020-01-01", periods=3, freq="D")
    values = np.arange(len(time) * len(node_ids), dtype="float64").reshape(len(time), len(node_ids))
    source = tmp_path / "basin_time.nc"
    xr.Dataset({"drainage": (("time", "node_id"), values)}, coords={"time": time, "node_id": node_ids}).to_netcdf(
        source
    )

    path = tmp_path / "basin_time.parquet"
    assert export_netcdf_table(source, path) == values.size
    assert pq.ParquetFile(path).metadata.num_row_groups == int(np.ceil(len(node_ids) / NETCDF_NODES_PER_ROW_GROUP))
    df = pd.read_parquet(path)
    assert df["drainage"].dtype == "float32"
    assert df["node_id"].is_monotonic_increasing
    expected = values[:, list(node_ids).index(5)]
    np.testing.assert_array_equal(df.loc[df.node_id == 5, "drainage"], expected.astype("float32"))


def test_netcdf_tables(tmp_path):
    config = {"input_dir": "input", "basin": {"time": "basin_time.nc"}, "solver": {"abstol": 0.01}}
    assert netcdf_tables(config, tmp_path) == {"Basin / time": tmp_path / "basin_time.nc"}


def test_table_slug():
    assert table_slug("TabulatedRatingCurve / static") == "tabulated_rating_curve_static"


def test_downcast_int64():
    table = downcast_int64(pa.table({"small": pa.array([1, 2], pa.int64()), "large": pa.array([1, 2**40], pa.int64())}))
    assert table.schema.field("small").type == pa.int32()
    assert table.schema.field("large").type == pa.int64()
