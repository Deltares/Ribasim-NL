import json
import re
import sqlite3
from contextlib import closing

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import xarray as xr
from ribasim import Model, Node
from ribasim.nodes import basin, level_boundary, tabulated_rating_curve
from ribasim_nl.goodness_of_fit import MIN_COMPARED_DAYS
from ribasim_nl.webmap import (
    LHM_SET,
    NETCDF_NODES_PER_ROW_GROUP,
    NUTRIENTS,
    SOURCE_SET,
    TIME_STEPS_PER_ROW_GROUP,
    TRACERS,
    checked_sets,
    color_domain,
    concentration_variable,
    derived_tracers,
    downcast_int64,
    export_basin_area,
    export_concentration,
    export_netcdf_table,
    export_nodes,
    export_observations,
    export_results,
    export_webmap,
    file_hash,
    lhm_boundary_labels,
    lhm_fractions,
    netcdf_tables,
    run_info,
    table_slug,
    tracer_sets,
    write_table,
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


def test_export_basin_area_storing_basins_first(tmp_path):
    """A storing Basin is below the flowing Basin it overlaps, whatever the order of the Basin / area table."""
    model = Model(starttime="2020-01-01", endtime="2020-01-03", crs="EPSG:28992")
    area = basin.Area(geometry=[MultiPolygon([box(149_900, 449_900, 150_050, 450_100)])])
    tables = [basin.Profile(area=[10.0, 100.0], level=[0.0, 1.0]), basin.State(level=[0.5]), area]
    for node_id, categorie in [(1, "doorgaand"), (2, "bergend"), (3, "hoofdwater"), (4, "bergend")]:
        model.basin.add(Node(node_id, Point(150_000 + node_id, 450_000), meta_categorie=categorie), tables)
    toml = tmp_path / "model.toml"
    model.write(toml)
    database = tmp_path / "input" / "database.gpkg"

    path = tmp_path / "basin_area.pmtiles"
    export_basin_area(database, export_nodes(database, tmp_path / "nodes.parquet"), path)
    assert gpd.read_file(path, layer="basin_area")["node_id"].tolist() == [2, 4, 1, 3]


def test_run_info(tmp_path):
    toml = tmp_path / "model.toml"
    results_dir = tmp_path / "results"
    assert run_info(toml, results_dir) == {}

    results_dir.mkdir()
    (results_dir / "ribasim.log").write_text(
        "┌ Info: Starting a Ribasim simulation at 2026-10-15T23:54:23.620.\n"
        "│   toml_path = model.toml\n"
        "┌ Info: The model finished successfully at 2026-10-16T01:28:57.134.\n"
        "└ @ Ribasim logging.jl:94\n"
    )
    assert run_info(toml, results_dir) == {"start": "2026-10-15T23:54:23.620", "end": "2026-10-16T01:28:57.134"}

    core = {"version": "2026.1.2-114-g954a73d", "commit": "https://x/954a73d126", "branch": "https://x/main"}
    (tmp_path / "runinfo.json").write_text(json.dumps({"commit": "abc123", "dirty": False, "core": core}))
    assert run_info(toml, results_dir) == {
        "start": "2026-10-15T23:54:23.620",
        "end": "2026-10-16T01:28:57.134",
        "commit": "abc123",
        "dirty": False,
        "core": core,
    }


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


def test_export_results(tmp_path):
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    time = pd.date_range("2020-01-01", periods=10, freq="D")
    node_ids = np.array([3, 1, 2], dtype="int32")
    level = np.arange(30, dtype="float64").reshape(10, 3)
    xr.Dataset(
        {"level": (("time", "node_id"), level, {"units": "m"}), "storage": (("time", "node_id"), level + 1.0)},
        coords={"time": time, "node_id": node_ids},
    ).to_netcdf(results_dir / "basin.nc")
    link_ids = np.array([10.0, 11.0])
    xr.Dataset(
        {
            "flow_rate": (("time", "link_id"), np.ones((10, 2))),
            "from_node_id": (("link_id",), np.array([1, 2], dtype="int32")),
        },
        coords={"time": time, "link_id": link_ids},
    ).to_netcdf(results_dir / "flow.nc")
    write_concentration(results_dir / "concentration.nc", node_ids, time, ["Continuity"])
    # Ribasim writes one row per control state change
    xr.Dataset(
        {
            "control_node_id": (("time",), np.array([8, 7, 8], dtype="int32")),
            "truth_state": (("time",), np.array(["T", "F", "F"])),
            "control_state": (("time",), np.array(["off", "on", "on"])),
        },
        coords={"time": pd.to_datetime(["2020-01-01", "2020-01-01", "2020-01-05"])},
    ).to_netcdf(results_dir / "control.nc")

    output = tmp_path / "webmap"
    output.mkdir()
    results = export_results(results_dir, output)

    assert len(results["times"]) == 10
    assert results["basin"]["count"] == 3
    assert results["basin"]["variables"]["level"]["units"] == "m"
    assert results["basin"]["variables"]["level_change"]["source"] == "level"
    assert set(results["flow"]["variables"]) == {"flow_rate"}
    assert list(results["concentration"]["variables"]) == ["Continuity"]
    assert pd.read_parquet(results["concentration"]["by_time"])["node_id"].tolist()[:3] == [1, 2, 3]
    assert pd.read_parquet(results["concentration"]["by_id"])["node_id"].iloc[0] == 1

    by_time = pq.ParquetFile(results["basin"]["by_time"])
    assert by_time.metadata.num_row_groups == int(np.ceil(10 / TIME_STEPS_PER_ROW_GROUP))
    df = by_time.read().to_pandas()
    assert df["node_id"].tolist()[:3] == [1, 2, 3]
    # Second time step, node 3 is the first column in the NetCDF
    assert df["level"].iloc[5] == level[1, 0]

    flow = pd.read_parquet(results["flow"]["by_id"])
    assert flow.columns.tolist() == ["link_id", "time", "flow_rate"]
    assert flow["link_id"].dtype == "int32"

    control = pd.read_parquet(results["control"])
    assert control.columns.tolist() == ["control_node_id", "time", "control_state"]
    assert control["control_node_id"].tolist() == [7, 8, 8]
    assert control["control_state"].tolist() == ["on", "off", "on"]
    assert control["control_node_id"].dtype == "int32"


def write_concentration(path, node_ids, time, substances):
    """Write a Ribasim `concentration.nc` where each value is the sum of the indices of its substance, time and node."""
    shape = (len(time), len(substances), len(node_ids))
    values = np.fromfunction(lambda t, s, n: s + t + n, shape, dtype="float64")
    xr.Dataset(
        {"concentration": (("time", "substance", "node_id"), values, {"units": "g m-3"})},
        coords={"time": time, "substance": substances, "node_id": node_ids},
    ).to_netcdf(path)
    return values


def test_export_concentration(tmp_path):
    source = tmp_path / "concentration.nc"
    time = pd.date_range("2020-01-01", periods=9, freq="D")
    # The values grow with the index of the substance, so Neerslag stays below Continuity, as a fraction does
    substances = ["Neerslag", "ResidenceTime", "Continuity", "NO3"]
    values = write_concentration(source, np.array([3, 1, 2], dtype="int32"), time, substances)

    ids = np.array([1, 2, 3], dtype="int32")
    by_id = tmp_path / "concentration_by_id.parquet"
    path = tmp_path / "concentration_by_time.parquet"
    times = time.to_numpy().astype("datetime64[ms]")
    rain = {"name": "rain", "tracers": ["Neerslag"], "boundaries": {}}
    missing = {"name": "missing", "tracers": ["Neerslag", "Rijn"], "boundaries": {}}
    set_ = export_concentration(source, ids, times, by_id, path, [rain, missing])

    assert set_["count"] == 3
    # Only the sets whose tracers are all in the results are kept
    assert set_["sets"] == [rain]
    # The nutrients first and sorted, then the tracers: Continuity, and those of the tracer sets
    assert list(set_["variables"]) == ["NO3", "Continuity", "Neerslag", "ResidenceTime"]
    variables = set_["variables"]
    assert variables["NO3"]["group"] == NUTRIENTS
    assert variables["NO3"]["units"] == "g m-3"
    assert {variables[name]["group"] for name in ["Continuity", "Neerslag", "ResidenceTime"]} == {TRACERS}
    assert variables["Neerslag"]["units"] == "-"
    assert variables["ResidenceTime"]["units"] == "d"
    # A fraction goes up to its highest value: the last time step, and the last of the three nodes in the file
    assert variables["Neerslag"]["domain"] == [0.0, values[:, 0].max()]
    # A nutrient goes up to its 98th percentile
    assert variables["NO3"]["domain"] == [0.0, pytest.approx(np.percentile(values[:, 3].astype("float32"), 98))]
    assert all(variable["scale"] == "linear" for variable in variables.values())

    by_time = pq.ParquetFile(path)
    assert by_time.metadata.num_row_groups == int(np.ceil(9 / TIME_STEPS_PER_ROW_GROUP))
    df = by_time.read().to_pandas()
    assert df["node_id"].tolist()[:3] == [1, 2, 3]
    assert df["NO3"].dtype == "float32"
    # Second time step, node 1 is the second column in the NetCDF
    assert df["Neerslag"].iloc[3] == values[1, 0, 1]
    # The residence time is written in seconds, and shown in days
    assert df["ResidenceTime"].iloc[3] == pytest.approx(values[1, 1, 1] / 86400)

    per_basin = pd.read_parquet(by_id)
    assert per_basin.columns.tolist() == ["node_id", "time", "Neerslag", "ResidenceTime", "Continuity", "NO3"]
    assert per_basin["node_id"].is_monotonic_increasing
    # Node 1 is the second column in the NetCDF
    np.testing.assert_array_equal(per_basin.loc[per_basin.node_id == 1, "Neerslag"], values[:, 0, 1].astype("float32"))
    # Only the id column has the statistics that find the row group of a Basin
    columns = pq.ParquetFile(by_id).metadata.row_group(0).to_dict()["columns"]
    assert [column["path_in_schema"] for column in columns if column["is_stats_set"]] == ["node_id"]


def test_concentration_variable_without_values_above_zero():
    """A substance that nothing reaches, or without values, still has a scale that can be drawn."""
    for values in (np.zeros(4, dtype="float32"), np.full(4, np.nan, dtype="float32")):
        for fraction in (False, True):
            assert concentration_variable("NO3", values, "g m-3", fraction)["domain"] == [0.0, 1.0]


def test_concentration_variable_keeps_sparse_substances_apart():
    """A fraction that reaches few Basins goes up to its highest value, not to a percentile near zero."""
    values = np.zeros(1000, dtype="float32")
    values[0] = 0.99
    assert concentration_variable("Noordzee", values, "g m-3", True)["domain"] == [0.0, pytest.approx(0.99)]


def test_lhm_fractions(tmp_path):
    """The fractions of the LHM are marked in the concentration tables of the model input, the nutrients are not."""
    database = tmp_path / "database.gpkg"
    with closing(sqlite3.connect(database)) as con:
        con.execute('CREATE TABLE "Basin / concentration" (node_id INTEGER, substance TEXT, meta_lhm_fraction BOOLEAN)')
        con.execute('CREATE TABLE "FlowBoundary / concentration" (node_id INTEGER, substance TEXT, meta_lhm_fraction)')
        con.execute('CREATE TABLE "LevelBoundary / concentration" (node_id INTEGER, substance TEXT)')
        con.executemany(
            'INSERT INTO "Basin / concentration" VALUES (?, ?, ?)', [(1, "Neerslag", 1), (2, "Neerslag", 1)]
        )
        con.executemany(
            'INSERT INTO "FlowBoundary / concentration" VALUES (?, ?, ?)', [(3, "Rijn", 1), (4, "NO3", None)]
        )
        con.execute('INSERT INTO "LevelBoundary / concentration" VALUES (5, "Noordzee")')
        con.commit()
    assert lhm_fractions(database) == {"Neerslag", "Rijn"}


def write_lhm_database(
    path, flow_boundary_rows, level_boundary_rows=(), basin_fractions=("Neerslag", "Drainage (doorgaand)")
):
    """Write the concentration tables of a model input with LHM fractions, like `assign_lhm_fractions`."""
    with closing(sqlite3.connect(path)) as con:
        con.execute('CREATE TABLE "Basin / concentration" (node_id INTEGER, substance TEXT, meta_lhm_fraction BOOLEAN)')
        con.executemany(
            'INSERT INTO "Basin / concentration" VALUES (?, ?, ?)', [(1, fraction, 1) for fraction in basin_fractions]
        )
        for table, rows in [("FlowBoundary", flow_boundary_rows), ("LevelBoundary", level_boundary_rows)]:
            con.execute(
                f'CREATE TABLE "{table} / concentration" '
                "(node_id INTEGER, substance TEXT, concentration REAL, meta_lhm_fraction BOOLEAN)"
            )
            # The table is one of the two boundary tables
            con.executemany(f'INSERT INTO "{table} / concentration" VALUES (?, ?, ?, ?)', rows)  # noqa: S608
        con.commit()


def test_tracer_sets(tmp_path):
    """The LHM fractions come first, as the default set, then the default tracers of the core."""
    database = tmp_path / "database.gpkg"
    write_lhm_database(
        database, [(4, "Rijn", 1.0, 1), (4, "NO3", 2.5, None), (3, "RWZI", 1.0, 1)], [(5, "Noordzee", 1.0, 1)]
    )
    node_types = {5: "LevelBoundary", 4: "FlowBoundary", 3: "FlowBoundary", 1: "Basin", 6: "LevelBoundary"}
    nodes = pd.DataFrame({"node_id": list(node_types), "node_type": list(node_types.values())})

    lhm, source = tracer_sets(database, nodes)
    assert lhm == {
        "name": LHM_SET,
        # Like `sort_lhm_fractions`: the initial water at the bottom, then drainage and RWZI, the rest, and rain on top
        "tracers": ["Initial", "Drainage (doorgaand)", "RWZI", "Noordzee", "Rijn", "Neerslag"],
        # LevelBoundary 6 has no LHM fraction, so its water has no label in this set
        "boundaries": {"3": "RWZI", "4": "Rijn", "5": "Noordzee"},
    }
    assert source == {
        "name": SOURCE_SET,
        "tracers": ["Initial", "Drainage", "SurfaceRunoff", "FlowBoundary", "LevelBoundary", "Precipitation"],
        "boundaries": {"3": "FlowBoundary", "4": "FlowBoundary", "5": "LevelBoundary", "6": "LevelBoundary"},
    }
    # Without LHM fractions only the default tracers of the core remain
    assert tracer_sets(tmp_path / "without_concentration.gpkg", nodes) == [source]


@pytest.mark.parametrize(
    ("basin_fractions", "derived"),
    [
        # Maaiveld labels the surface runoff of the secondary Basins, the rest of it is that of the primary Basins
        (["Neerslag", "Maaiveld"], {"Maaiveld (doorgaand)": {"tracer": "SurfaceRunoff", "minus": ["Maaiveld"]}}),
        # A model that labels it is not derived
        (["Neerslag", "Maaiveld", "Maaiveld (doorgaand)"], None),
        # Nor that of a model without Maaiveld, whose surface runoff has no label
        (["Neerslag"], None),
    ],
)
def test_tracer_sets_derive_lhm_fractions(tmp_path, basin_fractions, derived):
    """The LHM set has the LHM fractions that follow from the default tracers of the core, next to their parts."""
    database = tmp_path / "database.gpkg"
    write_lhm_database(database, [], basin_fractions=basin_fractions)
    lhm, _ = tracer_sets(database, pd.DataFrame({"node_id": [1], "node_type": ["Basin"]}))
    assert lhm.get("derived") == derived
    runoff = ["Maaiveld (doorgaand)", "Maaiveld"] if len(basin_fractions) > 1 else []
    assert lhm["tracers"] == ["Initial", *runoff, "Neerslag"]


def test_derived_tracers():
    """A derived tracer is a tracer minus the tracers that label part of its water."""
    runoff = np.array([[0.5, 0.25], [0.0, 1.0]], dtype="float32")
    data = {"SurfaceRunoff": runoff, "Maaiveld": np.array([[0.25, 0.25], [0.0, 0.5]], dtype="float32")}
    definition = {"tracer": "SurfaceRunoff", "minus": ["Maaiveld"]}
    lhm = {"name": "lhm", "tracers": ["Maaiveld (doorgaand)", "Maaiveld"], "boundaries": {}}
    sets, values = derived_tracers([lhm | {"derived": {"Maaiveld (doorgaand)": definition}}], data)
    assert sets == [lhm | {"derived": {"Maaiveld (doorgaand)": definition}}]
    np.testing.assert_array_equal(values["Maaiveld (doorgaand)"], [[0.25, 0.0], [0.0, 0.5]])

    # Results without the tracer have no derived tracer, and results with the derived tracer need none
    without = {"Maaiveld": data["Maaiveld"]}
    assert derived_tracers([lhm | {"derived": {"Maaiveld (doorgaand)": definition}}], without) == (
        [lhm | {"tracers": ["Maaiveld"]}],
        {},
    )
    native = data | {"Maaiveld (doorgaand)": runoff}
    assert derived_tracers([lhm | {"derived": {"Maaiveld (doorgaand)": definition}}], native) == ([lhm], {})

    # A part that is larger than the whole labels water that the tracer does not
    with pytest.raises(AssertionError, match=r"Maaiveld exceed SurfaceRunoff by 0\.5"):
        derived_tracers([lhm | {"derived": {"x": definition}}], data | {"Maaiveld": runoff + 0.5})


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([(3, "Rijn", 0.5, 1)], "Rijn of node 3 in FlowBoundary / concentration is 0.5"),
        ([(3, "Rijn", 1.0, 1), (3, "Maas", 1.0, 1)], "two"),
    ],
)
def test_lhm_boundary_labels_label_all_water(tmp_path, rows, message):
    """The LHM fraction of a boundary labels all its water: it has one, with a concentration of 1."""
    database = tmp_path / "database.gpkg"
    write_lhm_database(database, rows)
    with pytest.raises(AssertionError, match=re.escape(message)):
        lhm_boundary_labels(database)


def test_checked_sets():
    """A set is kept if the results have all its tracers, and it may not label any water twice."""
    half = np.full((2, 3), 0.5, dtype="float32")
    data = {"Continuity": np.ones((2, 3), dtype="float32"), "A": half, "B": half, "C": half}
    ab = {"name": "ab", "tracers": ["A", "B"], "boundaries": {}}
    assert checked_sets([ab, {"name": "ad", "tracers": ["A", "D"], "boundaries": {}}], data) == [ab]
    # Without Continuity the fractions of a set have no total
    assert checked_sets([ab], {"A": half, "B": half}) == []
    with pytest.raises(AssertionError, match=r"The tracers of abc exceed Continuity by 0\.5"):
        checked_sets([{"name": "abc", "tracers": ["A", "B", "C"], "boundaries": {}}], data)


def test_export_concentration_needs_the_basins_and_times_of_the_results(tmp_path):
    source = tmp_path / "concentration.nc"
    time = pd.date_range("2020-01-01", periods=3, freq="D")
    write_concentration(source, np.array([1, 2], dtype="int32"), time, ["Cl"])
    times = time.to_numpy().astype("datetime64[ms]")
    paths = tmp_path / "by_id.parquet", tmp_path / "by_time.parquet"

    with pytest.raises(AssertionError, match="Basins"):
        export_concentration(source, np.array([1, 3], dtype="int32"), times, *paths)
    with pytest.raises(AssertionError, match="time steps"):
        export_concentration(source, np.array([1, 2], dtype="int32"), times[:2], *paths)


def test_export_observations(tmp_path, observation_run, observed):
    database, results_dir = observation_run
    output = tmp_path / "webmap"
    (output / "results").mkdir(parents=True)
    # The times of the results as in the manifest, with one more at which nothing is measured
    times = pd.DatetimeIndex([str(t) for t in np.append(observed.index.to_numpy(), np.datetime64("2021-01-01"))])
    observation = export_observations(database, results_dir, times, output)

    assert observation is not None
    assert observation["count"] == 2
    assert observation["min_days"] == MIN_COMPARED_DAYS
    assert observation["variables"]["level"]["units"] == "m"
    assert observation["variables"]["level"]["scale"] == "symlog"
    assert observation["variables"]["level"]["domain"] == pytest.approx([-0.5, 0.5])
    # The flow rate is simulated exactly, which would make an empty scale
    assert observation["variables"]["flow_rate"]["domain"] == [-1.0, 1.0]

    metrics = pd.read_parquet(observation["metrics"])
    assert metrics.columns.tolist() == ["node_id", "name", "variable", "days", "nse", "kge", "bias"]
    assert metrics["node_id"].tolist() == [4, 5]
    assert metrics["name"].tolist() == ["afvoer", "peil"]
    assert metrics["days"].tolist() == [len(observed)] * 2
    assert metrics["nse"].iloc[0] == 1.0

    # The flow rate of Outlet 2 is that of its outflow link, the second link
    links = pd.read_parquet(observation["links"])
    assert links.values.tolist() == [[4, 2]]

    by_time = pd.read_parquet(observation["by_time"])
    assert by_time.columns.tolist() == ["node_id", "flow_rate", "level"]
    assert by_time["node_id"].tolist()[:2] == [4, 5]
    # Ordered by time, then by node
    flow_rate = by_time["flow_rate"].to_numpy().reshape(len(times), 2)
    level = by_time["level"].to_numpy().reshape(len(times), 2)
    np.testing.assert_array_equal(flow_rate[:-1, 0], 0.0)
    np.testing.assert_allclose(level[:-1, 1], 0.5)
    # Observation 4 measures no level and 5 no flow rate, and neither measures at the last time
    assert np.isnan(flow_rate[:, 1]).all() and np.isnan(level[:, 0]).all()
    assert np.isnan(flow_rate[-1, 0]) and np.isnan(level[-1, 1])


def test_color_domain():
    values = np.linspace(-100.0, 100.0, 1001)
    assert color_domain(values, "linear") == pytest.approx([-96.0, 96.0])
    assert color_domain(values, "diverging") == pytest.approx([-98.0, 98.0])
    assert color_domain(np.array([0.0, 1.0, 10.0, 100.0]), "log")[1] <= 100.0
    low, high = color_domain(np.array([1e-15] * 10 + [1.0] * 90), "log")
    assert low == pytest.approx(high * 1e-4)


def test_netcdf_tables(tmp_path):
    config = {"input_dir": "input", "basin": {"time": "basin_time.nc"}, "solver": {"abstol": 0.01}}
    assert netcdf_tables(config, tmp_path) == {"Basin / time": tmp_path / "basin_time.nc"}


def test_table_slug():
    assert table_slug("TabulatedRatingCurve / static") == "tabulated_rating_curve_static"


def test_downcast_int64():
    table = downcast_int64(pa.table({"small": pa.array([1, 2], pa.int64()), "large": pa.array([1, 2**40], pa.int64())}))
    assert table.schema.field("small").type == pa.int32()
    assert table.schema.field("large").type == pa.int64()


def test_write_table_float_encoding(tmp_path):
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int32()),
            "x": pa.array([5.1, 5.2], pa.float32()),
            "level": pa.array([1.0, 1.0], pa.float64()),
            "coords": pa.array([[5.1, 52.1, 5.2, 52.2], [5.3, 52.3]], pa.list_(pa.float32())),
        }
    )
    path = tmp_path / "table.parquet"
    write_table(table, path)
    assert pq.read_table(path).equals(table)
    columns = pq.ParquetFile(path).metadata.row_group(0)
    encodings = {columns.column(i).path_in_schema: columns.column(i).encodings for i in range(columns.num_columns)}
    assert "BYTE_STREAM_SPLIT" in encodings["x"]
    assert "BYTE_STREAM_SPLIT" in encodings["coords.list.element"]
    assert "BYTE_STREAM_SPLIT" not in encodings["id"]
    assert "BYTE_STREAM_SPLIT" not in encodings["level"]
