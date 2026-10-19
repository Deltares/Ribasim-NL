"""Regression tests for mapping-driven foreign inflows."""

from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from ribasim import Node
from ribasim.nodes import flow_boundary
from ribasim_nl.validation import validate_model
from shapely.geometry import Point

from ribasim_nl import Model, add_transboundary_inflow


@pytest.fixture
def model() -> Model:
    """Create a foreign boundary and an unrelated RWZI at the same coordinates."""
    model = Model(starttime="2020-01-01", endtime="2020-01-03", crs="EPSG:28992")
    for node_id, name, category in [(1, "River", "buitenlandse aanvoer"), (2, "RWZI", "RWZI")]:
        model.flow_boundary.add(
            Node(node_id=node_id, name=name, geometry=Point(100, 200)),
            [flow_boundary.Static(flow_rate=[9.0])],
        )
        model.node.df.loc[node_id, "meta_categorie"] = category
        model.node.df.loc[node_id, "meta_code_waterbeheerder"] = f"code-{node_id}"
    model.flow_boundary.static.df = model.flow_boundary.static.df
    return model


def overview(tmp_path: Path, **changes: object) -> Path:
    """Write one editable overview row."""
    row = {
        "id": "BA_001",
        "name": "River",
        "authority_name": "BrabantseDelta",
        "authority_aquo": 25,
        "coordinate_x": 100,
        "coordinate_y": 200,
        "timeseries": "",
        "constant_global": 0.1,
        "constant_summer": 0.0,
        "constant_winter": 0.0,
    }
    row.update(changes)
    path = tmp_path / "overview.csv"
    pd.DataFrame([row]).to_csv(path, index=False)
    return path


def measurements(tmp_path: Path, dates: list[str], values: list[float | None]) -> None:
    """Write a measurement file shared by all test modes."""
    folder = tmp_path / "aangeleverd"
    folder.mkdir(exist_ok=True)
    pd.DataFrame({"Datum": dates, "River": values}).to_csv(folder / "rivers.csv", index=False)


@pytest.mark.parametrize("authority", ["BrabantseDelta", 25], ids=["name", "integer-code"])
@pytest.mark.parametrize("input", ["constant_global", "constant_summer", "constant_winter"])
def test_constants_preserve_nodes_and_unrelated_forcing(model: Model, tmp_path: Path, authority: str | int, input: str):
    nodes = model.node.df.copy(deep=True)
    links = model.link.df.copy(deep=True)
    unrelated = model.flow_boundary.static.df.query("node_id == 2").copy(deep=True)
    path = overview(tmp_path)
    add_transboundary_inflow(model, path, authority=authority, input=input)
    pd.testing.assert_frame_equal(model.node.df, nodes)
    pd.testing.assert_frame_equal(model.link.df, links)
    pd.testing.assert_frame_equal(
        model.flow_boundary.static.df.query("node_id == 2").reset_index(drop=True),
        unrelated.reset_index(drop=True),
    )
    expected = 0.1 if input == "constant_global" else 0.0
    assert model.flow_boundary.static.df.query("node_id == 1")["flow_rate"].item() == expected
    assert model.flow_boundary.time.df is None


def test_coordinate_mapping_does_not_rename_nodes(model: Model, tmp_path: Path):
    nodes = model.node.df.copy(deep=True)
    path = overview(tmp_path, name="Renamed river", coordinate_x=100.5)
    add_transboundary_inflow(model, path, authority=25, input="constant_global", mapping="coordinate")
    pd.testing.assert_frame_equal(model.node.df, nodes)


@pytest.mark.parametrize("input", ["constant_summer", "constant_winter"])
def test_missing_seasonal_constant_falls_back(model: Model, tmp_path: Path, input: str, caplog):
    path = overview(tmp_path, **{input: None})
    add_transboundary_inflow(model, path, authority=25, input=input)
    assert model.flow_boundary.static.df.query("node_id == 1")["flow_rate"].item() == 0.1
    assert "using constant_global" in caplog.text


@pytest.mark.parametrize("frequency, expected", [("D", [2.0, 0.1, 0.1]), ("h", [1.0, 3.0] + [0.1] * 47)])
def test_resampling_and_global_fallback(model: Model, tmp_path: Path, frequency: str, expected: list[float]):
    path = overview(tmp_path, timeseries=r"aangeleverd\rivers")
    measurements(tmp_path, ["2020-01-01 00:00:00", "2020-01-01 01:00:00"], [1.0, 3.0])
    nodes = model.node.df.copy(deep=True)
    add_transboundary_inflow(model, path, authority=25, frequency=frequency)
    result = model.flow_boundary.time.df
    assert result["flow_rate"].tolist() == expected
    assert result["node_id"].eq(1).all()
    assert set(model.flow_boundary.static.df["node_id"]) == {2}
    pd.testing.assert_frame_equal(model.node.df, nodes)


@pytest.mark.parametrize(
    "dates, values",
    [
        (["2020-01-01", "2020-01-02", "2020-01-03"], [1.0, 2.0, 3.0]),
        (["2020-01-01", "2020-01-03"], [1.0, 3.0]),
    ],
    ids=["daily", "missing-day"],
)
def test_daily_to_hourly_interpolates_without_extrapolation(
    model: Model, tmp_path: Path, dates: list[str], values: list[float]
) -> None:
    """Interpolate between measurements and use the global constant outside their coverage."""
    model.starttime = pd.Timestamp("2019-12-31")
    model.endtime = pd.Timestamp("2020-01-03 23:00:00")
    path = overview(tmp_path, timeseries="aangeleverd/rivers")
    measurements(tmp_path, dates, values)
    add_transboundary_inflow(model, path, authority=25, frequency="h")
    expected = [0.1] * 24 + [1.0 + hour / 24 for hour in range(49)] + [0.1] * 23
    assert model.flow_boundary.time.df["flow_rate"].tolist() == pytest.approx(expected)


def test_interpolation_uses_measurements_outside_model_period(model: Model, tmp_path: Path) -> None:
    """Retain bracketing measurements and their timestamps when the model starts between samples."""
    model.starttime = pd.Timestamp("2020-01-01 12:30:00")
    model.endtime = pd.Timestamp("2020-01-01 14:30:00")
    path = overview(tmp_path, timeseries="aangeleverd/rivers")
    measurements(tmp_path, ["2020-01-01", "2020-01-02"], [0.0, 24.0])
    add_transboundary_inflow(model, path, authority=25, frequency="h")
    assert model.flow_boundary.time.df["flow_rate"].tolist() == [12.5, 13.5, 14.5]


def test_downsampling_does_not_interpolate_missing_days(model: Model, tmp_path: Path) -> None:
    """Keep global fallback for missing daily bins when averaging hourly data."""
    path = overview(tmp_path, timeseries="aangeleverd/rivers")
    measurements(
        tmp_path,
        ["2020-01-01 00:00:00", "2020-01-01 01:00:00", "2020-01-03 00:00:00", "2020-01-03 01:00:00"],
        [1.0, 3.0, 5.0, 7.0],
    )
    add_transboundary_inflow(model, path, authority=25)
    assert model.flow_boundary.time.df["flow_rate"].tolist() == [2.0, 0.1, 6.0]


@pytest.mark.parametrize(
    "case",
    [
        "missing_file",
        "missing_column",
        "empty_values",
        "empty_file",
        "missing_timestamps",
        "nat_timestamps",
        "outside_period",
    ],
)
def test_unavailable_timeseries_uses_global(model: Model, tmp_path: Path, case: str, caplog):
    path = overview(tmp_path, timeseries="aangeleverd/rivers")
    if case == "missing_column":
        measurements(tmp_path, ["2020-01-01"], [2.0])
        data = pd.read_csv(tmp_path / "aangeleverd" / "rivers.csv")
        data.rename(columns={"River": "Other"}).to_csv(tmp_path / "aangeleverd" / "rivers.csv", index=False)
    elif case == "empty_values":
        measurements(tmp_path, ["2020-01-01"], [None])
    elif case == "empty_file":
        measurements(tmp_path, [], [])
    elif case == "missing_timestamps":
        measurements(tmp_path, ["", ""], [2.0, 3.0])
    elif case == "nat_timestamps":
        measurements(tmp_path, ["NaT"], [2.0])
    elif case == "outside_period":
        measurements(tmp_path, ["2019-01-01"], [2.0])
    add_transboundary_inflow(model, path, authority=25)
    static = model.flow_boundary.static.df
    time = model.flow_boundary.time.df
    if time is None:
        assert static.query("node_id == 1")["flow_rate"].item() == 0.1
    else:
        assert time["flow_rate"].eq(0.1).all()
    assert "constant_global" in caplog.text


def test_explicit_measurement_column_and_repeated_switching(model: Model, tmp_path: Path):
    path = overview(tmp_path, timeseries="aangeleverd/rivers", timeseries_column="Measurement")
    measurements(tmp_path, ["2020-01-01", "2020-01-02", "2020-01-03"], [1.0, 2.0, 3.0])
    data_path = tmp_path / "aangeleverd" / "rivers.csv"
    pd.read_csv(data_path).rename(columns={"River": "Measurement"}).to_csv(data_path, index=False)
    add_transboundary_inflow(model, path, authority=25)
    expected = model.flow_boundary.time.df.copy(deep=True)
    add_transboundary_inflow(model, path, authority=25)
    pd.testing.assert_frame_equal(model.flow_boundary.time.df, expected)
    add_transboundary_inflow(model, path, authority=25, input="constant_global")
    assert model.flow_boundary.time.df is None
    assert model.flow_boundary.static.df["node_id"].is_unique
    add_transboundary_inflow(model, path, authority=25)
    pd.testing.assert_frame_equal(model.flow_boundary.time.df, expected)


def test_duplicate_wall_clock_times_and_negative_values(model: Model, tmp_path: Path, caplog):
    """Average duplicate samples before interpolating a coarser series, then clip negative rates."""
    path = overview(tmp_path, timeseries="aangeleverd/rivers")
    measurements(
        tmp_path,
        ["2020-01-02 02:00:00", "2020-01-01 00:00:00", "2020-01-02 02:00:00"],
        [1.0, -2.0, 3.0],
    )
    add_transboundary_inflow(model, path, authority=25)
    assert model.flow_boundary.time.df["flow_rate"].tolist() == pytest.approx([0.0, -2.0 + 4.0 * 24 / 26, 0.1])
    assert "negative flow" in caplog.text


@pytest.mark.parametrize(
    "changes, kwargs, message",
    [
        ({"name": "Unknown"}, {}, "not found"),
        ({"coordinate_x": 102}, {"mapping": "coordinate"}, "within 1 m"),
        ({"constant_global": -1}, {}, "non-negative"),
        ({"constant_global": None}, {}, "finite"),
        ({"constant_global": float("inf")}, {}, "finite"),
        ({"authority_name": "DeDommel"}, {}, "disagree"),
        ({}, {"authority": "Unknown"}, "Unknown water authority"),
        ({}, {"frequency": "W"}, "resampling frequency"),
    ],
)
def test_invalid_inputs_leave_model_unchanged(model: Model, tmp_path: Path, changes: dict, kwargs: dict, message: str):
    path = overview(tmp_path, **changes)
    static = model.flow_boundary.static.df.copy(deep=True)
    nodes = model.node.df.copy(deep=True)
    with pytest.raises(ValueError, match=message):
        add_transboundary_inflow(model, path, **({"authority": 25} | kwargs))
    pd.testing.assert_frame_equal(model.flow_boundary.static.df, static)
    pd.testing.assert_frame_equal(model.node.df, nodes)


def test_hydroobject_mapping_is_explicitly_unsupported(model: Model, tmp_path: Path):
    with pytest.raises(NotImplementedError, match="hydroobject_global_id"):
        add_transboundary_inflow(model, overview(tmp_path), authority=25, mapping="hydroobject")


def test_duplicate_and_missing_mapping_rows_are_rejected(model: Model, tmp_path: Path):
    path = overview(tmp_path)
    frame = pd.read_csv(path)
    pd.concat([frame, frame.assign(id="BA_002")]).to_csv(path, index=False)
    with pytest.raises(ValueError, match="exactly once"):
        add_transboundary_inflow(model, path, authority=25)
    frame.assign(authority_aquo=27, authority_name="DeDommel").to_csv(path, index=False)
    with pytest.raises(ValueError, match="exactly once"):
        add_transboundary_inflow(model, path, authority=25)


def test_ambiguous_coordinates_are_rejected(model: Model, tmp_path: Path):
    model.node.df.loc[2, "meta_categorie"] = "buitenlandse aanvoer"
    with pytest.raises(ValueError, match="found 2"):
        add_transboundary_inflow(model, overview(tmp_path), authority=25, mapping="coordinate")


def test_malformed_timestamps_and_path_traversal_raise(model: Model, tmp_path: Path):
    path = overview(tmp_path, timeseries="aangeleverd/rivers")
    measurements(tmp_path, ["not a date"], [1.0])
    with pytest.raises(ValueError):
        add_transboundary_inflow(model, path, authority=25)
    path = overview(tmp_path, timeseries="../outside")
    with pytest.raises(ValueError, match="within the overview directory"):
        add_transboundary_inflow(model, path, authority=25)


def test_daily_mean_retains_repeated_wall_clock_measurements(model: Model, tmp_path: Path) -> None:
    path = overview(tmp_path, timeseries="aangeleverd/rivers")
    measurements(
        tmp_path,
        ["2020-01-01 02:00:00", "2020-01-01 02:00:00", "2020-01-01 03:00:00"],
        [1.0, 3.0, 5.0],
    )
    add_transboundary_inflow(model, path, authority=25)
    assert model.flow_boundary.time.df["flow_rate"].iloc[0] == 3.0


def test_authority_filtering_and_shared_file_cache(model: Model, tmp_path: Path, monkeypatch) -> None:
    from ribasim_nl import transboundary_inflow

    model.node.df.loc[2, "meta_categorie"] = "buitenlandse aanvoer"
    path = overview(tmp_path, timeseries="aangeleverd/rivers")
    table = pd.read_csv(path)
    table = pd.concat(
        [
            table,
            table.assign(id="BA_002", name="RWZI"),
            table.assign(id="OTHER", authority_aquo=27, authority_name="DeDommel", name="Not in this model"),
        ],
        ignore_index=True,
    )
    table.to_csv(path, index=False)
    measurements(tmp_path, ["2020-01-01", "2020-01-02"], [1.0, 3.0])
    data_path = tmp_path / "aangeleverd" / "rivers.csv"
    data = pd.read_csv(data_path)
    data["RWZI"] = data["River"] * 2
    data.to_csv(data_path, index=False)
    original = transboundary_inflow._read_timeseries
    calls = []

    def read(path: Path) -> pd.DataFrame:
        calls.append(path)
        return original(path)

    monkeypatch.setattr(transboundary_inflow, "_read_timeseries", read)
    add_transboundary_inflow(model, path, authority=25)
    assert len(calls) == 1
    validate_model(model)


@pytest.mark.parametrize("issue", ["overlap", "missing", "duplicate", "invalid_rate"])
def test_model_wide_flow_boundary_validation(model: Model, issue: str) -> None:
    if issue == "overlap":
        model.flow_boundary.time.df = pd.DataFrame({"node_id": [1], "time": [model.starttime], "flow_rate": [1.0]})
        message = "both static and time"
    elif issue == "missing":
        model.flow_boundary.static.df = model.flow_boundary.static.df.query("node_id != 1")
        message = "missing node IDs"
    elif issue == "duplicate":
        frame = model.flow_boundary.static.df
        model.flow_boundary.static.df = pd.concat([frame, frame], ignore_index=True)
        message = "unique"
    else:
        model.flow_boundary.static.df.loc[:, "flow_rate"] = float("inf")
        message = "finite and non-negative"
    with pytest.raises(ValueError, match=message):
        validate_model(model)


@pytest.mark.parametrize("input", ["timeseries", "constant_summer", "constant_winter"])
def test_requested_input_and_global_fallback_are_separate_groups(model: Model, tmp_path: Path, input: str) -> None:
    model.node.df.loc[2, "meta_categorie"] = "buitenlandse aanvoer"
    path = overview(tmp_path, timeseries="aangeleverd/rivers", constant_winter=1.5)
    first = pd.read_csv(path)
    second = first.assign(
        id="BA_002", name="RWZI", timeseries="", constant_global=0.7, constant_summer=None, constant_winter=None
    )
    pd.concat([first, second], ignore_index=True).to_csv(path, index=False)
    measurements(tmp_path, ["2020-01-01", "2020-01-02", "2020-01-03"], [1.0, 2.0, 3.0])
    add_transboundary_inflow(model, path, authority=25, input=input)
    static = model.flow_boundary.static.df.set_index("node_id")["flow_rate"]
    assert static.at[2] == 0.7
    if input == "timeseries":
        assert set(static.index) == {2}
        assert model.flow_boundary.time.df["node_id"].eq(1).all()
        assert model.flow_boundary.time.df["flow_rate"].tolist() == [1.0, 2.0, 3.0]
    else:
        assert static.at[1] == (0.0 if input == "constant_summer" else 1.5)
        assert model.flow_boundary.time.df is None
    validate_model(model)


@pytest.mark.parametrize("frequency", ["D", "h"])
def test_shared_file_is_resampled_once_with_per_node_fallbacks(model: Model, tmp_path: Path, frequency: str) -> None:
    """Batch shared measurements, including aliases and unavailable columns, without mixing fallbacks."""
    from ribasim_nl import transboundary_inflow

    for node_id, name in [(3, "Other"), (4, "Empty"), (5, "Absent")]:
        model.flow_boundary.add(
            Node(node_id=node_id, name=name, geometry=Point(node_id * 100, 200)),
            [flow_boundary.Static(flow_rate=[9.0])],
        )
        model.node.df.loc[node_id, "meta_categorie"] = "buitenlandse aanvoer"
    path = overview(tmp_path, timeseries=r"aangeleverd\rivers", timeseries_column="River")
    first = pd.read_csv(path)
    pd.concat(
        [
            first,
            first.assign(id="BA_003", name="Other", timeseries="./aangeleverd/rivers", constant_global=0.7),
            first.assign(id="BA_004", name="Empty", timeseries_column="Empty", constant_global=0.9),
            first.assign(id="BA_005", name="Absent", timeseries_column="Missing", constant_global=1.1),
        ],
        ignore_index=True,
    ).to_csv(path, index=False)
    measurements(tmp_path, ["2020-01-01 00:00:00", "2020-01-01 01:00:00"], [1.0, 3.0])
    data_path = tmp_path / "aangeleverd" / "rivers.csv"
    pd.read_csv(data_path).assign(Empty=None, Ignored=float("inf")).to_csv(data_path, index=False)

    with patch.object(transboundary_inflow, "_resample", wraps=transboundary_inflow._resample) as resample:
        add_transboundary_inflow(model, path, authority=25, frequency=frequency)
    resample.assert_called_once()
    assert isinstance(resample.call_args.args[0], pd.DataFrame)
    time = model.flow_boundary.time.df
    for node_id, fallback in [(1, 0.1), (3, 0.7)]:
        expected = [2.0, fallback, fallback] if frequency == "D" else [1.0, 3.0] + [fallback] * 47
        assert time.loc[time["node_id"].eq(node_id), "flow_rate"].tolist() == expected
    assert model.flow_boundary.static.df.set_index("node_id")["flow_rate"].to_dict() == {2: 9.0, 4: 0.9, 5: 1.1}
    validate_model(model)


def test_coordinate_mapping_preserves_filtered_row_alignment(model: Model, tmp_path: Path) -> None:
    """Match multiple coordinates in mapping order, including the one-metre tolerance edge."""
    model.node.df.loc[2, "meta_categorie"] = "buitenlandse aanvoer"
    model.node.df.loc[2, "geometry"] = Point(500, 600)
    path = overview(tmp_path, coordinate_x=101)
    first = pd.read_csv(path)
    pd.concat(
        [
            first.assign(id="OTHER", authority_aquo=27, authority_name="DeDommel"),
            first.assign(id="BA_002", name="Alias", coordinate_x=500, coordinate_y=600, constant_global=0.7),
            first,
        ],
        ignore_index=True,
    ).to_csv(path, index=False)
    add_transboundary_inflow(model, path, authority=25, input="constant_global", mapping="coordinate")
    assert model.flow_boundary.static.df.set_index("node_id")["flow_rate"].to_dict() == {1: 0.1, 2: 0.7}
    validate_model(model)


@pytest.mark.parametrize("frequency", ["D", "h"])
def test_shared_columns_interpolate_independently(model: Model, tmp_path: Path, frequency: str, caplog) -> None:
    """Keep column-specific interpolation, clipping and fallback after filtering and reordering mapping rows."""
    model.starttime = pd.Timestamp("2019-12-31")
    model.endtime = pd.Timestamp("2020-01-04")
    model.node.df.loc[2, "meta_categorie"] = "buitenlandse aanvoer"
    path = overview(tmp_path, timeseries="aangeleverd/rivers", timeseries_column=" ")
    first = pd.read_csv(path)
    pd.concat(
        [
            first.assign(id="BA_002", name="RWZI", constant_global=0.7),
            first.assign(id="OTHER", authority_aquo=27, authority_name="DeDommel"),
            first,
        ],
        ignore_index=True,
    ).to_csv(path, index=False)
    measurements(tmp_path, ["2020-01-01", "2020-01-02", "2020-01-03"], [0.0, 24.0, 48.0])
    data_path = tmp_path / "aangeleverd" / "rivers.csv"
    pd.read_csv(data_path).assign(RWZI=[8.0, "invalid", -8.0]).to_csv(data_path, index=False)
    add_transboundary_inflow(model, path, authority=25, frequency=frequency)
    time = model.flow_boundary.time.df
    outside = 1 if frequency == "D" else 24
    river = [0.0, 24.0, 48.0] if frequency == "D" else list(range(49))
    rwzi = [8.0, 0.7, 0.0] if frequency == "D" else [max(8.0 - hour / 3, 0.0) for hour in range(49)]
    for node_id, fallback, values in [(1, 0.1, river), (2, 0.7, rwzi)]:
        expected = [fallback] * outside + values + [fallback] * outside
        assert time.loc[time["node_id"].eq(node_id), "flow_rate"].tolist() == pytest.approx(expected)
    assert "non-numeric measurements" in caplog.text
    assert "negative flow rates" in caplog.text
    assert model.flow_boundary.static.df is None
    validate_model(model)


def test_invalid_shared_measurements_leave_forcing_unchanged(model: Model, tmp_path: Path) -> None:
    """Reject an infinite selected measurement before replacing any model forcing."""
    model.node.df.loc[2, "meta_categorie"] = "buitenlandse aanvoer"
    path = overview(tmp_path, timeseries="aangeleverd/rivers")
    first = pd.read_csv(path)
    pd.concat([first, first.assign(id="BA_002", name="RWZI")], ignore_index=True).to_csv(path, index=False)
    measurements(tmp_path, ["2020-01-01"], [1.0])
    data_path = tmp_path / "aangeleverd" / "rivers.csv"
    pd.read_csv(data_path).assign(RWZI=float("inf")).to_csv(data_path, index=False)
    static = model.flow_boundary.static.df.copy(deep=True)
    with pytest.raises(ValueError, match="Infinite measurements"):
        add_transboundary_inflow(model, path, authority=25)
    pd.testing.assert_frame_equal(model.flow_boundary.static.df, static)
    assert model.flow_boundary.time.df is None


@pytest.mark.parametrize(
    "dates, values",
    [
        (["2020-01-01 00:00:00", "2020-01-01 01:00:00"], [1e308, 1e308]),
        (["2020-01-01", "2020-01-03"], [-1e308, 1e308]),
    ],
    ids=["averaging", "interpolation"],
)
def test_resampling_overflow_leaves_forcing_unchanged(
    model: Model, tmp_path: Path, dates: list[str], values: list[float]
) -> None:
    """Reject non-finite resampling results even when the source measurements are finite."""
    path = overview(tmp_path, timeseries="aangeleverd/rivers")
    measurements(tmp_path, dates, values)
    static = model.flow_boundary.static.df.copy(deep=True)
    with pytest.raises(ValueError, match="resampled"):
        add_transboundary_inflow(model, path, authority=25)
    pd.testing.assert_frame_equal(model.flow_boundary.static.df, static)
    assert model.flow_boundary.time.df is None
