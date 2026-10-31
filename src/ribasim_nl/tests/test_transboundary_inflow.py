import pandas as pd
import pytest
from ribasim import Node
from ribasim.nodes import basin, flow_boundary
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.transboundary_inflow import (
    CONSTANTS,
    add_transboundary_inflow,
    median_flow_rates,
    read_overview,
    read_timeseries,
    set_constant_inflow,
)
from shapely.geometry import Point

from ribasim_nl import Model, transboundary_inflow

OVERVIEW = """id,authority,name,coordinate_x,coordinate_y,timeseries,timeseries_column,constant_global,constant_summer,constant_winter
BA_1,A,Measured,0,0,measured.csv,Measured,2.0,1.0,3.0
BA_2,A,Constant,100,0,,,0.1,0.2,0.3
BA_3,B,Elsewhere,0,0,,,0.1,0.1,0.1
"""


@pytest.fixture
def overview(tmp_path, monkeypatch):
    """An overview of three inflows, of which the first is measured in `measured.csv`."""
    path = tmp_path / "buitenlandse_aanvoer.csv"
    path.write_text(OVERVIEW)
    monkeypatch.setattr(transboundary_inflow, "OVERVIEW_CSV", path)
    monkeypatch.setattr(transboundary_inflow, "TIMESERIES_DIR", tmp_path)
    return path


def inflow_model() -> Model:
    """FlowBoundaries 1 and 2 of authority A, and RWZI 3, discharge into Basin 4."""
    model = Model(starttime="2017-01-01", endtime="2017-01-05", crs="EPSG:28992")
    model.basin.add(Node(4, Point(50, -50)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0])])
    for node_id, name, x in ((1, "Measured", 0), (2, "Constant", 100)):
        model.flow_boundary.add(
            Node(node_id, Point(x, 0), name=name, meta_categorie="buitenlandse aanvoer"),
            [flow_boundary.Static(flow_rate=[0.1])],
        )
    model.flow_boundary.add(
        Node(3, Point(50, 0), name="RWZI", meta_categorie="RWZI"), [flow_boundary.Static(flow_rate=[0.5])]
    )
    for node_id in (1, 2, 3):
        model.link.add(model.flow_boundary[node_id], model.basin[4])
    return model


def static_flow_rates(model: Model) -> dict[int, float]:
    assert model.flow_boundary.static.df is not None
    return model.flow_boundary.static.df.set_index("node_id")["flow_rate"].to_dict()


def test_overview():
    """The inflows of the repository, of known water authorities, all with constants."""
    overview = read_overview()
    assert set(overview["authority"]) <= set(waterbeheercode)
    assert overview["timeseries"].dropna().str.match(r"^(aangeleverd|verwerkt)/[^/]+\.csv$").all()
    assert len(read_overview("BrabantseDelta")) == 7


def test_overview_invalid(overview):
    overview.write_text(OVERVIEW.replace("measured.csv,Measured", "measured.csv,"))
    with pytest.raises(AssertionError, match="timeseries_column"):
        read_overview()
    overview.write_text(OVERVIEW.replace("0.1,0.2,0.3", "0.1,,0.3"))
    with pytest.raises(AssertionError, match="non-negative"):
        read_overview()
    overview.write_text(OVERVIEW.replace("BA_3", "BA_2"))
    with pytest.raises(AssertionError, match="Duplicate inflow ids"):
        read_overview()


def test_read_timeseries(tmp_path, monkeypatch):
    """Daily means of local wall-clock time, of numeric values, with negative values clipped."""
    monkeypatch.setattr(transboundary_inflow, "TIMESERIES_DIR", tmp_path)
    (tmp_path / "measured.csv").write_text(
        "﻿Datum,Measured,Other\n"
        "2017-10-29 02:00:00,1.0,x\n"
        "2017-10-29 02:00:00,3.0,x\n"  # the autumn DST fall-back repeats an hour
        "2017-10-29 01:00:00,-,x\n"
        "2017-10-28 23:59:59.971000,2.0,x\n"  # rounded off by Excel, at the start of the 29th
        "2017-10-31 12:00:00,-1.0,x\n"
        ",5.0,x\n"
    )
    flow_rate = read_timeseries("measured.csv", "Measured")
    expected = pd.Series(
        [2.0, 0.0], index=pd.DatetimeIndex(["2017-10-29", "2017-10-31"], name="Datum"), name="Measured"
    )
    pd.testing.assert_series_equal(flow_rate, expected, check_freq=False, check_index_type=False)


def test_median_flow_rates():
    days = pd.date_range("2017-01-01", "2017-12-31", freq="D")
    flow_rate = pd.Series(days.month.isin(range(4, 10)).astype(float) + 1.0, index=days, name="Measured")
    assert median_flow_rates(flow_rate) == {"constant_global": 2.0, "constant_summer": 2.0, "constant_winter": 1.0}
    with pytest.raises(AssertionError, match="summer and winter"):
        median_flow_rates(flow_rate.loc["2017-04":"2017-09"])


@pytest.mark.parametrize("constant", CONSTANTS)
def test_set_constant_inflow(overview, constant):
    model = inflow_model()
    model.flow_boundary.time.df = pd.DataFrame({"node_id": [1, 1], "time": ["2017-01-01", "2017-01-05"]}).assign(
        flow_rate=4.0
    )
    set_constant_inflow(model, "A", constant)
    expected = read_overview("A").set_index("name")[constant]
    assert static_flow_rates(model) == {1: expected["Measured"], 2: expected["Constant"], 3: 0.5}
    assert model.flow_boundary.time.df is None


def test_add_transboundary_inflow(overview):
    """Gaps are interpolated, and outside the measurements the flow rate is `constant_global`."""
    (overview.parent / "measured.csv").write_text(
        "Datum,Measured\n2016-12-31 12:00,0.0\n2017-01-01 12:00,1.0\n2017-01-03 12:00,3.0\n2017-01-04 12:00,-1.0\n"
    )
    model = inflow_model()
    add_transboundary_inflow(model, "A", model.starttime, model.endtime)
    assert static_flow_rates(model) == {3: 0.5, 2: 0.1}
    time = model.flow_boundary.time.df
    assert time is not None
    assert (time["node_id"] == 1).all()
    assert time["time"].tolist() == list(pd.date_range("2017-01-01", "2017-01-05", freq="D"))
    assert time["flow_rate"].tolist() == [1.0, 2.0, 3.0, 0.0, 2.0]


def test_unmatched_inflows(overview):
    model = inflow_model()
    model.node.df.loc[2, "name"] = "Renamed"
    with pytest.raises(ValueError, match=r"inflows without FlowBoundary: \['Constant'\].*\{2: 'Renamed'\}"):
        set_constant_inflow(model, "A")
    model.node.df.loc[2, "name"] = "Constant"
    model.node.df.loc[2, "geometry"] = Point(100, 20)
    with pytest.raises(ValueError, match=r"further than 10.0 m.*'BA_2': 20.0"):
        set_constant_inflow(model, "A")
    # authority B has an inflow, but its model no FlowBoundary
    with pytest.raises(ValueError, match=r"inflows without FlowBoundary: \['Elsewhere'\]"):
        set_constant_inflow(Model(starttime="2017-01-01", endtime="2017-01-05", crs="EPSG:28992"), "B")


def test_no_inflows(overview):
    model = inflow_model()
    model.node.df.loc[[1, 2], "meta_categorie"] = "RWZI"
    add_transboundary_inflow(model, "C", model.starttime, model.endtime)
    set_constant_inflow(model, "C")
    assert static_flow_rates(model) == {1: 0.1, 2: 0.1, 3: 0.5}
