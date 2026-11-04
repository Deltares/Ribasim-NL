import geopandas as gpd
import pandas as pd
from ribasim import Node
from ribasim.nodes import basin, outlet
from ribasim_nl.observations import LinkEnd, _is_sum, _points, _strings, add_observation_nodes
from shapely.geometry import Point

from ribasim_nl import Model


def test_parse_cells():
    assert _points("[<POINT (1.5 2)>, <POINT (-3 4e+05)>]") == [Point(1.5, 2), Point(-3, 4e5)]
    assert _strings("['Basin', 'Pump']") == ["Basin", "Pump"]


def test_is_sum():
    assert _is_sum(float("nan"), 1)
    assert not _is_sum(float("nan"), 2)
    assert _is_sum("negatief_maken", 1)
    assert _is_sum("optellen", 3)
    assert _is_sum("link1+link2", 2)
    assert not _is_sum("link1-link2", 2)


def test_add_observation_nodes():
    """Basin 1 -> Outlet 2 -> Junction 4 -> Basin 5, and Basin 1 -> Outlet 3 -> Junction 4."""
    model = Model(starttime="2020-01-01", endtime="2020-01-03", crs="EPSG:28992")
    for node_id, x in ((1, 0), (5, 30)):
        model.basin.add(
            Node(node_id, Point(x, 0)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])]
        )
    for node_id, y in ((2, 1), (3, -1)):
        model.outlet.add(Node(node_id, Point(10, y)), [outlet.Static(flow_rate=[1.0])])
    model.junction.add(Node(4, Point(20, 0)))
    for from_node, to_node in (
        (model.basin[1], model.outlet[2]),
        (model.basin[1], model.outlet[3]),
        (model.outlet[2], model.junction[4]),
        (model.outlet[3], model.junction[4]),
        (model.junction[4], model.basin[5]),
    ):
        model.link.add(from_node, to_node)

    def end(y: float) -> LinkEnd:
        return LinkEnd("Outlet", Point(10.5, y), "Basin", True)

    coupling = gpd.GeoDataFrame(
        {
            "MeetreeksC": ["outlet", "sum", "elders", "netto", "gemaal"],
            "Aan/Af": ["Aanvoer", "Afvoer", "Afvoer", "Afvoer", "Afvoer"],
            "negate": [True, False, False, False, False],
            "ends": [
                [end(1)],
                [end(1), end(-1)],
                [LinkEnd("Outlet", Point(100, 0), "Basin", True)],
                None,
                # a Pump in the koppeltabel that is an Outlet in the model
                [LinkEnd("Pump", Point(10.5, -1), "Basin", True)],
            ],
            "koppeling": ["", "", "", "combines 2 links with link1-link2", ""],
        },
        geometry=[Point(12, 3), Point(20, 1), Point(100, 0), Point(50, 0), Point(12, -3)],
        crs="EPSG:28992",
    )
    time = pd.date_range("2019-12-31", "2020-01-04", name="time")
    measurements = {
        "Aanvoer": pd.DataFrame({"outlet": [1.0, 2.0, None, 4.0, 5.0]}, index=time),
        "Afvoer": pd.DataFrame({"sum": 1.0, "elders": 1.0, "netto": 1.0}, index=time),
    }

    model = add_observation_nodes(model, coupling, measurements)
    # all series get an Observation node at the measurement location, also when it cannot be linked
    observations = model.node.df[model.node.df["node_type"] == "Observation"]
    assert observations["name"].tolist() == ["outlet", "sum", "elders", "netto", "gemaal"]
    assert observations.geometry.iloc[0].equals(Point(12, 3))
    links = model.link.df[model.link.df["link_type"] == "observation"]
    observed = dict(zip(links["from_node_id"].map(observations["name"]), links["to_node_id"], strict=True))
    # the sum of both outlets is the flow of the Junction that collects them
    assert observed == {"outlet": 2, "sum": 4, "gemaal": 3}
    assert observations["meta_koppeling"].tolist() == [
        "",
        "",
        "0 Outlet nodes at the location in the koppeltabel",
        "combines 2 links with link1-link2",
        "",
    ]
    # measurements in the model period, with the sign of the flow_rate
    time_df = model.observation.time.df
    first = time_df[time_df["node_id"] == observations.index[0]]
    assert first["variable"].unique().tolist() == ["flow_rate"]
    assert first["time"].tolist() == [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-03")]
    assert first["value"].tolist() == [-2.0, -4.0]
