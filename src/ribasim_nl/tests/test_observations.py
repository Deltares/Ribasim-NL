import geopandas as gpd
import pandas as pd
import pytest
from ribasim import Node
from ribasim.nodes import basin, observation, outlet
from ribasim_nl.observations import (
    LinkEnd,
    _points,
    _signs,
    _strings,
    add_observation_nodes,
    link_observations,
)
from shapely.geometry import Point

from ribasim_nl import Model


def test_parse_cells():
    assert _points("[<POINT (1.5 2)>, <POINT (-3 4e+05)>]") == [Point(1.5, 2), Point(-3, 4e5)]
    assert _strings("['Basin', 'Pump']") == ["Basin", "Pump"]


def test_signs():
    assert _signs(float("nan"), 1) == [1]
    assert _signs(float("nan"), 2) is None
    assert _signs("negatief_maken", 1) == [1]
    assert _signs("optellen", 3) == [1, 1, 1]
    assert _signs("link1+link2", 2) == [1, 1]
    assert _signs("link1-link2", 2) == [1, -1]
    assert _signs("link2-link1", 2) == [-1, 1]
    assert _signs("link1+link2+link3+link4-link5+link6", 6) == [1, 1, 1, 1, -1, 1]
    # each link once
    assert _signs("link1-link2", 3) is None
    assert _signs("link1-link1", 2) is None
    assert _signs("link1*link2", 2) is None


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
            "signs": [[1], [1, 1], [1], None, [1]],
            "koppeling": ["", "", "", "combines 2 links with link1*link2", ""],
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
        "combines 2 links with link1*link2",
        "",
    ]
    # measurements in the model period, with the sign of the flow_rate
    time_df = model.observation.time.df
    first = time_df[time_df["node_id"] == observations.index[0]]
    assert first["variable"].unique().tolist() == ["flow_rate"]
    assert first["time"].tolist() == [pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-03")]
    assert first["value"].tolist() == [-2.0, -4.0]


def basin_tree_model(towards_basin: bool) -> Model:
    """A Basin tree in which Junction 6 carries the inflow of Outlet 2 minus the outflow of Outlet 3.

    Basin 8 -> Outlet 2 -> Junction 6 -> Outlet 3 -> Basin 9, with Junction 6 - Junction 5 -> Basin 1, and
    Basin 10 -> Outlet 4 -> Junction 5. The link between the Junctions points to Basin 1 if `towards_basin`.
    """
    model = Model(starttime="2020-01-01", endtime="2020-01-03", crs="EPSG:28992")
    for node_id, x, y in ((1, 0, 0), (8, 40, 10), (9, 40, -10), (10, 20, 20)):
        model.basin.add(
            Node(node_id, Point(x, y)),
            [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])],
        )
    for node_id, x, y in ((2, 30, 5), (3, 30, -5), (4, 20, 10)):
        model.outlet.add(Node(node_id, Point(x, y)), [outlet.Static(flow_rate=[1.0])])
    model.junction.add(Node(5, Point(10, 0)))
    model.junction.add(Node(6, Point(20, 0)))
    between = (model.junction[6], model.junction[5]) if towards_basin else (model.junction[5], model.junction[6])
    for from_node, to_node in (
        (model.basin[8], model.outlet[2]),
        (model.outlet[2], model.junction[6]),
        (model.junction[6], model.outlet[3]),
        (model.outlet[3], model.basin[9]),
        between,
        (model.junction[5], model.basin[1]),
        (model.basin[10], model.outlet[4]),
        (model.outlet[4], model.junction[5]),
    ):
        model.link.add(from_node, to_node)
    return model


def net_flow_coupling(signs: list[int]) -> gpd.GeoDataFrame:
    """A series of the flow of Outlet 2 into Basin 1 and of Basin 1 into Outlet 3, with `signs`."""
    return gpd.GeoDataFrame(
        {
            "MeetreeksC": ["netto"],
            "Aan/Af": ["Afvoer"],
            "negate": [False],
            "ends": [
                [LinkEnd("Outlet", Point(30, 5), "Basin", False), LinkEnd("Outlet", Point(30, -5), "Basin", True)]
            ],
            "signs": [signs],
            "koppeling": [""],
        },
        geometry=[Point(25, 0)],
        crs="EPSG:28992",
    )


@pytest.mark.parametrize(
    ("towards_basin", "signs", "sign"),
    [(True, [1, -1], 1), (False, [1, -1], -1), (True, [-1, 1], -1), (False, [-1, 1], 1)],
)
def test_add_observation_nodes_net_flow(towards_basin: bool, signs: list[int], sign: int) -> None:
    """The net flow is that of the link of Junction 6 towards the Basin, in the direction of that link."""
    model = basin_tree_model(towards_basin)
    time = pd.date_range("2020-01-01", "2020-01-03", name="time")
    measurements = {"Aanvoer": pd.DataFrame(index=time), "Afvoer": pd.DataFrame({"netto": [1.0, 2.0, 3.0]}, index=time)}
    model = add_observation_nodes(model, net_flow_coupling(signs), measurements)
    links = model.link.df[model.link.df["link_type"] == "observation"]
    assert links["to_node_id"].tolist() == [6]
    assert model.observation.time.df["value"].tolist() == [sign * 1.0, sign * 2.0, sign * 3.0]

    # with Outlet 4, Junction 5 combines the inflows minus the outflow, but not the other way around
    for signs, observed in (([1, 1, -1], [5]), ([-1, 1, -1], [])):
        coupling = net_flow_coupling(signs)
        coupling.at[0, "ends"] = [LinkEnd("Outlet", Point(20, 10), "Basin", False), *coupling.at[0, "ends"]]
        model = add_observation_nodes(basin_tree_model(towards_basin), coupling, measurements)
        links = model.link.df[model.link.df["link_type"] == "observation"]
        assert links["to_node_id"].tolist() == observed
    assert model.node.df["meta_koppeling"].dropna().tolist() == ["no Junction combines exactly its 3 links"]


def test_link_observations(monkeypatch: pytest.MonkeyPatch) -> None:
    """An Observation that was not linked yet is linked, and its values get the sign of the observed flow rate."""
    model = basin_tree_model(towards_basin=False)
    model.observation.add(
        Node(7, Point(25, 0), name="netto", meta_aan_af="Afvoer", meta_koppeling="not linked yet"),
        [observation.Time(time=["2020-01-01", "2020-01-02"], variable="flow_rate", value=[1.0, 2.0])],
    )
    monkeypatch.setattr("ribasim_nl.observations.read_coupling", lambda authority: net_flow_coupling([1, -1]))
    model = link_observations(model, "Waterschap")
    links = model.link.df[model.link.df["link_type"] == "observation"]
    assert links[["from_node_id", "to_node_id"]].values.tolist() == [[7, 6]]
    assert model.observation.time.df["value"].tolist() == [-1.0, -2.0]
    assert model.node.df.at[7, "meta_koppeling"] == ""
