import numpy as np
import pytest
from ribasim import Node
from ribasim.nodes import basin, discrete_control, level_boundary, outlet
from ribasim_nl.capacity import bound_supply_inflows
from shapely.geometry import MultiPolygon, Point, Polygon

from ribasim_nl import Model

AREA = 1e6  # m2


def square(x: float) -> MultiPolygon:
    half = AREA**0.5 / 2
    return MultiPolygon([Polygon([(x - half, -half), (x + half, -half), (x + half, half), (x - half, half)])])


@pytest.fixture
def model() -> Model:
    """Two basins in a row, with a controlled Outlet in between, draining to a LevelBoundary.

    Basin 1 -> Outlet 3 -> Basin 2 -> Outlet 4 -> LevelBoundary 5, DiscreteControl 6 controls Outlet 3.
    """
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    for node_id, x, target_level in ((1, 0, 1.0), (2, 2000, 0.0)):
        model.basin.add(
            Node(node_id, Point(x, 0), meta_categorie="doorgaand"),
            [
                basin.Profile(level=[-1.0, 2.0], area=[100.0, 100.0]),
                basin.State(level=[target_level]),
                basin.Area(geometry=[square(x)], meta_streefpeil=[target_level]),
            ],
        )
    model.outlet.add(
        Node(3, Point(1000, 0)),
        [
            outlet.Static(
                control_state=["aanvoer", "afvoer"],
                flow_rate=[1.0, 100.0],
                max_flow_rate=[1.0, 10.0],
                min_upstream_level=[0.9, 1.0],
                max_downstream_level=[np.inf, np.inf],
            )
        ],
    )
    model.outlet.add(Node(4, Point(3000, 0)), [outlet.Static(flow_rate=[100.0])])
    model.level_boundary.add(Node(5, Point(4000, 0)), [level_boundary.Static(level=[-1.0])])
    model.discrete_control.add(
        Node(6, Point(1000, 100)),
        [
            discrete_control.Variable(compound_variable_id=[1, 2], listen_node_id=[1, 2], variable=["level", "level"]),
            discrete_control.Condition(
                compound_variable_id=[1, 2], condition_id=[1, 2], threshold_high=[1.0, 0.0], threshold_low=[1.0, 0.0]
            ),
            discrete_control.Logic(truth_state=["TT", "TF", "FT", "FF"], control_state=["afvoer", "aanvoer"] * 2),
        ],
    )
    for from_node, to_node in (
        (model.basin[1], model.outlet[3]),
        (model.outlet[3], model.basin[2]),
        (model.basin[2], model.outlet[4]),
        (model.outlet[4], model.level_boundary[5]),
        (model.discrete_control[6], model.outlet[3]),
    ):
        model.link.add(from_node, to_node)
    return model


def static(model: Model, node_id: int, control_state: str | None = None):
    df = model.outlet.static.df
    assert df is not None
    rows = df["node_id"] == node_id
    if control_state is not None:
        rows &= df["control_state"] == control_state
    return df[rows].iloc[0]


def test_bound_supply_inflows(model):
    assert bound_supply_inflows(model) == [3]
    # supply up to the target level of Basin 2, drainage stays unbounded
    assert static(model, 3, "aanvoer")["max_downstream_level"] == 0.0
    assert np.isinf(static(model, 3, "afvoer")["max_downstream_level"])
    # nothing left to bound
    assert bound_supply_inflows(model) == []
