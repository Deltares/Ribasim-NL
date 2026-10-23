import pytest
from ribasim import Node
from ribasim.nodes import basin, discrete_control, level_boundary, manning_resistance, pump
from ribasim_nl.validation import (
    _connector_through_junction_issues,
    _listen_to_junction_issues,
    _manning_resistance_issues,
    _uncontrolled_control_state_issues,
)
from shapely.geometry import Point

from ribasim_nl import Model


@pytest.fixture
def model() -> Model:
    """Two pumps that meet in a Junction, which drains through a third pump.

    Basin 1 -> Pump 3 -> Junction 6 -> Junction 7 -> Pump 5 -> LevelBoundary 8
    Basin 2 -> Pump 4 ----^
    """
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    for node_id, y in ((1, 10), (2, -10)):
        model.basin.add(
            Node(node_id, Point(0, y)),
            [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])],
        )
    for node_id, (x, y) in ((3, (10, 10)), (4, (10, -10)), (5, (40, 0))):
        model.pump.add(Node(node_id, Point(x, y)), [pump.Static(flow_rate=[1.0])])
    model.junction.add(Node(6, Point(20, 0)))
    model.junction.add(Node(7, Point(30, 0)))
    model.level_boundary.add(Node(8, Point(50, 0)), [level_boundary.Static(level=[0.0])])
    for from_node, to_node in (
        (model.basin[1], model.pump[3]),
        (model.basin[2], model.pump[4]),
        (model.pump[3], model.junction[6]),
        (model.pump[4], model.junction[6]),
        (model.junction[6], model.junction[7]),
        (model.junction[7], model.pump[5]),
        (model.pump[5], model.level_boundary[8]),
    ):
        model.link.add(from_node, to_node)
    return model


def test_connector_through_junction(model):
    issues = _connector_through_junction_issues(model)
    assert len(issues) == 1
    assert "[(3, 5), (4, 5)]" in issues[0]
    with pytest.raises(ValueError, match="through Junctions"):
        model.validate_ribasim_nl()


def test_connector_through_junction_to_basin(model):
    # Draining into a Basin instead of a Pump through the Junctions is valid
    model.remove_node(5, remove_links=True)
    model.basin.add(
        Node(9, Point(40, 0)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])]
    )
    model.link.add(model.junction[7], model.basin[9])
    assert _connector_through_junction_issues(model) == []


@pytest.mark.parametrize(
    ("length", "manning_n", "valid"), [(10.0, 0.01, True), (1.0, 0.03, False), (100.0, 0.001, False)]
)
def test_manning_resistance_bounds(length, manning_n, valid):
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.manning_resistance.add(
        Node(1, Point(0, 0)),
        [manning_resistance.Static(length=[length], manning_n=[manning_n], profile_width=[2.0], profile_slope=[3.0])],
    )
    issues = _manning_resistance_issues(model)
    assert issues == [] if valid else "[1]" in issues[0]


def test_listen_to_junction(model):
    from ribasim.nodes import discrete_control

    model.discrete_control.add(
        Node(9, Point(20, 10)),
        [
            discrete_control.Variable(listen_node_id=[6], variable=["level"], compound_variable_id=[1]),
            discrete_control.Condition(threshold_high=[1.0], compound_variable_id=[1], condition_id=[1]),
            discrete_control.Logic(truth_state=["T", "F"], control_state=["on", "off"]),
        ],
    )
    assert _listen_to_junction_issues(model) == [
        "Control nodes must not listen to a Junction; invalid (control, Junction) node IDs: [(9, 6)]"
    ]


def test_uncontrolled_control_state(model):
    # Pump 5 gets an aanvoer and an afvoer state, without a DiscreteControl to switch between them
    model.remove_node(5, remove_links=True)
    model.pump.add(Node(5, Point(40, 0)), [pump.Static(control_state=["aanvoer", "afvoer"], flow_rate=[0.0, 1.0])])
    model.link.add(model.junction[7], model.pump[5])
    model.link.add(model.pump[5], model.level_boundary[8])
    issues = _uncontrolled_control_state_issues(model)
    assert issues == ["Pump nodes with several control states must be controlled; invalid node IDs: [5]"]

    model.discrete_control.add(
        Node(9, Point(40, 10)),
        [
            discrete_control.Variable(compound_variable_id=1, listen_node_id=[1], variable=["level"]),
            discrete_control.Condition(compound_variable_id=1, condition_id=1, threshold_high=[0.5]),
            discrete_control.Logic(truth_state=["F", "T"], control_state=["aanvoer", "afvoer"]),
        ],
    )
    model.link.add(model.discrete_control[9], model.pump[5])
    assert _uncontrolled_control_state_issues(model) == []
