import pytest
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, pump
from ribasim_nl.validation import (
    _connector_through_junction_issues,
    _flow_demand_capacity_issues,
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


@pytest.fixture
def controlled_outlet_model() -> Model:
    """An Outlet between models with supply and drainage states, and an inlet of the other model with a FlowDemand.

    Basin 1 -> Outlet 2 -> LevelBoundary 3, LevelBoundary 5 -> Outlet 6 -> Basin 8,
    with DiscreteControl 4 -> Outlet 2 and FlowDemand 7 -> Outlet 6.
    """
    from ribasim.nodes import discrete_control, flow_demand, outlet

    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    for node_id, x in ((1, 0), (8, 60)):
        model.basin.add(
            Node(node_id, Point(x, 0)),
            [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])],
        )
    model.outlet.add(
        Node(2, Point(10, 0)),
        [outlet.Static(control_state=["aanvoer", "afvoer"], flow_rate=[0.0, 10.0], min_upstream_level=[0.2, 0.2])],
    )
    model.outlet.add(Node(6, Point(50, 0)), [outlet.Static(flow_rate=[1.0])])
    for node_id, x in ((3, 20), (5, 40)):
        model.level_boundary.add(Node(node_id, Point(x, 0)), [level_boundary.Static(level=[0.0])])
    model.discrete_control.add(
        Node(4, Point(10, 10)),
        [
            discrete_control.Variable(listen_node_id=[1], variable=["level"], compound_variable_id=[1]),
            discrete_control.Condition(threshold_high=[0.5], compound_variable_id=[1], condition_id=[1]),
            discrete_control.Logic(truth_state=["T", "F"], control_state=["afvoer", "aanvoer"]),
        ],
    )
    model.flow_demand.add(Node(7, Point(50, 10)), [flow_demand.Static(demand=[0.5], demand_priority=[1])])
    for from_node, to_node in (
        (model.basin[1], model.outlet[2]),
        (model.outlet[2], model.level_boundary[3]),
        (model.level_boundary[5], model.outlet[6]),
        (model.outlet[6], model.basin[8]),
        (model.discrete_control[4], model.outlet[2]),
        (model.flow_demand[7], model.outlet[6]),
    ):
        model.link.add(from_node, to_node)
    return model


def test_uncontrolled_control_states(controlled_outlet_model):
    model = controlled_outlet_model
    assert _uncontrolled_control_state_issues(model) == []
    model.remove_node(4, remove_links=True)
    issues = _uncontrolled_control_state_issues(model)
    assert len(issues) == 1
    assert "Outlet" in issues[0] and "[2]" in issues[0]


def test_flow_demand_capacity(controlled_outlet_model):
    model = controlled_outlet_model
    assert _flow_demand_capacity_issues(model) == []
    # without allocation, Ribasim ignores the part of a demand above max_flow_rate
    model.outlet.static.df.loc[model.outlet.static.df.node_id == 6, "max_flow_rate"] = 0.4
    assert _flow_demand_capacity_issues(model) == [
        "FlowDemand must not exceed the max_flow_rate of its Pump or Outlet in any control state; "
        "invalid (FlowDemand, connector) node IDs: [(7, 6)]"
    ]


def test_merge_outlets_keeps_discrete_control(controlled_outlet_model):
    # An outlet merged with an inlet with a FlowDemand keeps its DiscreteControl and control states
    model = controlled_outlet_model
    model.outlet.static.df.loc[model.outlet.static.df.node_id == 6, "max_downstream_level"] = 0.4
    model.remove_node(3, remove_links=True)
    model.remove_node(5, remove_links=True)
    model.merge_outlets(2, 6)
    static = model.outlet.static.df.set_index("node_id")
    assert static.loc[[2], "control_state"].tolist() == ["aanvoer", "afvoer"]
    assert static.loc[[2], "max_downstream_level"].tolist() == [0.4, 0.4]
    control = model.link.df[model.link.df["link_type"] == "control"]
    assert sorted(control[["from_node_id", "to_node_id"]].values.tolist()) == [[4, 2], [7, 2]]
    assert _uncontrolled_control_state_issues(model) == []
