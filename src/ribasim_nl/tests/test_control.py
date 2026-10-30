import numpy as np
import pytest
from ribasim import Node
from ribasim.nodes import basin, discrete_control, level_boundary, outlet
from ribasim_nl.control import set_continuous_level_control, set_discrete_level_control
from ribasim_nl.coupling_level_common import LEVEL_UPDATE_PROTECTION_COLUMN, THRESHOLD_UPDATE_PROTECTION_COLUMN
from shapely.geometry import MultiPolygon, Point, Polygon

from ribasim_nl import Model


def square(x: float) -> MultiPolygon:
    return MultiPolygon([Polygon([(x - 500, -500), (x + 500, -500), (x + 500, 500), (x - 500, 500)])])


@pytest.fixture
def model() -> Model:
    """LevelBoundary 1 -> Outlet 2 -> Basin 3 -> Outlet 4 -> LevelBoundary 5, DiscreteControl 6 controls Outlet 2.

    Links 1-4 are flow links, 5 is the control link and 6 the listen link of DiscreteControl 6.
    """
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.level_boundary.add(Node(1, Point(0, 0)), [level_boundary.Static(level=[0.0])])
    model.outlet.add(
        Node(2, Point(1000, 0)),
        [
            outlet.Static(
                control_state=["aanvoer", "afvoer"],
                flow_rate=[1.0, 0.0],
                max_flow_rate=[1.0, 0.0],
                max_downstream_level=[-0.52, np.inf],
                meta_from_level=[0.0, 0.0],
            )
        ],
    )
    model.basin.add(
        Node(3, Point(2000, 0)),
        [
            basin.Profile(level=[-2.0, 1.0], area=[100.0, 100.0]),
            basin.State(level=[-0.52]),
            basin.Area(geometry=[square(2000)], meta_streefpeil=[-0.52]),
        ],
    )
    model.outlet.add(Node(4, Point(3000, 0)), [outlet.Static(flow_rate=[10.0])])
    model.level_boundary.add(Node(5, Point(4000, 0)), [level_boundary.Static(level=[1.0])])
    model.discrete_control.add(
        Node(6, Point(1000, 100)),
        [
            discrete_control.Variable(compound_variable_id=[1], listen_node_id=[3], variable=["level"]),
            discrete_control.Condition(
                compound_variable_id=[1], condition_id=[1], threshold_high=[-0.495], threshold_low=[-0.545]
            ),
            discrete_control.Logic(truth_state=["F", "T"], control_state=["aanvoer", "afvoer"]),
        ],
    )
    for from_node, to_node in (
        (model.level_boundary[1], model.outlet[2]),
        (model.outlet[2], model.basin[3]),
        (model.basin[3], model.outlet[4]),
        (model.outlet[4], model.level_boundary[5]),
        (model.discrete_control[6], model.outlet[2]),
    ):
        model.link.add(from_node, to_node)
    return model


def test_set_continuous_level_control(model):
    control_id = set_continuous_level_control(
        model, 2, listen_node_id=3, level=[-0.52, -0.53, -0.55, -1.0], flow_rate=[0.0, 23.0, 46.0, 46.0], name="inlaat"
    )
    # the DiscreteControl is replaced, keeping its node and link IDs
    assert control_id == 6
    assert (model.node.df.node_type != "DiscreteControl").all()
    assert model.discrete_control.variable.df is None or model.discrete_control.variable.df.empty
    assert model.get_node_type(control_id) == "ContinuousControl"
    assert model.node.df.at[control_id, "name"] == "inlaat"
    control_links = model.link.df[model.link.df.link_type == "control"]
    assert control_links[["from_node_id", "to_node_id"]].values.tolist() == [[control_id, 2]]
    assert control_links.index.tolist() == [5]
    listen_links = model.link.df[model.link.df.link_type == "listen"]
    assert listen_links[["from_node_id", "to_node_id"]].values.tolist() == [[3, control_id]]
    assert listen_links.index.tolist() == [6]
    assert sorted(model.link.df.index) == [1, 2, 3, 4, 5, 6]

    function = model.continuous_control.function.df
    assert function.input.tolist() == [-1.0, -0.55, -0.53, -0.52]
    assert function.output.tolist() == [46.0, 46.0, 23.0, 0.0]
    assert model.continuous_control.variable.df.listen_node_id.tolist() == [3]

    static = model.outlet.static.df[model.outlet.static.df.node_id == 2]
    assert len(static) == 1
    row = static.iloc[0]
    assert row.max_flow_rate == 46.0
    assert row.min_flow_rate == 0.0
    assert np.isinf(row.max_downstream_level)
    assert row.meta_from_level == 0.0
    assert row[LEVEL_UPDATE_PROTECTION_COLUMN]
    assert model.outlet.static.df[model.outlet.static.df.node_id == 4].flow_rate.tolist() == [10.0]


def test_set_continuous_level_control_to_pump(model):
    control_id = set_continuous_level_control(
        model, 4, listen_node_id=3, level=[-1.0, -0.52, -0.42], flow_rate=[0.0, 5.0, 30.0], node_type="Pump"
    )
    # Outlet 4 had no DiscreteControl, so the ContinuousControl and its links get new IDs
    assert control_id == 7
    assert sorted(model.link.df.index) == [1, 2, 3, 4, 5, 6, 7, 8]
    assert model.get_node_type(4) == "Pump"
    assert (model.outlet.static.df.node_id != 4).all()
    assert model.pump.static.df.set_index("node_id").at[4, "max_flow_rate"] == 30.0
    assert set(model.link.df[(model.link.df.from_node_id == 4) | (model.link.df.to_node_id == 4)].link_type) == {
        "flow",
        "control",
    }


@pytest.mark.parametrize(
    ("listen_node_id", "level", "flow_rate"),
    [
        (5, [-1.0, 0.0], [0.0, 1.0]),  # not a Basin
        (3, [-1.0], [1.0]),  # too few points
        (3, [-1.0, -1.0], [0.0, 1.0]),  # duplicate levels
        (3, [-1.0, 0.0], [0.0, -1.0]),  # negative flow rate
    ],
)
def test_set_continuous_level_control_invalid(model, listen_node_id, level, flow_rate):
    with pytest.raises(ValueError):
        set_continuous_level_control(model, 2, listen_node_id=listen_node_id, level=level, flow_rate=flow_rate)


def test_set_discrete_level_control(model):
    control_id = set_discrete_level_control(
        model,
        2,
        listen_node_id=3,
        level=[-0.59, -0.57],
        flow_rate=[0.0, 34.5, 40.0],
        hysteresis=[0.005, 0.0025],
        min_upstream_level=-0.65,
        name="peil boezem",
    )
    # the DiscreteControl is replaced, keeping its node and link IDs
    assert control_id == 6
    assert model.get_node_type(control_id) == "DiscreteControl"
    assert model.node.df.at[control_id, "name"] == "peil boezem"
    assert sorted(model.link.df.index) == [1, 2, 3, 4, 5, 6]
    listen_links = model.link.df[model.link.df.link_type == "listen"]
    assert listen_links[["from_node_id", "to_node_id"]].values.tolist() == [[3, control_id]]

    condition = model.discrete_control.condition.df
    assert condition.condition_id.tolist() == [1, 2]
    np.testing.assert_allclose(condition.threshold_high, [-0.59, -0.57])
    np.testing.assert_allclose(condition.threshold_low, [-0.595, -0.5725])
    assert condition[THRESHOLD_UPDATE_PROTECTION_COLUMN].all()
    logic = model.discrete_control.logic.df
    assert logic.truth_state.tolist() == ["FF", "TF", "TT"]
    assert logic.control_state.tolist() == ["stap 0", "stap 1", "stap 2"]

    static = model.outlet.static.df[model.outlet.static.df.node_id == 2]
    assert static.control_state.tolist() == ["stap 0", "stap 1", "stap 2"]
    assert static.flow_rate.tolist() == [0.0, 34.5, 40.0]
    assert (static.max_flow_rate == 40.0).all()
    assert (static.min_upstream_level == -0.65).all()
    assert (static.meta_from_level == 0.0).all()
    assert static[LEVEL_UPDATE_PROTECTION_COLUMN].all()


@pytest.mark.parametrize(
    ("level", "flow_rate", "hysteresis"),
    [
        ([-0.6], [0.0], 0.0025),  # no flow rate above the level
        ([-0.6, -0.601], [0.0, 1.0, 2.0], 0.0025),  # descending levels
        ([-0.6, -0.599], [0.0, 1.0, 2.0], 0.0025),  # a step switches off below where the one below switches on
        ([-0.6], [0.0, 1.0], 0.0),  # no hysteresis
        ([-0.6], [0.0, -1.0], 0.0025),  # negative flow rate
    ],
)
def test_set_discrete_level_control_invalid(model, level, flow_rate, hysteresis):
    with pytest.raises(ValueError):
        set_discrete_level_control(model, 2, listen_node_id=3, level=level, flow_rate=flow_rate, hysteresis=hysteresis)
