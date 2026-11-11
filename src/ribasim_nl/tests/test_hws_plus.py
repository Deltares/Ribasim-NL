from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from ribasim import Node
from ribasim.input_base import NodeData
from ribasim.nodes import (
    basin,
    discrete_control,
    flow_boundary,
    level_boundary,
    manning_resistance,
    outlet,
    pump,
)
from ribasim_nl.flow_paths import Routes
from ribasim_nl.hws_plus import changes, hws_plus
from shapely.geometry import Point

from ribasim_nl import Model

# HWS Basins A and B, regional Basins R1 and R2 of Hollands Noorderkwartier (waterbeheercode 12)
A, B, R1, R2 = 8000001, 8000002, 1200001, 1200002
TIME = pd.date_range("2020-01-01", periods=4, name="time")


def control(model: Model, node_id: int, point: Point, listen_node_id: int, target: NodeData) -> None:
    """A DiscreteControl that switches `target` between "on" and "off" by the level of `listen_node_id`."""
    model.discrete_control.add(
        Node(node_id, point),
        [
            discrete_control.Variable(listen_node_id=[listen_node_id], variable=["level"], compound_variable_id=[1]),
            discrete_control.Condition(threshold_high=[1.0], compound_variable_id=[1], condition_id=[1]),
            discrete_control.Logic(truth_state=["T", "F"], control_state=["on", "off"]),
        ],
    )
    model.link.add(model.discrete_control[node_id], target)


@pytest.fixture
def coupled() -> Model:
    """A coupled model of the HWS Basins A and B, and the regional Basins R1 and R2.

    - Outlet 8000003 from A to B, controlled by DiscreteControl 8000020 by the level of A;
    - Outlet 8000031 from B to the sea, LevelBoundary 8000030;
    - FlowBoundary 1200040, an RWZI of the regional model, into B;
    - Outlet 1200003 from R1 into the tree of A, through Junction 8000010;
    - Pump 1200004 and Outlet 1200005 from B into the tree of R2, through Junction 1200010, whose link with R2 points
      away from R2. The Pump is controlled by DiscreteControl 1200020 by the level of B;
    - ManningResistance 1200006 from R1 to B, whose water flows from B to R1;
    - Outlet 1200007 from R1 to R2.
    """
    model = Model(starttime=TIME[0], endtime=TIME[-1], crs="EPSG:28992")
    tables = [basin.Profile(level=[0.0, 2.0], area=[1e4, 1e4]), basin.State(level=[1.0])]
    for node_id, point in ((A, Point(0, 0)), (B, Point(1000, 0)), (R1, Point(0, 600)), (R2, Point(1000, 600))):
        model.basin.add(Node(node_id, point), tables)
    states = pump.Static(flow_rate=[1.0, 0.0], control_state=["on", "off"])
    model.outlet.add(Node(8000003, Point(500, 0)), [outlet.Static(flow_rate=[1.0, 0.0], control_state=["on", "off"])])
    model.outlet.add(Node(8000031, Point(1500, 0)), [outlet.Static(flow_rate=[1.0])])
    model.level_boundary.add(Node(8000030, Point(2000, 0)), [level_boundary.Static(level=[0.0])])
    model.flow_boundary.add(Node(1200040, Point(1000, -200)), [flow_boundary.Static(flow_rate=[0.1])])
    model.outlet.add(Node(1200003, Point(0, 400)), [outlet.Static(flow_rate=[1.0])])
    model.junction.add(Node(8000010, Point(0, 200)))
    model.pump.add(Node(1200004, Point(900, 300)), [states])
    model.outlet.add(Node(1200005, Point(1100, 300)), [outlet.Static(flow_rate=[1.0])])
    model.junction.add(Node(1200010, Point(1000, 400)))
    mr = manning_resistance.Static(length=[100.0], manning_n=[0.04], profile_width=[10.0], profile_slope=[1.0])
    model.manning_resistance.add(Node(1200006, Point(500, 300)), [mr])
    model.outlet.add(Node(1200007, Point(500, 800)), [outlet.Static(flow_rate=[1.0])])
    for from_node_id, to_node_id in (
        (A, 8000003),
        (8000003, B),
        (B, 8000031),
        (8000031, 8000030),
        (1200040, B),
        (R1, 1200003),
        (1200003, 8000010),
        (8000010, A),
        (B, 1200004),
        (1200004, 1200010),
        (B, 1200005),
        (1200005, 1200010),
        (R2, 1200010),
        (R1, 1200006),
        (1200006, B),
        (R1, 1200007),
        (1200007, R2),
    ):
        model.link.add(model.get_node(from_node_id), model.get_node(to_node_id))
    control(model, 8000020, Point(500, -100), A, model.outlet[8000003])
    control(model, 1200020, Point(900, 200), B, model.pump[1200004])
    return model


def flow_results(model: Model) -> xr.DataArray:
    """The flow rates of a run of `coupled`, of each link along the structure it connects to."""
    flows = {
        1200003: [1.0, 2.0, 2.0, 3.0],
        1200004: [0.5, 0.5, 0.0, 0.0],
        1200006: [-1.0, -1.0, -2.0, -2.0],
    }
    links = model.link.df[model.link.df["link_type"] == "flow"]
    values = np.zeros((len(TIME), len(links)))
    for column, (from_node_id, to_node_id) in enumerate(zip(links["from_node_id"], links["to_node_id"], strict=True)):
        values[:, column] = flows.get(from_node_id, flows.get(to_node_id, [0.0] * len(TIME)))
    # the link IDs of Ribasim results are floats
    coords = {"time": TIME, "link_id": links.index.to_numpy().astype(float)}
    return xr.DataArray(values, coords=coords, dims=("time", "link_id"))


def concentration_results() -> xr.DataArray:
    """The concentrations of a run of `coupled`, with initial water and rain in R1 only."""
    substances = ["Continuity", "Initial", "Neerslag"]
    values = np.zeros((len(TIME), len(substances), 4))
    values[:, 0, :] = 1.0
    values[:, 1, 0] = [0.9, 0.8, 0.8, 0.7]
    values[:, 2, 0] = [0.1, 0.2, 0.2, 0.3]
    coords = {"time": TIME, "substance": substances, "node_id": [R1, R2, A, B]}
    return xr.DataArray(values, coords=coords, dims=("time", "substance", "node_id"))


def link_pairs(model: Model) -> set[tuple[int, int]]:
    links = model.link.df
    return set(zip(links["from_node_id"], links["to_node_id"], strict=True))


def test_changes():
    values = np.array([[1.0, 0.0], [1.0, 0.0], [2.0, 0.0], [2.0, 0.0], [2.0, 0.0]])
    expected = [[True, True], [False, False], [True, False], [False, False], [True, True]]
    assert changes(values).tolist() == expected


def test_hws_plus(coupled: Model, tmp_path: Path):
    link_ids = coupled.link.df.reset_index().set_index(["from_node_id", "to_node_id"])["link_id"]
    model = hws_plus(coupled, flow_results(coupled), concentration_results())

    node_type = model.node.df["node_type"]
    assert node_type.to_dict() == {
        A: "Basin",
        B: "Basin",
        8000003: "Outlet",
        8000031: "Outlet",
        8000010: "Junction",
        1200010: "Junction",
        8000020: "DiscreteControl",
        8000030: "LevelBoundary",
        1200040: "FlowBoundary",
        1200003: "FlowBoundary",
        1200004: "Pump",
        1200005: "Pump",
        1200006: "Pump",
        R1: "Terminal",
        R2: "Terminal",
    }
    assert Routes(model).simplified_links() == {
        (A, 8000003),
        (8000003, B),
        (B, 8000031),
        (8000031, 8000030),
        (1200040, B),
        (1200003, A),
        (B, 1200004),
        (1200004, R2),
        (B, 1200005),
        (1200005, R2),
        (B, 1200006),
        (1200006, R1),
    }
    # the links keep their IDs, also those that are reversed: of the ManningResistance with the HWS, and of R2 with
    # its tree, which now points to the Terminal
    pairs = model.link.df.reset_index().set_index(["from_node_id", "to_node_id"])["link_id"]
    assert pairs[(B, 1200006)] == link_ids[(1200006, B)]
    assert pairs[(1200010, R2)] == link_ids[(R2, 1200010)]
    assert pairs[(1200004, 1200010)] == link_ids[(1200004, 1200010)]
    assert (R1, 1200003) not in link_pairs(model)
    assert (A, 8000020) in link_pairs(model)
    assert (
        model.node.df.loc[[1200003, 1200006, R1], "meta_couple_authority"].tolist() == ["HollandsNoorderkwartier"] * 3
    )

    # the flows of the run, without repeated values
    time = model.flow_boundary.time.df.set_index(["node_id", "time"])["flow_rate"]
    assert time[1200003].tolist() == [1.0, 2.0, 3.0]
    assert time[1200003].index.tolist() == [TIME[0], TIME[1], TIME[3]]
    pump_time = model.pump.time.df.set_index(["node_id", "time"])["flow_rate"]
    assert pump_time[1200004].tolist() == [0.5, 0.0, 0.0]
    assert pump_time[1200005].tolist() == [0.0, 0.0]
    assert pump_time[1200006].tolist() == [1.0, 2.0, 2.0]

    # the concentrations of R1, with the tracer of its authority, and that of Rijkswaterstaat in the rest of the HWS
    concentration = model.flow_boundary.concentration.df.set_index(["node_id", "substance"])
    assert concentration.loc[(1200003, "Initial"), "concentration"].tolist() == [0.9, 0.8, 0.7]
    assert concentration.loc[(1200003, "Neerslag"), "concentration"].tolist() == [0.1, 0.2, 0.3]
    assert concentration.loc[(1200003, "HollandsNoorderkwartier"), "concentration"] == 1.0
    assert concentration.loc[(1200040, "HollandsNoorderkwartier"), "concentration"] == 1.0
    level_concentration = model.level_boundary.concentration.df
    assert level_concentration[["node_id", "substance"]].to_numpy().tolist() == [[8000030, "Rijkswaterstaat"]]
    for table in (model.basin.concentration, model.basin.concentration_state):
        assert sorted(table.df.loc[table.df["substance"] == "Rijkswaterstaat", "node_id"]) == [A, B]

    model.write(tmp_path / "hws_plus.toml")


def test_hws_plus_both_directions(coupled: Model):
    """A structure that exchanges water with the HWS in both directions cannot become a boundary."""
    flow = flow_results(coupled)
    link_id = coupled.link.df.index[coupled.link.df["from_node_id"] == 1200003][0]
    flow.loc[{"link_id": float(link_id), "time": TIME[0]}] = -1.0
    with pytest.raises(ValueError, match="both directions"):
        hws_plus(coupled, flow, concentration_results())
