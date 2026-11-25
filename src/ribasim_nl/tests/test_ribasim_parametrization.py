import datetime

import pytest
from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet, pump
from ribasim_nl.ribasim_parametrization import (
    reassign_level_boundaries,
    set_dynamic_level_boundaries,
    set_static_level_boundaries,
)
from ribasim_nl.structure_locations import CROSSING_COLUMNS
from shapely.geometry import Point

from ribasim_nl import Model


@pytest.fixture
def model() -> Model:
    """A Basin and three LevelBoundaries with a level time series."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.basin.add(Node(1, Point(0, 0)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0])])
    for node_id in (2, 3, 4):
        model.level_boundary.add(Node(node_id, Point(node_id, 0)), [level_boundary.Static(level=[0.0])])
    model.level_boundary.static.df = None
    time = [datetime.datetime(2020, 1, 1), datetime.datetime(2020, 6, 1)]
    set_dynamic_level_boundaries(model, time, [-2.3456, 10.0])
    return model


def test_set_static_level_boundaries(model):
    set_static_level_boundaries(model, [2, 4], level=0.0)

    assert model.level_boundary.static.df[["node_id", "level"]].to_numpy().tolist() == [[2, 0.0], [4, 0.0]]
    assert model.level_boundary.time.df["node_id"].unique().tolist() == [3]

    # overwrite an existing static level, and drop the time table once it is empty
    set_static_level_boundaries(model, [3, 4], level=-0.4)
    assert model.level_boundary.static.df[["node_id", "level"]].to_numpy().tolist() == [
        [2, 0.0],
        [3, -0.4],
        [4, -0.4],
    ]
    assert model.level_boundary.time.df is None


def test_set_static_level_boundaries_not_level_boundary(model):
    with pytest.raises(AssertionError, match="Not LevelBoundary nodes"):
        set_static_level_boundaries(model, [1], level=0.0)


def test_reassign_level_boundaries_beside_crossing():
    """Basin 1 -> Pump 2 -> LevelBoundary 3 -> Outlet 4 -> Basin 1, with the Pump at its gemaal, 30 m from its crossing."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.basin.add(Node(1, Point(0, 0)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0])])
    model.pump.add(Node(2, Point(130, 0)), [pump.Static(flow_rate=[1.0])])
    model.level_boundary.add(Node(3, Point(300, 0)), [level_boundary.Static(level=[1.5])])
    model.outlet.add(Node(4, Point(100, 20)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[1], model.pump[2])
    model.link.add(model.pump[2], model.level_boundary[3])
    model.link.add(model.level_boundary[3], model.outlet[4])
    model.link.add(model.outlet[4], model.basin[1])
    model.node.df[CROSSING_COLUMNS] = float("nan")
    model.node.df.loc[2, CROSSING_COLUMNS] = [100.0, 0.0]

    reassign_level_boundaries(model, [3])

    boundaries = model.node.df[model.node.df["node_type"] == "LevelBoundary"]
    assert 3 not in boundaries.index and len(boundaries) == 2
    links = model.link.df
    beside = {
        int(connector): boundaries.geometry[boundary]
        for connector, boundary in [
            *zip(links["from_node_id"], links["to_node_id"], strict=True),
            *zip(links["to_node_id"], links["from_node_id"], strict=True),
        ]
        if boundary in boundaries.index
    }
    # 50 m from the crossing of the Pump, not from its gemaal, so its key and its authority do not follow the gemaal
    assert beside[2].equals(Point(150, 0))
    assert beside[4].equals(Point(150, 20))
