import datetime

import pytest
from peilbeheerst_model.ribasim_parametrization import (
    set_dynamic_level_boundaries,
    set_static_level_boundaries,
)
from ribasim import Node
from ribasim.nodes import basin, level_boundary
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
