from typing import Any

import numpy as np
import pytest
from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet
from ribasim_nl.split_basins import split_basin_along_route
from shapely.geometry import LineString, MultiPolygon, Point, Polygon

from ribasim_nl import Model, junctionify


@pytest.fixture
def model() -> Model:
    """A canal from x=0 to 3000, Basin 3, with inflows from Outlets 2 and 11, and an outflow to Outlet 4.

    Outlet 2 is at the start of the canal, Outlet 11 joins it from the side at x=300, and Outlet 4 is at its end.
    """
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.level_boundary.add(Node(1, Point(-200, 0)), [level_boundary.Static(level=[0.0])])
    model.outlet.add(Node(2, Point(-100, 0)), [outlet.Static(flow_rate=[1.0])])
    model.basin.add(
        Node(3, Point(1500, 0), name="Kanaal", meta_categorie="hoofdwater"),
        [
            basin.Profile(level=[-5.0, 0.0], area=[1000.0, 300000.0]),
            basin.State(level=[-0.4]),
            basin.Static(precipitation=[0.0], potential_evaporation=[0.0], drainage=[0.0], infiltration=[0.0]),
            basin.Area(geometry=[MultiPolygon([Polygon([(0, -50), (3000, -50), (3000, 50), (0, 50)])])]),
        ],
    )
    model.outlet.add(Node(4, Point(3100, 0)), [outlet.Static(flow_rate=[1.0])])
    model.level_boundary.add(Node(5, Point(3200, 0)), [level_boundary.Static(level=[-1.0])])
    model.level_boundary.add(Node(10, Point(300, -400)), [level_boundary.Static(level=[0.0])])
    model.outlet.add(Node(11, Point(300, -300)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.level_boundary[1], model.outlet[2])
    model.link.add(model.outlet[2], model.basin[3], geometry=LineString([(-100, 0), (300, 0), (1500, 0)]))
    model.link.add(model.basin[3], model.outlet[4], geometry=LineString([(1500, 0), (3100, 0)]))
    model.link.add(model.outlet[4], model.level_boundary[5])
    model.link.add(model.level_boundary[10], model.outlet[11])
    model.link.add(model.outlet[11], model.basin[3], geometry=LineString([(300, -300), (300, 0), (1500, 0)]))
    return model


def test_split_basin_along_route(model):
    route = LineString([(-100, 0), (300, 0), (3100, 0)])
    basin_ids, manning_ids = split_basin_along_route(
        model,
        3,
        route,
        [Point(1000, 10), Point(2000, -5)],
        basin_names=["Kanaal A", "Kanaal B", "Kanaal C"],
        manning_names=["AB", "BC"],
        manning_n=0.04,
        profile_slope=1.0,
    )
    # the middle section keeps the Basin node
    assert basin_ids == [12, 3, 13]
    assert manning_ids == [14, 15]
    nodes = model.node.df
    assert nodes.loc[basin_ids, "name"].tolist() == ["Kanaal A", "Kanaal B", "Kanaal C"]
    assert nodes.loc[basin_ids, "meta_categorie"].tolist() == ["hoofdwater"] * 3
    # between where Outlet 11 joins the route and the cut, and between the cut and Outlet 4
    assert nodes.loc[basin_ids, "geometry"].x.tolist() == pytest.approx([650, 1500, 2500])
    assert nodes.loc[manning_ids, "geometry"].x.tolist() == pytest.approx([1000, 2000])
    assert (nodes.loc[[*basin_ids, *manning_ids], "geometry"].y == 0).all()

    area = model.basin.area.df.set_index("node_id").geometry
    assert area[basin_ids].area.tolist() == pytest.approx([100000.0] * 3)
    profile = model.basin.profile.df
    for node_id in basin_ids:
        assert profile[profile.node_id == node_id].area.tolist() == pytest.approx([1000 / 3, 100000.0])
    for table in (model.basin.state, model.basin.static):
        assert sorted(table.df.node_id) == sorted(basin_ids)

    static = model.manning_resistance.static.df.set_index("node_id")
    assert static.loc[manning_ids, "length"].tolist() == pytest.approx([850.0, 1000.0])
    assert static.loc[manning_ids, "profile_width"].tolist() == pytest.approx([100.0, 100.0])
    assert (static.manning_n == 0.04).all() and (static.profile_slope == 1.0).all()

    # the links keep their IDs, and run along the route to the Basin node of their section
    links = model.link.df
    assert sorted(links.index) == list(range(1, 11))
    assert links.loc[[2, 3, 6], ["from_node_id", "to_node_id"]].values.tolist() == [[2, 12], [13, 4], [11, 12]]
    assert list(links.at[6, "geometry"].coords) == [(300, -300), (300, 0), (650, 0)]
    assert list(links.at[2, "geometry"].coords) == [(-100, 0), (300, 0), (650, 0)]
    assert list(links.at[3, "geometry"].coords) == [(2500, 0), (3100, 0)]
    manning_links = links.loc[7:].sort_index()
    assert manning_links[["from_node_id", "to_node_id"]].values.tolist() == [[12, 14], [14, 3], [3, 15], [15, 13]]
    # the links of the sections run along the route, and are merged into Junctions where they overlap
    junctionify(model)
    model.validate_ribasim_nl()


def test_split_basin_along_route_invalid(model):
    route = LineString([(-100, 0), (3100, 0)])
    kwargs: dict[str, Any] = {
        "basin_names": ["A", "B"],
        "manning_names": ["AB"],
        "manning_n": 0.04,
        "profile_slope": 1.0,
    }
    with pytest.raises(ValueError, match="not a Basin"):
        split_basin_along_route(model, 2, route, [Point(1000, 0)], **kwargs)
    with pytest.raises(ValueError, match="outside"):
        split_basin_along_route(model, 3, route, [Point(3050, 0)], **kwargs)
    with pytest.raises(ValueError, match="one more Basin name"):
        split_basin_along_route(model, 3, route, [Point(1000, 0), Point(2000, 0)], **kwargs)
    assert np.isclose(model.basin.profile.df.area.sum(), 301000.0)
