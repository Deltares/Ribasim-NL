from ribasim import Node
from ribasim.nodes import basin, level_boundary, outlet
from ribasim_nl.flow_paths import simplified_links
from ribasim_nl.junctions import find_common_linestring, junctionify
from shapely.geometry import LineString, Point

from ribasim_nl import Model


def test_find_common_linestring():
    a = LineString([(0, 1), (1, 0), (2, 0), (3, 0)])
    b = LineString([(0, -1), (1, 0), (2, 0), (3, 0)])
    common, mapping, stripped = find_common_linestring([a, b])
    assert [list(c.coords) for c in common] == [[(1, 0), (2, 0), (3, 0)]]
    assert mapping == [0, 0]
    assert [list(s.coords) for s in stripped] == [[(0, 1), (1, 0)], [(0, -1), (1, 0)]]


def test_find_common_linestring_contained():
    # b is fully part of a, so its first segment is kept separate
    a = LineString([(0, 0), (1, 0), (2, 0), (3, 0)])
    b = LineString([(1, 0), (2, 0), (3, 0)])
    common, mapping, stripped = find_common_linestring([a, b])
    assert [list(c.coords) for c in common] == [[(2, 0), (3, 0)]]
    assert mapping == [0, 0]
    assert [list(s.coords) for s in stripped] == [[(0, 0), (1, 0), (2, 0)], [(1, 0), (2, 0)]]

    # starting from the begin points
    common, mapping, stripped = find_common_linestring([a.reverse(), b.reverse()], converging=False)
    assert [list(c.coords) for c in common] == [[(3, 0), (2, 0)]]
    assert [list(s.coords) for s in stripped] == [[(2, 0), (1, 0), (0, 0)], [(2, 0), (1, 0)]]


def test_junctionify():
    """Outlets 2 and 3 drain into Basin 1 along a shared route, and into LevelBoundary 6 along another."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    for node_id, x in ((1, 0), (4, 20), (5, 20)):
        model.basin.add(
            Node(node_id, Point(x, 10 * (node_id == 5))),
            [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])],
        )
    model.level_boundary.add(Node(6, Point(10, 20)), [level_boundary.Static(level=[0.0])])
    for node_id, y in ((2, 1), (3, -1), (7, 9), (8, 11)):
        model.outlet.add(Node(node_id, Point(10, y)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[4], model.outlet[2])
    model.link.add(model.basin[5], model.outlet[3])
    model.link.add(model.outlet[2], model.basin[1], geometry=LineString([(10, 1), (5, 0), (2, 0), (0, 0)]))
    model.link.add(model.outlet[3], model.basin[1], geometry=LineString([(10, -1), (5, 0), (2, 0), (0, 0)]))
    # no Junction is put in front of a LevelBoundary
    model.link.add(model.basin[4], model.outlet[7])
    model.link.add(model.basin[5], model.outlet[8])
    model.link.add(model.outlet[7], model.level_boundary[6], geometry=LineString([(10, 9), (10, 15), (10, 20)]))
    model.link.add(model.outlet[8], model.level_boundary[6], geometry=LineString([(10, 11), (10, 15), (10, 20)]))

    model = junctionify(model)
    junctions = model.node.df.index[model.node.df.node_type == "Junction"]
    assert len(junctions) == 1
    junction = junctions[0]
    assert model.node.df.at[junction, "geometry"].equals(Point(5, 0))
    links = model.link.df.set_index(["from_node_id", "to_node_id"]).geometry
    assert list(links[(junction, 1)].coords) == [(5, 0), (2, 0), (0, 0)]
    assert list(links[(2, junction)].coords) == [(10, 1), (5, 0)]
    assert list(links[(3, junction)].coords) == [(10, -1), (5, 0)]
    assert (8, 6) in links.index


def test_junctionify_inflow_and_outflow():
    """Outlet 2 drains into Basin 1 and Outlet 3 is fed by it along a shared route, which becomes one tree."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.basin.add(
        Node(1, Point(0, 0)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])]
    )
    for node_id, y in ((2, 1), (3, -1)):
        model.outlet.add(Node(node_id, Point(10, y)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.outlet[2], model.basin[1], geometry=LineString([(10, 1), (5, 0), (0, 0)]))
    model.link.add(model.basin[1], model.outlet[3], geometry=LineString([(0, 0), (5, 0), (10, -1)]))
    links_before = simplified_links(model)

    model = junctionify(model)
    (junction,) = model.node.df.index[model.node.df.node_type == "Junction"]
    assert model.node.df.at[junction, "geometry"].equals(Point(5, 0))
    assert simplified_links(model) == links_before
    links = model.link.df.set_index(["from_node_id", "to_node_id"]).geometry
    assert set(links.index) == {(1, junction), (2, junction), (junction, 3)}
    assert list(links[(1, junction)].coords) == [(0, 0), (5, 0)]
