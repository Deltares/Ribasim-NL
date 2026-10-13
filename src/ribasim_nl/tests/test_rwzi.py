from ribasim import Node
from ribasim.nodes import basin, flow_boundary, outlet
from ribasim_nl.rwzi import attach_rwzi_junctions
from shapely.geometry import LineString, Point

from ribasim_nl import Model


def rwzi_model() -> Model:
    """Outlet 3 drains Basin 2 into Basin 1 through Junction 4, RWZI 5 discharges into Basin 1 through Junction 6."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    for node_id, x in ((1, 0), (2, 100)):
        model.basin.add(
            Node(node_id, Point(x, 0)),
            [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])],
        )
    model.outlet.add(Node(3, Point(90, 0)), [outlet.Static(flow_rate=[1.0])])
    model.junction.add(Node(4, Point(50, 0)))
    model.flow_boundary.add(Node(5, Point(60, 30), meta_rwzi_code="R1"), [flow_boundary.Static(flow_rate=[0.1])])
    model.junction.add(Node(6, Point(60, 20), meta_rwzi_code="R1"))
    model.link.add(model.basin[2], model.outlet[3])
    model.link.add(model.outlet[3], model.junction[4], geometry=LineString([(90, 0), (70, 5), (50, 0)]))
    model.link.add(model.junction[4], model.basin[1])
    model.link.add(model.flow_boundary[5], model.junction[6])
    model.link.add(model.junction[6], model.basin[1])
    return model


def test_attach_rwzi_junctions():
    model = attach_rwzi_junctions(rwzi_model())
    # on the nearest point of the link from Outlet 3 into Basin 1, not the link out of Basin 2
    line = LineString([(90, 0), (70, 5), (50, 0)])
    location = model.node.df.at[6, "geometry"]
    assert location.distance(line.interpolate(line.project(Point(60, 20)))) < 1e-9
    links = model.link.df.set_index(["from_node_id", "to_node_id"]).geometry
    assert set(links.index) == {(2, 3), (3, 6), (6, 4), (4, 1), (5, 6)}
    assert links[(3, 6)].coords[-1] == links[(6, 4)].coords[0] == location.coords[0]
    assert links[(5, 6)].coords[:] == [(60, 30), location.coords[0]]


def test_attach_rwzi_junctions_too_far():
    model = attach_rwzi_junctions(rwzi_model(), max_distance=5.0)
    assert model.node.df.at[6, "geometry"].equals(Point(60, 20))
    assert (6, 1) in model.link.df.set_index(["from_node_id", "to_node_id"]).index
