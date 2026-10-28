from ribasim import Node
from ribasim.nodes import basin, flow_boundary, outlet
from ribasim_nl.rwzi import attach_rwzi_junctions, create_rwzi_basin_coupling
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
    model.flow_boundary.add(Node(5, Point(60, 30), meta_rwzi_code=1.0), [flow_boundary.Static(flow_rate=[0.1])])
    model.junction.add(Node(6, Point(60, 20), meta_rwzi_code=1.0))
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


def test_attach_rwzi_junctions_out_of_basin():
    """Along the link out of Basin 1, as the FlowBoundary cannot discharge into its Outlet."""
    model = rwzi_model()
    model.outlet.add(Node(7, Point(-50, 0)), [outlet.Static(flow_rate=[1.0])])
    line = LineString([(0, 0), (60, 19), (-50, 0)])
    model.link.add(model.basin[1], model.outlet[7], geometry=line)
    model = attach_rwzi_junctions(model)
    location = model.node.df.at[6, "geometry"]
    assert location.distance(line.interpolate(line.project(Point(60, 20)))) < 1e-9
    links = model.link.df.set_index(["from_node_id", "to_node_id"]).geometry
    assert set(links.index) == {(2, 3), (3, 4), (4, 1), (1, 7), (5, 6), (6, 1)}
    # the link to Basin 1 runs back along the link out of it
    assert links[(6, 1)].coords[:] == [location.coords[0], (0, 0)]
    assert links[(1, 7)].equals(line)


def test_attach_rwzi_junctions_not_bergend():
    """Not on the link of a bergend Basin, even if it is nearer."""
    model = rwzi_model()
    model.basin.add(
        Node(8, Point(60, 25), meta_categorie="bergend"),
        [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])],
    )
    model.outlet.add(Node(9, Point(55, 25), meta_categorie="bergend"), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[8], model.outlet[9])
    model.link.add(model.outlet[9], model.basin[1], geometry=LineString([(55, 25), (60, 21), (0, 0)]))
    model = attach_rwzi_junctions(model)
    assert (3, 6) in model.link.df.set_index(["from_node_id", "to_node_id"]).index


def test_attach_rwzi_junctions_other_basin():
    """Along the link out of Basin 2 if configured, at any distance, and only once."""
    model = attach_rwzi_junctions(rwzi_model(), max_distance=5.0, basins={1: 2})
    links = model.link.df.set_index(["from_node_id", "to_node_id"]).geometry
    assert set(links.index) == {(2, 3), (3, 4), (4, 1), (5, 6), (6, 2)}
    assert model.node.df.at[6, "geometry"].distance(links[(2, 3)]) < 1e-9
    again = attach_rwzi_junctions(model, basins={1: 2}).link.df.set_index(["from_node_id", "to_node_id"]).geometry
    assert links.equals(again)


def test_attach_rwzi_junctions_not_in_model():
    """Left as is if the configured Basin is not in the model, such as before coupling."""
    model = attach_rwzi_junctions(rwzi_model(), basins={1: 10})
    assert model.node.df.at[6, "geometry"].equals(Point(60, 20))


def test_attach_rwzi_junctions_authorities():
    """Only for the given authorities, or if the Basin is configured, of any authority."""
    model = rwzi_model()
    model.node.df["meta_waterbeheerder"] = "A"
    unchanged = attach_rwzi_junctions(model.model_copy(deep=True), authorities={"B"})
    assert (6, 1) in unchanged.link.df.set_index(["from_node_id", "to_node_id"]).index
    model = attach_rwzi_junctions(model, basins={1: 2}, authorities={"B"})
    assert (6, 2) in model.link.df.set_index(["from_node_id", "to_node_id"]).index


def test_create_rwzi_basin_coupling():
    """RWZIs couple to the Basin they are in, or the nearest one within `max_distance`, if they have an authority."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.basin.add(
        Node(1, Point(0, 0), meta_waterbeheerder="A", meta_categorie="doorgaand"),
        [
            basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]),
            basin.State(level=[0.5]),
            basin.Area(geometry=[Point(0, 0).buffer(100)]),
        ],
    )
    for node_id, x, authority in ((2, 50, "A"), (3, 600, "A"), (4, 50, None), (5, 50, "B")):
        model.terminal.add(
            Node(node_id, Point(x, 0), meta_rwzi_code=float(node_id), meta_discharge_couple_authority=authority)
        )
    lookup, unmatched = create_rwzi_basin_coupling(model, max_distance=100)
    assert lookup == {2.0: 1}
    assert list(unmatched.index) == [3]
    lookup, unmatched = create_rwzi_basin_coupling(model, max_distance=1000)
    assert lookup == {2.0: 1, 3.0: 1}
    assert unmatched.empty
