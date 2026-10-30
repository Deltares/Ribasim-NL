import geopandas as gpd
import pytest
from ribasim import Node
from ribasim.nodes import basin, outlet
from ribasim_nl.flow_paths import (
    Routes,
    attach_to_tree,
    branch_from_route,
    junction_tree,
    merge_inflows,
    merge_outflows,
    place_on_link,
    prune_junction,
    rebuild_junction_trees,
    simplified_links,
    tree_links,
)
from shapely.geometry import LineString, Point, box

from ribasim_nl import Model


def route_model() -> Model:
    """Basin 1 drains through Junction 4 into Outlets 2 and 3, and Outlet 5 drains into Basin 1."""
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.basin.add(
        Node(1, Point(0, 0)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])]
    )
    for node_id, point in ((2, Point(100, 10)), (3, Point(100, -10)), (5, Point(-100, 0))):
        model.outlet.add(Node(node_id, point), [outlet.Static(flow_rate=[1.0])])
    model.junction.add(Node(4, Point(50, 0)))
    model.link.add(model.basin[1], model.junction[4])
    model.link.add(model.junction[4], model.outlet[2])
    model.link.add(model.junction[4], model.outlet[3])
    model.link.add(model.outlet[5], model.basin[1])
    return model


def link_pairs(model: Model) -> set[tuple[int, int]]:
    return set(model.link.df.set_index(["from_node_id", "to_node_id"]).index)


def test_routes():
    model = route_model()
    routes = Routes(model)
    link_ids = model.link.df.reset_index().set_index(["from_node_id", "to_node_id"])["link_id"]
    assert routes.ends(link_ids[(1, 4)]) == {2, 3}
    assert routes.ends(link_ids[(4, 2)], upstream=True) == {1}
    downstream = routes.paths(1)
    assert set(downstream) == {link_ids[(1, 4)], link_ids[(4, 2)], link_ids[(4, 3)]}
    assert downstream[link_ids[(4, 2)]] == [link_ids[(1, 4)], link_ids[(4, 2)]]
    assert set(routes.paths(1, upstream=True)) == {link_ids[(5, 1)]}


def test_prune_junction():
    model = route_model()
    model.remove_links(model.link.df.index[model.link.df["to_node_id"] == 3].tolist())
    prune_junction(model, 4)
    assert link_pairs(model) == {(1, 2), (5, 1)}
    assert model.link.df.set_index("to_node_id").at[2, "geometry"].coords[:] == [(0, 0), (50, 0), (100, 10)]
    assert 4 not in model.node.df.index


def test_branch_from_route():
    model = route_model()
    model.outlet.add(Node(6, Point(75, -20)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[1], model.outlet[6])
    junction_id = branch_from_route(model, 6, basin_id=1, towards_id=3)
    location = model.node.df.at[junction_id, "geometry"]
    assert location.distance(LineString([(50, 0), (100, -10)])) < 1e-9
    assert link_pairs(model) == {(1, 4), (4, 2), (4, junction_id), (junction_id, 3), (junction_id, 6), (5, 1)}

    # an existing Junction within the margin is used
    model = route_model()
    model.outlet.add(Node(6, Point(50, -20)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[1], model.outlet[6])
    assert branch_from_route(model, 6, basin_id=1, towards_id=2, margin=20.0) == 4
    assert (4, 6) in link_pairs(model)

    with pytest.raises(ValueError, match="should only have a flow link"):
        branch_from_route(model, 2, basin_id=1, towards_id=3)


def test_merge_outflows():
    """Outlet 5 drains through Junctions 9 and 7 into Basin 1, and Outlet 8 through Junction 7."""
    model = route_model()
    model.remove_links(model.link.df.index[model.link.df["from_node_id"] == 5].tolist())
    model.outlet.add(Node(8, Point(-100, 20)), [outlet.Static(flow_rate=[1.0])])
    model.junction.add(Node(7, Point(-50, 0)))
    model.junction.add(Node(9, Point(-75, 0)))
    model.link.add(model.outlet[5], model.junction[9])
    model.link.add(model.junction[9], model.junction[7])
    model.link.add(model.outlet[8], model.junction[7])
    model.link.add(model.junction[7], model.basin[1])
    assert merge_outflows(model, [5, 8]) == 7
    assert link_pairs(model) == {(1, 4), (4, 2), (4, 3), (5, 7), (8, 7), (7, 1)}
    assert 9 not in model.node.df.index
    model.outlet.add(Node(10, Point(-100, -20)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.outlet[10], model.basin[1])
    with pytest.raises(ValueError, match="common Junction"):
        merge_outflows(model, [5, 10])


def test_merge_inflows_and_place_on_link():
    model = route_model()
    for node_id, point in ((6, Point(-40, 30)), (7, Point(-60, 30))):
        model.outlet.add(Node(node_id, point), [outlet.Static(flow_rate=[1.0])])
        model.link.add(model.basin[1], model.outlet[node_id])
    junction_id = merge_inflows(model, 1, [6, 7])
    assert model.node.df.at[junction_id, "geometry"].equals(Point(-50, 30))
    assert {(1, junction_id), (junction_id, 6), (junction_id, 7)} <= link_pairs(model)

    link_id = model.link.df.index[model.link.df["from_node_id"] == 5][0]
    place_on_link(model, junction_id, 1, link_id)
    assert model.node.df.at[junction_id, "geometry"].equals(Point(-50, 0))
    basin_link = model.link.df.set_index(["from_node_id", "to_node_id"]).at[(1, junction_id), "geometry"]
    assert basin_link.coords[:] == [(0, 0), (-50, 0)]
    outlet_link = model.link.df.set_index(["from_node_id", "to_node_id"]).at[(junction_id, 6), "geometry"]
    assert outlet_link.coords[0] == (-50, 0)

    with pytest.raises(ValueError, match="should have one flow link"):
        place_on_link(model, 2, 1, link_id)


def test_tree_links():
    routes = {
        1: [(0.0, 0.0), (10.0, 0.0), (20.0, 0.0), (30.0, 5.0)],
        2: [(0.0, 0.0), (10.0, 0.0), (20.0, 0.0), (30.0, -5.0)],
        3: [(0.0, 0.0), (10.0, 0.0), (10.0, 10.0)],
    }
    links = tree_links(routes)
    # name the Junctions by their location, the end of the link into them, which comes first
    locations = {end: coords[-1] for _, end, coords in links if end[0] == "junction"}
    links = {(locations.get(start, start), locations.get(end, end)): coords for start, end, coords in links}
    first, second = (10.0, 0.0), (20.0, 0.0)
    assert links == {
        ("root", first): [(0.0, 0.0), (10.0, 0.0)],
        (first, ("end", 3)): [(10.0, 0.0), (10.0, 10.0)],
        (first, second): [(10.0, 0.0), (20.0, 0.0)],
        (second, ("end", 1)): [(20.0, 0.0), (30.0, 5.0)],
        (second, ("end", 2)): [(20.0, 0.0), (30.0, -5.0)],
    }

    # branches that pass the same coordinates, as along overlapping waterways, have their own Junctions
    a, b = (10.0, 0.0), (20.0, 0.0)
    overlapping = {
        1: [(0.0, 0.0), a, b, (30.0, 5.0)],
        2: [(0.0, 0.0), a, b, (30.0, -5.0)],
        3: [(0.0, 0.0), (0.0, 10.0), a, b, (30.0, 6.0)],
        4: [(0.0, 0.0), (0.0, 10.0), a, b, (30.0, -6.0)],
    }
    pairs = [(start, end) for start, end, _ in tree_links(overlapping)]
    assert len(pairs) == len(set(pairs)) == 6
    assert sum(end[0] == "junction" for _, end in pairs) == 2

    # a route that is part of another one keeps its last segment, and routes split at the root without a Junction
    links = {(start, end): coords for start, end, coords in tree_links({1: routes[3][:2], 2: routes[1]})}
    assert links == {("root", ("end", 1)): routes[3][:2], ("root", ("end", 2)): routes[1]}


def test_attach_to_tree():
    """Point 2 branches off the route, and point 3, which is nearer to point 2 than to the route, off its branch."""
    routes = attach_to_tree({1: [(0.0, 0.0), (100.0, 0.0)]}, (0.0, 0.0), {2: (50.0, 10.0), 3: (60.0, 20.0)})
    assert routes == {
        1: [(0.0, 0.0), (50.0, 0.0), (100.0, 0.0)],
        2: [(0.0, 0.0), (50.0, 0.0), (50.0, 10.0)],
        3: [(0.0, 0.0), (50.0, 0.0), (50.0, 10.0), (60.0, 20.0)],
    }
    # without routes, the first point is attached to the start
    assert attach_to_tree({}, (0.0, 0.0), {1: (10.0, 0.0)}) == {1: [(0.0, 0.0), (10.0, 0.0)]}


def rebuild_model():
    """`route_model` with an area, and Outlet 6 that drains into Basin 1 from where Outlets 2 and 3 are fed."""
    model = route_model()
    model.basin.area.df = gpd.GeoDataFrame({"node_id": [1]}, geometry=[box(-150, -50, 150, 50)], crs="EPSG:28992")
    model.outlet.add(Node(6, Point(100, 0)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.outlet[6], model.basin[1], geometry=LineString([(100, 0), (0, 20), (0, 0)]))
    return model


def test_junction_tree():
    model = rebuild_model()
    routes = Routes(model)
    link_ids = model.link.df.reset_index().set_index(["from_node_id", "to_node_id"])["link_id"]
    assert junction_tree(routes, 1) == ({4}, [link_ids[(1, 4)]], {2: link_ids[(4, 2)], 3: link_ids[(4, 3)]})
    assert junction_tree(routes, 1, upstream=True) == (set(), [], {5: link_ids[(5, 1)], 6: link_ids[(6, 1)]})
    # a fixed Junction, for instance an observed one, is reached instead
    assert junction_tree(routes, 1, fixed={4}) == (set(), [], {4: link_ids[(1, 4)]})


def test_rebuild_junction_trees():
    model = rebuild_model()
    links_before = simplified_links(model)
    link_ids = model.link.df.reset_index().set_index(["from_node_id", "to_node_id"])["link_id"]
    waterways = gpd.GeoDataFrame({"main-route": [True]}, geometry=[LineString([(-100, 0), (100, 0)])], crs="EPSG:28992")
    changed, junction_ids = rebuild_junction_trees(model, [1], waterways)

    assert simplified_links(model) == links_before
    assert 4 not in model.node.df.index
    # the outflows split where the waterway ends, and the inflow of Outlet 6 follows the outflows to there
    (junction_id,) = junction_ids
    assert model.node.df.at[junction_id, "geometry"].coords[:] == [(100, 0)]
    geometries = model.link.df.set_index(["from_node_id", "to_node_id"])["geometry"]
    assert geometries[(1, junction_id)].coords[:] == [(0, 0), (100, 0)]
    assert geometries[(6, 1)].coords[:] == [(100, 0), (0, 0)]
    assert geometries[(5, 1)].coords[:] == [(-100, 0), (0, 0)]
    # the links to the nodes reached keep their ids
    assert model.link.df.at[link_ids[(4, 2)], "from_node_id"] == junction_id
    assert set(changed) >= {link_ids[(4, 2)], link_ids[(4, 3)], link_ids[(5, 1)], link_ids[(6, 1)]}
