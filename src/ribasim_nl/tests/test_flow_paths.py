import math

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from ribasim import Node
from ribasim.nodes import basin, manning_resistance, outlet
from ribasim_nl.flow_paths import (
    Digitized,
    Routes,
    attach_to_tree,
    branch_from_route,
    drainage_capacity,
    junction_tree,
    merge_basin_trees,
    merge_inflows,
    merge_outflows,
    place_on_link,
    prune_junction,
    rebuild_junction_trees,
    rebuild_rws_junction_trees,
    simplified_links,
    towards_basin,
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
    paths = routes.paths(1)
    assert set(paths) == {link_ids[(1, 4)], link_ids[(4, 2)], link_ids[(4, 3)], link_ids[(5, 1)]}
    assert paths[link_ids[(4, 2)]] == [link_ids[(1, 4)], link_ids[(4, 2)]]


def mixed_model() -> Model:
    """`route_model` where Outlet 5 drains into Basin 1 through Junction 4 too, with its link towards the Basin."""
    model = route_model()
    model.remove_links(model.link.df.index[model.link.df["from_node_id"].isin([1, 5])].tolist())
    model.link.add(model.junction[4], model.basin[1])
    model.link.add(model.outlet[5], model.junction[4])
    return model


def test_routes_basin_tree():
    """A group of Junctions with one Basin connects it with the other nodes, whichever way its links point."""
    model = mixed_model()
    routes = Routes(model)
    link_ids = model.link.df.reset_index().set_index(["from_node_id", "to_node_id"])["link_id"]
    assert routes.tree_basin(4) == 1
    assert routes.ends(link_ids[(5, 4)]) == {1}
    assert routes.ends(link_ids[(4, 2)], upstream=True) == {1}
    assert routes.ends(link_ids[(4, 1)], upstream=True) == {5}
    assert routes.subtree(link_ids[(4, 1)]) == ({5}, {2, 3})
    assert simplified_links(model) == {(1, 2), (1, 3), (5, 1)}
    assert set(routes.paths(1)) == set(link_ids)
    # the flow rate of Junction 4 is the net flow of its link towards the Basin, as in Ribasim, not its outflow
    assert routes.flow_rate_links(4) == [link_ids[(4, 1)]]
    assert routes.flow_rate_links(5) == [link_ids[(5, 4)]]
    # without a Basin it is the outflow
    flow = pd.DataFrame({"from_node_id": [2, 3, 4, 4], "to_node_id": [4, 4, 5, 6]}, index=[10, 11, 12, 13])
    node_types = pd.Series(["Outlet", "Outlet", "Junction", "Outlet", "Outlet"], index=[2, 3, 4, 5, 6])
    assert Routes.from_tables(flow, node_types).flow_rate_links(4) == [12, 13]


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
    model.outlet.add(Node(6, Point(100, 5)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.outlet[6], model.basin[1], geometry=LineString([(100, 5), (0, 20), (0, 0)]))
    return model


def test_junction_tree():
    model = rebuild_model()
    routes = Routes(model)
    link_ids = model.link.df.reset_index().set_index(["from_node_id", "to_node_id"])["link_id"]
    reached = {link_ids[(4, 2)]: 2, link_ids[(4, 3)]: 3, link_ids[(5, 1)]: 5, link_ids[(6, 1)]: 6}
    assert junction_tree(routes, 1) == ({4}, [link_ids[(1, 4)]], reached)
    # a fixed Junction, for instance an observed one, is reached instead
    reached = {link_ids[(1, 4)]: 4, link_ids[(5, 1)]: 5, link_ids[(6, 1)]: 6}
    assert junction_tree(routes, 1, fixed={4}) == (set(), [], reached)


def test_rebuild_junction_trees():
    model = rebuild_model()
    links_before = simplified_links(model)
    link_ids = model.link.df.reset_index().set_index(["from_node_id", "to_node_id"])["link_id"]
    waterways = gpd.GeoDataFrame({"main-route": [True]}, geometry=[LineString([(-100, 0), (100, 0)])], crs="EPSG:28992")
    changed, junction_ids = rebuild_junction_trees(model, [1], waterways)

    assert simplified_links(model) == links_before
    assert 4 not in model.node.df.index
    # the inflow of Outlet 6 and the outflows share one tree, which splits where the waterway ends
    (junction_id,) = junction_ids
    assert model.node.df.at[junction_id, "geometry"].coords[:] == [(100, 0)]
    geometries = model.link.df.set_index(["from_node_id", "to_node_id"])["geometry"]
    assert geometries[(1, junction_id)].coords[:] == [(0, 0), (100, 0)]
    assert geometries[(6, junction_id)].coords[:] == [(100, 5), (100, 0)]
    assert geometries[(5, 1)].coords[:] == [(-100, 0), (0, 0)]
    # the links to the nodes reached keep their ids
    assert model.link.df.at[link_ids[(4, 2)], "from_node_id"] == junction_id
    assert set(changed) >= {link_ids[(4, 2)], link_ids[(4, 3)], link_ids[(5, 1)], link_ids[(6, 1)]}


def test_rebuild_rws_junction_trees_lake(tmp_path):
    """The Basin of a lake follows the fairways and the main route, as `test_rebuild_junction_trees` its waterway."""
    model = rebuild_model()
    model.node.df.loc[1, "name"] = "IJsselmeer"
    fairway_path = tmp_path / "fairway.gpkg"
    gpd.GeoDataFrame(geometry=[LineString([(-100, 0), (0, 0)])], crs="EPSG:28992").to_file(fairway_path)
    main_route = gpd.GeoDataFrame(geometry=[LineString([(0, 0), (100, 0)])], crs="EPSG:28992")
    links_before = simplified_links(model)
    # the Top10NL is not read for a lake
    rebuild_rws_junction_trees(model, [1], tmp_path / "top10nl.gpkg", fairway_path, main_route)

    assert simplified_links(model) == links_before
    geometries = model.link.df.set_index(["from_node_id", "to_node_id"])["geometry"]
    (junction_id,) = model.node.df.index[model.node.df["node_type"] == "Junction"]
    assert model.node.df.at[junction_id, "geometry"].coords[:] == [(100, 0)]
    assert geometries[(1, junction_id)].coords[:] == [(0, 0), (100, 0)]
    assert geometries[(5, 1)].coords[:] == [(-100, 0), (0, 0)]


def test_merge_basin_trees():
    """The inflow of Outlet 6 follows the outflows to Junction 4, and joins their tree there."""
    model = route_model()
    model.outlet.add(Node(6, Point(50, 10)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.outlet[6], model.basin[1], geometry=LineString([(50, 10), (50, 0), (0, 0)]))
    links_before = simplified_links(model)
    (junction_id,) = merge_basin_trees(model)
    assert simplified_links(model) == links_before
    assert model.node.df.at[junction_id, "geometry"].coords[:] == [(50, 0)]
    assert link_pairs(model) == {(1, junction_id), (junction_id, 2), (junction_id, 3), (6, junction_id), (5, 1)}
    # a tree that would not change is kept
    assert merge_basin_trees(model) == []


def test_prune_junction_in_basin_tree():
    """A Junction of a Basin tree with two links merges them, keeping the direction of the node other than the Basin."""
    model = mixed_model()
    model.remove_links(model.link.df.index[model.link.df["to_node_id"].isin([2, 3])].tolist())
    prune_junction(model, 4)
    assert link_pairs(model) == {(5, 1)}
    assert model.link.df.set_index("from_node_id").at[5, "geometry"].coords[:] == [(-100, 0), (50, 0), (0, 0)]


def test_branch_from_route_in_basin_tree():
    """A node fed by the Basin can branch off the route of an inflow."""
    model = route_model()
    model.outlet.add(Node(6, Point(-50, -20)), [outlet.Static(flow_rate=[1.0])])
    model.link.add(model.basin[1], model.outlet[6])
    junction_id = branch_from_route(model, 6, basin_id=1, towards_id=5)
    assert link_pairs(model) == {(1, 4), (4, 2), (4, 3), (5, junction_id), (junction_id, 1), (junction_id, 6)}
    assert simplified_links(model) == {(1, 2), (1, 3), (5, 1), (1, 6)}


def test_towards_basin():
    capacity = {1: 10.0, 2: 1.0, 3: math.inf, 4: math.inf}
    # a link of inflows only points towards the Basin, of outflows only away from it
    assert towards_basin({1}, set(), capacity)
    assert not towards_basin(set(), {1}, capacity, digitized_away=False)
    # the direction in which the lines along it are digitized decides, else the larger capacity
    assert not towards_basin({1}, {2}, capacity, digitized_away=True)
    assert towards_basin({1}, {2}, capacity)
    assert not towards_basin({2}, {3}, capacity)
    # else the side with more nodes, else away from the Basin
    assert towards_basin({3, 2}, {4}, capacity)  # both unlimited
    assert towards_basin({5, 6}, {7}, capacity)
    assert not towards_basin({5}, {7}, capacity)


def test_digitized():
    digitized = Digitized([LineString([(0, 0), (10, 0), (20, 0)]), LineString([(0, 5), (0, 0)])])
    assert digitized.along([(0.0, 0.0), (10.0, 0.0), (20.0, 0.0)]) is True
    assert digitized.along([(20.0, 0.0), (10.0, 0.0), (0.0, 5.0)]) is None  # mostly off the lines
    assert digitized.along([(20.0, 0.0), (10.0, 0.0), (0.0, 0.0), (0.0, 5.0)]) is False
    # lines digitized both ways have no direction
    assert (
        Digitized([LineString([(0, 0), (1, 0)]), LineString([(1, 0), (0, 0)])]).along([(0.0, 0.0), (1.0, 0.0)]) is None
    )


def test_drainage_capacity():
    model = route_model()
    model.outlet.static.df = pd.DataFrame(
        {
            "node_id": [2, 2, 3, 5],
            "control_state": ["afvoer", "aanvoer", None, None],
            "flow_rate": [4.0, 9.0, 99999.0, 1.0],
            "max_flow_rate": [np.nan, np.nan, np.nan, np.nan],
        }
    )
    model.manning_resistance.add(
        Node(6, Point(0, 50)),
        [manning_resistance.Static(length=[10.0], manning_n=[0.04], profile_width=[5.0], profile_slope=[1.0])],
    )
    assert drainage_capacity(model) == {2: 4.0, 3: math.inf, 5: 1.0, 6: math.inf}
