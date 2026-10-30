"""Routes of flow links through Junctions, and changes to them that only change the layout.

A Junction only merges or splits flow, so the flow links from a node through Junctions up to the next non-Junction
nodes form the route of that node. Ribasim replaces each pair of links into and out of a Junction by a direct link,
see `simplify_graph!` in the Ribasim core. Moving or adding Junctions on a route therefore changes how links are drawn,
not how water flows, as long as each node keeps exchanging water with the same non-Junction nodes.
"""

import itertools
import logging
import pathlib
import typing
from collections import defaultdict, deque

import geopandas as gpd
import numpy as np
import shapely
from ribasim import Node
from shapely.geometry import LineString, Point
from shapely.ops import substring

from ribasim_nl.layout import move_nodes
from ribasim_nl.model import Model
from ribasim_nl.network_snapping import lines_in, read_main_waterways, route_basin_links

LOG = logging.getLogger(__name__)

Coord = tuple[float, float]


def _line(model: Model, link_id: int) -> LineString:
    assert model.link.df is not None
    return typing.cast(LineString, model.link.df.at[link_id, "geometry"])


def _point(model: Model, node_id: int) -> Point:
    assert model.node.df is not None
    return typing.cast(Point, model.node.df.at[node_id, "geometry"])


def _end(links: gpd.GeoDataFrame, link_id: int, upstream: bool) -> int:
    """The node a link comes from if `upstream`, else goes to."""
    return int(typing.cast(int, links.at[link_id, "from_node_id" if upstream else "to_node_id"]))


def _substring(line: LineString, start: float, end: float) -> LineString:
    part = substring(line, start, end)
    assert isinstance(part, LineString), f"No line between {start} and {end} along {line}"
    return part


def _coord(model: Model, node_id: int) -> Coord:
    point = _point(model, node_id)
    return (point.x, point.y)


def _name(model: Model, link_id: int) -> str:
    assert model.link.df is not None
    return str(model.link.df.at[link_id, "name"])


class Routes:
    """The routes of the flow links of a model, as they are when the Routes are made."""

    def __init__(self, model: Model) -> None:
        assert model.node.df is not None
        assert model.link.df is not None
        self.flow = model.link.df[model.link.df["link_type"] == "flow"]
        self.node_types = model.node.df["node_type"]
        self._out: dict[int, list[int]] = defaultdict(list)
        self._in: dict[int, list[int]] = defaultdict(list)
        for link_id, from_node_id, to_node_id in zip(
            self.flow.index.tolist(), self.flow["from_node_id"].tolist(), self.flow["to_node_id"].tolist(), strict=True
        ):
            self._out[from_node_id].append(link_id)
            self._in[to_node_id].append(link_id)
        self._ends: dict[tuple[int, bool], frozenset[int]] = {}

    def links_at(self, node_id: int, upstream: bool) -> list[int]:
        """The flow links into `node_id` if `upstream`, else out of it."""
        return list((self._in if upstream else self._out).get(node_id, []))

    def next_node(self, link_id: int, upstream: bool) -> int:
        """The node a flow link comes from if `upstream`, else goes to."""
        return _end(self.flow, link_id, upstream)

    def ends(self, link_id: int, upstream: bool = False) -> frozenset[int]:
        """The first non-Junction nodes downstream of a flow link, or upstream if `upstream`, through Junctions."""
        return self._node_ends(self.next_node(link_id, upstream), upstream, set())

    def _node_ends(self, node_id: int, upstream: bool, visiting: set[int]) -> frozenset[int]:
        if self.node_types[node_id] != "Junction":
            return frozenset([node_id])
        key = (node_id, upstream)
        if key in self._ends:
            return self._ends[key]
        if node_id in visiting:  # a cycle of Junctions
            return frozenset()
        visiting.add(node_id)
        links = self.links_at(node_id, upstream)
        result = frozenset().union(*(self._node_ends(self.next_node(i, upstream), upstream, visiting) for i in links))
        self._ends[key] = result
        return result

    def paths(self, node_id: int, upstream: bool = False) -> dict[int, list[int]]:
        """The route of `node_id` downstream, or upstream if `upstream`: the path from `node_id` to each of its links.

        Only links whose own route in the other direction ends at `node_id` alone are part of it, so for a Basin these
        are the links that only carry water out of that Basin, or upstream, only into it.
        """
        paths: dict[int, list[int]] = {}
        queue = deque((i, [i]) for i in self.links_at(node_id, upstream))
        while queue:
            link_id, path = queue.popleft()
            if link_id in paths or self.ends(link_id, not upstream) != {node_id}:
                continue
            paths[link_id] = path
            next_node = self.next_node(link_id, upstream)
            if self.node_types[next_node] == "Junction":
                queue.extend((i, [*path, i]) for i in self.links_at(next_node, upstream))
        return paths


def _coords(geometry: LineString) -> list[Coord]:
    return [(c[0], c[1]) for c in geometry.coords]


def _dedupe(coords: list[Coord]) -> list[Coord]:
    return [c for i, c in enumerate(coords) if i == 0 or c != coords[i - 1]]


def split_distance(geometry: LineString, point: Point, margin: float = 1.0) -> float:
    """The distance along `geometry` of the point nearest to `point`, at least `margin` from its ends if possible."""
    margin = min(margin, geometry.length / 4)
    return min(max(geometry.project(point), margin), geometry.length - margin)


def geometry_along(model: Model, path: list[int], distance: float, upstream: bool) -> LineString:
    """The line from the start of a route `path`, see `Routes.paths`, to `distance` along its last link."""
    geometries = [_line(model, i) for i in path]
    last = geometries[-1]
    if upstream:
        parts = [g.reverse() for g in geometries[:-1]] + [_substring(last, distance, last.length).reverse()]
    else:
        parts = [*geometries[:-1], _substring(last, 0.0, distance)]
    return LineString(_dedupe([c for part in parts for c in _coords(part)]))


def split_link(model: Model, link_id: int, distance: float, node_id: int | None = None) -> int:
    """Split a flow link at `distance` along it with a Junction: `node_id` moved there, or else a new one.

    Returns the node id of the Junction. Other links of an existing `node_id` move their ends along.
    """
    assert model.link.df is not None
    links = model.link.df
    geometry = _line(model, link_id)
    assert 0.0 < distance < geometry.length, f"Cannot split link {link_id} at its end"
    upstream, downstream = _substring(geometry, 0.0, distance), _substring(geometry, distance, geometry.length)
    location = Point(downstream.coords[0])
    if node_id is None:
        node_id = model.junction.add(Node(geometry=location)).node_id
    else:
        assert model.get_node_type(node_id) == "Junction", f"Node {node_id} is not a Junction"
        move_nodes(model, {node_id: location})
    assert node_id is not None
    to_node_id = _end(links, link_id, upstream=False)
    name = _name(model, link_id)
    model.link.df.loc[link_id, "to_node_id"] = node_id
    model.link.df.loc[[link_id], "geometry"] = gpd.GeoSeries([upstream], index=[link_id], crs=links.crs)
    model.link.add(model.get_node(node_id), model.get_node(to_node_id), geometry=downstream, name=name)
    return node_id


def prune_junction(model: Model, junction_id: int) -> None:
    """Remove a Junction without inflow or outflow, or merge the two links of a Junction that only passes flow on.

    Neighboring Junctions are pruned in turn. A Junction with other links, such as observation links, is kept.
    """
    assert model.link.df is not None
    assert model.node.df is not None
    if junction_id not in model.node.df.index or model.get_node_type(junction_id) != "Junction":
        return
    links = model.link.df
    touching = links[(links["from_node_id"] == junction_id) | (links["to_node_id"] == junction_id)]
    if (touching["link_type"] != "flow").any():
        return
    inflow = touching[touching["to_node_id"] == junction_id]
    outflow = touching[touching["from_node_id"] == junction_id]
    if inflow.empty or outflow.empty:
        neighbors = {*inflow["from_node_id"], *outflow["to_node_id"]}
        model.remove_node(junction_id, remove_links=True)
        for neighbor in neighbors:
            prune_junction(model, int(neighbor))
    elif len(inflow) == 1 and len(outflow) == 1:
        in_id, out_id = int(inflow.index[0]), int(outflow.index[0])
        geometry = LineString(_dedupe(_coords(_line(model, in_id)) + _coords(_line(model, out_id))))
        to_node_id = _end(links, out_id, upstream=False)
        model.remove_links([out_id])
        model.link.df.loc[in_id, "to_node_id"] = to_node_id
        model.link.df.loc[[in_id], "geometry"] = gpd.GeoSeries([geometry], index=[in_id], crs=links.crs)
        model.remove_node(junction_id)


def remove_flow_link(model: Model, link_id: int) -> None:
    """Remove a flow link, and prune the Junctions at its ends, see `prune_junction`."""
    assert model.link.df is not None
    ends = [_end(model.link.df, link_id, upstream) for upstream in (True, False)]
    model.remove_links([link_id])
    for node_id in ends:
        prune_junction(model, node_id)


def branch_from_route(model: Model, node_id: int, basin_id: int, towards_id: int, margin: float = 1.0) -> int:
    """Let the link from `basin_id` into `node_id` branch off the route from `basin_id` towards `towards_id`.

    The branch starts at a Junction at the point of that route nearest to `node_id`, or at the existing Junction if
    that point is within `margin` of one, and runs straight to `node_id`. Returns the node id of that Junction.
    """
    assert model.link.df is not None
    assert model.node.df is not None
    flow = model.link.df[model.link.df["link_type"] == "flow"]
    inflow = flow[flow["to_node_id"] == node_id]
    if inflow["from_node_id"].tolist() != [basin_id]:
        raise ValueError(f"Node {node_id} should only have a flow link from Basin {basin_id}.")
    remove_flow_link(model, int(inflow.index[0]))

    paths = Routes(model).paths(basin_id)
    links = model.link.df
    route = {i for path in paths.values() if _end(links, path[-1], upstream=False) == towards_id for i in path}
    if not route:
        raise ValueError(f"No route from Basin {basin_id} to node {towards_id}.")
    point = _point(model, node_id)
    link_id = min(route, key=lambda i: _line(model, i).distance(point))
    geometry = _line(model, link_id)
    distance = geometry.project(point)
    from_node_id, to_node_id = _end(links, link_id, upstream=True), _end(links, link_id, upstream=False)
    if distance <= margin and model.get_node_type(from_node_id) == "Junction":
        junction_id = from_node_id
    elif distance >= geometry.length - margin and model.get_node_type(to_node_id) == "Junction":
        junction_id = to_node_id
    else:
        junction_id = split_link(model, link_id, split_distance(geometry, point, margin))
    model.link.add(model.get_node(junction_id), model.get_node(node_id))
    return junction_id


def merge_outflows(model: Model, node_ids: list[int]) -> int:
    """Connect `node_ids` with straight links to the first Junction downstream that all of their routes pass.

    The Junctions they passed before are pruned. Returns the node id of the Junction.
    """
    assert model.link.df is not None
    routes = Routes(model)

    def downstream_junctions(node_id: int) -> list[int]:
        links = routes.links_at(node_id, upstream=False)
        assert len(links) == 1, f"Node {node_id} should have one outflow link"
        junctions = []
        next_node = routes.next_node(links[0], upstream=False)
        while routes.node_types[next_node] == "Junction" and next_node not in junctions:
            junctions.append(next_node)
            links = routes.links_at(next_node, upstream=False)
            if len(links) != 1:
                break
            next_node = routes.next_node(links[0], upstream=False)
        return junctions

    sequences = [downstream_junctions(i) for i in node_ids]
    common = [j for j in sequences[0] if all(j in s for s in sequences[1:])]
    if not common:
        raise ValueError(f"The routes of nodes {node_ids} do not pass a common Junction.")
    junction_id = common[0]
    for node_id in node_ids:
        (link_id,) = routes.links_at(node_id, upstream=False)
        if routes.next_node(link_id, upstream=False) == junction_id:
            continue
        name = _name(model, link_id)
        model.remove_links([link_id])
        model.link.add(model.get_node(node_id), model.get_node(junction_id), name=name)
        prune_junction(model, routes.next_node(link_id, upstream=False))
    return junction_id


def merge_inflows(model: Model, basin_id: int, node_ids: list[int]) -> int:
    """Replace the flow links from `basin_id` to `node_ids` by a link to a new Junction, with straight links on.

    The Junction is at the centroid of `node_ids`, see `place_on_link` to move it. Returns its node id.
    """
    assert model.link.df is not None
    assert model.node.df is not None
    flow = model.link.df[model.link.df["link_type"] == "flow"]
    links = flow[(flow["from_node_id"] == basin_id) & flow["to_node_id"].isin(node_ids)]
    if sorted(links["to_node_id"]) != sorted(node_ids):
        raise ValueError(f"Nodes {node_ids} should each have one flow link from Basin {basin_id}.")
    location = model.node.df.geometry[node_ids].union_all().centroid
    junction = model.junction.add(Node(geometry=location))
    model.remove_links(links.index.tolist())
    model.link.add(model.get_node(basin_id), junction)
    for node_id in node_ids:
        model.link.add(junction, model.get_node(node_id))
    return junction.node_id


def place_on_link(model: Model, node_id: int, basin_id: int, link_id: int) -> None:
    """Move `node_id` onto the point of `link_id` nearest to it, with its link with `basin_id` along their route.

    `link_id` is on the route of `basin_id` in either direction, see `Routes.paths`, and `node_id` has a single flow
    link with `basin_id`. Its other links move their ends along.
    """
    assert model.link.df is not None
    assert model.node.df is not None
    flow = model.link.df[model.link.df["link_type"] == "flow"]
    from_basin = flow[(flow["from_node_id"] == basin_id) & (flow["to_node_id"] == node_id)]
    to_basin = flow[(flow["from_node_id"] == node_id) & (flow["to_node_id"] == basin_id)]
    if len(from_basin) + len(to_basin) != 1:
        raise ValueError(f"Node {node_id} should have one flow link with Basin {basin_id}.")

    routes = Routes(model)
    for upstream in (False, True):
        path = routes.paths(basin_id, upstream).get(link_id)
        if path is not None:
            break
    else:
        raise ValueError(f"Link {link_id} is not on the route of Basin {basin_id}.")
    distance = split_distance(_line(model, link_id), _point(model, node_id))
    along = geometry_along(model, path, distance, upstream)
    move_nodes(model, {node_id: Point(along.coords[-1])})
    if from_basin.empty:
        along = along.reverse()
    basin_link = (from_basin if to_basin.empty else to_basin).index
    model.link.df.loc[basin_link, "geometry"] = gpd.GeoSeries([along], index=basin_link, crs=model.link.df.crs)


def simplified_links(model: Model) -> set[tuple[int, int]]:
    """The flow links between non-Junction nodes that Ribasim makes of the flow links, through Junctions."""
    routes = Routes(model)
    result = set()
    for link_id in routes.flow.index:
        from_node_id = routes.next_node(link_id, upstream=True)
        if routes.node_types[from_node_id] != "Junction":
            result.update((from_node_id, to_node_id) for to_node_id in routes.ends(link_id))
    return result


def junction_tree(
    routes: Routes, basin_id: int, upstream: bool = False, fixed: typing.Container[int] = ()
) -> tuple[set[int], list[int], dict[int, int]]:
    """The Junctions that only pass flow out of `basin_id`, or into it if `upstream`, and the nodes they reach.

    Junctions in `fixed`, and Junctions that also pass flow of other nodes, are not part of the tree but reached by it.

    :return: the Junctions of the tree, the links between `basin_id` and them, and per node reached, its link
    """
    paths = routes.paths(basin_id, upstream)
    junctions: set[int] = set()
    links: list[int] = []
    reached: dict[int, int] = {}
    queue = deque(routes.links_at(basin_id, upstream))
    while queue:
        link_id = queue.popleft()
        node_id = routes.next_node(link_id, upstream)
        if (
            routes.node_types[node_id] == "Junction"
            and node_id not in fixed
            and all(i in paths for i in routes.links_at(node_id, not upstream))
        ):
            if node_id in junctions:
                raise ValueError(f"Junction {node_id} is reached twice from Basin {basin_id}.")
            junctions.add(node_id)
            links.append(link_id)
            queue.extend(routes.links_at(node_id, upstream))
        else:
            if node_id in reached:
                raise ValueError(f"Node {node_id} is reached twice from Basin {basin_id}.")
            reached[node_id] = link_id
    return junctions, links, reached


class _Trie:
    """Routes from one point by their coordinates, such that routes that share their start share a branch."""

    def __init__(self) -> None:
        self.children: dict[Coord, _Trie] = {}
        self.ends: list[int] = []


type _TreeNode = typing.Literal["root"] | tuple[typing.Literal["junction", "end"], int]


def tree_links(routes: dict[int, list[Coord]]) -> list[tuple[_TreeNode, _TreeNode, list[Coord]]]:
    """The links of the tree of routes that all start at the same point, with a Junction where they split.

    The last segment of each route is its own, so a route that is part of another one still ends in a branch.

    :param routes: per key, the coordinates of a route
    :return: the links from the start "root" to Junctions ("junction", number), and to the end ("end", key) of each
        route, with their coordinates, such that the link into a Junction comes before the links out of it
    """
    assert routes, "No routes"
    origin = next(iter(routes.values()))[0]
    root = _Trie()
    for key, coords in routes.items():
        assert len(coords) >= 2 and coords[0] == origin, f"Route {key} does not start at {origin}"
        node = root
        for coord in coords[1:-1]:
            node = node.children.setdefault(coord, _Trie())
        node.ends.append(key)

    links: list[tuple[_TreeNode, _TreeNode, list[Coord]]] = []
    stack: list[tuple[_Trie, _TreeNode, list[Coord]]] = [(root, "root", [origin])]
    while stack:
        node, start, coords = stack.pop()
        if node is not root and len(node.children) + len(node.ends) > 1:
            junction: _TreeNode = ("junction", len(links))
            links.append((start, junction, coords))
            start, coords = junction, [coords[-1]]
        links.extend((start, ("end", key), [*coords, routes[key][-1]]) for key in node.ends)
        stack.extend((child, start, [*coords, coord]) for coord, child in node.children.items())
    return links


def attach_to_tree(routes: dict[int, list[Coord]], start: Coord, points: dict[int, Coord]) -> dict[int, list[Coord]]:
    """Attach points to the tree of routes from `start`, each with a straight line from the nearest point of the tree.

    The points nearest to the tree are attached first, such that the others can branch off their lines. Where a line
    branches off a segment, a point is inserted in all routes along that segment, such that they share it.

    :param routes: per key, the coordinates of a route from `start`
    :param start: the start of the routes, from which the first point is attached if there are no routes
    :param points: per key, a point to attach
    :return: the routes, with a route to each point
    """
    routes = {key: list(coords) for key, coords in routes.items()}
    remaining = dict(points)
    while remaining:
        segments = list(
            {(start, start)} | {(a, b) for coords in routes.values() for a, b in itertools.pairwise(coords)}
        )
        a, b = (np.array([segment[i] for segment in segments]) for i in (0, 1))
        keys = list(remaining)
        p = np.array([remaining[key] for key in keys])
        ab = b - a
        lengths = np.maximum((ab**2).sum(axis=1), 1e-12)
        t = np.clip(((p[:, None, :] - a[None]) * ab[None]).sum(axis=2) / lengths, 0.0, 1.0)
        nearest = a[None] + t[..., None] * ab[None]
        distance = np.hypot(*(nearest - p[:, None, :]).transpose(2, 0, 1))
        i, j = np.unravel_index(np.argmin(distance), distance.shape)
        segment = segments[j]
        key = keys[i]
        if t[i, j] <= 0.0 or t[i, j] >= 1.0:
            branch = segment[0] if t[i, j] <= 0.0 else segment[1]
        else:
            branch = (float(nearest[i, j, 0]), float(nearest[i, j, 1]))
            for coords in routes.values():
                for k in range(len(coords) - 1):
                    if (coords[k], coords[k + 1]) == segment:
                        coords.insert(k + 1, branch)
                        break
        prefix = [start]
        for coords in routes.values():
            if branch in coords:
                prefix = coords[: coords.index(branch) + 1]
                break
        routes[key] = _dedupe([*prefix, branch, remaining.pop(key)])
    return routes


def rebuild_junction_trees(
    model: Model,
    basin_ids: typing.Iterable[int],
    waterways: gpd.GeoDataFrame,
    tolerance: float = 10.0,
) -> tuple[list[int], list[int]]:
    """Route the flow links of Basins along the waterways, with their Junctions where the routes split.

    The Junctions of a Basin are removed if they only pass the flow out of, or into, that Basin, see `junction_tree`,
    except observed Junctions and RWZI Junctions. The links to the nodes they reach are routed from the Basin node
    along the shortest paths of one network, see `ribasim_nl.network_snapping.route_basin_links`, such that the
    routes out of and into a Basin share their common parts, and new Junctions are added where they split. Nodes far
    from the waterways branch off straight from the nearest point of these routes, see `attach_to_tree`, also over
    open water, where routes over the water surface would run in parallel. Each node keeps exchanging water with the
    same nodes, which is asserted, and the links to the nodes reached keep their link ids. Nodes are not moved and
    links are not offset, see `ribasim_nl.layout.tidy_layout`.

    :param model: Ribasim model
    :param basin_ids: Basins to rebuild the Junctions of
    :param waterways: waterways with a boolean "main-route" column, see `ribasim_nl.network_snapping.snap_to_network`
    :param tolerance: buffer around the basin area within which waterways are used, defaults to 10.0

    :return: the ids of the links and of the Junctions that are new or changed
    """
    assert model.node.df is not None
    assert model.link.df is not None
    assert model.basin.area.df is not None
    before = simplified_links(model)
    routes = Routes(model)
    nodes, links = model.node.df, model.link.df
    # Junctions with observation links are observed as they are, and RWZI Junctions are found by their code, see
    # `ribasim_nl.rwzi.attach_rwzi_junctions`
    fixed = set(links.loc[links["link_type"] != "flow", ["from_node_id", "to_node_id"]].to_numpy().ravel().tolist())
    if "meta_rwzi_code" in nodes.columns:
        fixed |= set(nodes.index[(nodes["node_type"] == "Junction") & nodes["meta_rwzi_code"].notna()].tolist())
    areas = model.basin.area.df.dissolve(by="node_id").geometry
    waterway_geometries = shapely.force_2d(waterways.geometry.to_numpy())
    waterway_tree = shapely.STRtree(waterway_geometries)
    main_route = waterways["main-route"].to_numpy(dtype=bool)

    old_link_ids = set(links.index)
    changed: list[int] = []
    new_junctions: list[int] = []
    for basin_id in basin_ids:
        if basin_id not in areas.index:
            LOG.warning(f"No area for Basin {basin_id}, its Junctions are not rebuilt")
            continue
        basin_point = _point(model, basin_id)
        trees = {upstream: junction_tree(routes, basin_id, upstream, fixed) for upstream in (False, True)}
        # per key: whether it flows into the Basin, the node reached and its link
        ends = [
            (upstream, node_id, link_id) for upstream, tree in trees.items() for node_id, link_id in tree[2].items()
        ]
        if not ends:
            continue
        area = areas[basin_id].buffer(tolerance)
        idx, lines = lines_in(waterway_geometries, waterway_tree, area)
        _, routed = route_basin_links(
            basin_point,
            {key: _point(model, node_id) for key, (_, node_id, _) in enumerate(ends)},
            list(lines),
            list(main_route[idx]),
        )
        coords = attach_to_tree(
            routed,
            (basin_point.x, basin_point.y),
            {key: _coord(model, node_id) for key, (_, node_id, _) in enumerate(ends) if key not in routed},
        )
        LOG.info(f"Basin {basin_id}: routed {len(routed)} of {len(ends)} links, the others branch off straight")

        for _, tree_links_, _ in trees.values():
            model.remove_links(tree_links_)
        for junction_id in set().union(*(tree[0] for tree in trees.values())):
            model.remove_node(junction_id)
        for upstream in (False, True):
            keys = [key for key, end in enumerate(ends) if end[0] == upstream]
            if not keys:
                continue
            junctions: dict[int, int] = {}
            for start, end, line_coords in tree_links({key: coords[key] for key in keys}):
                node_ids = []
                for tree_node in (start, end):
                    if tree_node == "root":
                        node_ids.append(basin_id)
                    elif tree_node[0] == "end":
                        node_ids.append(ends[tree_node[1]][1])
                    else:
                        if tree_node[1] not in junctions:
                            # the link into a Junction comes first, and ends at its location
                            junction = model.junction.add(Node(geometry=Point(line_coords[-1])))
                            junctions[tree_node[1]] = junction.node_id
                        node_ids.append(junctions[tree_node[1]])
                geometry = LineString(line_coords)
                if upstream:
                    node_ids, geometry = node_ids[::-1], geometry.reverse()
                if isinstance(end, tuple) and end[0] == "end":
                    # the link to the node reached keeps its id
                    link_id = ends[end[1]][2]
                    model.link.df.loc[link_id, ["from_node_id", "to_node_id"]] = node_ids
                    model.link.df.loc[[link_id], "geometry"] = gpd.GeoSeries(
                        [geometry], index=[link_id], crs=model.link.df.crs
                    )
                    changed.append(link_id)
                else:
                    model.link.add(model.get_node(node_ids[0]), model.get_node(node_ids[1]), geometry=geometry)
            new_junctions.extend(junctions.values())
            if "meta_waterbeheerder" in nodes.columns:
                model.node.df.loc[list(junctions.values()), "meta_waterbeheerder"] = nodes.at[
                    basin_id, "meta_waterbeheerder"
                ]
        routes = Routes(model)

    new_links = sorted(set(model.link.df.index) - old_link_ids)
    after = simplified_links(model)
    assert after == before, f"Rebuilding the Junctions changed the links {sorted(after ^ before)[:10]}"
    return sorted({*changed, *new_links}), new_junctions


def rebuild_junction_trees_along_top10nl(
    model: Model,
    basin_ids: typing.Collection[int],
    top10nl_path: pathlib.Path,
    main_route: gpd.GeoDataFrame,
    tolerance: float = 250.0,
) -> tuple[list[int], list[int]]:
    """Rebuild the Junctions of Basins along the Top10NL water lines and a main route, see `rebuild_junction_trees`.

    This is used for the hws model and again after coupling, such that its Basins keep their routes. Its connector
    nodes are often outside the area of their Basin, so waterways up to `tolerance` around it are used.
    """
    assert model.basin.area.df is not None
    areas = model.basin.area.df[model.basin.area.df["node_id"].isin(basin_ids)]
    mask = shapely.union_all(areas.geometry.buffer(tolerance).to_numpy())
    waterways = read_main_waterways(top10nl_path, mask, main_route)
    return rebuild_junction_trees(model, basin_ids, waterways, tolerance)
