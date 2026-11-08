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
import pandas as pd
import shapely
from ribasim import Node
from shapely.geometry import LineString, Point
from shapely.ops import substring

from ribasim_nl.layout import move_nodes
from ribasim_nl.model import Model
from ribasim_nl.network_snapping import LineReader, lines_in, read_main_waterways, route_basin_links

LOG = logging.getLogger(__name__)

Coord = tuple[float, float]
# a node of a tree of routes, see `tree_links`
type _TreeNode = typing.Literal["root"] | tuple[typing.Literal["junction", "end"], int]


def _line(model: Model, link_id: int) -> LineString:
    return typing.cast(LineString, model.link.df.at[link_id, "geometry"])


def _point(model: Model, node_id: int) -> Point:
    return typing.cast(Point, model.node.df.at[node_id, "geometry"])


def _end(links: pd.DataFrame, link_id: int, upstream: bool) -> int:
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
    return str(model.link.df.at[link_id, "name"])


class Routes:
    """The routes of the flow links of a model through Junctions, as they are when the Routes are made.

    Ribasim removes the Junctions without rows in `Junction / static`, the plain Junctions, before the simulation, see
    `simplify_graph!` in the Ribasim core. A connected group of plain Junctions, ignoring the direction of the links,
    with exactly one Basin as a neighbor, connects each other neighbor with that Basin, in the direction of its link
    with the group: a Basin tree. The direction of the links within a Basin tree, and with its Basin, does not matter.
    Other groups connect each node upstream of a Junction with each node downstream of it, along the links.
    """

    def __init__(self, model: Model) -> None:
        links = model.link.df
        self._build(links[links["link_type"] == "flow"], model.node.df["node_type"], model.junction._routed_node_ids())

    @classmethod
    def from_tables(
        cls, flow: pd.DataFrame, node_types: pd.Series, routed: typing.Collection[int] = frozenset()
    ) -> "Routes":
        """The routes of flow links with a `from_node_id` and `to_node_id`, with the Junctions in `routed` kept."""
        routes = cls.__new__(cls)
        routes._build(flow, node_types, routed)
        return routes

    def _build(self, flow: pd.DataFrame, node_types: pd.Series, routed: typing.Collection[int]) -> None:
        self.flow = flow
        self.node_types = node_types
        self.plain = set(self.node_types.index[self.node_types == "Junction"].tolist()) - set(routed)
        self._out: dict[int, list[int]] = defaultdict(list)
        self._in: dict[int, list[int]] = defaultdict(list)
        for link_id, from_node_id, to_node_id in zip(
            self.flow.index.tolist(), self.flow["from_node_id"].tolist(), self.flow["to_node_id"].tolist(), strict=True
        ):
            self._out[from_node_id].append(link_id)
            self._in[to_node_id].append(link_id)
        self._ends: dict[tuple[int, bool], frozenset[int]] = {}
        self._groups()

    def _groups(self) -> None:
        """Find the groups of plain Junctions, and the tree of each group with exactly one Basin, from that Basin."""
        # per plain Junction, its group, named after its smallest Junction
        self.group: dict[int, int] = {}
        for start in sorted(self.plain):
            if start in self.group:
                continue
            self.group[start] = start
            stack = [start]
            while stack:
                node_id = stack.pop()
                for other in self.neighbors(node_id):
                    if other in self.plain and other not in self.group:
                        self.group[other] = start
                        stack.append(other)
        basins: dict[int, set[int]] = defaultdict(set)
        for node_id, group in self.group.items():
            basins[group].update(i for i in self.neighbors(node_id) if self.node_types[i] == "Basin")
        # per group, its Basin if it has exactly one
        self.group_basin: dict[int, int] = {g: next(iter(b)) for g, b in basins.items() if len(b) == 1}
        # per Junction of a Basin tree, the link towards the Basin, and per Basin the links of its trees
        self._parent: dict[int, int] = {}
        self._basin_links: dict[int, list[int]] = defaultdict(list)
        self._tree_links: set[int] = set()
        for group, basin_id in sorted(self.group_basin.items()):
            queue = deque((link_id, basin_id) for link_id in self.links(basin_id))
            while queue:
                link_id, near = queue.popleft()
                node_id = self.other_node(link_id, near)
                if self.group.get(node_id) != group or node_id in self._parent:
                    continue
                self._parent[node_id] = link_id
                self._basin_links[basin_id].append(link_id)
                self._tree_links.add(link_id)
                queue.extend((i, node_id) for i in self.links(node_id) if i != link_id)

    def links(self, node_id: int) -> list[int]:
        """The flow links into and out of `node_id`."""
        return [*self._in.get(node_id, []), *self._out.get(node_id, [])]

    def neighbors(self, node_id: int) -> list[int]:
        """The nodes connected to `node_id` by a flow link."""
        return [self.other_node(i, node_id) for i in self.links(node_id)]

    def other_node(self, link_id: int, node_id: int) -> int:
        """The node at the other end of a flow link."""
        from_node_id = _end(self.flow, link_id, upstream=True)
        return _end(self.flow, link_id, upstream=False) if from_node_id == node_id else from_node_id

    def links_at(self, node_id: int, upstream: bool) -> list[int]:
        """The flow links into `node_id` if `upstream`, else out of it."""
        return list((self._in if upstream else self._out).get(node_id, []))

    def next_node(self, link_id: int, upstream: bool) -> int:
        """The node a flow link comes from if `upstream`, else goes to."""
        return _end(self.flow, link_id, upstream)

    def tree_basin(self, node_id: int) -> int | None:
        """The Basin of the Basin tree of a plain Junction, if it is in one."""
        group = self.group.get(node_id)
        return None if group is None else self.group_basin.get(group)

    def parent_link(self, node_id: int) -> int | None:
        """The link of a Junction of a Basin tree towards its Basin."""
        return self._parent.get(node_id)

    def is_tree_link(self, link_id: int) -> bool:
        """Whether a link connects a Junction of a Basin tree with its Basin, or with another Junction of the tree."""
        return link_id in self._tree_links

    def basin_links(self, basin_id: int) -> list[int]:
        """The links of the Basin trees of `basin_id`, nearest to the Basin first, without its other links."""
        return list(self._basin_links.get(basin_id, []))

    def subtree(self, link_id: int) -> tuple[frozenset[int], frozenset[int]]:
        """The nodes beyond a link of a Basin, away from the Basin, that flow into the Basin, and that it flows into.

        For a link of a Basin tree, these are the nodes reached through the Junctions beyond it. For a link with
        another node, that node itself, by the direction of the link.
        """
        if link_id in self._tree_links:
            far = self._junction_end(link_id)
            inflows: set[int] = set()
            outflows: set[int] = set()
            for i in self.links(far):
                if i != link_id:
                    ins, outs = self.subtree(i)
                    inflows |= ins
                    outflows |= outs
            return frozenset(inflows), frozenset(outflows)
        from_node_id, to_node_id = self.next_node(link_id, upstream=True), self.next_node(link_id, upstream=False)
        inflow = to_node_id in self._parent or (
            from_node_id not in self._parent and self.node_types[to_node_id] == "Basin"
        )
        return (frozenset([from_node_id]), frozenset()) if inflow else (frozenset(), frozenset([to_node_id]))

    def ends(self, link_id: int, upstream: bool = False) -> frozenset[int]:
        """The first nodes downstream of a flow link, or upstream if `upstream`, other than plain Junctions.

        A link of a Basin tree that points towards its Basin flows into the Basin, and gets its water from the nodes
        beyond it that flow into the Basin. A link that points away from the Basin flows into the nodes beyond it that
        the Basin flows into.
        """
        node_id = self.next_node(link_id, upstream)
        if node_id not in self.plain:
            return frozenset([node_id])
        basin_id = self.tree_basin(node_id)
        if basin_id is None:
            return self._node_ends(node_id, upstream, set())
        if self._parent.get(node_id) != link_id:
            return frozenset([basin_id])
        inflows, outflows = self.subtree(link_id)
        return inflows if upstream else outflows

    def node_ends(self, node_id: int, upstream: bool = False) -> frozenset[int]:
        """The first nodes downstream of a plain Junction outside a Basin tree, or upstream, see `ends`."""
        return self._node_ends(node_id, upstream, set())

    def _node_ends(self, node_id: int, upstream: bool, visiting: set[int]) -> frozenset[int]:
        if node_id not in self.plain:
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

    def paths(self, basin_id: int) -> dict[int, list[int]]:
        """The route of a Basin: the path from `basin_id` to each link of its Basin trees, and to its direct links.

        Links with plain Junctions of groups that are not a Basin tree are not part of it.
        """
        paths = {i: [i] for i in self.links(basin_id) if self.other_node(i, basin_id) not in self.plain}
        for link_id in self.basin_links(basin_id):
            junction_id = self._junction_end(link_id)
            near = self.other_node(link_id, junction_id)
            paths[link_id] = [link_id] if near == basin_id else [*paths[self._parent[near]], link_id]
            for i in self.links(junction_id):
                other = self.other_node(i, junction_id)
                if i != link_id and other != basin_id and other not in self._parent:
                    paths[i] = [*paths[link_id], i]
        return paths

    def simplified_links(self) -> set[tuple[int, int]]:
        """The flow links between nodes other than plain Junctions that Ribasim makes of the flow links."""
        result = set()
        for link_id in self.flow.index:
            from_node_id = self.next_node(link_id, upstream=True)
            if from_node_id not in self.plain:
                result.update((from_node_id, to_node_id) for to_node_id in self.ends(link_id))
        # the Basin of a tree flows into each node that the tree flows into, whichever way its links point
        for basin_id in set(self.group_basin.values()):
            for link_id in self.links(basin_id):
                if self.is_tree_link(link_id):
                    result.update((basin_id, node_id) for node_id in self.subtree(link_id)[1])
        return result

    def _junction_end(self, link_id: int) -> int:
        """The Junction of a Basin tree link that is farthest from the Basin."""
        to_node_id = self.next_node(link_id, upstream=False)
        return to_node_id if self._parent.get(to_node_id) == link_id else self.next_node(link_id, upstream=True)


def _coords(geometry: LineString) -> list[Coord]:
    return [(c[0], c[1]) for c in geometry.coords]


def _dedupe(coords: list[Coord]) -> list[Coord]:
    return [c for i, c in enumerate(coords) if i == 0 or c != coords[i - 1]]


def split_distance(geometry: LineString, point: Point, margin: float = 1.0) -> float:
    """The distance along `geometry` of the point nearest to `point`, at least `margin` from its ends if possible."""
    margin = min(margin, geometry.length / 4)
    return min(max(geometry.project(point), margin), geometry.length - margin)


def route_parts(model: Model, start_id: int, path: list[int]) -> list[LineString]:
    """The geometries of the links of a path from `start_id`, each in the direction away from `start_id`."""
    links = model.link.df
    parts = []
    node_id = start_id
    for link_id in path:
        from_node_id, to_node_id = _end(links, link_id, upstream=True), _end(links, link_id, upstream=False)
        geometry = _line(model, link_id)
        if geometry is None:
            geometry = LineString([_point(model, from_node_id), _point(model, to_node_id)])
        if from_node_id == node_id:
            parts.append(geometry)
            node_id = to_node_id
        else:
            assert to_node_id == node_id, f"Link {link_id} does not continue the path from node {start_id}"
            parts.append(geometry.reverse())
            node_id = from_node_id
    return parts


def geometry_along(model: Model, start_id: int, path: list[int], distance: float) -> LineString:
    """The line from `start_id` along a path of links, see `Routes.paths`, to `distance` along its last link.

    `distance` is measured along the last link in its own direction.
    """
    parts = route_parts(model, start_id, path)
    last = _line(model, path[-1])
    if parts[-1].equals_exact(last, 0.0):
        parts[-1] = _substring(last, 0.0, distance)
    else:
        parts[-1] = _substring(last, distance, last.length).reverse()
    return LineString(_dedupe([c for part in parts for c in _coords(part)]))


def split_link(model: Model, link_id: int, distance: float, node_id: int | None = None) -> int:
    """Split a flow link at `distance` along it with a Junction: `node_id` moved there, or else a new one.

    Returns the node id of the Junction. Other links of an existing `node_id` move their ends along.
    """
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

    A Junction of a Basin tree, see `Routes`, only needs two links, in any direction. Neighboring Junctions are pruned
    in turn. A Junction with other links, such as observation links, is kept.
    """
    if junction_id not in model.node.df.index or model.get_node_type(junction_id) != "Junction":
        return
    links = model.link.df
    touching = links[(links["from_node_id"] == junction_id) | (links["to_node_id"] == junction_id)]
    if (touching["link_type"] != "flow").any():
        return
    routes = Routes(model)
    if junction_id not in routes.plain:
        return
    inflow = touching[touching["to_node_id"] == junction_id]
    outflow = touching[touching["from_node_id"] == junction_id]
    basin_id = routes.tree_basin(junction_id)
    if (basin_id is None and (inflow.empty or outflow.empty)) or len(touching) < 2:
        neighbors = {*inflow["from_node_id"], *outflow["to_node_id"]}
        model.remove_node(junction_id, remove_links=True)
        for neighbor in neighbors:
            prune_junction(model, int(neighbor))
    elif len(touching) == 2 and (basin_id is not None or len(inflow) == 1):
        first, second = (int(i) for i in touching.index)
        a, b = routes.other_node(first, junction_id), routes.other_node(second, junction_id)
        # keep the direction of the link of a node other than the Basin and plain Junctions, which flows in or out
        if b != basin_id and b not in routes.plain:
            first, second, a, b = second, first, b, a
        keep, drop = first, second
        if _end(links, keep, upstream=False) == junction_id:
            from_node_id, to_node_id, ordered = a, b, [keep, drop]
        else:
            from_node_id, to_node_id, ordered = b, a, [drop, keep]
        parts = route_parts(model, from_node_id, ordered)
        geometry = LineString(_dedupe([c for part in parts for c in _coords(part)]))
        model.remove_links([drop])
        model.link.df.loc[keep, ["from_node_id", "to_node_id"]] = [from_node_id, to_node_id]
        model.link.df.loc[[keep], "geometry"] = gpd.GeoSeries([geometry], index=[keep], crs=links.crs)
        model.remove_node(junction_id)


def remove_flow_link(model: Model, link_id: int) -> None:
    """Remove a flow link, and prune the Junctions at its ends, see `prune_junction`."""
    ends = [_end(model.link.df, link_id, upstream) for upstream in (True, False)]
    model.remove_links([link_id])
    for node_id in ends:
        prune_junction(model, node_id)


def branch_from_route(model: Model, node_id: int, basin_id: int, towards_id: int, margin: float = 1.0) -> int:
    """Let the link from `basin_id` into `node_id` branch off the route between `basin_id` and `towards_id`.

    The branch starts at a Junction at the point of that route nearest to `node_id`, or at the existing Junction if
    that point is within `margin` of one, and runs straight to `node_id`. Returns the node id of that Junction.
    """
    flow = model.link.df[model.link.df["link_type"] == "flow"]
    inflow = flow[flow["to_node_id"] == node_id]
    if inflow["from_node_id"].tolist() != [basin_id]:
        raise ValueError(f"Node {node_id} should only have a flow link from Basin {basin_id}.")
    remove_flow_link(model, int(inflow.index[0]))

    routes = Routes(model)
    links = model.link.df
    route = {
        i
        for path in routes.paths(basin_id).values()
        if towards_id in (_end(links, path[-1], upstream=False), _end(links, path[-1], upstream=True))
        for i in path
    }
    if not route:
        raise ValueError(f"No route from Basin {basin_id} to node {towards_id}.")
    point = _point(model, node_id)
    link_id = min(route, key=lambda i: _line(model, i).distance(point))
    geometry = _line(model, link_id)
    distance = geometry.project(point)
    from_node_id, to_node_id = _end(links, link_id, upstream=True), _end(links, link_id, upstream=False)
    if distance <= margin and from_node_id in routes.plain:
        junction_id = from_node_id
    elif distance >= geometry.length - margin and to_node_id in routes.plain:
        junction_id = to_node_id
    else:
        junction_id = split_link(model, link_id, split_distance(geometry, point, margin))
    model.link.add(model.get_node(junction_id), model.get_node(node_id))
    return junction_id


def merge_outflows(model: Model, node_ids: list[int]) -> int:
    """Connect `node_ids` with straight links to the first Junction downstream that all of their routes pass.

    The Junctions they passed before are pruned. Returns the node id of the Junction.
    """
    routes = Routes(model)

    def downstream_junctions(node_id: int) -> list[int]:
        links = routes.links_at(node_id, upstream=False)
        assert len(links) == 1, f"Node {node_id} should have one outflow link"
        junctions = []
        next_node = routes.next_node(links[0], upstream=False)
        while next_node in routes.plain and next_node not in junctions:
            junctions.append(next_node)
            parent_link = routes.parent_link(next_node)
            if parent_link is not None:
                # a Junction of a Basin tree, towards its Basin
                next_node = routes.other_node(parent_link, next_node)
                continue
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

    `link_id` is on the route of `basin_id`, see `Routes.paths`, and `node_id` has a single flow link with `basin_id`. Its other links move their ends along.
    """
    flow = model.link.df[model.link.df["link_type"] == "flow"]
    from_basin = flow[(flow["from_node_id"] == basin_id) & (flow["to_node_id"] == node_id)]
    to_basin = flow[(flow["from_node_id"] == node_id) & (flow["to_node_id"] == basin_id)]
    if len(from_basin) + len(to_basin) != 1:
        raise ValueError(f"Node {node_id} should have one flow link with Basin {basin_id}.")

    path = Routes(model).paths(basin_id).get(link_id)
    if path is None:
        raise ValueError(f"Link {link_id} is not on the route of Basin {basin_id}.")
    distance = split_distance(_line(model, link_id), _point(model, node_id))
    along = geometry_along(model, basin_id, path, distance)
    move_nodes(model, {node_id: Point(along.coords[-1])})
    if from_basin.empty:
        along = along.reverse()
    basin_link = (from_basin if to_basin.empty else to_basin).index
    model.link.df.loc[basin_link, "geometry"] = gpd.GeoSeries([along], index=basin_link, crs=model.link.df.crs)


def simplified_links(model: Model) -> set[tuple[int, int]]:
    """The flow links between nodes other than plain Junctions that Ribasim makes of the flow links, see `Routes`."""
    return Routes(model).simplified_links()


def junction_tree(
    routes: Routes, basin_id: int, fixed: typing.Container[int] = ()
) -> tuple[set[int], list[int], dict[int, int]]:
    """The Junctions of the Basin trees of `basin_id`, see `Routes`, up to the Junctions in `fixed`, and what they reach.

    Junctions in `fixed` are not part of the tree but reached by it, like the other nodes. Links of the Basin with
    plain Junctions of other groups are left out.

    :return: the Junctions of the tree, the links between `basin_id` and them, and per link to a node reached, that node
    """
    junctions: set[int] = set()
    links: list[int] = []
    reached: dict[int, int] = {}
    queue = deque((link_id, basin_id) for link_id in routes.links(basin_id))
    while queue:
        link_id, near = queue.popleft()
        node_id = routes.other_node(link_id, near)
        if routes.is_tree_link(link_id) and routes.parent_link(node_id) == link_id and node_id not in fixed:
            junctions.add(node_id)
            links.append(link_id)
            queue.extend((i, node_id) for i in routes.links(node_id) if i != link_id)
        elif (node_id in routes.plain and routes.parent_link(node_id) != link_id) or node_id == basin_id:
            # a Junction of another group, or a cycle
            continue
        else:
            reached[link_id] = node_id
    return junctions, links, reached


# a capacity of at least this is a placeholder for an unlimited capacity, like the 99999 of some Outlets
UNLIMITED_CAPACITY = 1e4


def drainage_capacity(model: Model) -> dict[int, float]:
    """The flow that each node can pass in the drainage situation, to choose the direction of Basin tree links.

    For Pumps and Outlets the largest `flow_rate` or `max_flow_rate` without control state, or in a control state with
    "afvoer". ManningResistances, and placeholders and missing maxima, are unlimited (infinite). Also the
    LinearResistances, TabulatedRatingCurves, FlowBoundaries and UserDemands, by their largest flow or demand.
    """
    capacity: dict[int, float] = {}

    def add(table: pd.DataFrame | None, columns: list[str]) -> None:
        if table is None or table.empty:
            return
        values = table[[c for c in columns if c in table.columns]].max(axis=1)
        values = values.where(values < UNLIMITED_CAPACITY, np.inf).fillna(np.inf)
        maxima = values.groupby(table["node_id"]).max()
        for node_id, value in zip(maxima.index.tolist(), maxima.tolist(), strict=True):
            capacity[node_id] = max(capacity.get(node_id, 0.0), value)

    for table in (model.pump.static.df, model.outlet.static.df):
        if table is not None:
            state = table["control_state"].astype("string").str.lower()
            add(table[state.isna() | state.str.contains("afvoer", na=False)], ["flow_rate", "max_flow_rate"])
    add(model.linear_resistance.static.df, ["max_flow_rate"])
    add(model.tabulated_rating_curve.static.df, ["flow_rate"])
    add(model.flow_boundary.static.df, ["flow_rate"])
    add(model.flow_boundary.time.df, ["flow_rate"])
    add(model.user_demand.static.df, ["demand"])
    add(model.user_demand.time.df, ["demand"])
    nodes = model.node.df
    capacity.update(dict.fromkeys(nodes.index[nodes["node_type"] == "ManningResistance"].tolist(), np.inf))
    return capacity


class Digitized:
    """The direction in which lines are digitized, to follow it where routes run along them.

    Routes along lines, see `ribasim_nl.network_snapping.route_basin_links`, take the coordinates of the lines, so each
    of their segments is a segment of a line, digitized in the direction of the route, or against it.
    """

    def __init__(
        self, lines: typing.Iterable[shapely.Geometry], routes: typing.Iterable[list[Coord]] | None = None
    ) -> None:
        """Take the direction of the segments of `lines`, only of those along `routes` if given."""
        geometries = shapely.force_2d(np.asarray(list(lines), dtype=object))
        xy, index = shapely.get_coordinates(geometries, return_index=True)
        same = index[1:] == index[:-1]
        needed = None
        if routes is not None:
            needed = {pair for route in routes for a, b in itertools.pairwise(route) for pair in ((a, b), (b, a))}
        # the segments digitized from their first to their second coordinate
        self.segments: set[tuple[Coord, Coord]] = set()
        for a, b in zip(map(tuple, xy[:-1][same].tolist()), map(tuple, xy[1:][same].tolist()), strict=True):
            if needed is None or (a, b) in needed:
                self.segments.add((a, b))
        # segments digitized both ways have no direction
        self.segments -= {(b, a) for a, b in self.segments}

    def along(self, coords: list[Coord]) -> bool | None:
        """Whether lines are digitized in the direction of a route, or against it, over most of its length.

        :return: None if lines cover less than half the route in one direction, or as much in both
        """
        forward = backward = total = 0.0
        for a, b in itertools.pairwise(coords):
            length = float(np.hypot(b[0] - a[0], b[1] - a[1]))
            total += length
            if (a, b) in self.segments:
                forward += length
            elif (b, a) in self.segments:
                backward += length
        if forward + backward <= 0.5 * total or forward == backward:
            return None
        return forward > backward


def towards_basin(
    inflows: typing.Collection[int],
    outflows: typing.Collection[int],
    capacity: typing.Mapping[int, float],
    digitized_away: bool | None = None,
) -> bool:
    """Whether a link of a Basin tree points towards its Basin, given the nodes beyond it that flow in and out.

    The direction does not change the simulation, only the sign of the flow of the link in the results. A link that
    only carries inflows points towards the Basin, and one that only carries outflows away from it. A link that
    carries both follows the main flow: the direction in which the lines it follows are digitized, if they decide,
    else the side with the larger `drainage_capacity`, else the side with more nodes, else away from the Basin.

    :param digitized_away: whether the lines along the link are digitized away from the Basin, see `Digitized.along`
    """
    if not outflows:
        return True
    if not inflows:
        return False
    if digitized_away is not None:
        return not digitized_away
    capacity_in = sum(capacity.get(i, 0.0) for i in inflows)
    capacity_out = sum(capacity.get(i, 0.0) for i in outflows)
    if capacity_in != capacity_out:
        return capacity_in > capacity_out
    return len(inflows) > len(outflows)


def build_basin_tree(
    model: Model,
    basin_id: int,
    ends: list[tuple[int, int]],
    coords: dict[int, list[Coord]],
    sides: dict[int, tuple[frozenset[int], frozenset[int]]],
    capacity: typing.Mapping[int, float],
    digitized: Digitized | None = None,
) -> tuple[list[int], list[int]]:
    """Connect the nodes reached by the Basin tree of `basin_id` along their routes, with Junctions where they split.

    Inflows and outflows share one tree, see `Routes`. The links to the nodes reached keep their ids and direction.
    The other links point towards the Basin or away from it, see `towards_basin`, given the nodes beyond them.

    :param ends: per key, the node reached and its link, which still exists
    :param coords: per key, the coordinates of the route from the Basin node to the node reached
    :param sides: per key, the nodes beyond the link that flow into the Basin, and out of it, see `Routes.subtree`
    :param capacity: the `drainage_capacity` of the model
    :param digitized: the lines the routes follow, see `towards_basin`
    :return: the ids of the links that changed or are new, and of the new Junctions
    """
    tree = tree_links(coords)
    children: dict[_TreeNode, list[_TreeNode]] = defaultdict(list)
    for start, end, _ in tree:
        children[start].append(end)

    def beyond(tree_node: _TreeNode) -> tuple[set[int], set[int]]:
        if tree_node != "root" and tree_node[0] == "end":
            return set(sides[tree_node[1]][0]), set(sides[tree_node[1]][1])
        inflows: set[int] = set()
        outflows: set[int] = set()
        for child in children[tree_node]:
            ins, outs = beyond(child)
            inflows |= ins
            outflows |= outs
        return inflows, outflows

    links = model.link.df
    junctions: dict[int, int] = {}
    changed: list[int] = []
    for start, end, line_coords in tree:
        node_ids = []
        for tree_node in (start, end):
            if tree_node == "root":
                node_ids.append(basin_id)
            elif tree_node[0] == "end":
                node_ids.append(ends[tree_node[1]][0])
            else:
                if tree_node[1] not in junctions:
                    # the link into a Junction comes first, and ends at its location
                    junctions[tree_node[1]] = model.junction.add(Node(geometry=Point(line_coords[-1]))).node_id
                node_ids.append(junctions[tree_node[1]])
        geometry = LineString(line_coords)
        if end != "root" and end[0] == "end":
            # the link to the node reached keeps its id and direction
            link_id = ends[end[1]][1]
            if _end(links, link_id, upstream=True) == node_ids[1]:
                node_ids, geometry = node_ids[::-1], geometry.reverse()
            model.link.df.loc[link_id, ["from_node_id", "to_node_id"]] = node_ids
            model.link.df.loc[[link_id], "geometry"] = gpd.GeoSeries([geometry], index=[link_id], crs=links.crs)
            changed.append(link_id)
        else:
            digitized_away = None if digitized is None else digitized.along(line_coords)
            if towards_basin(*beyond(end), capacity, digitized_away):
                node_ids, geometry = node_ids[::-1], geometry.reverse()
            model.link.add(model.get_node(node_ids[0]), model.get_node(node_ids[1]), geometry=geometry)
            changed.append(int(model.link.df.index[-1]))
    if "meta_waterbeheerder" in model.node.df.columns:
        authority = typing.cast(str, model.node.df.at[basin_id, "meta_waterbeheerder"])
        model.node.df.loc[list(junctions.values()), "meta_waterbeheerder"] = authority
    return changed, list(junctions.values())


def _tree_splits(tree: list[tuple[_TreeNode, _TreeNode, list[Coord]]]) -> set[frozenset[int]]:
    """The keys of the routes beyond each link into a Junction of a tree, see `tree_links`."""
    children: dict[_TreeNode, list[_TreeNode]] = defaultdict(list)
    for start, end, _ in tree:
        children[start].append(end)

    def keys(tree_node: _TreeNode) -> frozenset[int]:
        if tree_node != "root" and tree_node[0] == "end":
            return frozenset([tree_node[1]])
        return frozenset().union(*(keys(child) for child in children[tree_node]))

    return {keys(end) for _, end, _ in tree if end != "root" and end[0] == "junction"}


def fixed_junctions(model: Model) -> set[int]:
    """The Junctions that are kept as they are when Basin trees are rebuilt.

    These are observed, or listened to, or found by their code, like the RWZI Junctions, see
    `ribasim_nl.rwzi.attach_rwzi_junctions`.
    """
    nodes, links = model.node.df, model.link.df
    fixed = set(links.loc[links["link_type"] != "flow", ["from_node_id", "to_node_id"]].to_numpy().ravel().tolist())
    if "meta_rwzi_code" in nodes.columns:
        fixed |= set(nodes.index[(nodes["node_type"] == "Junction") & nodes["meta_rwzi_code"].notna()].tolist())
    return fixed


class _Trie:
    """Routes from one point by their coordinates, such that routes that share their start share a branch."""

    def __init__(self) -> None:
        self.children: dict[Coord, _Trie] = {}
        self.ends: list[int] = []


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


def merge_basin_trees(model: Model, read_lines: LineReader | None = None) -> list[int]:
    """Give each Basin one Junction tree for its inflows and outflows, along the current routes of its links.

    The routes of the links of a Basin are followed through the Junctions of its Basin trees, see
    `Routes`, up to the `fixed_junctions`, and joined where they share
    their coordinates from the Basin node on, see `build_basin_tree`. A Basin whose tree would
    not change is left as it is. Each node keeps exchanging water with the same nodes, which is asserted.

    :param read_lines: reads the lines that the routes follow within an area, to follow the direction in which they
        are digitized, see `towards_basin`
    :return: the new Junctions
    """
    before = simplified_links(model)
    # the trees of different Basins do not share links, so changing one leaves the routes of the others as they are
    routes = Routes(model)
    fixed = fixed_junctions(model)
    capacity = drainage_capacity(model)
    nodes = model.node.df
    # first find the trees that change, to read the lines around them at once
    changes = []
    for basin_id in sorted(nodes.index[nodes["node_type"] == "Basin"].tolist()):
        junctions, tree_link_ids, reached = junction_tree(routes, basin_id, fixed)
        if len(reached) < 2:
            continue
        paths = routes.paths(basin_id)
        ends = [(node_id, link_id) for link_id, node_id in reached.items()]
        origin = _coord(model, basin_id)
        coords = {}
        for key, (node_id, link_id) in enumerate(ends):
            parts = route_parts(model, basin_id, paths[link_id])
            route = _dedupe([origin, *(c for part in parts for c in _coords(part))])
            coords[key] = route if len(route) >= 2 else [origin, _coord(model, node_id)]
        # the tree as the nodes reached beyond each of its links between the Basin and Junctions
        current = {frozenset(k for k, (_, i) in enumerate(ends) if link_id in paths[i]) for link_id in tree_link_ids}
        if _tree_splits(tree_links(coords)) == current:
            continue
        sides = {key: routes.subtree(link_id) for key, (_, link_id) in enumerate(ends)}
        changes.append((basin_id, junctions, tree_link_ids, ends, coords, sides))

    digitized = None
    if read_lines is not None and changes:
        area = shapely.union_all(
            [shapely.MultiPoint([c for route in change[4].values() for c in route]).envelope for change in changes]
        )
        routes_coords = [route for change in changes for route in change[4].values()]
        digitized = Digitized(read_lines(shapely.buffer(area, 1.0)), routes_coords)
    new_junctions: list[int] = []
    for basin_id, junctions, tree_link_ids, ends, coords, sides in changes:
        model.remove_links(tree_link_ids)
        for junction_id in sorted(junctions):
            model.remove_node(junction_id)
        new_junctions.extend(build_basin_tree(model, basin_id, ends, coords, sides, capacity, digitized)[1])
    after = simplified_links(model)
    assert after == before, f"Merging the Basin trees changed the links {sorted(after ^ before)[:10]}"
    return new_junctions


def rebuild_junction_trees(
    model: Model,
    basin_ids: typing.Iterable[int],
    waterways: gpd.GeoDataFrame,
    tolerance: float = 10.0,
) -> tuple[list[int], list[int]]:
    """Route the flow links of Basins along the waterways, with one Junction tree per Basin, split where they split.

    The Junctions of the Basin trees of a Basin, see `junction_tree`, are removed, except the `fixed_junctions`. The
    links to the nodes they reach are routed from the Basin node along the shortest paths of one network, see
    `ribasim_nl.network_snapping.route_basin_links`, such that its inflows and outflows share one tree, see
    `build_basin_tree`. Nodes far from the waterways branch off straight from the nearest point of these routes, see
    `attach_to_tree`, also over open water, where routes over the water surface would run in parallel. Each node keeps
    exchanging water with the same nodes, which is asserted, and the links to the nodes reached keep their link ids.
    Nodes are not moved and links are not offset, see `ribasim_nl.layout.tidy_layout`.

    :param model: Ribasim model
    :param basin_ids: Basins to rebuild the Junctions of
    :param waterways: waterways with a boolean "main-route" column, see `ribasim_nl.network_snapping.snap_to_network`
    :param tolerance: buffer around the basin area within which waterways are used, defaults to 10.0

    :return: the ids of the links and of the Junctions that are new or changed
    """
    assert model.basin.area.df is not None
    before = simplified_links(model)
    routes = Routes(model)
    fixed = fixed_junctions(model)
    capacity = drainage_capacity(model)
    areas = model.basin.area.df.dissolve(by="node_id").geometry
    waterway_geometries = shapely.force_2d(waterways.geometry.to_numpy())
    waterway_tree = shapely.STRtree(waterway_geometries)
    main_route = waterways["main-route"].to_numpy(dtype=bool)

    old_link_ids = set(model.link.df.index)
    changed: list[int] = []
    new_junctions: list[int] = []
    # the trees of different Basins do not share links, so rebuilding one leaves the routes of the others as they are
    for basin_id in sorted(set(basin_ids)):
        if basin_id not in areas.index:
            LOG.warning(f"No area for Basin {basin_id}, its Junctions are not rebuilt")
            continue
        basin_point = _point(model, basin_id)
        junctions, tree_link_ids, reached = junction_tree(routes, basin_id, fixed)
        # per key: the node reached and its link
        ends = [(node_id, link_id) for link_id, node_id in reached.items()]
        if not ends:
            continue
        sides = {key: routes.subtree(link_id) for key, (_, link_id) in enumerate(ends)}
        area = areas[basin_id].buffer(tolerance)
        idx, lines = lines_in(waterway_geometries, waterway_tree, area)
        _, routed = route_basin_links(
            basin_point,
            {key: _point(model, node_id) for key, (node_id, _) in enumerate(ends)},
            list(lines),
            list(main_route[idx]),
        )
        coords = attach_to_tree(
            routed,
            (basin_point.x, basin_point.y),
            {key: _coord(model, node_id) for key, (node_id, _) in enumerate(ends) if key not in routed},
        )
        LOG.info(f"Basin {basin_id}: routed {len(routed)} of {len(ends)} links, the others branch off straight")

        model.remove_links(tree_link_ids)
        for junction_id in sorted(junctions):
            model.remove_node(junction_id)
        changed_links, junction_ids = build_basin_tree(
            model, basin_id, ends, coords, sides, capacity, Digitized(lines, coords.values())
        )
        changed.extend(changed_links)
        new_junctions.extend(junction_ids)

    new_links = sorted(set(model.link.df.index) - old_link_ids)
    after = simplified_links(model)
    assert after == before, f"Rebuilding the Junctions changed the links {sorted(after ^ before)[:10]}"
    return sorted({*changed, *new_links} & set(model.link.df.index)), new_junctions


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
