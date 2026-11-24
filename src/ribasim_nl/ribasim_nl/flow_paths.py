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
from ribasim_nl.network_snapping import (
    LineReader,
    lines_in,
    read_lake_waterways,
    read_lines_on_water,
    read_main_waterways,
    route_basin_links,
)

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

    def flow_rate_links(self, node_id: int) -> list[int]:
        """The flow links whose total flow is the flow rate of a node, as in Ribasim.

        That is the outflow of a node, but for a Junction of a Basin tree the flow of its link towards the Basin, in
        the direction of that link: the net flow of the nodes beyond it, see `subtree`.
        """
        parent_link = self._parent.get(node_id)
        return [parent_link] if parent_link is not None else self.links_at(node_id, upstream=False)

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

    The link into `node_id` comes from the Basin, or from a Junction of its tree, see `Routes`: the regional models
    merge the links of a Basin into a tree where they share a route.
    """
    flow = model.link.df[model.link.df["link_type"] == "flow"]
    inflow = flow[flow["to_node_id"] == node_id]
    sources = inflow["from_node_id"].tolist()
    if len(sources) != 1 or (sources[0] != basin_id and Routes(model).tree_basin(sources[0]) != basin_id):
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


# Maximum distance between the nodes of `combine_nodes` and their Junction [m]: the nodes of one structure, such as
# the pumps of a gemaal and the inlet next to it, not nodes along the boundary of a Basin
MAX_COMBINE_DISTANCE = 250.0


def tree_sides(routes: Routes, node_ids: typing.Iterable[int]) -> tuple[int, dict[int, tuple[int, bool]]] | None:
    """The Basin that all nodes exchange water with through one link each, directly or through its Basin tree.

    If the nodes have two such Basins, like structures next to each other between two Basins, it is the one downstream
    of the node with the lowest id.

    :return: the Basin, and per node its link towards the Basin and whether that link is upstream of the node, so the
        Basin flows into the node, or None if there is no such Basin
    """
    sides: dict[int, dict[int, tuple[int, bool]]] = {}
    for node_id in sorted(set(node_ids)):
        sides[node_id] = {}
        for upstream in (False, True):
            links = routes.links_at(node_id, upstream)
            if len(links) != 1:
                continue
            ends = routes.ends(links[0], upstream)
            neighbor = routes.next_node(links[0], upstream)
            if len(ends) == 1 and (neighbor in ends or routes.tree_basin(neighbor) in ends):
                (basin_id,) = ends
                if routes.node_types[basin_id] == "Basin" and basin_id not in sides[node_id]:
                    sides[node_id][basin_id] = (links[0], upstream)
    first = next(iter(sides.values()), {})
    for basin_id in first:
        if all(basin_id in side for side in sides.values()):
            return basin_id, {node_id: side[basin_id] for node_id, side in sides.items()}
    return None


def combine_nodes(
    model: Model, node_ids: typing.Collection[int], max_distance: float = MAX_COMBINE_DISTANCE
) -> int | None:
    """Give nodes that exchange water with the same Basin a Junction of their own, which combines exactly them.

    The flow rate of that Junction is the net flow of these nodes into the Basin, see `Routes.flow_rate_links`, such as
    that of a gemaal and the inlet next to it, of which the links meet at the Basin node, or run side by side. The
    link of the node with the lowest id, see `tree_sides`, is split at its point nearest to the other nodes, at least
    10 m from its ends if possible. The other nodes get a straight link to the Junction there, and the Junctions they
    leave are pruned, see `prune_junction`. Each node keeps exchanging water with the same nodes, which is asserted.

    Nothing changes if the nodes do not share a Basin, if a node is more than `max_distance` from the Junction, or if
    a Junction between only some of the nodes and the Basin has other links than flow links, such as an observed
    Junction, of which the flow rate would change.

    :param model: Ribasim model
    :param node_ids: nodes to combine, at least two
    :param max_distance: maximum distance between the nodes and their Junction
    :return: the Junction, or None if the nodes are not combined
    """
    assert len(set(node_ids)) >= 2, "Combine at least two nodes"
    routes = Routes(model)
    found = tree_sides(routes, node_ids)
    if found is None:
        return None
    basin_id, sides = found
    first, *others = sorted(sides)

    # the Junctions between each node and the Basin
    links = model.link.df
    other_links = set(links.loc[links["link_type"] != "flow", ["from_node_id", "to_node_id"]].to_numpy().ravel())
    ancestors = {}
    for node_id, (link_id, upstream) in sides.items():
        ancestors[node_id] = set()
        junction_id = routes.next_node(link_id, upstream)
        while junction_id != basin_id:
            ancestors[node_id].add(junction_id)
            parent_link = routes.parent_link(junction_id)
            assert parent_link is not None, f"Junction {junction_id} is not in the tree of Basin {basin_id}"
            junction_id = routes.other_node(parent_link, junction_id)
    shared = set.intersection(*ancestors.values())
    if (set.union(*ancestors.values()) - shared) & other_links:
        return None

    link_id, upstream = sides[first]
    geometry = _line(model, link_id)
    if geometry is None or geometry.length == 0:
        return None
    points = [_point(model, node_id) for node_id in others]
    distance = split_distance(geometry, shapely.MultiPoint(points).centroid, margin=10.0)
    location = geometry.interpolate(distance)
    if max(location.distance(point) for point in [_point(model, first), *points]) > max_distance:
        return None

    before = routes.simplified_links()
    junction = model.junction.add(Node(geometry=Point(location.x, location.y)))
    if "meta_waterbeheerder" in model.node.df.columns:
        authority = typing.cast(str, model.node.df.at[basin_id, "meta_waterbeheerder"])
        model.node.df.loc[[junction.node_id], "meta_waterbeheerder"] = authority
    # the link of the first node keeps its id on the part with that node
    start, end = _substring(geometry, 0.0, distance), _substring(geometry, distance, geometry.length)
    neighbor = routes.next_node(link_id, upstream)
    name = _name(model, link_id)
    if upstream:
        model.link.df.loc[link_id, "from_node_id"] = junction.node_id
        model.link.df.loc[[link_id], "geometry"] = gpd.GeoSeries([end], index=[link_id], crs=links.crs)
        model.link.add(model.get_node(neighbor), junction, geometry=start, name=name)
    else:
        model.link.df.loc[link_id, "to_node_id"] = junction.node_id
        model.link.df.loc[[link_id], "geometry"] = gpd.GeoSeries([start], index=[link_id], crs=links.crs)
        model.link.add(junction, model.get_node(neighbor), geometry=end, name=name)
    for node_id in others:
        link_id, upstream = sides[node_id]
        ends = [location, _point(model, node_id)] if upstream else [_point(model, node_id), location]
        model.link.df.loc[link_id, "from_node_id" if upstream else "to_node_id"] = junction.node_id
        model.link.df.loc[[link_id], "geometry"] = gpd.GeoSeries([LineString(ends)], index=[link_id], crs=links.crs)
    # prune the Junctions the nodes left once all their links are moved, as pruning merges and removes links
    for node_id in others:
        prune_junction(model, routes.next_node(*sides[node_id]))
    after = simplified_links(model)
    assert after == before, f"Combining nodes {sorted(sides)} changed the links {sorted(after ^ before)[:10]}"
    return junction.node_id


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
        self,
        lines: typing.Iterable[shapely.Geometry],
        routes: typing.Iterable[list[Coord]] | None = None,
        points: typing.Collection[Coord] = (),
    ) -> None:
        """Take the direction of the segments of `lines`, only of those along `routes` if given.

        :param points: points inserted into the routes where they lie on a segment, see `_insert_points`, which split
            the segments of the lines in the same way
        """
        geometries = shapely.force_2d(np.asarray(list(lines), dtype=object))
        xy, index = shapely.get_coordinates(geometries, return_index=True)
        same = index[1:] == index[:-1]
        needed = None
        if routes is not None:
            needed = {pair for route in routes for a, b in itertools.pairwise(route) for pair in ((a, b), (b, a))}
        on_routes = None if needed is None else {c for pair in needed for c in pair}
        # the segments digitized from their first to their second coordinate
        self.segments: set[tuple[Coord, Coord]] = set()
        for a, b in zip(map(tuple, xy[:-1][same].tolist()), map(tuple, xy[1:][same].tolist()), strict=True):
            if on_routes is not None and (a not in on_routes or b not in on_routes):
                continue
            for c, d in itertools.pairwise(_insert_points([a, b], points) if points else [a, b]):
                if needed is None or (c, d) in needed:
                    self.segments.add((c, d))
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
    reuse: typing.Mapping[Coord, int] | None = None,
) -> tuple[list[int], list[int]]:
    """Connect the nodes reached by the Basin tree of `basin_id` along their routes, with Junctions where they split.

    Inflows and outflows share one tree, see `Routes`. The links to the nodes reached keep their ids and direction.
    The other links point towards the Basin or away from it, see `towards_basin`, given the nodes beyond them.

    :param ends: per key, the node reached and its link, which still exists
    :param coords: per key, the coordinates of the route from the Basin node to the node reached
    :param sides: per key, the nodes beyond the link that flow into the Basin, and out of it, see `Routes.subtree`
    :param capacity: the `drainage_capacity` of the model
    :param digitized: the lines the routes follow, see `towards_basin`
    :param reuse: per location, a Junction without links that is the Junction of the tree there, which must be one
    :return: the ids of the links that changed or are new, and of the new Junctions
    """
    reuse = {} if reuse is None else reuse
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
                    location = line_coords[-1]
                    junctions[tree_node[1]] = (
                        reuse[location]
                        if location in reuse
                        else model.junction.add(Node(geometry=Point(location))).node_id
                    )
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
    unused = set(reuse.values()) - set(junctions.values())
    assert not unused, f"The tree of Basin {basin_id} has no Junction at the location of Junctions {sorted(unused)}"
    new = [junction_id for junction_id in junctions.values() if junction_id not in set(reuse.values())]
    if "meta_waterbeheerder" in model.node.df.columns:
        authority = typing.cast(str, model.node.df.at[basin_id, "meta_waterbeheerder"])
        model.node.df.loc[new, "meta_waterbeheerder"] = authority
    return changed, new


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
    `ribasim_nl.rwzi.attach_rwzi_junctions`. `merge_basin_trees` merges the trees through the RWZI Junctions that are
    attached to a route.
    """
    links = model.link.df
    fixed = set(links.loc[links["link_type"] != "flow", ["from_node_id", "to_node_id"]].to_numpy().ravel().tolist())
    return fixed | rwzi_junctions(model)


def rwzi_junctions(model: Model, attached: bool | None = None) -> set[int]:
    """The RWZI Junctions, see `ribasim_nl.rwzi.attach_rwzi_junctions`, only those attached to a route or not.

    An RWZI Junction that is attached splits a flow link, so it has more flow links than the one from its RWZI and the
    one to its Basin.
    """
    nodes = model.node.df
    if "meta_rwzi_code" not in nodes.columns:
        return set()
    junctions = nodes.index[(nodes["node_type"] == "Junction") & nodes["meta_rwzi_code"].notna()]
    if attached is None:
        return set(junctions.tolist())
    flow = model.link.df[model.link.df["link_type"] == "flow"]
    degree = pd.concat([flow["from_node_id"], flow["to_node_id"]]).value_counts()
    is_attached = degree.reindex(junctions, fill_value=0).to_numpy() > 2
    return set(junctions[is_attached == attached].tolist())


def _insert_points(coords: list[Coord], points: typing.Collection[Coord], tolerance: float = 1e-6) -> list[Coord]:
    """Insert the points that lie on a segment of a route, strictly between its coordinates, into that segment."""
    result = [coords[0]]
    for a, b in itertools.pairwise(coords):
        dx, dy = b[0] - a[0], b[1] - a[1]
        length2 = dx * dx + dy * dy
        on = []
        for p in points:
            if length2 == 0.0 or p in (a, b):
                continue
            t = ((p[0] - a[0]) * dx + (p[1] - a[1]) * dy) / length2
            if 0.0 < t < 1.0 and np.hypot(a[0] + t * dx - p[0], a[1] + t * dy - p[1]) <= tolerance:
                on.append((t, p))
        result.extend(p for _, p in sorted(on))
        result.append(b)
    return result


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


# Maximum distance between a node and the routes of its Basin to branch its link off them [m], like the distance within
# which a node is attached to the network, see `ribasim_nl.network_snapping.route_basin_links`
MAX_BRANCH_DISTANCE = 100.0


def branch_off_routes(
    coords: dict[int, list[Coord]],
    branch: typing.Collection[int],
    max_distance: float = MAX_BRANCH_DISTANCE,
) -> dict[int, list[Coord]]:
    """Let routes from the same point follow the other routes up to their point nearest to its end, and go straight on.

    This is for the routes of links that are added after the links of a Basin are routed, such as those from another
    model: routed along other lines, they run beside the routes of the Basin without a point in common. The routes
    whose ends are nearest to the other routes branch off first, so the others can branch off them, see
    `attach_to_tree`.

    A route is kept if its end is more than `max_distance` from the other routes. Routes that are a straight line do
    not follow water, so no route branches off them.

    :param coords: per key, the coordinates of a route
    :param branch: keys of the routes to branch off the others
    :param max_distance: maximum distance between the end of a route and the other routes
    :return: the routes, of which the others have a point where a route branches off
    """
    tree = {key: route for key, route in coords.items() if key not in branch and len(route) > 2}
    if not tree:
        return coords
    lines = shapely.union_all([LineString(route) for route in tree.values()])
    near = {key: coords[key][-1] for key in sorted(branch) if lines.distance(Point(coords[key][-1])) <= max_distance}
    if not near:
        return coords
    return coords | attach_to_tree(tree, next(iter(tree.values()))[0], near)


def merge_basin_trees(
    model: Model,
    read_lines: LineReader | None = None,
    branch: typing.Collection[int] = (),
    max_branch_distance: float = MAX_BRANCH_DISTANCE,
) -> list[int]:
    """Give each Basin one Junction tree for its inflows and outflows, along the current routes of its links.

    The routes of the links of a Basin are followed through the Junctions of its Basin trees, see
    `Routes`, up to the `fixed_junctions`, and joined where they share
    their coordinates from the Basin node on, see `build_basin_tree`. The links to `branch` first branch off the other
    routes of their Basin, see `branch_off_routes`. A Basin whose tree would not change is left as it is. Each node
    keeps exchanging water with the same nodes, which is asserted.

    The routes are also followed through the RWZI Junctions that are attached to a route, see `rwzi_junctions`, which
    stay where they are, with their code. The routes that run along the link that such a Junction split get its
    location as well, so that they stay joined beyond it, instead of running side by side from there.

    :param read_lines: reads the lines that the routes follow within an area, to follow the direction in which they
        are digitized, see `towards_basin`
    :param branch: links between a node and its Basin, or the tree of its Basin, to branch off the routes of that Basin
    :param max_branch_distance: maximum distance between such a node and these routes
    :return: the new Junctions
    """
    before = simplified_links(model)
    # the trees of different Basins do not share links, so changing one leaves the routes of the others as they are
    routes = Routes(model)
    attached = rwzi_junctions(model, attached=True)
    fixed = fixed_junctions(model) - attached
    capacity = drainage_capacity(model)
    nodes = model.node.df
    # first find the trees that change, to read the lines around them at once
    changes = []
    for basin_id in sorted(nodes.index[nodes["node_type"] == "Basin"].tolist()):
        # an RWZI Junction at which the routes no longer split, as a route that passed it now branches off before it,
        # stays fixed, see `branch_off_routes`
        basin_fixed = set(fixed)
        while True:
            junctions, tree_link_ids, reached = junction_tree(routes, basin_id, basin_fixed)
            if len(reached) < 2:
                break
            paths = routes.paths(basin_id)
            ends = [(node_id, link_id) for link_id, node_id in reached.items()]
            origin = _coord(model, basin_id)
            kept = {_coord(model, junction_id): junction_id for junction_id in sorted(junctions & attached)}
            assert len(kept) == len(junctions & attached), f"RWZI Junctions of Basin {basin_id} at the same location"
            coords = {}
            for key, (node_id, link_id) in enumerate(ends):
                parts = route_parts(model, basin_id, paths[link_id])
                route = _dedupe([origin, *(c for part in parts for c in _coords(part))])
                route = route if len(route) >= 2 else [origin, _coord(model, node_id)]
                coords[key] = _insert_points(route, kept.keys()) if kept else route
            branched = branch_off_routes(
                coords, {key for key, (_, link_id) in enumerate(ends) if link_id in branch}, max_branch_distance
            )
            splits = {line[-1] for _, end, line in tree_links(branched) if end != "root" and end[0] == "junction"}
            unsplit = {junction_id for location, junction_id in kept.items() if location not in splits}
            if not unsplit:
                break
            basin_fixed |= unsplit
        if len(reached) < 2:
            continue
        # the tree as the nodes reached beyond each of its links between the Basin and Junctions
        current = {frozenset(k for k, (_, i) in enumerate(ends) if link_id in paths[i]) for link_id in tree_link_ids}
        if branched == coords and _tree_splits(tree_links(coords)) == current:
            continue
        sides = {key: routes.subtree(link_id) for key, (_, link_id) in enumerate(ends)}
        changes.append((basin_id, junctions, tree_link_ids, ends, branched, sides, kept))

    digitized = None
    if read_lines is not None and changes:
        area = shapely.union_all(
            [shapely.MultiPoint([c for route in change[4].values() for c in route]).envelope for change in changes]
        )
        routes_coords = [route for change in changes for route in change[4].values()]
        points = [location for change in changes for location in change[6]]
        digitized = Digitized(read_lines(shapely.buffer(area, 1.0)), routes_coords, points)
    new_junctions: list[int] = []
    for basin_id, junctions, tree_link_ids, ends, coords, sides, kept in changes:
        model.remove_links(tree_link_ids)
        for junction_id in sorted(junctions - set(kept.values())):
            model.remove_node(junction_id)
        new_junctions.extend(build_basin_tree(model, basin_id, ends, coords, sides, capacity, digitized, kept)[1])
    after = simplified_links(model)
    assert after == before, f"Merging the Basin trees changed the links {sorted(after ^ before)[:10]}"
    return new_junctions


def rebuild_junction_trees(
    model: Model,
    basin_ids: typing.Iterable[int],
    waterways: gpd.GeoDataFrame,
    tolerance: float = 10.0,
    water: gpd.GeoDataFrame | None = None,
    routing_areas: typing.Mapping[int, shapely.Geometry] | None = None,
) -> tuple[list[int], list[int]]:
    """Route the flow links of Basins along the waterways, with one Junction tree per Basin, split where they split.

    The Junctions of the Basin trees of a Basin, see `junction_tree`, are removed, except the `fixed_junctions`. The
    links to the nodes they reach are routed from the Basin node along the shortest paths of one network, see
    `ribasim_nl.network_snapping.route_basin_links`, such that its inflows and outflows share one tree, see
    `build_basin_tree`. With open `water`, such as a lake, the waterways are connected over it, and nodes far from
    them are routed over it, straight from the nearest point of the waterways from which that is possible. The others
    branch off straight from the nearest point of these routes, see `attach_to_tree`. Each node keeps exchanging water
    with the same nodes, which is asserted, and the links to the nodes reached keep their link ids. Nodes are not moved
    and links are not offset, see `ribasim_nl.layout.tidy_layout`.

    :param model: Ribasim model
    :param basin_ids: Basins to rebuild the Junctions of
    :param waterways: waterways with a boolean "main-route" column, see `ribasim_nl.network_snapping.snap_to_network`
    :param tolerance: buffer around the basin area within which waterways are used, defaults to 10.0
    :param water: open water, over which the waterways are connected and nodes far from them are routed to them, see
        `ribasim_nl.network_snapping.route_basin_links`, defaults to None. The waterways on it, such as fairways, are
        not digitized in the direction of the flow, so they do not set the direction of links, see `towards_basin`.
    :param routing_areas: per Basin, an area beyond its basin area in which the waterways are used as well, for
        connector nodes that are connected to it over a long distance, defaults to None

    :return: the ids of the links and of the Junctions that are new or changed
    """
    assert model.basin.area.df is not None
    routing_areas = {} if routing_areas is None else routing_areas
    before = simplified_links(model)
    routes = Routes(model)
    fixed = fixed_junctions(model)
    capacity = drainage_capacity(model)
    areas = model.basin.area.df.dissolve(by="node_id").geometry
    waterway_geometries = shapely.force_2d(waterways.geometry.to_numpy())
    waterway_tree = shapely.STRtree(waterway_geometries)
    main_route = waterways["main-route"].to_numpy(dtype=bool)
    water_geometries = np.array([]) if water is None else shapely.force_2d(water.geometry.to_numpy())
    water_tree = shapely.STRtree(water_geometries)

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
        if basin_id in routing_areas:
            area = area.union(routing_areas[basin_id])
        idx, lines = lines_in(waterway_geometries, waterway_tree, area)
        basin_water = shapely.intersection(
            shapely.union_all(water_geometries[water_tree.query(area, predicate="intersects")]), area
        )
        _, routed = route_basin_links(
            basin_point,
            {key: _point(model, node_id) for key, (node_id, _) in enumerate(ends)},
            list(lines),
            list(main_route[idx]),
            water=None if basin_water.is_empty else basin_water,
            area=area,
            open_water=True,
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
        digitized = Digitized(lines, coords.values()) if water is None else None
        changed_links, junction_ids = build_basin_tree(model, basin_id, ends, coords, sides, capacity, digitized)
        changed.extend(changed_links)
        new_junctions.extend(junction_ids)

    new_links = sorted(set(model.link.df.index) - old_link_ids)
    after = simplified_links(model)
    assert after == before, f"Rebuilding the Junctions changed the links {sorted(after ^ before)[:10]}"
    return sorted({*changed, *new_links} & set(model.link.df.index)), new_junctions


# The lakes in which the network of Rijkswaterstaat has a few straight lines between its Basins and structures, rather
# than the waterways of OpenStreetMap, see pipeline/rijkswaterstaat/3_netwerk.py
LAKES = ("IJsselmeer", "Markermeer", "IJmeer", "Gouwzee")


def rebuild_rws_junction_trees(
    model: Model,
    basin_ids: typing.Collection[int],
    top10nl_path: pathlib.Path,
    fairway_path: pathlib.Path,
    main_route: gpd.GeoDataFrame,
    tolerance: float = 250.0,
    routing_areas: typing.Mapping[int, shapely.Geometry] | None = None,
    lake_hydro_objects: typing.Sequence[pathlib.Path] = (),
) -> tuple[list[int], list[int]]:
    """Rebuild the Junctions of Rijkswaterstaat Basins along the waterways and a main route, see `rebuild_junction_trees`.

    This is used for the hws model and again after coupling, such that its Basins keep their routes. Its connector
    nodes are often outside the area of their Basin, so waterways up to `tolerance` around it are used. These are the
    Top10NL water lines, except for the Basins of the `LAKES`, named after them. These follow the fairways, and cross
    their own water to the nodes on their shores, see `ribasim_nl.network_snapping.read_lake_waterways`.

    :param model: Ribasim model
    :param basin_ids: Basins to rebuild the Junctions of
    :param top10nl_path: Top10NL GeoPackage
    :param fairway_path: GeoPackage with the fairways of OpenStreetMap
    :param main_route: lines of the main route, see `ribasim_nl.rws_kunstwerken.main_route`
    :param tolerance: buffer around the basin area within which waterways are used, defaults to 250.0
    :param routing_areas: per Basin, an area beyond its basin area in which the waterways are used as well, see
        `rebuild_junction_trees`, defaults to None
    :param lake_hydro_objects: network.gpkg files of the water authorities around the lakes, whose hydro-objects on the
        water of a lake are its waterways too, such as the channels of harbours, see `read_lines_on_water`. Nodes in a
        harbour then join the lake along its channel, rather than straight from the nearest fairway across the land.

    :return: the ids of the links and of the Junctions that are new or changed
    """
    assert model.basin.area.df is not None
    routing_areas = {} if routing_areas is None else routing_areas
    is_lake = model.node.df.loc[sorted(basin_ids), "name"].isin(LAKES)
    changed: list[int] = []
    junctions: list[int] = []
    for lake in (False, True):
        ids = is_lake.index[is_lake == lake].tolist()
        areas = model.basin.area.df[model.basin.area.df["node_id"].isin(ids)]
        if areas.empty:
            continue
        extra = [routing_areas[i] for i in ids if i in routing_areas]
        mask = shapely.union_all([*areas.geometry.buffer(tolerance).to_numpy(), *extra])
        if lake:
            waterways = read_lake_waterways(fairway_path, mask, main_route)
            if lake_hydro_objects:
                on_lakes = read_lines_on_water(lake_hydro_objects, shapely.union_all(areas.geometry.to_numpy()))
                waterways = gpd.GeoDataFrame(pd.concat([waterways, on_lakes], ignore_index=True), crs=waterways.crs)
            changed_links, new_junctions = rebuild_junction_trees(
                model, ids, waterways, tolerance, water=areas, routing_areas=routing_areas
            )
        else:
            waterways = read_main_waterways(top10nl_path, mask, main_route)
            changed_links, new_junctions = rebuild_junction_trees(
                model, ids, waterways, tolerance, routing_areas=routing_areas
            )
        changed.extend(changed_links)
        junctions.extend(new_junctions)
    return sorted(set(changed) & set(model.link.df.index)), junctions
