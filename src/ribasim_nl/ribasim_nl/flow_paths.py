"""Routes of flow links through Junctions, and changes to them that only change the layout.

A Junction only merges or splits flow, so the flow links from a node through Junctions up to the next non-Junction
nodes form the route of that node. Ribasim replaces each pair of links into and out of a Junction by a direct link,
see `simplify_graph!` in the Ribasim core. Moving or adding Junctions on a route therefore changes how links are drawn,
not how water flows, as long as each node keeps exchanging water with the same non-Junction nodes.
"""

import typing
from collections import defaultdict, deque

import geopandas as gpd
from ribasim import Node
from shapely.geometry import LineString, Point
from shapely.ops import substring

from ribasim_nl.layout import move_nodes
from ribasim_nl.model import Model

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
