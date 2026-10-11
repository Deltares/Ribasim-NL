"""Route link geometries of a Ribasim-scheme along the network of hydro-objects.

Nodes are never moved: connector nodes stay at the location of the structure they represent, and Basin nodes stay
where they are. Only the geometries of the flow links between a Basin and a connector node are changed. Such a link is
drawn as a straight line from the connector node to the nearest hydro-object within the basin area, followed by the
shortest route along the hydro-objects to the point nearest to the Basin node, and a straight line to the Basin node.
Links that share a route can afterwards be merged with `ribasim_nl.junctions.junctionify`.

This relies on the network of hydro-objects written by the network-based profile-generation:
src/ribasim_nl/ribasim_nl/profiles/*.py.
"""

import itertools
import logging
import pathlib
import typing
from collections import defaultdict

import geopandas as gpd
import networkx as nx
import numpy as np
import shapely
import shapely.affinity
import tqdm
from ribasim_nl.link_geometries import MAX_STRAIGHT_LINE_RATIO, accept_length

from ribasim_nl import Model

LOG = logging.getLogger(__name__)

CONNECTOR_NODE_TYPES = "ManningResistance", "Outlet", "Pump"

type Coord = tuple[float, float]


def read_hydro_objects(profiles_path: pathlib.Path) -> gpd.GeoDataFrame:
    """Read the network of hydro-objects written by the profile-generation."""
    fn = profiles_path / "network.gpkg"
    if not fn.exists():
        msg = f"File not found: {fn}\nRegenerate the profiles to create {fn}"
        raise FileNotFoundError(msg)
    hydro_objects = gpd.read_file(fn, layer="hydro_objects")
    assert (hydro_objects.geom_type == "LineString").all(), "Hydro-objects must be single-part LineStrings"
    return hydro_objects


def _split_coords(coords: list[Coord], distances: list[float]) -> list[list[Coord]]:
    """Split a line at sorted distances along it, strictly between its ends.

    :return: the coordinates of the pieces, of which consecutive pieces share their end and start coordinate
    """
    line = shapely.LineString(coords)
    bounds = [0.0, *distances, line.length]
    points = [coords[0], *((p.x, p.y) for p in (line.interpolate(d) for d in distances)), coords[-1]]
    pieces = []
    for start, end, a, b in zip(bounds[:-1], bounds[1:], points[:-1], points[1:], strict=True):
        piece = shapely.ops.substring(line, start, end)
        inner = [(x, y) for x, y in piece.coords][1:-1] if piece.geom_type == "LineString" else []
        pieces.append([a, *inner, b])
    return pieces


class _BasinNetwork:
    """Graph of the hydro-objects within a single basin area, onto which points can be attached.

    Graph nodes are coordinates, and each edge stores its coordinates and whether it is part of the main route.
    """

    def __init__(self, lines: list[shapely.LineString], main_route: list[bool]) -> None:
        self.graph = nx.Graph()
        for line, main in zip(lines, main_route, strict=True):
            coords: list[Coord] = [(x, y) for x, y in line.coords]
            u, v = coords[0], coords[-1]
            # skip closed loops, and of parallel hydro-objects between the same points keep only the shortest
            if u == v or (self.graph.has_edge(u, v) and self.graph[u][v]["weight"] <= line.length):
                continue
            self.graph.add_edge(u, v, weight=line.length, coords=coords, main=bool(main))
        self._index()

    def _index(self) -> None:
        """Index the edges, their geometry, main route flag and connected component."""
        self.edges: list[tuple[Coord, Coord]] = list(self.graph.edges)
        self.geometries = np.array([shapely.LineString(self.graph.edges[e]["coords"]) for e in self.edges])
        self.tree = shapely.STRtree(self.geometries)
        self.main = np.array([self.graph.edges[e]["main"] for e in self.edges], dtype=bool)
        node_component = {}
        for i, nodes in enumerate(nx.connected_components(self.graph)):
            node_component.update(dict.fromkeys(nodes, i))
        self.component = np.array([node_component[u] for u, _ in self.edges], dtype=int)

    def nearest_edge(self, point: shapely.Point, candidates: np.ndarray) -> int:
        """Index of the candidate edge nearest to a point."""
        return int(candidates[np.argmin(shapely.distance(point, self.geometries[candidates]))])

    def split(self, points: dict[typing.Hashable, tuple[shapely.Point, int]], eps: float = 1e-3) -> dict:
        """Attach points to the graph, splitting the edges they are projected on.

        :param points: per key, the point and the index of the edge to project it on
        :param eps: projections within this distance of an edge end attach to that end
        :return: per key, the graph node the point is attached to
        """
        by_edge: dict[int, list[tuple[float, typing.Hashable]]] = defaultdict(list)
        for key, (point, edge) in points.items():
            by_edge[edge].append((float(self.geometries[edge].project(point)), key))

        attached = {}
        for edge, projections in by_edge.items():
            u, v = self.edges[edge]
            data = self.graph.edges[u, v]
            coords = data["coords"]
            length = self.geometries[edge].length
            inner = sorted({d for d, _ in projections if eps < d < length - eps})
            pieces = _split_coords(coords, inner)
            node_at = dict(zip(inner, (piece[0] for piece in pieces[1:]), strict=True))
            for d, key in projections:
                attached[key] = coords[0] if d <= eps else coords[-1] if d >= length - eps else node_at[d]
            if inner:
                self.graph.remove_edge(u, v)
                # pieces keep the weight per unit length of the edge
                factor = data["weight"] / length
                for piece in pieces:
                    weight = factor * shapely.LineString(piece).length
                    self.graph.add_edge(piece[0], piece[-1], weight=weight, coords=piece, main=data["main"])
        self._index()
        return attached

    def bridge_gaps(self, max_gap: float, penalty: float = 2.0) -> None:
        """Connect dangling ends to the nearest edge of another connected component within `max_gap`.

        Hydro-objects are often not connected where a culvert is missing in the data. The weight of a bridge is its
        length times `penalty`, such that routes prefer the hydro-objects.
        """
        component = dict(zip((u for u, _ in self.edges), self.component, strict=True))
        component.update(zip((v for _, v in self.edges), self.component, strict=True))
        requests: dict[typing.Hashable, tuple[shapely.Point, int]] = {}
        for node, degree in self.graph.degree():
            if degree != 1:
                continue
            point = shapely.Point(node)
            nearby = self.tree.query(point, predicate="dwithin", distance=max_gap)
            nearby = nearby[self.component[nearby] != component[node]]
            if len(nearby) > 0:
                requests[node] = (point, self.nearest_edge(point, nearby))
        for node, target in self.split(requests).items():
            if node != target:
                gap = [node, target]
                weight = penalty * shapely.LineString(gap).length
                self.graph.add_edge(node, target, weight=weight, coords=gap, main=False)
        self._index()

    def connect_components(self, penalty: float = 2.0) -> None:
        """Connect all connected components by the shortest straight bridges between them (a minimum spanning tree).

        The weight of a bridge is its length times `penalty`, such that routes prefer the hydro-objects.
        """
        components = np.unique(self.component)
        if len(components) < 2:
            return
        merged = [shapely.union_all(self.geometries[self.component == c]) for c in components]
        tree = nx.Graph()
        for i, j in itertools.combinations(range(len(components)), 2):
            tree.add_edge(i, j, weight=shapely.distance(merged[i], merged[j]))
        requests: dict[typing.Hashable, tuple[shapely.Point, int]] = {}
        for i, j in nx.minimum_spanning_edges(tree, data=False):
            line = shapely.shortest_line(merged[i], merged[j])
            for k, c, point in ((0, components[i], line.coords[0]), (1, components[j], line.coords[-1])):
                point = shapely.Point(point)
                requests[(i, j, k)] = (point, self.nearest_edge(point, np.flatnonzero(self.component == c)))
        attached = self.split(requests)
        for i, j in nx.minimum_spanning_edges(tree, data=False):
            a, b = attached[(i, j, 0)], attached[(i, j, 1)]
            gap = [a, b]
            self.graph.add_edge(a, b, weight=penalty * shapely.LineString(gap).length, coords=gap, main=False)
        self._index()

    def path_coords(self, path: list[Coord]) -> list[Coord]:
        """Coordinates along a path of graph nodes."""
        coords = [path[0]]
        for a, b in itertools.pairwise(path):
            edge = self.graph[a][b]["coords"]
            coords.extend((edge if edge[0] == a else edge[::-1])[1:])
        return coords


def _dedupe(coords: list[Coord]) -> list[Coord]:
    """Remove consecutive duplicate coordinates."""
    return [c for i, c in enumerate(coords) if i == 0 or c != coords[i - 1]]


def route_basin_links(
    basin_point: shapely.Point,
    connectors: dict[int, shapely.Point],
    lines: list[shapely.LineString],
    main_route: list[bool],
    *,
    basin_area: shapely.Polygon | shapely.MultiPolygon | None = None,
    max_gap: float = 100.0,
    max_attach_distance: float = 100.0,
    max_straight_line_ratio: float = MAX_STRAIGHT_LINE_RATIO,
) -> tuple[shapely.Point, dict[int, list[Coord]]]:
    """Route the links between a Basin node and its connector nodes along the hydro-objects in the basin.

    Dangling ends of the hydro-objects are connected to other parts of the network within `max_gap`, and the remaining
    parts are connected by the shortest bridges between them. The Basin node is attached to its nearest main route
    hydro-object (if any), and each connector node to its nearest hydro-object. If `basin_area` is given and contains the attachment point of the Basin
    node, and at least half of the links can be routed from there, the Basin node is moved there. A connector is not
    routed if it is farther than `max_attach_distance` from the network or farther from it than from the Basin node, or
    if the route is not less than `max_straight_line_ratio` times the straight line, as in
    `ribasim_nl.link_geometries.fix_link_geometries`.

    :param basin_point: location of the Basin node
    :param connectors: locations of the connector nodes, by key
    :param lines: hydro-objects within the basin area
    :param main_route: per hydro-object, whether it is part of the main route
    :param basin_area: area within which the Basin node may be moved onto the network, defaults to None (not moved)
    :param max_gap: maximum gap between hydro-objects to bridge
    :param max_attach_distance: maximum distance between a connector node and the network
    :param max_straight_line_ratio: maximum ratio between route length and straight line length

    :return: the (possibly moved) location of the Basin node, and per routed connector key the coordinates from the
        Basin node to the connector node
    """
    assert len(lines) == len(main_route)
    assert -1 not in connectors
    network = _BasinNetwork(lines, main_route)
    if not connectors or len(network.edges) == 0:
        return basin_point, {}
    network.bridge_gaps(max_gap)
    network.connect_components()
    candidates = np.arange(len(network.edges))

    # the Basin node is attached to the main route, if available
    main = candidates[network.main[candidates]]
    to_attach: dict[typing.Hashable, tuple[shapely.Point, int]] = {
        -1: (basin_point, network.nearest_edge(basin_point, main if len(main) > 0 else candidates))
    }
    for key, point in connectors.items():
        edge = network.nearest_edge(point, candidates)
        distance = network.geometries[edge].distance(point)
        if distance <= max_attach_distance and distance < point.distance(basin_point):
            to_attach[key] = (point, edge)
    attached = network.split(to_attach)
    anchor = attached.pop(-1)

    paths = nx.single_source_dijkstra_path(network.graph, anchor, weight="weight")

    def route(location: shapely.Point) -> dict[int, list[Coord]]:
        routes = {}
        for key, node in attached.items():
            point = connectors[key]
            coords = _dedupe([(location.x, location.y), *network.path_coords(paths[node]), (point.x, point.y)])
            if accept_length(shapely.LineString(coords), location, point, max_straight_line_ratio):
                routes[key] = coords
        return routes

    # move the Basin node onto the network if that is within its area, not on a connector node, and at least half of
    # the links can be routed, such that it represents the network
    anchor_point = shapely.Point(anchor)
    if (
        basin_area is not None
        and basin_area.contains(anchor_point)
        and min(anchor_point.distance(p) for p in connectors.values()) > 1.0
    ):
        routes = route(anchor_point)
        if 2 * len(routes) >= len(connectors):
            return anchor_point, routes
    return basin_point, route(basin_point)


def straight_link_geometries(model: Model, tolerance: float = 0.01) -> Model:
    """Make link geometries start at their from-node and end at their to-node.

    Link ends within `tolerance` of their node are moved onto it, other link geometries are replaced by straight lines.

    :param model: Ribasim model
    :param tolerance: maximum distance between a link end and its node to keep the link geometry

    :return: updated Ribasim model
    """
    assert model.node.df is not None
    assert model.link.df is not None
    link_df = model.link.df.copy()
    from_points = model.node.df.geometry.reindex(link_df["from_node_id"]).to_numpy()
    to_points = model.node.df.geometry.reindex(link_df["to_node_id"]).to_numpy()
    geometries = link_df.geometry.to_numpy()
    invalid = (shapely.distance(shapely.get_point(geometries, 0), from_points) > tolerance) | (
        shapely.distance(shapely.get_point(geometries, -1), to_points) > tolerance
    )
    LOG.info(f"Replaced {invalid.sum()} link geometries that do not connect their nodes by straight lines")
    link_df["geometry"] = [
        shapely.LineString([start, end] if straight else [start, *geometry.coords[1:-1], end])
        for geometry, start, end, straight in zip(geometries, from_points, to_points, invalid, strict=True)
    ]
    model.link.df = link_df
    return model


def _bergend_nodes(model: Model, basin_id: int) -> list[int]:
    """The ManningResistance and Basin nodes of the bergend part connected to a doorgaand Basin node."""
    assert model.link.df is not None
    assert model.node.df is not None
    if "meta_categorie" not in model.node.df.columns:
        return []
    links = model.link.df[model.link.df["link_type"] == "flow"]
    category = model.node.df["meta_categorie"]
    manning_ids = links.loc[links["to_node_id"] == basin_id, "from_node_id"]
    manning_ids = manning_ids[model.node.df.loc[manning_ids, "node_type"].to_numpy() == "ManningResistance"]
    upstream = links[links["to_node_id"].isin(manning_ids)].set_index("to_node_id")["from_node_id"]
    upstream = upstream[category.reindex(upstream).to_numpy() == "bergend"]
    return [*map(int, upstream.index), *map(int, upstream)]


def snap_to_network(
    model: Model, hydro_objects: gpd.GeoDataFrame, *, tolerance: float = 10.0, move_basins: bool = True
) -> Model:
    """Route flow links between a Basin and a connector node along the hydro-objects, and move Basin nodes onto them.

    Connector nodes are never moved, such that they stay at the location of the structure they represent. Basin nodes
    are moved onto the main route nearest to them if `move_basins` is True, see `route_basin_links`, and the nodes of
    their bergend part along with them. Links that cannot be routed, links to bergend basins and links to boundary
    nodes keep their geometry, unless it does not connect their nodes, see `straight_link_geometries`.

    :param model: Ribasim model
    :param hydro_objects: hydro-objects, with a boolean "main-route" column
    :param tolerance: buffer around the basin area within which hydro-objects are used, defaults to 10.0
    :param move_basins: move non-bergend Basin nodes onto the network, defaults to True

    :return: updated Ribasim model
    """
    model = straight_link_geometries(model)
    assert model.node.df is not None
    assert model.link.df is not None
    assert model.basin.area.df is not None
    nodes = model.node.df
    links = model.link.df

    # flow links between a non-bergend Basin and a connector node
    node_type = nodes["node_type"]
    from_type = links["from_node_id"].map(node_type)
    to_type = links["to_node_id"].map(node_type)
    to_basin = from_type.isin(CONNECTOR_NODE_TYPES) & (to_type == "Basin")
    from_basin = (from_type == "Basin") & to_type.isin(CONNECTOR_NODE_TYPES)
    selected = links[(links["link_type"] == "flow") & (to_basin | from_basin)].copy()
    selected["basin_id"] = selected["to_node_id"].where(to_basin[selected.index], selected["from_node_id"])
    selected["connector_id"] = selected["from_node_id"].where(to_basin[selected.index], selected["to_node_id"])
    if "meta_categorie" in nodes.columns:
        bergend = nodes["meta_categorie"] == "bergend"
        selected = selected[~selected["basin_id"].map(bergend) & ~selected["connector_id"].map(bergend)]
    if "meta_categorie" in selected.columns:
        selected = selected[selected["meta_categorie"] != "bergend"]

    areas = model.basin.area.df.dissolve(by="node_id")["geometry"]
    ho_geometries = shapely.force_2d(hydro_objects.geometry.to_numpy())
    tree = shapely.STRtree(ho_geometries)
    main_route = hydro_objects["main-route"].to_numpy(dtype=bool)

    new_geometries: dict[int, shapely.LineString] = {}
    new_locations: dict[int, shapely.Point] = {}
    for basin_id, group in tqdm.tqdm(selected.groupby("basin_id"), "Routing links"):
        if basin_id not in areas.index:
            LOG.debug(f"No basin area found for Basin #{basin_id}")
            continue
        # hydro-objects within the basin area, clipped to keep those crossing its boundary connected
        area = areas[basin_id].buffer(tolerance)
        idx = tree.query(area, predicate="intersects")
        parts, part_index = shapely.get_parts(shapely.intersection(ho_geometries[idx], area), return_index=True)
        is_line = shapely.get_type_id(parts) == shapely.GeometryType.LINESTRING
        basin_point = typing.cast(shapely.Point, nodes.at[basin_id, "geometry"])
        location, routes = route_basin_links(
            basin_point,
            {link_id: nodes.at[cid, "geometry"] for link_id, cid in group["connector_id"].items()},
            list(parts[is_line]),
            list(main_route[idx][part_index[is_line]]),
            basin_area=areas[basin_id] if move_basins else None,
        )
        if not location.equals(basin_point):
            new_locations[int(basin_id)] = location
        for link_id, coords in routes.items():
            if to_basin[link_id]:
                coords = coords[::-1]
            new_geometries[link_id] = shapely.LineString(coords)

    LOG.info(f"Routed {len(new_geometries)} of {len(selected)} links along the hydro-objects")
    link_df = model.link.df.copy()
    link_df.loc[list(new_geometries), "geometry"] = gpd.GeoSeries(new_geometries, crs=link_df.crs)
    model.link.df = link_df

    LOG.info(f"Moved {len(new_locations)} Basin nodes onto the hydro-objects")
    node_df = model.node.df.copy()
    for basin_id, location in new_locations.items():
        old = node_df.at[basin_id, "geometry"]
        dx, dy = location.x - old.x, location.y - old.y
        node_df.at[basin_id, "geometry"] = location
        for node_id in _bergend_nodes(model, basin_id):
            node_df.at[node_id, "geometry"] = shapely.affinity.translate(node_df.at[node_id, "geometry"], dx, dy)
    model.node.df = node_df

    # links to moved nodes that are not routed are redrawn as straight lines
    return straight_link_geometries(model)


def snap_model(model: Model, profiles_path: pathlib.Path, *, tolerance: float = 10.0) -> Model:
    """Snap a model to the hydro-objects of the profile-generation, see `snap_to_network`."""
    assert model.node.df is not None
    node_geometry = model.node.df.geometry.copy()
    model = snap_to_network(model, read_hydro_objects(profiles_path), tolerance=tolerance)
    moved = ~model.node.df.geometry.geom_equals_exact(node_geometry, tolerance=0)
    moved_types = set(model.node.df.loc[moved, "node_type"])
    assert moved_types <= {"Basin", "ManningResistance"}, f"Only Basin nodes may move, not {moved_types}"
    return model
