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
import pandas as pd
import rasterio.features
import rasterio.transform
import scipy.sparse
import scipy.sparse.csgraph
import shapely
import shapely.affinity
import shapely.ops
import tqdm

from ribasim_nl import Model
from ribasim_nl.link_geometries import MAX_STRAIGHT_LINE_RATIO, accept_length

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

    def add_culverts(self, culverts: list[shapely.LineString], snap: float) -> None:
        """Add culverts as edges, between the hydro-objects nearest to their ends within `snap`.

        Hydro-objects are often interrupted where a culvert passes under a road, so the culverts connect them.
        """
        requests: dict[typing.Hashable, tuple[shapely.Point, int]] = {}
        for i, culvert in enumerate(culverts):
            ends: dict[typing.Hashable, tuple[shapely.Point, int]] = {}
            for k, point in enumerate((shapely.Point(culvert.coords[0]), shapely.Point(culvert.coords[-1]))):
                nearby = self.tree.query(point, predicate="dwithin", distance=snap)
                if len(nearby) > 0:
                    ends[(i, k)] = (point, self.nearest_edge(point, nearby))
            if len(ends) == 2:
                requests |= ends
        attached = self.split(requests)
        for i, culvert in enumerate(culverts):
            if (i, 0) not in attached:
                continue
            a, b = attached[(i, 0)], attached[(i, 1)]
            if a == b or self.graph.has_edge(a, b):
                continue
            coords = _dedupe([a, *((x, y) for x, y in culvert.coords), b])
            self.graph.add_edge(a, b, weight=shapely.LineString(coords).length, coords=coords, main=False)
        self._index()

    def bridge_gaps(self, max_gap: float, water: shapely.Geometry | None, max_water_gap: float) -> None:
        """Connect dangling ends to the nearest edge of another connected component, if that is plausible.

        That is, if the gap is at most `max_gap`, such as small gaps in the digitization, or at most `max_water_gap` and
        the gap is crossed over `water`, such as hydro-objects ending at the shore of a lake.
        """
        if water is not None:
            water = shapely.buffer(water, 1.0)
            shapely.prepare(water)
        component = dict(zip((u for u, _ in self.edges), self.component, strict=True))
        component.update(zip((v for _, v in self.edges), self.component, strict=True))
        requests: dict[typing.Hashable, tuple[shapely.Point, int]] = {}
        for node, degree in self.graph.degree():
            if degree != 1:
                continue
            point = shapely.Point(node)
            reach = max_water_gap if water is not None and water.contains(point) else max_gap
            nearby = self.tree.query(point, predicate="dwithin", distance=reach)
            nearby = nearby[self.component[nearby] != component[node]]
            for edge in nearby[np.argsort(shapely.distance(point, self.geometries[nearby]))]:
                target = shapely.ops.nearest_points(point, self.geometries[edge])[1]
                if point.distance(target) <= max_gap or (
                    water is not None and water.contains(shapely.LineString([point, target]))
                ):
                    requests[node] = (point, int(edge))
                    break
        for node, target in self.split(requests).items():
            if node != target:
                gap = [node, target]
                self.graph.add_edge(node, target, weight=shapely.LineString(gap).length, coords=gap, main=False)
        self._index()

    def connect_components(self, penalty: float, max_bridge: float) -> None:
        """Connect the connected components by the shortest straight bridges between them, a minimum spanning tree.

        Only components within `max_bridge` of each other are connected.
        These bridges are a guess, so their weight is their length times `penalty`, such that routes only use them
        when the hydro-objects are not connected otherwise. They are marked with `guess=True`.
        """
        components = np.unique(self.component)
        if len(components) < 2:
            return
        merged = [shapely.union_all(self.geometries[self.component == c]) for c in components]
        tree = shapely.STRtree(merged)
        candidates = nx.Graph()
        for i, geometry in enumerate(merged):
            nearby = tree.query(geometry, predicate="dwithin", distance=max_bridge)
            nearby = nearby[nearby != i]
            for j, distance in zip(nearby, shapely.distance(geometry, np.asarray(merged)[nearby]), strict=True):
                candidates.add_edge(i, int(j), weight=distance)
        mst = list(nx.minimum_spanning_edges(candidates, data=False))
        requests: dict[typing.Hashable, tuple[shapely.Point, int]] = {}
        for i, j in mst:
            line = shapely.shortest_line(merged[i], merged[j])
            for k, c, coord in ((0, components[i], line.coords[0]), (1, components[j], line.coords[-1])):
                point = shapely.Point(coord)
                requests[(i, j, k)] = (point, self.nearest_edge(point, np.flatnonzero(self.component == c)))
        attached = self.split(requests)
        for i, j in mst:
            a, b = attached[(i, j, 0)], attached[(i, j, 1)]
            gap = [a, b]
            self.graph.add_edge(
                a, b, weight=penalty * shapely.LineString(gap).length, coords=gap, main=False, guess=True
            )
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
    culverts: list[shapely.LineString] | None = None,
    water: shapely.Geometry | None = None,
    basin_area: shapely.Polygon | shapely.MultiPolygon | None = None,
    max_gap: float = 2.0,
    max_water_gap: float = 500.0,
    culvert_snap: float = 10.0,
    guess_penalty: float | None = None,
    max_guess: float = 1000.0,
    max_attach_distance: float = 100.0,
    max_straight_line_ratio: float = MAX_STRAIGHT_LINE_RATIO,
) -> tuple[shapely.Point, dict[int, list[Coord]]]:
    """Route the links between a Basin node and its connector nodes along the hydro-objects in the basin.

    The hydro-objects are connected by the `culverts`, and dangling ends are connected to other parts of the network
    if the gap is small or crossed over `water`, see `_BasinNetwork.bridge_gaps`. Then only the longest connected part
    of the network is used. The Basin node is attached to its nearest main route hydro-object in this part (if any),
    and each connector node to its nearest hydro-object. If `basin_area` is given and contains the attachment point of
    the Basin node, and at least half of the links can be routed from there, the Basin node is moved there. A
    connector is not routed if it is farther than `max_attach_distance` from this part or farther from it than from the
    Basin node, or if the route is not less than `max_straight_line_ratio` times the straight line, as in
    `ribasim_nl.link_geometries.fix_link_geometries`.

    :param basin_point: location of the Basin node
    :param connectors: locations of the connector nodes, by key
    :param lines: hydro-objects within the basin area
    :param main_route: per hydro-object, whether it is part of the main route
    :param culverts: culverts within the basin area, defaults to None
    :param water: water surface within the basin area, defaults to None
    :param basin_area: area within which the Basin node may be moved onto the network, defaults to None (not moved)
    :param max_gap: maximum gap between hydro-objects to bridge
    :param max_water_gap: maximum gap between hydro-objects to bridge over water
    :param culvert_snap: maximum distance between a culvert end and the hydro-objects
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
    if culverts:
        network.add_culverts(culverts, culvert_snap)
    network.bridge_gaps(max_gap, water, max_water_gap)
    if guess_penalty is not None:
        network.connect_components(guess_penalty, max_guess)

    # use the longest connected part of the network, typically the main waterway
    lengths = np.bincount(network.component, weights=shapely.length(network.geometries))
    candidates = np.flatnonzero(network.component == np.argmax(lengths))

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


def route_over_water(
    basin_point: shapely.Point,
    connectors: dict[int, shapely.Point],
    water: shapely.Geometry,
    area: shapely.Geometry,
    *,
    land_cost: float = 20.0,
    max_land: float = 50.0,
    max_cells: int = 1_000_000,
    max_straight_line_ratio: float = MAX_STRAIGHT_LINE_RATIO,
) -> dict[int, list[Coord]]:
    """Route the links between a Basin node and its connector nodes over the water surface in the basin area.

    This is for basins where the hydro-objects do not cover the water, such as lakes and harbours. The route is the
    least-cost path on a raster of the area, where crossing land costs `land_cost` times as much as crossing water,
    such that it only crosses land where needed, for instance where a culvert is missing in the water surfaces. Routes
    that cross more than `max_land` of land, or are not less than `max_straight_line_ratio` times the straight line,
    are not used.

    :param basin_point: location of the Basin node
    :param connectors: locations of the connector nodes, by key
    :param water: water surface within the basin area
    :param area: the basin area, outside which routes cannot go
    :param land_cost: cost of crossing land relative to water
    :param max_land: maximum length of land crossed by a route
    :param max_cells: maximum number of raster cells, which sets the cell size
    :param max_straight_line_ratio: maximum ratio between route length and straight line length

    :return: per routed connector key, the coordinates from the Basin node to the connector node
    """
    x0, y0, x1, y1 = shapely.bounds(area)
    cell = max(10.0, np.sqrt((x1 - x0) * (y1 - y0) / max_cells))
    ncol, nrow = int(np.ceil((x1 - x0) / cell)) + 1, int(np.ceil((y1 - y0) / cell)) + 1
    transform = rasterio.transform.Affine(cell, 0.0, x0, 0.0, -cell, y0 + nrow * cell)
    inside = rasterio.features.rasterize([area], (nrow, ncol), transform=transform, all_touched=True).astype(bool)
    wet = rasterio.features.rasterize([water], (nrow, ncol), transform=transform, all_touched=True).astype(bool)
    cost = np.where(wet, 1.0, land_cost)
    wet_area = shapely.buffer(water, cell)
    shapely.prepare(wet_area)

    def cell_of(point: shapely.Point) -> int:
        row, col = rasterio.transform.rowcol(transform, point.x, point.y)
        return int(np.clip(row, 0, nrow - 1)) * ncol + int(np.clip(col, 0, ncol - 1))

    # graph of the cells within the area, with 8 neighbors
    index = np.arange(nrow * ncol).reshape(nrow, ncol)
    sources, targets, weights = [], [], []
    for dr, dc in ((0, 1), (1, 0), (1, 1), (1, -1)):
        rows = slice(max(0, -dr), nrow - max(0, dr))
        cols = slice(max(0, -dc), ncol - max(0, dc))
        a = index[rows, cols].ravel()
        b = a + dr * ncol + dc
        ok = inside.ravel()[a] & inside.ravel()[b]
        a, b = a[ok], b[ok]
        sources.append(a)
        targets.append(b)
        weights.append(np.hypot(dr, dc) * cell * (cost.ravel()[a] + cost.ravel()[b]) / 2)
    graph = scipy.sparse.coo_matrix(
        (np.concatenate(weights), (np.concatenate(sources), np.concatenate(targets))), shape=(nrow * ncol,) * 2
    ).tocsr()
    start = cell_of(basin_point)
    _, predecessors = scipy.sparse.csgraph.dijkstra(graph, directed=False, indices=start, return_predecessors=True)

    routes = {}
    for key, point in connectors.items():
        node = cell_of(point)
        if node != start and predecessors[node] < 0:
            continue
        path = [node]
        while path[-1] != start:
            path.append(int(predecessors[path[-1]]))
        rows, cols = np.divmod(np.array(path[::-1]), ncol)
        xs, ys = rasterio.transform.xy(transform, rows, cols)
        # simplify the staircase of cell centers, keeping the exact end points
        middle = shapely.simplify(shapely.LineString(np.column_stack([xs, ys])), cell) if len(path) > 1 else None
        inner: list[Coord] = [(float(x), float(y)) for x, y in middle.coords[1:-1]] if middle is not None else []
        coords = _dedupe([(basin_point.x, basin_point.y), *inner, (point.x, point.y)])
        line = shapely.LineString(coords)
        if not accept_length(line, basin_point, point, max_straight_line_ratio):
            continue
        # crossing land is a guess, only accept it over short distances, such as a road with a missing culvert
        if shapely.difference(line, wet_area).length <= max_land:
            routes[key] = coords
    return routes


def straight_link_geometries(model: Model, tolerance: float = 1.0) -> Model:
    """Make link geometries start at their from-node and end at their to-node.

    Link ends within `tolerance` of their node are moved onto it, other link geometries are replaced by straight lines.

    :param model: Ribasim model
    :param tolerance: maximum distance between a link end and its node to keep the link geometry

    :return: updated Ribasim model
    """
    assert model.node.df is not None
    assert model.link.df is not None
    link_df = model.link.df
    from_points = model.node.df.geometry.reindex(link_df["from_node_id"]).to_numpy()
    to_points = model.node.df.geometry.reindex(link_df["to_node_id"]).to_numpy()
    geometries = link_df.geometry.to_numpy()
    invalid = (shapely.distance(shapely.get_point(geometries, 0), from_points) > tolerance) | (
        shapely.distance(shapely.get_point(geometries, -1), to_points) > tolerance
    )
    LOG.info(f"Replaced {invalid.sum()} link geometries that do not connect their nodes by straight lines")
    link_df["geometry"] = gpd.GeoSeries(
        [
            shapely.LineString([start, end] if straight else [start, *geometry.coords[1:-1], end])
            for geometry, start, end, straight in zip(geometries, from_points, to_points, invalid, strict=True)
        ],
        index=link_df.index,
        crs=link_df.crs,
    )
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
    model: Model,
    hydro_objects: gpd.GeoDataFrame,
    *,
    culverts: gpd.GeoDataFrame | None = None,
    water: gpd.GeoDataFrame | None = None,
    tolerance: float = 10.0,
    move_basins: bool = True,
    route_water: bool = True,
    link_ids: list[int] | None = None,
) -> Model:
    """Route flow links between a Basin and a connector node along the hydro-objects, and move Basin nodes onto them.

    Connector nodes are never moved, such that they stay at the location of the structure they represent. Basin nodes
    are moved onto the main route nearest to them if `move_basins` is True, see `route_basin_links`, and the nodes of
    their bergend part along with them. Links that cannot be routed, links to bergend basins and links to boundary
    nodes keep their geometry, unless it does not connect their nodes, see `straight_link_geometries`.

    :param model: Ribasim model
    :param hydro_objects: hydro-objects, with a boolean "main-route" column
    :param culverts: culverts (lines) that connect hydro-objects, defaults to None
    :param water: water surfaces (polygons), across which hydro-objects may be connected, defaults to None
    :param tolerance: buffer around the basin area within which hydro-objects are used, defaults to 10.0
    :param move_basins: move non-bergend Basin nodes onto the network, defaults to True
    :param route_water: route links that cannot be routed along the hydro-objects over the water surface, see
        `route_over_water`, defaults to True
    :param link_ids: only route these links, defaults to None (all)

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
    if link_ids is not None:
        selected = selected[selected.index.isin(link_ids)]
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
    culvert_geometries = shapely.force_2d(culverts.geometry.to_numpy()) if culverts is not None else np.array([])
    culvert_tree = shapely.STRtree(culvert_geometries)
    water_geometries = shapely.force_2d(water.geometry.to_numpy()) if water is not None else np.array([])
    water_tree = shapely.STRtree(water_geometries)

    def lines_in(
        geometries: np.ndarray, tree: shapely.STRtree, area: shapely.Geometry
    ) -> tuple[np.ndarray, np.ndarray]:
        """Indices and LineString parts of the geometries clipped to an area."""
        idx = tree.query(area, predicate="intersects")
        parts, part_index = shapely.get_parts(shapely.intersection(geometries[idx], area), return_index=True)
        is_line = shapely.get_type_id(parts) == shapely.GeometryType.LINESTRING
        return idx[part_index[is_line]], parts[is_line]

    new_geometries: dict[int, shapely.LineString] = {}
    new_locations: dict[int, shapely.Point] = {}
    for basin_id, group in tqdm.tqdm(selected.groupby("basin_id"), "Routing links"):
        if basin_id not in areas.index:
            LOG.debug(f"No basin area found for Basin #{basin_id}")
            continue
        # hydro-objects within the basin area, clipped to keep those crossing its boundary connected
        area = areas[basin_id].buffer(tolerance)
        idx, lines = lines_in(ho_geometries, tree, area)
        _, basin_culverts = lines_in(culvert_geometries, culvert_tree, area)
        basin_water = shapely.intersection(
            shapely.union_all(water_geometries[water_tree.query(area, predicate="intersects")]), area
        )
        basin_point = typing.cast(shapely.Point, nodes.at[basin_id, "geometry"])
        connectors = {
            int(typing.cast(int, link_id)): typing.cast(shapely.Point, nodes.at[cid, "geometry"])
            for link_id, cid in group["connector_id"].items()
        }
        location, routes = route_basin_links(
            basin_point,
            connectors,
            list(lines),
            list(main_route[idx]),
            culverts=list(basin_culverts),
            water=None if basin_water.is_empty else basin_water,
            basin_area=areas[basin_id] if move_basins else None,
        )
        if not location.equals(basin_point):
            new_locations[int(basin_id)] = location
        unrouted = {k: p for k, p in connectors.items() if k not in routes}
        if route_water and unrouted and not basin_water.is_empty:
            routes |= route_over_water(location, unrouted, basin_water, area)
        for link_id, coords in routes.items():
            if to_basin[link_id]:
                coords = coords[::-1]
            new_geometries[link_id] = shapely.LineString(coords)

    LOG.info(f"Routed {len(new_geometries)} of {len(selected)} links along the hydro-objects")
    model.link.df.loc[list(new_geometries), "geometry"] = gpd.GeoSeries(new_geometries, crs=model.link.df.crs)

    LOG.info(f"Moved {len(new_locations)} Basin nodes onto the hydro-objects")
    moved: dict[int, shapely.Point] = {}
    for basin_id, location in new_locations.items():
        old = typing.cast(shapely.Point, nodes.at[basin_id, "geometry"])
        moved[basin_id] = location
        for node_id in _bergend_nodes(model, basin_id):
            point = typing.cast(shapely.Point, nodes.at[node_id, "geometry"])
            moved[node_id] = shapely.affinity.translate(point, location.x - old.x, location.y - old.y)
    nodes.loc[list(moved), "geometry"] = gpd.GeoSeries(moved, crs=nodes.crs)

    # links to moved nodes that are not routed are redrawn as straight lines
    return straight_link_geometries(model)


def read_waterways(top10nl_path: pathlib.Path, mask: shapely.Geometry) -> gpd.GeoDataFrame:
    """Read the Top10NL water lines within a mask, to complement the hydro-objects."""
    lines = gpd.read_file(top10nl_path, layer="top10nl_waterdeel_lijn", mask=mask).explode(ignore_index=True)
    lines = lines[lines.geom_type == "LineString"]
    return gpd.GeoDataFrame(
        {"main-route": np.zeros(len(lines), dtype=bool)},
        geometry=shapely.force_2d(lines.geometry.to_numpy()),
        crs=lines.crs,
    )


def straight_flow_link_ids(model: Model) -> list[int]:
    """The flow links with a straight geometry."""
    assert model.link.df is not None
    flow = model.link.df[model.link.df["link_type"] == "flow"]
    return flow.index[shapely.get_num_points(flow.geometry.to_numpy()) == 2].tolist()


def snap_to_waterways(
    model: Model,
    top10nl_path: pathlib.Path,
    link_ids: list[int],
    *,
    main_route: gpd.GeoDataFrame | None = None,
    tolerance: float = 10.0,
) -> Model:
    """Route links along the Top10NL water lines and over the Top10NL water surfaces, without moving nodes.

    This is a fallback for links that cannot be routed along the hydro-objects of a water authority, see
    `snap_to_network`.

    :param model: Ribasim model
    :param top10nl_path: Top10NL GeoPackage
    :param link_ids: links to route
    :param main_route: lines of the main route, such as a network of main waterways, defaults to None
    :param tolerance: buffer around the basin area within which waterways are used, defaults to 10.0

    :return: updated Ribasim model
    """
    assert model.link.df is not None
    assert model.basin.area.df is not None
    basin_ids = set(model.link.df.loc[link_ids, ["from_node_id", "to_node_id"]].to_numpy().ravel())
    areas = model.basin.area.df[model.basin.area.df["node_id"].isin(basin_ids)]
    mask = shapely.union_all(areas.geometry.buffer(tolerance).to_numpy())
    waterways = [read_waterways(top10nl_path, mask)]
    if main_route is not None:
        lines = main_route.explode(ignore_index=True)
        lines = lines[lines.geom_type == "LineString"]
        waterways.append(
            gpd.GeoDataFrame(
                {"main-route": np.ones(len(lines), dtype=bool)},
                geometry=shapely.force_2d(lines.geometry.to_numpy()),
                crs=lines.crs,
            )
        )
    water = gpd.read_file(top10nl_path, layer="top10nl_waterdeel_vlak", mask=mask)
    LOG.info(f"Routing {len(link_ids)} links along the Top10NL waterways")
    return snap_to_network(
        model,
        pd.concat(waterways, ignore_index=True),
        water=water,
        tolerance=tolerance,
        move_basins=False,
        link_ids=link_ids,
    )


def snap_model(
    model: Model,
    profiles_path: pathlib.Path,
    *,
    crossings_path: pathlib.Path | None = None,
    water_path: pathlib.Path | None = None,
    top10nl_path: pathlib.Path | None = None,
    tolerance: float = 10.0,
) -> Model:
    """Snap a model to the hydro-objects of the profile-generation, see `snap_to_network`.

    :param model: Ribasim model
    :param profiles_path: directory with the network.gpkg of the profile-generation
    :param crossings_path: GeoPackage with a duikersifonhevel layer of culverts, defaults to None
    :param water_path: GeoPackage with water surfaces, such as from the BGT, defaults to None
    :param top10nl_path: Top10NL GeoPackage, of which the water lines complement the hydro-objects, defaults to None
    :param tolerance: buffer around the basin area within which hydro-objects are used, defaults to 10.0
    """
    assert model.node.df is not None
    node_geometry = model.node.df.geometry.copy()
    hydro_objects = read_hydro_objects(profiles_path)
    if top10nl_path is not None:
        assert model.basin.area.df is not None
        x0, y0, x1, y1 = model.basin.area.df.total_bounds
        mask = shapely.box(x0, y0, x1, y1)
        hydro_objects = pd.concat([hydro_objects, read_waterways(top10nl_path, mask)], ignore_index=True)
    culverts = None if crossings_path is None else gpd.read_file(crossings_path, layer="duikersifonhevel")
    water = None if water_path is None else gpd.read_file(water_path)
    model = snap_to_network(model, hydro_objects, culverts=culverts, water=water, tolerance=tolerance)
    assert model.node.df is not None
    moved = ~model.node.df.geometry.geom_equals_exact(node_geometry, tolerance=0)
    moved_types = set(model.node.df.loc[moved, "node_type"])
    assert moved_types <= {"Basin", "ManningResistance"}, f"Only Basin nodes may move, not {moved_types}"
    return model
