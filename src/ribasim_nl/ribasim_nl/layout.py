"""Layout of a model on the map: nodes not on top of each other, and links without overlapping segments.

This only changes node and link geometries, which Ribasim does not use in the simulation.
"""

import logging
import typing
from collections import defaultdict
from pathlib import Path

import geopandas as gpd
import numpy as np
import shapely

from ribasim_nl.bgt_water import BGT_WATER, read_bgt_water
from ribasim_nl.model import Model

LOG = logging.getLogger(__name__)

# Node types in order of how freely they can be moved to separate them from other nodes
MOVABLE_NODE_TYPES = (
    "DiscreteControl",
    "ContinuousControl",
    "PidControl",
    "FlowDemand",
    "LevelDemand",
    "Observation",
    "LevelBoundary",
    "FlowBoundary",
    "Terminal",
    "Junction",
    "UserDemand",
    "Outlet",
    "Pump",
    "TabulatedRatingCurve",
    "LinearResistance",
    "ManningResistance",
)


def _set_link_ends(model: Model, node_ids: typing.Iterable[int]) -> None:
    """Make the links of nodes start and end at the current node locations, keeping their other coordinates.

    A link without a geometry gets a straight line.
    """
    links = model.link.df
    node_ids = set(node_ids)
    affected = links.index[links["from_node_id"].isin(node_ids) | links["to_node_id"].isin(node_ids)]
    nodes = model.node.df.geometry
    starts = shapely.get_coordinates(nodes.loc[links.loc[affected, "from_node_id"]].to_numpy())
    ends = shapely.get_coordinates(nodes.loc[links.loc[affected, "to_node_id"]].to_numpy())
    geometries = []
    for line, start, end in zip(links.loc[affected, "geometry"], starts, ends, strict=True):
        if line is None:
            geometries.append(shapely.LineString([start, end]))
            continue
        coords = np.array(typing.cast(shapely.LineString, line).coords)[:, :2]
        coords[0] = start
        coords[-1] = end
        geometries.append(shapely.LineString(coords))
    links.loc[affected, "geometry"] = gpd.GeoSeries(geometries, index=affected, crs=links.crs)


def move_nodes(model: Model, locations: dict[int, shapely.Point]) -> None:
    """Move nodes, and the ends of their links along."""
    if not locations:
        return
    model.node.df.loc[list(locations), "geometry"] = gpd.GeoSeries(locations, crs=model.node.df.crs)
    _set_link_ends(model, locations)


def place_bergend_nodes(model: Model, distance: float = 100.0, directions: int = 16) -> None:
    """Place each bergend Basin `distance` from its doorgaand Basin, and its connector halfway.

    The direction is the one with the most room to other links and nodes, within the area of the doorgaand Basin. If
    no direction fits at `distance`, half that distance is tried, and so on, and finally also directions outside the
    area.

    :param model: Ribasim model, with meta_categorie "bergend" for the bergend Basins
    :param distance: distance between the bergend and the doorgaand Basin
    :param directions: number of directions to try
    """
    nodes, links = model.node.df, model.link.df
    if "meta_categorie" not in nodes.columns or model.basin.area.df is None:
        return
    flow = links[links["link_type"] == "flow"]
    downstream = flow.groupby("from_node_id")["to_node_id"].agg(list)
    areas = model.basin.area.df.dissolve(by="node_id").geometry
    bergend = nodes.index[(nodes["node_type"] == "Basin") & (nodes["meta_categorie"] == "bergend")]

    # bergend Basin -> connector -> doorgaand Basin
    chains = []
    for basin_id in bergend:
        connectors = downstream.get(basin_id, [])
        if len(connectors) != 1 or len(downstream.get(connectors[0], [])) != 1:
            continue
        doorgaand = downstream[connectors[0]][0]
        if nodes.at[doorgaand, "node_type"] == "Basin" and doorgaand in areas.index:
            chains.append((int(basin_id), int(connectors[0]), int(doorgaand)))
    chain_nodes = {node_id for chain in chains for node_id in chain[:2]}

    # the other links and nodes to keep away from
    other_links = flow[~flow["from_node_id"].isin(chain_nodes) & ~flow["to_node_id"].isin(chain_nodes)]
    obstacles = np.concatenate([other_links.geometry.to_numpy(), nodes.geometry[~nodes.index.isin(chain_nodes)]])
    tree = shapely.STRtree(obstacles)

    angles = np.linspace(0, 2 * np.pi, directions, endpoint=False)
    locations: dict[int, shapely.Point] = {}
    for basin_id, connector_id, doorgaand in chains:
        origin = typing.cast(shapely.Point, nodes.at[doorgaand, "geometry"])
        area = areas[doorgaand]
        # shorter distances, and finally the shortest also outside the area
        for length, within in [(distance / 2**k, True) for k in range(4)] + [(distance / 8, False)]:
            ends = np.asarray(shapely.points(origin.x + length * np.cos(angles), origin.y + length * np.sin(angles)))
            lines = shapely.linestrings(
                np.stack([np.tile([origin.x, origin.y], (directions, 1)), shapely.get_coordinates(ends)], axis=1)
            )
            inside = shapely.contains(area, lines) if within else np.ones(directions, dtype=bool)
            if not inside.any():
                continue
            # room: the distance from the far half of the line to the nearest obstacle
            far = np.asarray(shapely.line_interpolate_point(lines, 0.5, normalized=True))
            halves = np.asarray(
                shapely.linestrings(np.stack([shapely.get_coordinates(far), shapely.get_coordinates(ends)], axis=1))
            )
            room = np.full(directions, -1.0)
            for i in np.flatnonzero(inside):
                nearby = tree.query(halves[i], predicate="dwithin", distance=length)
                room[i] = shapely.distance(halves[i], obstacles[nearby]).min() if len(nearby) else length
            best = int(np.argmax(room))
            locations[basin_id] = ends[best]
            locations[connector_id] = far[best]
            break
    LOG.info(f"Placed {len(locations) // 2} of {len(chains)} bergend Basins")
    move_nodes(model, locations)


def _right_offset(coords: np.ndarray, distance: float) -> np.ndarray:
    """Offset the inner coordinates of a line to its right, keeping its ends."""
    keep = np.r_[True, np.any(np.diff(coords, axis=0) != 0, axis=1)]
    if keep.sum() < 3:
        return coords
    coords = coords[keep]
    segments = np.diff(coords, axis=0)
    lengths = np.hypot(segments[:, 0], segments[:, 1])
    normals = np.column_stack([segments[:, 1], -segments[:, 0]]) / lengths[:, None]
    miter = normals[:-1] + normals[1:]
    norm = np.hypot(miter[:, 0], miter[:, 1])
    reversal = norm < 1e-6
    miter[reversal] = normals[1:][reversal]
    norm[reversal] = 1.0
    miter /= norm[:, None]
    # limit the length of sharp miters
    scale = distance / np.maximum(np.einsum("ij,ij->i", miter, normals[1:]), 0.5)
    result = coords.copy()
    result[1:-1] += miter * scale[:, None]
    return result


def offset_links(
    model: Model, distance: float = 5.0, link_ids: typing.Iterable[int] | None = None, bend: bool = False
) -> None:
    """Offset links to the right of their direction, like traffic, keeping their ends at their nodes.

    Links that share a route in opposite directions, such as the inflows and outflows of a Basin, then no longer
    overlap, while links that share a route in the same direction stay together. With `bend`, straight links get a
    point halfway that is offset as well.
    """
    links = model.link.df
    # the given links, which may also be observation links, or else all flow links
    flow = links[links.index.isin(list(link_ids))] if link_ids is not None else links[links["link_type"] == "flow"]
    geometries = []
    for geometry in flow.geometry:
        coords = np.array(geometry.coords)[:, :2]
        distinct = np.r_[True, np.any(np.diff(coords, axis=0) != 0, axis=1)]
        if bend and distinct.sum() == 2:  # straight, possibly with repeated points
            start, end = coords[distinct]
            coords = np.array([start, (start + end) / 2, end])
        geometries.append(shapely.LineString(_right_offset(coords, distance)))
    links.loc[flow.index, "geometry"] = gpd.GeoSeries(geometries, index=flow.index, crs=links.crs)


# Maximum distance over which a Junction on land is moved onto the water [m]
MAX_WATER_DISTANCE = 25.0


class Water:
    """The water surfaces of the BGT, read per tile when they are needed, see `ribasim_nl.bgt_water.read_bgt_water`.

    :param path: GeoParquet file of the BGT water
    :param tile: size of the tiles that are read [m]
    :param margin: water that is read around a tile, within which points of the tile are looked up [m]
    """

    def __init__(self, path: Path = BGT_WATER, tile: float = 5000.0, margin: float = MAX_WATER_DISTANCE) -> None:
        self.path, self.tile, self.margin = path, tile, margin
        self._tiles: dict[tuple[int, int], tuple[np.ndarray, shapely.STRtree]] = {}

    def _near(self, point: shapely.Point) -> tuple[np.ndarray, shapely.STRtree]:
        """The water of the tile of a point, with its margin."""
        key = (int(np.floor(point.x / self.tile)), int(np.floor(point.y / self.tile)))
        if key not in self._tiles:
            x, y = key[0] * self.tile, key[1] * self.tile
            area = shapely.box(x, y, x + self.tile, y + self.tile).buffer(self.margin)
            geometries = read_bgt_water(area, path=self.path, columns=["geometry"]).geometry.to_numpy()
            self._tiles[key] = geometries, shapely.STRtree(geometries)
        return self._tiles[key]

    def contains(self, point: shapely.Point) -> bool:
        """Whether a point lies on the water, or on its edge."""
        _, tree = self._near(point)
        return len(tree.query(point, predicate="intersects")) > 0

    def onto(self, point: shapely.Point, max_distance: float, inside: float = 0.5) -> shapely.Point | None:
        """The nearest point of the water to a point on land, and `inside` further in that direction if still water.

        :return: None if the point lies on the water, or more than `max_distance` from it
        """
        assert max_distance <= self.margin, f"Water is read up to {self.margin} m around a tile, not {max_distance} m"
        geometries, tree = self._near(point)
        index, distance = tree.query_nearest(point, max_distance=max_distance, return_distance=True, all_matches=False)
        if len(index) == 0 or distance[0] <= 0.0:
            return None
        polygon = geometries[index[0]]
        (x0, y0), (x1, y1) = shapely.get_coordinates(shapely.shortest_line(point, polygon))
        further = shapely.Point(x1 + (x1 - x0) * inside / distance[0], y1 + (y1 - y0) * inside / distance[0])
        return further if polygon.contains(further) else shapely.Point(x1, y1)


def separate_nodes(model: Model, min_distance: float = 10.0, water: Water | None = None) -> None:
    """Move nodes that are within `min_distance` of another node, such that no nodes are on top of each other.

    Of two such nodes, the most movable one is moved, see `MOVABLE_NODE_TYPES`; Basins are never moved. It is moved to
    the first free location around the other node, starting in the direction away from it, or for a Junction the first
    free location on the `water`, if given and there is one.
    """
    nodes = model.node.df
    ids = nodes.index.to_numpy()
    xy = shapely.get_coordinates(nodes.geometry.to_numpy())[:, :2].copy()
    pairs = shapely.STRtree(nodes.geometry.to_numpy()).query(
        nodes.geometry.to_numpy(), predicate="dwithin", distance=min_distance
    )
    pairs = pairs[:, pairs[0] != pairs[1]]
    if pairs.shape[1] == 0:
        return

    rank = {node_type: i for i, node_type in enumerate(MOVABLE_NODE_TYPES)}
    node_rank = np.array([rank.get(t, len(rank)) for t in nodes["node_type"]])
    is_basin = (nodes["node_type"] == "Basin").to_numpy()
    # per pair, the most movable node moves, or of equally movable nodes the later one
    i, j = pairs
    mover = np.where((node_rank[i] < node_rank[j]) | ((node_rank[i] == node_rank[j]) & (ids[i] > ids[j])), i, j)
    other = np.where(mover == i, j, i)
    keep = ~is_basin[mover]
    partner = dict(zip(mover[keep].tolist(), other[keep].tolist(), strict=False))

    # a grid of the node locations, with cells of min_distance, to find free locations
    grid: dict[tuple[int, int], set[int]] = defaultdict(set)

    def cell(point: np.ndarray) -> tuple[int, int]:
        return int(np.floor(point[0] / min_distance)), int(np.floor(point[1] / min_distance))

    for k in range(len(xy)):
        grid[cell(xy[k])].add(k)

    def free(point: np.ndarray, exclude: int) -> bool:
        cx, cy = cell(point)
        for gx in (cx - 1, cx, cx + 1):
            for gy in (cy - 1, cy, cy + 1):
                for k in grid.get((gx, gy), ()):
                    if k != exclude and np.hypot(*(xy[k] - point)) < min_distance:
                        return False
        return True

    angles = np.linspace(0, 2 * np.pi, 12, endpoint=False)
    is_junction = (nodes["node_type"] == "Junction").to_numpy()
    locations: dict[int, shapely.Point] = {}
    for k in sorted(partner, key=lambda k: ids[k]):
        if free(xy[k], k):
            continue
        q = xy[partner[k]]
        away = np.arctan2(*(xy[k] - q)[::-1]) if np.hypot(*(xy[k] - q)) > 1e-3 else 0.0
        around = (
            c
            for radius in min_distance * np.array([1.0, 1.5, 2.0, 3.0, 4.0])
            for c in q + radius * np.column_stack([np.cos(away + angles), np.sin(away + angles)])
            if free(c, k)
        )
        if water is not None and is_junction[k]:
            options = list(around)
            found = next((c for c in options if water.contains(shapely.Point(c))), options[0] if options else None)
        else:
            found = next(around, None)
        if found is not None:
            grid[cell(xy[k])].discard(k)
            xy[k] = found
            grid[cell(found)].add(k)
            locations[int(ids[k])] = shapely.Point(found)
    LOG.info(f"Moved {len(locations)} nodes away from other nodes")
    move_nodes(model, locations)


def junctions_onto_water(
    model: Model,
    water: Water,
    max_distance: float = MAX_WATER_DISTANCE,
    node_ids: typing.Collection[int] | None = None,
) -> None:
    """Move the Junctions that lie on land, within `max_distance` of the water, onto the water, see `Water.onto`.

    A Junction is where routes join or split, which belongs on the water, but routes can share a stretch over land,
    such as through a culvert or from a structure on a dike. The ends of its links move along.

    :param node_ids: the Junctions to move, defaults to all Junctions
    """
    nodes = model.node.df
    junctions = nodes[nodes["node_type"] == "Junction"]
    if node_ids is not None:
        junctions = junctions[junctions.index.isin(list(node_ids))]
    locations: dict[int, shapely.Point] = {}
    for node_id, point in junctions.geometry.items():
        location = water.onto(typing.cast(shapely.Point, point), max_distance)
        if location is not None:
            locations[int(typing.cast(int, node_id))] = location
    LOG.info(f"Moved {len(locations)} of {len(junctions)} Junctions onto the water")
    move_nodes(model, locations)


def resolve_overlaps(model: Model, distance: float = 5.0, min_length: float = 1.0, max_iterations: int = 5) -> None:
    """Offset links that still overlap other links further to their right, see `offset_links`.

    These are for instance the links of neighboring Basins along a shared waterway.
    """
    for _ in range(max_iterations):
        pairs = overlapping_links(model, min_length)
        if pairs.shape[1] == 0:
            return
        # of each pair, the later link moves
        offset_links(model, distance, set(pairs.max(axis=0).tolist()), bend=True)
    LOG.warning(f"{overlapping_links(model, min_length).shape[1]} pairs of links still overlap")


def tidy_layout(
    model: Model,
    *,
    offset: float = 5.0,
    min_distance: float = 10.0,
    bergend_distance: float = 100.0,
    water_path: Path | None = None,
) -> Model:
    """Make the layout of a model readable: no nodes on top of each other, and no overlapping links.

    The inflows and outflows of a Basin share one Junction tree, see `ribasim_nl.flow_paths.Routes`, so links stay on
    their route, and only links that still overlap are offset. See `place_bergend_nodes`, `separate_nodes` and
    `resolve_overlaps`. With the BGT water of `water_path`, see `Water`, Junctions are put on the water, see
    `junctions_onto_water`, and kept there when they are moved apart.
    """
    place_bergend_nodes(model, bergend_distance)
    water = None if water_path is None else Water(water_path)
    if water is not None:
        junctions_onto_water(model, water)
    separate_nodes(model, min_distance, water)
    resolve_overlaps(model, offset)
    return model


def coincident_nodes(model: Model, distance: float) -> np.ndarray:
    """Pairs of node ids that are within `distance` of each other, as an array of shape (2, n)."""
    points = model.node.df.geometry.to_numpy()
    pairs = shapely.STRtree(points).query(points, predicate="dwithin", distance=distance)
    pairs = pairs[:, pairs[0] < pairs[1]]
    return model.node.df.index.to_numpy()[pairs]


def overlapping_links(model: Model, min_length: float) -> np.ndarray:
    """Pairs of flow and observation link ids that overlap over more than `min_length`, as an array of shape (2, n)."""
    links = model.link.df[model.link.df["link_type"].isin(["flow", "observation"])]
    geometries = links.geometry.to_numpy()
    pairs = shapely.STRtree(geometries).query(geometries, predicate="intersects")
    pairs = pairs[:, pairs[0] < pairs[1]]
    overlap = shapely.length(shapely.intersection(geometries[pairs[0]], geometries[pairs[1]])) > min_length
    return links.index.to_numpy()[pairs[:, overlap]]
