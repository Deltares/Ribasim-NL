"""Layout of a model on the map: nodes not on top of each other, and links without overlapping segments.

This only changes node and link geometries, which Ribasim does not use in the simulation.
"""

import logging
import typing
from collections import defaultdict

import geopandas as gpd
import numpy as np
import shapely

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
    """Make the links of nodes start and end at the current node locations, keeping their other coordinates."""
    links = model.link.df
    node_ids = set(node_ids)
    affected = links.index[links["from_node_id"].isin(node_ids) | links["to_node_id"].isin(node_ids)]
    nodes = model.node.df.geometry
    geometries = []
    for link_id in affected:
        coords = np.array(typing.cast(shapely.LineString, links.at[link_id, "geometry"]).coords)[:, :2]
        coords[0] = nodes[links.at[link_id, "from_node_id"]].coords[0][:2]
        coords[-1] = nodes[links.at[link_id, "to_node_id"]].coords[0][:2]
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


def offset_junctions(model: Model, distance: float = 5.0, junction_ids: typing.Iterable[int] | None = None) -> None:
    """Offset the Junctions to the right of the direction of the flow through them, along with their links.

    The Junctions of the inflows and outflows of a Basin then no longer coincide. Only `junction_ids` are offset if
    given, else all Junctions.
    """
    nodes, links = model.node.df, model.link.df
    flow = links[links["link_type"] == "flow"]
    locations: dict[int, shapely.Point] = {}
    if junction_ids is None:
        junction_ids = nodes.index[nodes["node_type"] == "Junction"]
    for junction_id in junction_ids:
        outgoing = flow[flow["from_node_id"] == junction_id].geometry
        incoming = flow[flow["to_node_id"] == junction_id].geometry
        # the direction of the single side, where the flows are combined
        if len(outgoing) == 1:
            coords = np.array(outgoing.iloc[0].coords)[:2, :2]
        elif len(incoming) == 1:
            coords = np.array(incoming.iloc[0].coords)[-2:, :2]
        else:
            continue
        dx, dy = coords[1] - coords[0]
        length = np.hypot(dx, dy)
        if length == 0:
            continue
        point = typing.cast(shapely.Point, nodes.at[junction_id, "geometry"])
        locations[int(junction_id)] = shapely.Point(point.x + distance * dy / length, point.y - distance * dx / length)
    move_nodes(model, locations)


def separate_nodes(model: Model, min_distance: float = 10.0) -> None:
    """Move nodes that are within `min_distance` of another node, such that no nodes are on top of each other.

    Of two such nodes, the most movable one is moved, see `MOVABLE_NODE_TYPES`; Basins are never moved. It is moved to
    the first free location around the other node, starting in the direction away from it.
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
    locations: dict[int, shapely.Point] = {}
    for k in sorted(partner, key=lambda k: ids[k]):
        if free(xy[k], k):
            continue
        q = xy[partner[k]]
        away = np.arctan2(*(xy[k] - q)[::-1]) if np.hypot(*(xy[k] - q)) > 1e-3 else 0.0
        for radius in min_distance * np.array([1.0, 1.5, 2.0, 3.0, 4.0]):
            candidates = q + radius * np.column_stack([np.cos(away + angles), np.sin(away + angles)])
            found = next((c for c in candidates if free(c, k)), None)
            if found is not None:
                grid[cell(xy[k])].discard(k)
                xy[k] = found
                grid[cell(found)].add(k)
                locations[int(ids[k])] = shapely.Point(found)
                break
    LOG.info(f"Moved {len(locations)} nodes away from other nodes")
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
) -> Model:
    """Make the layout of a model readable: no nodes on top of each other, and no overlapping links.

    See `place_bergend_nodes`, `offset_links`, `offset_junctions`, `separate_nodes` and `resolve_overlaps`.
    """
    place_bergend_nodes(model, bergend_distance)
    offset_links(model, offset)
    offset_junctions(model, offset)
    separate_nodes(model, min_distance)
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
