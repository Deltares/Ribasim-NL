"""Shared helpers to lump a network: reading layers, splitting lines and snapping points to the network."""

import logging
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import numpy.typing as npt
import pandas as pd
import pyogrio
import shapely
from shapely.geometry import LineString, MultiPolygon, Point, Polygon

logger = logging.getLogger(__name__)


def merge_grouped(left: pd.DataFrame, right: pd.DataFrame, **kwargs: Any) -> pd.DataFrame:
    """Inner merge, with the rows grouped by key, in order of first appearance of the key in `left`.

    This is the row order of an inner merge in pandas < 2.2, with which the input models were made in 2024.
    Their node and link IDs follow from it. Supports the keys `on`, `left_on` and `left_index`.
    """
    assert kwargs.get("how", "inner") == "inner", "only inner merges are grouped by key"
    if kwargs.get("left_index"):
        keys = left.index.to_frame(index=False)
    else:
        columns = kwargs.get("on", kwargs.get("left_on"))
        assert columns is not None, "merge_grouped needs `on`, `left_on` or `left_index`"
        keys = left[list(np.atleast_1d(columns))].reset_index(drop=True)
    group = keys.groupby(list(keys.columns), sort=False, dropna=False).ngroup().to_numpy()

    position = "__left_position"
    assert position not in left.columns
    merged = left.assign(**{position: np.arange(len(left))}).merge(right, **kwargs)
    order = np.argsort(group[merged[position].to_numpy()], kind="stable")
    merged = merged.iloc[order].drop(columns=position)
    # merging on columns gives a new index, in the order of the rows
    if not (kwargs.get("left_index") or kwargs.get("right_index")):
        merged = merged.reset_index(drop=True)
    return merged


def quicksort_order(values: npt.ArrayLike) -> np.ndarray:
    """The order of `values` that `numpy.argsort` gave in numpy < 2 without AVX-512: an unstable introsort.

    The 2024 input models were made with it. The order of equal values, which an unstable sort decides, sets the link
    IDs of the boundaries. Implemented in Python, so it does not depend on the numpy version or CPU.
    """
    array = np.asarray(values, dtype=float)
    # numpy orders NaN last, the comparisons below do not
    assert not np.isnan(array).any(), "cannot order NaN"
    v = array.tolist()
    t = list(range(len(v)))
    if len(t) <= 1:
        return np.array(t, dtype=int)

    def heapsort(start: int, n: int) -> None:
        # 1-based heap on t[start : start + n]
        def get(k: int) -> int:
            return t[start + k - 1]

        def put(k: int, value: int) -> None:
            t[start + k - 1] = value

        def sift(i: int, n: int, tmp: int) -> None:
            j = i << 1
            while j <= n:
                if j < n and v[get(j)] < v[get(j + 1)]:
                    j += 1
                if v[tmp] < v[get(j)]:
                    put(i, get(j))
                    i = j
                    j += j
                else:
                    break
            put(i, tmp)

        for i in range(n >> 1, 0, -1):
            sift(i, n, get(i))
        while n > 1:
            tmp = get(n)
            put(n, get(1))
            n -= 1
            sift(1, n, tmp)

    small = 15
    pl, pr = 0, len(t) - 1
    depth = 2 * ((len(t)).bit_length() - 1)
    stack: list[tuple[int, int, int]] = []
    while True:
        if depth < 0:
            heapsort(pl, pr - pl + 1)
        else:
            while pr - pl > small:
                # partition around the median of three
                pm = pl + ((pr - pl) >> 1)
                if v[t[pm]] < v[t[pl]]:
                    t[pm], t[pl] = t[pl], t[pm]
                if v[t[pr]] < v[t[pm]]:
                    t[pr], t[pm] = t[pm], t[pr]
                if v[t[pm]] < v[t[pl]]:
                    t[pm], t[pl] = t[pl], t[pm]
                vp = v[t[pm]]
                pi, pj = pl, pr - 1
                t[pm], t[pj] = t[pj], t[pm]
                while True:
                    pi += 1
                    while v[t[pi]] < vp:
                        pi += 1
                    pj -= 1
                    while vp < v[t[pj]]:
                        pj -= 1
                    if pi >= pj:
                        break
                    t[pi], t[pj] = t[pj], t[pi]
                t[pi], t[pr - 1] = t[pr - 1], t[pi]
                depth -= 1
                # continue with the smaller partition, stack the larger one
                if pi - pl < pr - pi:
                    stack.append((pi + 1, pr, depth))
                    pr = pi - 1
                else:
                    stack.append((pl, pi - 1, depth))
                    pl = pi + 1
            # insertion sort of the small partition
            for pi in range(pl + 1, pr + 1):
                vi = t[pi]
                pj = pi
                while pj > pl and v[vi] < v[t[pj - 1]]:
                    t[pj] = t[pj - 1]
                    pj -= 1
                t[pj] = vi
        if not stack:
            return np.array(t, dtype=int)
        pl, pr, depth = stack.pop()


POLYGON_TYPES = ["Polygon", "MultiPolygon"]


def overlay_intersection(df1: gpd.GeoDataFrame, df2: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """The intersections of two layers of polygons, as `geopandas.overlay` made them before geopandas 1.

    Invalid polygons and intersections are fixed with `buffer(0)`, and the intersections are grouped by the polygon of
    `df2`, in order of first appearance, see `merge_grouped`. The polygonal parts of collections are kept. The 2024
    input models were made with it, and the areas of their basins depend on it.
    """

    def fix(df: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
        df = df.reset_index(drop=True)
        if df.geom_type.isin(POLYGON_TYPES).all():
            geometry = df.geometry.copy()
            invalid = ~geometry.is_valid
            geometry[invalid] = geometry[invalid].buffer(0)
            df = df.set_geometry(geometry)
        return df

    df1, df2 = fix(df1), fix(df2)
    # as in geopandas < 1, the type of the first geometry decides which intersections are kept
    assert df1.geom_type.iloc[0] in POLYGON_TYPES, "only intersections of polygons are supported"
    idx1, idx2 = df2.sindex.query(df1.geometry, predicate="intersects", sort=True)
    intersections = (
        df1.geometry.take(idx1).reset_index(drop=True).intersection(df2.geometry.take(idx2).reset_index(drop=True))
    )
    polygons = intersections.geom_type.isin(POLYGON_TYPES)
    intersections[polygons] = intersections[polygons].buffer(0)

    pairs = pd.DataFrame({"__idx1": idx1, "__idx2": idx2})
    result = merge_grouped(pairs, df1.drop(columns=df1.geometry.name), left_on="__idx1", right_index=True)
    result = merge_grouped(
        result, df2.drop(columns=df2.geometry.name), left_on="__idx2", right_index=True, suffixes=("_1", "_2")
    )
    result = gpd.GeoDataFrame(result, geometry=intersections, crs=df1.crs).drop(columns=["__idx1", "__idx2"])

    # keep the polygonal parts of collections, and only polygons
    collections = result.geom_type == "GeometryCollection"
    if collections.any():
        parts = result.loc[collections, ["geometry"]].reset_index(drop=True).explode(index_parts=True)
        parts = parts.reset_index(level=0)
        parts.loc[~parts.geom_type.isin(POLYGON_TYPES), "geometry"] = None
        result.loc[collections, "geometry"] = parts.dissolve(by="level_0").geometry.to_numpy()
    return result.loc[result.geom_type.isin(POLYGON_TYPES)].reset_index(drop=True)


def read_layer(
    path: Path,
    layer: str | None = None,
    explode: bool = True,
    remove_z: bool = False,
    code_column: str | None = None,
) -> gpd.GeoDataFrame | None:
    """Read a layer of a GeoPackage, or a file with one layer. Returns None if the GeoPackage lacks the layer.

    Optionally explode multi-part geometries, remove the Z coordinates, and keep only the
    column `code_column` (case insensitive, else the row number), as `code`.
    """
    if not path.exists():
        raise FileNotFoundError(path)
    if path.suffix.lower() == ".gpkg":
        if layer not in pyogrio.list_layers(path)[:, 0]:
            return None
        gdf = gpd.read_file(path, layer=layer)
    else:
        gdf = gpd.read_file(path)
    if explode:
        gdf = gdf.explode(ignore_index=True)
    if remove_z:
        gdf = gdf.set_geometry(gdf.geometry.force_2d())
    if code_column is not None:
        for column in (code_column, code_column.lower(), code_column.upper()):
            if column in gdf.columns:
                break
        else:
            gdf = gdf.reset_index(drop=True).reset_index()
            column = "index"
        gdf = gdf[[column, "geometry"]].rename(columns={column: "code"})
    return gdf


def drop_duplicate_geometries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Drop rows with the geometry of an earlier row."""
    duplicated = gdf.duplicated("geometry")
    if duplicated.any():
        logger.info(f"Dropping {duplicated.sum()} rows with duplicate geometries")
    return gdf.loc[~duplicated].copy()


def generate_nodes_from_edges(edges: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Number the edges, and generate the nodes at their ends, numbered in order of first appearance.

    Returns the edges, with their `from_node` and `to_node`, and the nodes.
    """
    edges = edges.copy()
    edges["edge_no"] = range(len(edges))
    edges.index = edges["edge_no"].to_numpy()
    ends = [(g.coords[0], g.coords[-1]) for g in edges.geometry]
    from_coords = [e[0] for e in ends]
    to_coords = [e[1] for e in ends]
    unique_coords = pd.unique(pd.Series(from_coords + to_coords, dtype=object))
    indexer = {c: i for i, c in enumerate(unique_coords)}
    nodes = gpd.GeoDataFrame(
        data={"node_no": range(len(unique_coords))},
        geometry=[Point(c) for c in unique_coords],
        crs=edges.crs,
    )
    edges["from_node"] = [indexer[c] for c in from_coords]
    edges["to_node"] = [indexer[c] for c in to_coords]
    return edges, nodes


def split_line_in_two(line: LineString, distance: float) -> list[LineString]:
    """Split a line in two at a distance along it. A distance at or beyond its ends returns the line."""
    if distance <= 0.0 or distance >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, coord in enumerate(coords):
        projected = line.project(Point(coord))
        if projected == distance:
            return [LineString(coords[: i + 1]), LineString(coords[i:])]
        if projected > distance:
            cp = line.interpolate(distance)
            point = (cp.x, cp.y) if len(coords[0]) == 2 else (cp.x, cp.y, cp.z)
            return [LineString([*coords[:i], point]), LineString([point, *coords[i:]])]
    raise ValueError(f"cannot split line of length {line.length} at {distance}")


def split_line_in_multiple(line: LineString, distances: Sequence[float]) -> list[LineString]:
    """Split a line at distances along it."""
    lines: list[LineString] = []
    distances = sorted(distances)
    new_line = line
    for i, distance in enumerate(distances):
        if i == 0:
            parts = split_line_in_two(line, distance)
            if len(distances) == 1:
                return parts
            if len(parts) == 1:
                # splitting at an end
                new_line = parts[0]
            else:
                lines.append(parts[0])
                new_line = parts[1]
        else:
            new_distance = distance - distances[i - 1]
            if new_distance == 0.0:
                continue
            parts = split_line_in_two(new_line, new_distance)
            if i == len(distances) - 1:
                lines.extend(parts)
            else:
                lines.append(parts[0])
                new_line = parts[1]
    return lines


def split_edges_by_dx(edges: gpd.GeoDataFrame, dx: float) -> gpd.GeoDataFrame:
    """Split edges in sections of about `dx` m, and suffix their `branch_id` with `___<section>`.

    The column `branch_id` is renamed to `edge_id`.
    """
    edges = edges.copy()
    sections: list[tuple[Any, int, LineString]] = []
    split_edges = []
    for index, line in zip(edges.index, edges.geometry, strict=True):
        n = round(line.length / dx)
        if n <= 1:
            continue
        dx_star = line.length / n
        new_lines = split_line_in_multiple(line, [(i + 1) * dx_star for i in range(n - 1)])
        sections.extend((index, j, new_line) for j, new_line in enumerate(new_lines))
        split_edges.append(index)

    attributes = edges.loc[[s[0] for s in sections], [c for c in edges.columns if c != "geometry"]]
    edges_to_add = gpd.GeoDataFrame(
        attributes.reset_index(drop=True),
        geometry=[s[2] for s in sections],
        crs=edges.crs,
    )
    if "branch_id" in edges_to_add.columns:
        # the 2024 models were made with pandas < 3, which formats a missing branch_id as None
        edges_to_add["branch_id"] = [
            f"{'None' if pd.isna(b) else b}___{s[1]}" for b, s in zip(edges_to_add["branch_id"], sections, strict=True)
        ]
    edges = gpd.GeoDataFrame(
        pd.concat([edges.loc[~edges.index.isin(split_edges)], edges_to_add], ignore_index=True),
        geometry="geometry",
        crs=edges.crs,
    )
    if "edge_no" in edges.columns:
        edges["edge_no"] = np.arange(len(edges))
    return edges.rename(columns={"branch_id": "edge_id"})


def snap_points_to_nodes_and_edges(
    points: gpd.GeoDataFrame,
    nodes: gpd.GeoDataFrame,
    edges: gpd.GeoDataFrame | None = None,
    edges_bufdist: float = 0.5,
    nodes_bufdist: float = 0.5,
    n_edges_to_node_limit: float = 1e10,
    min_length_edge: float = 2.0,
) -> gpd.GeoDataFrame:
    """Snap points to the nearest node within `nodes_bufdist`, else to the nearest edge within `edges_bufdist`.

    A point is only snapped to a node with less than `n_edges_to_node_limit` edges. On an edge, a point is kept at
    least 0.5 m from its ends, and edges shorter than `min_length_edge` are skipped. Points are not moved otherwise.
    """
    points = points.copy()
    new_points = points.geometry.tolist()
    for i, point in enumerate(points.geometry):
        node_ix = nodes.index.to_numpy()[nodes.intersects(point.buffer(nodes_bufdist))]
        if len(node_ix) >= 1:
            distances = np.array([n.distance(point) for n in nodes.loc[node_ix].geometry.to_numpy()])
            new_point = nodes.geometry.loc[node_ix[np.argmin(distances)]]
            if edges is None:
                new_points[i] = new_point
                continue
            connected_edges = edges.loc[new_point.buffer(0.000001).intersects(edges.geometry.to_numpy())]
            if len(connected_edges) < n_edges_to_node_limit:
                new_points[i] = new_point
                continue
        if edges is None:
            continue
        lines = edges.geometry.to_numpy()[edges.geometry.intersects(point.buffer(edges_bufdist))]
        lines = lines[[line.length > min_length_edge for line in lines]]
        if len(lines) == 0:
            continue
        # project the point on the edge, at least 0.5 m from its ends
        along = [line.project(point) for line in lines]
        along = [min(d, line.length - 0.5) if d > 0.5 else 0.5 for line, d in zip(lines, along, strict=True)]
        candidates = np.array([line.interpolate(d) for line, d in zip(lines, along, strict=True)], dtype=object)
        distances = np.array([c.distance(point) for c in candidates])
        within = np.where(distances <= edges_bufdist)[0]
        if len(within) >= 1:
            new_points[i] = candidates[within][np.argmin(distances[within])]
    points["geometry"] = new_points
    return points


def get_node_no_and_edge_no_for_points(
    points: gpd.GeoDataFrame,
    nodes: gpd.GeoDataFrame,
    edges: gpd.GeoDataFrame | None = None,
) -> gpd.GeoDataFrame:
    """Add the `node_no` of the first node on each point, else the `edge_no` of the first edge on it, else -1."""
    points = points.copy()
    node_no = np.full(len(points), -1, dtype=int)
    node_buffers = shapely.buffer(nodes.geometry.to_numpy(), 0.000001)
    for i, point in enumerate(points.geometry):
        ix = np.flatnonzero(shapely.intersects(node_buffers, point))
        if len(ix) >= 1:
            node_no[i] = nodes["node_no"].iloc[ix[0]]
    points["node_no"] = node_no
    if edges is None:
        points["edge_no"] = None
        return points
    edge_no = np.full(len(points), -1, dtype=int)
    edge_buffers = shapely.buffer(edges.geometry.to_numpy(), 0.000001)
    for i, point in enumerate(points.geometry):
        if node_no[i] != -1:
            continue
        ix = np.flatnonzero(shapely.intersects(edge_buffers, point))
        if len(ix) >= 1:
            edge_no[i] = edges["edge_no"].iloc[ix[0]]
    points["edge_no"] = edge_no
    return points


def split_edges_by_split_nodes(
    split_nodes: gpd.GeoDataFrame,
    edges: gpd.GeoDataFrame,
    buffer_distance: float = 0.5,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Split the edges at the split nodes on them, at more than `buffer_distance` from their ends.

    The split edges get the suffix `_<section>` in their `edge_id`. Returns the split nodes, with their new
    `node_no` and `edge_no`, the edges and their nodes.
    """
    edges_orig = edges.copy()
    split_geometry = split_nodes.geometry
    for i, edge in enumerate(edges_orig.geometry):
        split_points = split_geometry.to_numpy()[split_geometry.intersects(edge.buffer(buffer_distance))]
        if len(split_points) == 0:
            continue
        ends = [edge.interpolate(x) for x in (0, edge.length)]
        split_points = [p for p in split_points if not any(p.intersects(n.buffer(buffer_distance)) for n in ends)]
        if len(split_points) == 0:
            continue
        split_lines = split_line_in_multiple(edge, [edge.project(p) for p in split_points])
        if len(split_lines) == 0:
            continue
        index = edges_orig.index.to_numpy()[i]
        edge_row = edges_orig.loc[[index]]
        edges_to_add = gpd.GeoDataFrame(
            pd.concat([edge_row] * len(split_lines)).drop(columns="geometry"),
            geometry=split_lines,
            crs=edges.crs,
        ).set_axis(np.arange(len(split_lines)) + 1 + edges.index.max())
        edges = pd.concat([edges, edges_to_add], axis=0).drop(index=index)

    if "edge_id" in edges.columns:
        edges["edge_id"] = edges["edge_id"].fillna("dummy")
        section = edges.groupby("edge_id", sort=False).cumcount() + 1
        sections = edges.groupby("edge_id", sort=False)["edge_id"].transform("size")
        edges["edge_id"] = [
            f"{e}_{s}" if n > 1 else e for e, s, n in zip(edges["edge_id"], section, sections, strict=True)
        ]
    edges, nodes = generate_nodes_from_edges(edges)
    split_nodes = get_node_no_and_edge_no_for_points(split_nodes, nodes=nodes, edges=edges)
    return split_nodes, edges, nodes


def remove_holes(gdf: gpd.GeoDataFrame, min_area: float) -> gpd.GeoDataFrame:
    """Remove the holes of (multi)polygons with an area up to `min_area`."""

    def remove_polygon_holes(polygon: Polygon) -> Polygon:
        holes = [i for i in polygon.interiors if not Polygon(i).area > min_area]
        return Polygon(shell=polygon.exterior, holes=holes)

    def remove_geometry_holes(geometry: Any) -> Any:
        if isinstance(geometry, Polygon):
            return remove_polygon_holes(geometry)
        if isinstance(geometry, MultiPolygon):
            return MultiPolygon([remove_polygon_holes(p) for p in geometry.geoms])
        return geometry

    gdf = gdf.copy()
    gdf["geometry"] = gdf.geometry.apply(remove_geometry_holes)
    return gdf


def extract_segment_from_linestring(line: LineString, point1: Point, point2: Point) -> LineString:
    """The part of a line between the projections of two points on it."""
    start, end = sorted([line.project(point1), line.project(point2)])
    coords = [line.interpolate(start).coords[0]]
    for seg_start, seg_end in zip(line.coords[:-1], line.coords[1:], strict=True):
        seg_start_dist = line.project(Point(seg_start))
        seg_end_dist = line.project(Point(seg_end))
        if seg_start_dist > end:
            break
        if seg_start_dist >= start and seg_end_dist <= end:
            if coords[-1] != seg_start:
                coords.append(seg_start)
            coords.append(seg_end)
    if coords[-1] != line.interpolate(end).coords[0]:
        coords.append(line.interpolate(end).coords[0])
    return LineString(coords)
