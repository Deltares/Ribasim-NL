"""The shortest paths along the hydro-objects through each boezem, from its crossings to its representative point.

A port of the shortest path notebooks of `peilbeheerst_model` by HKV, of 2024. The paths become the geometry of the links to
and from the boezem basins in the input models, see `ribasim_nl.crossings.model.embed_boezems`.

For each boezem, a peilgebied with `peilgebied_cat` 1, the hydro-objects inside it are split into segments, snapped to
each other and turned into a graph, whose components are connected one by one to the largest component, each time at the
closest pair of nodes. The paths run from the node nearest to each crossing to the node nearest to the representative point.
"""

import logging
import typing
from pathlib import Path

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import pyogrio
import shapely
import shapely.ops
from pyproj import CRS
from scipy.spatial import KDTree
from shapely.geometry import LineString, MultiLineString, Point
from shapely.ops import split
from shapely.wkt import dumps

logger = logging.getLogger(__name__)


def split_line_at_point(line: LineString, point: Point) -> MultiLineString:
    """Split a line around a point into its first and last part, or keep it whole if that does not split it in 2 or 3."""
    parts = typing.cast(list[LineString], list(split(line, point.buffer(1e-4)).geoms))
    if len(parts) in [2, 3]:
        return MultiLineString([parts[0], parts[-1]])
    return MultiLineString([line])


def split_lines_at_intersections(gdf_object: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Split each line at its first point intersection with another line, if that is a point."""
    split_lines = []
    for idx, row in gdf_object.iterrows():
        was_split = False
        possible_matches = gdf_object.iloc[gdf_object.sindex.intersection(row.geometry.bounds)].drop(idx)
        precise_matches = possible_matches[possible_matches.intersects(row.geometry)]
        for _, match in precise_matches.iterrows():
            if row.geometry.intersects(match.geometry):
                intersection = row.geometry.intersection(match.geometry)
                if isinstance(intersection, Point):
                    try:
                        for geom in split_line_at_point(row.geometry, intersection).geoms:
                            new_row = row.copy()
                            new_row["geometry"] = geom
                            split_lines.append(new_row)
                        was_split = True
                    except ValueError as e:
                        logger.warning(f"Error splitting line: {e}")
                # only one split per line
                break
        if not was_split:
            split_lines.append(row)
    return gpd.GeoDataFrame(split_lines, columns=gdf_object.columns)


def cut_linestring_at_interval(line: LineString, interval: float) -> list[LineString]:
    """Cut a line into equal straight segments of at most about `interval`."""
    num_segments = int(np.ceil(line.length / interval))
    if num_segments == 1:
        return [line]
    points = [line.interpolate(distance) for distance in np.linspace(0, line.length, num_segments + 1)]
    return [LineString([points[i], points[i + 1]]) for i in range(num_segments)]


def explode_linestrings(gdf: gpd.GeoDataFrame, interval: float) -> gpd.GeoDataFrame:
    """Cut all lines into segments, see `cut_linestring_at_interval`."""
    segments = []
    for _, row in gdf.iterrows():
        segments.extend(cut_linestring_at_interval(row.geometry, interval))
    return gpd.GeoDataFrame(geometry=segments, crs=gdf.crs)


def connect_linestrings_within_distance(gdf: gpd.GeoDataFrame, max_distance: float = 4) -> gpd.GeoDataFrame:
    """Extend each line end to the nearest other line within `max_distance`, at the point of that line nearest to it."""
    gdf = gdf.explode(ignore_index=False, index_parts=True)
    gdf["geometry"] = gdf.make_valid()
    gdf["geometry"] = gdf.geometry.apply(shapely.force_2d)
    gdf = gdf[~gdf.is_empty].copy()

    change_idx, change_geom = [], []
    for index, line in zip(gdf.index, typing.cast(list[LineString], list(gdf.geometry)), strict=True):
        ps = shapely.get_parts(line.boundary)
        if len(ps) != 2:
            continue
        p0, p1 = ps

        p0_changed, p1_changed = False, False
        idx0 = gdf.sindex.query(p0.buffer(max_distance), predicate="intersects")
        if len(idx0) > 0:
            dist0 = gdf.iloc[idx0].distance(p0)
            if (dist0 > 10e-8).any():
                geom = typing.cast(LineString, gdf.geometry.at[dist0[dist0 > 10e-8].idxmin()])
                p0 = geom.interpolate(geom.project(p0))
                p0_changed = True

        idx1 = gdf.sindex.query(p1.buffer(max_distance), predicate="intersects")
        if len(idx1) > 0:
            dist1 = gdf.iloc[idx1].distance(p1)
            if (dist1 > 10e-8).any():
                geom = typing.cast(LineString, gdf.geometry.at[dist1[dist1 > 10e-8].idxmin()])
                p1 = geom.interpolate(geom.project(p1))
                p1_changed = True

        if p0_changed or p1_changed:
            coords = list(line.coords)
            if p0_changed:
                coords = list(p0.coords) + coords
            if p1_changed:
                coords = coords + list(p1.coords)
            change_idx.append(index)
            change_geom.append(LineString(coords))

    if len(change_idx) > 0:
        gdf.loc[change_idx, "geometry"] = change_geom
    return gdf


def connect_components(graph: nx.Graph) -> None:
    """Connect all components to the largest, each time the closest component at its closest pair of nodes.

    The 2024 code recomputed all distances for each connection. This keeps, for each node outside the largest component,
    the distance to the nearest node in it, which gives the same connections if no two distances are equal.
    """
    components = list(nx.connected_components(graph))
    largest = max(components, key=len)
    if len(components) == 1:
        return
    nodes = list(graph.nodes)
    position = {node: i for i, node in enumerate(nodes)}
    xy = np.array([graph.nodes[node]["geometry"].coords[0] for node in nodes])
    label = np.empty(len(nodes), dtype=int)
    members = []
    for i, component in enumerate(components):
        indices = np.array([position[node] for node in component])
        label[indices] = i
        members.append(indices)
    in_largest = label == components.index(largest)

    # for each node outside the largest component, its distance to it and its nearest node in it
    outside = np.flatnonzero(~in_largest)
    inside = np.flatnonzero(in_largest)
    distance, nearest = KDTree(xy[inside]).query(xy[outside])
    best_distance = np.full(len(nodes), np.inf)
    best_node = np.full(len(nodes), -1)
    best_distance[outside] = distance
    best_node[outside] = inside[nearest]

    while not in_largest.all():
        candidates = np.flatnonzero(~in_largest)
        node_in_smaller = candidates[np.argmin(best_distance[candidates])]
        node_in_largest = best_node[node_in_smaller]
        geom1 = graph.nodes[nodes[node_in_largest]]["geometry"]
        geom2 = graph.nodes[nodes[node_in_smaller]]["geometry"]
        graph.add_edge(
            nodes[node_in_largest], nodes[node_in_smaller], geometry=LineString([geom1.coords[0], geom2.coords[0]])
        )

        added = members[label[node_in_smaller]]
        in_largest[added] = True
        remaining = np.flatnonzero(~in_largest)
        if len(remaining) == 0:
            break
        distance, nearest = KDTree(xy[added]).query(xy[remaining])
        closer = distance < best_distance[remaining]
        best_distance[remaining[closer]] = distance[closer]
        best_node[remaining[closer]] = added[nearest[closer]]


def boezem_shortest_paths(
    rhws: pd.Series,
    crossings: gpd.GeoDataFrame,
    hydroobject: gpd.GeoDataFrame,
    crs: CRS | None,
    distance_interval: float,
) -> gpd.GeoDataFrame:
    """The crossings of one boezem, with the shortest path from each to the representative point of the boezem."""
    gdf_rhws_single = gpd.GeoDataFrame(rhws.to_frame().T, geometry="geometry", crs=crs)
    globalid = gdf_rhws_single.globalid.iloc[0]
    gdf_cross_single = crossings[(crossings.peilgebied_from == globalid) | (crossings.peilgebied_to == globalid)].copy()

    gdf_object = gpd.clip(hydroobject, gdf_rhws_single).reset_index(drop=True)
    gdf_object = gdf_object.explode(index_parts=False).reset_index(drop=True)
    gdf_object = gdf_object[~gdf_object.is_empty].copy()
    gdf_object = gdf_object[gdf_object.length > 1e-7].copy()
    gdf_object = split_lines_at_intersections(gdf_object)
    gdf_object = explode_linestrings(gdf_object, distance_interval)
    gdf_object = connect_linestrings_within_distance(gdf_object)
    gdf_object = gdf_object.explode(index_parts=False).reset_index(drop=True)
    gdf_object = gdf_object[~gdf_object.is_empty].copy()
    gdf_object = gdf_object[gdf_object.length > 1e-7].copy()

    # the line ends are the nodes, numbered in the order of their geometry
    nodes_gdf = gdf_object.copy()
    nodes_gdf["geometry"] = nodes_gdf.geometry.boundary
    nodes_gdf = nodes_gdf.explode(index_parts=True)
    nodes_gdf.insert(0, "node_id", -1)
    for node_id, (_, group) in enumerate(nodes_gdf.groupby("geometry"), start=1):
        nodes_gdf.loc[group.index, "node_id"] = node_id

    startpoints, distances = nodes_gdf.sindex.nearest(gdf_cross_single.geometry, return_all=False, return_distance=True)
    startpoints = nodes_gdf.node_id.iloc[startpoints[1, :]].to_numpy()
    gdf_cross_single["node_id"] = startpoints
    gdf_cross_single["node_id_distance"] = distances

    # the end point is the node nearest to the representative point, other than a start point
    df_endpoint = nodes_gdf[~nodes_gdf.node_id.isin(gdf_cross_single.node_id)].copy()
    endpoint, _ = df_endpoint.sindex.nearest(rhws.representative_point, return_all=False, return_distance=True)
    endpoint = df_endpoint.node_id.iat[endpoint[1, 0]]

    graph = nx.Graph()
    for node_id, group in nodes_gdf.groupby("node_id"):
        graph.add_node(node_id, geometry=group.geometry.iat[0])
    for idx0, group in nodes_gdf.groupby(level=0):
        node_from, node_to = group.node_id
        line_geom = typing.cast(LineString, gdf_object.geometry.at[idx0])
        graph.add_edge(node_from, node_to, length=line_geom.length, geometry=line_geom)
    connect_components(graph)

    gdf_cross_single["shortest_path"] = shapely.geometry.GeometryCollection()  # ty: ignore[invalid-assignment]
    for startpoint in startpoints:
        path = nx.shortest_path(graph, source=startpoint, target=endpoint, weight="length", method="dijkstra")
        lines = [graph.get_edge_data(path[i], path[i + 1])["geometry"] for i in range(len(path) - 1)]
        merged = shapely.ops.linemerge(lines)
        gdf_cross_single.loc[gdf_cross_single.node_id == startpoint, "shortest_path"] = merged  # ty: ignore[invalid-assignment]
    return gdf_cross_single


def shortest_paths(crossings_path: Path, distance_interval: float = 50.0) -> gpd.GeoDataFrame:
    """The crossings of all boezems with their shortest path, as WKT, see `boezem_shortest_paths`.

    As in 2024, a boezem whose paths fail is left out, with a warning.
    """
    data = {name: gpd.read_file(crossings_path, layer=name) for name, _ in pyogrio.list_layers(crossings_path)}
    peilgebied = data["peilgebied"]
    gdf_rhws = peilgebied.loc[peilgebied["peilgebied_cat"] == 1].copy()
    gdf_rhws["representative_point"] = gdf_rhws.representative_point()
    gdf_rhws = gdf_rhws.reset_index(drop=True)
    crossings = data["crossings_hydroobject_filtered"]
    crossings = crossings.loc[crossings["agg_links_in_use"].astype(bool)].copy()

    out = []
    for index, rhws in gdf_rhws.iterrows():
        try:
            out.append(boezem_shortest_paths(rhws, crossings, data["hydroobject"], gdf_rhws.crs, distance_interval))
        except Exception as e:
            logger.warning(f"No shortest paths for boezem {index}, {rhws.globalid}: {e}")
    gdf_out = gpd.GeoDataFrame(pd.concat(out))
    gdf_out["shortest_path"] = gdf_out["shortest_path"].apply(lambda geom: dumps(geom) if geom is not None else None)
    return gdf_out
