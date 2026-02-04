"""
Find the shortest path between all basin crossings.

Per basin, the shortest paths between its crossings are considered as "doorgaand". The remaining paths (or water bodies)
are termed as "bergend". Depending on these classifications, profiles are differently determined and implemented within
Ribasim.
"""

import itertools
import logging

import geopandas as gpd
import momepy
import networkx as nx
import numpy as np
import shapely
import tqdm
from shapely.ops import nearest_points, split

from peilbeheerst_model.shortest_path import connect_linestrings_within_distance

LOG = logging.getLogger(__name__)


def simplify_geodata(
    gdf: gpd.GeoDataFrame, tolerance: float | None = None, col_in_use: str | None = None
) -> gpd.GeoDataFrame:
    """Simplify geospatial data.

    Simplification entails removing duplicates, and optionally 'almost duplicates': Removing geospatial data that is
    within a tolerance of any other entry.

    :param gdf: geospatial data
    :param tolerance: tolerance of uniqueness, defaults to None
    :param col_in_use: column-name with flagging of geospatial data that is to be used, defaults to None

    :type gdf: geopandas.GeoDataFrame
    :type tolerance: float, optional
    :type col_in_use: str, optional

    :return: simplified geospatial data
    :rtype: geopandas.GeoDataFrame
    """
    # logging: initial size of geospatial dataset
    _size = len(gdf)

    # simplify based on data flagging
    if col_in_use:
        assert col_in_use in gdf.columns
        gdf = gdf[gdf[col_in_use]].reset_index(drop=True)

    # simplify based on data proximity
    if tolerance:
        temp = gdf.sjoin(gdf, how="inner", predicate="dwithin", distance=tolerance)
        gdf = gdf.iloc[temp.groupby(level=0)["index_right"].first()]

    # remove duplicates
    gdf: gpd.GeoDataFrame = gdf.drop_duplicates(subset="geometry", ignore_index=True)

    # logging: size reduction
    LOG.info(f"Geo-data compressed: {_size} -> {len(gdf)}")

    # return "simplified" geospatial dataset
    return gdf


def split_hydro_objects(
    hydro_objects: gpd.GeoDataFrame, split_locations: gpd.GeoDataFrame, *, buffer: float = 1e-2, redraw: bool = False
) -> gpd.GeoDataFrame:
    """Split the hydro-objects at point-locations, enforcing node-locations on the graph.

    :param hydro_objects: geospatial data of hydro-objects
    :param split_locations: geospatial data of locations where to enforce a split in the hydro-objects

    :type hydro_objects: geopandas.GeoDataFrame
    :type split_locations: geopandas.GeoDataFrame

    :return: split hydro-objects
    :rtype: geopandas.GeoDataFrame
    """
    if redraw:
        hydro_objects = (
            gpd.GeoDataFrame(geometry=[hydro_objects.union_all()], crs=hydro_objects.crs)
            .explode()
            .reset_index(drop=True)
        )

    # select non-split locations
    temp = split_locations.buffer(buffer).intersection(hydro_objects.union_all())
    temp = temp[~temp.is_empty]
    subset = split_locations[split_locations.index.isin(temp[temp.type == "LineString"].index)]

    # split hydro-objects
    for p in tqdm.tqdm(subset.geometry.values, "Splitting hydro-objects at crossings"):
        dist = p.distance(hydro_objects.geometry.values)
        if sum(dist < buffer) == 1:
            (line,) = hydro_objects.loc[dist < buffer, "geometry"].values
            _p = line.interpolate(line.project(p))
            hydro_objects.loc[dist < buffer, "geometry"] = shapely.MultiLineString(split(line, _p))

    if redraw:
        hydro_objects = hydro_objects.explode().reset_index(drop=True)

    return hydro_objects


def fully_connected_network(
    hydro_objects: gpd.GeoDataFrame, *, buffer: float = 1e-2, reset_multi_index: bool = True
) -> gpd.GeoDataFrame:
    """Ensure the hydro-objects are properly connected.

    In using a buffer, hydro-objects are fully connected by searching for other hydro-objects at the endpoints within a
    buffer and align the endpoints of these hydro-objects.

    If any hydro-object is defined as a `MultiLineString`, it is "exploded" to `LineString`-objects. Generally, this
    results in a single `LineString`, which is assigned to the hydro-object. In case this results in multiple
    `LineString`-objects, the resulting multi-indexing (from the "exploding") can be reset to a (new) single indexing
    (`reset_multi_index=True`, default).

    :param hydro_objects: geospatial data of hydro-objects
    :param buffer: buffer-radius near endpoints within which other hydro-objects are connected, defaults to 1e-2
    :param reset_multi_index: reset multi-indexing to single-indexing, defaults to True

    :type hydro_objects: geopandas.GeoDataFrame
    :type buffer: float, optional
    :type reset_multi_index: bool, optional

    :return: fully connected network of hydro-objects
    :rtype: geopandas.GeoDataFrame
    """
    # check for "real" MultiLineString-entries
    ml = hydro_objects[hydro_objects.geometry.type == "MultiLineString"]
    if len(ml) > 0:
        ml["n_linestring"] = ml.geometry.apply(lambda mls: len(mls.geoms))
        n_ml = sum(ml["n_linestring"] > 1)
        if n_ml > 0:
            LOG.warning("MultiLineString-entries found when generating a graph")
    else:
        n_ml = 0

    # preprocess (Multi)LineString-entries
    hydro_objects = connect_linestrings_within_distance(hydro_objects, buffer)
    if n_ml == 0:
        hydro_objects.index = hydro_objects.index.droplevel(-1)
    elif reset_multi_index:
        hydro_objects.reset_index(drop=True, inplace=True)
    return hydro_objects


def generate_graph(hydro_objects: gpd.GeoDataFrame) -> nx.Graph:
    """Generate graph from network of hydro-objects.

    :param hydro_objects: geospatial data of hydro-objects
    :type hydro_objects: geopandas.GeoDataFrame

    :return: graph
    :rtype: networkx.Graph
    """
    # generate graph
    graph = momepy.gdf_to_nx(hydro_objects[["geometry"]], length="weight")
    return graph


def select_crossings(
    basin: shapely.Polygon | shapely.MultiPolygon,
    crossings: gpd.GeoDataFrame,
    *,
    buffer: float = 1e-2,
    internal: bool = True,
) -> list[shapely.Point]:
    """Select basin/network crossings at basin-borders.

    :param basin: Ribasim-basin
    :param crossings: crossings-data
    :param buffer: buffer-distance at basin-border in selecting crossings, defaults to 1e-2
    :param internal: include internal crossings, defaults to True

    :type basin: shapely.Polygon | shapely.MultiPolygon
    :type crossings: geopandas.GeoDataFrame
    :type buffer: float, optional
    :type internal: bool, optional

    :return: list of border crossings
    :rtype: list[shapely.Point]

    :raises TypeError: if `basin` is not a (Multi)Polygon
    """
    # crossings-selection polygon

    def selection(polygon: shapely.Polygon) -> shapely.Polygon:
        return (polygon if internal else polygon.exterior).buffer(buffer)

    # define buffered basin border(s)
    if isinstance(basin, shapely.Polygon):
        borders = selection(basin)
    elif isinstance(basin, shapely.MultiPolygon):
        borders = [selection(p) for p in basin.geoms]
    else:
        msg = f"Basin must be a (Multi)Polygon: {type(basin)=}"
        raise TypeError(msg)
    borders = shapely.MultiPolygon(borders)

    # select crossings at border(s)
    selection = crossings[crossings.within(borders)]
    return selection.geometry.tolist()


def crossing_to_node(graph: nx.Graph | shapely.MultiPoint, crossing: shapely.Point) -> tuple[float, float]:
    """Snap a crossing (shapely.Point) to its nearest graph-node.

    :param graph: graph (or graph-nodes)
    :param crossing: crossing location

    :type graph: networkx.Graph | shapely.MultiPoint
    :type crossing: shapely.Point

    :return: graph-node's coordinates
    :rtype: tuple[float, float]
    """
    if isinstance(graph, nx.Graph):
        graph = shapely.MultiPoint(graph.nodes)

    _, nearest_node = nearest_points(crossing, graph)
    return nearest_node.x, nearest_node.y


def full_graph_search(basin: shapely.Polygon, graph: nx.Graph, crossings: (shapely.Point, ...), **kwargs) -> bool:
    """Estimation whether a full graph search would be computationally more efficient than a source-target search.

    In case the graph is 'dense' and/or the number of crossings constitute for a percentage of the graph's nodes, a full
    pairwise comparison of the shortest paths between all the nodes of the graph is performed after which the nodes
    representing the crossings are selected. This is considered more efficient than searching every route between pairs
    of crossings.

    The graph is considered dense if there are 'a lot of' edges compared to the number of nodes. What 'a lot of' means,
    is calculated by the following comparison:

        |E| > a * |N|**2

    where `|E|` is the number of edges; `|N|` the number of nodes; and `a` a user-defined threshold (`dense_threshold`).

    Specifically for the case of searching the shortest paths in basins, the 'flatness' of the basin hints to the extent
    of overlapping routes: 'Flat' (or stretched-out) basins will generally have a single main route from which the edges
    sprawl to other connections. In such a basin, a full graph search would be computationally more efficient than a
    source-target search, as many shortest paths share the same edge(s).

    Thus, if any of the following three criteria are met, a full graph search is considered beneficial (i.e.,
    computationally more efficient than a source-target search):
     1. The graph is 'dense';
     2. The crossings cover 'a lot of' the graph's nodes;
     3. The basin is 'flat'.

    What constitutes for 'dense', 'a lot of', and 'flat' can be controlled by setting thresholds.

    :param basin: basin corresponding to the `graph`
    :param graph: graph corresponding to the `basin`
    :param crossings: crossings to pairwise connect
    :param kwargs: optional arguments (thresholds)

    :key dense_threshold: threshold above which the graph is considered 'dense' (`a` in the above formula),
        defaults to 0.5
    :key flatness_threshold: width-length ratio below which the basin is considered 'flat', defaults to 0.1
    :key min_coverage_threshold: minimum coverage of the graph's nodes by the crossings, defaults to 0.9
    :key max_coverage_threshold: maximum coverage of the graph's nodes by the crossings by which the 'flatness'
        criterion is disabled, defaults to 0.1

    :type basin: shapely.Polygon
    :type graph: networkx.Graph
    :type crossings: (shapely.Point, ...)

    :return: whether to perform a full graph search
    :rtype: bool
    """
    # optional arguments
    dense_threshold: float = kwargs.get("dense_threshold", 0.5)
    flatness_threshold: float = kwargs.get("flatness_threshold", 0.1)
    min_coverage_threshold: float = kwargs.get("min_coverage_threshold", 0.9)
    max_coverage_threshold: float = kwargs.get("max_coverage_threshold", 0.1)

    # validate optional arguments
    assert dense_threshold >= 0
    assert 0 <= flatness_threshold <= 1
    assert 0 <= min_coverage_threshold <= 1
    assert 0 <= max_coverage_threshold <= 1

    # density graph
    density = graph.number_of_edges() > dense_threshold * (graph.number_of_nodes() ** 2)

    # graph coverage
    graph_coverage = len(crossings) / graph.number_of_nodes()
    coverage = graph_coverage >= min_coverage_threshold

    # basin flatness
    _v = basin.length**2 - 16 * basin.area
    if _v < 0:
        flatness = False
    else:
        width = 0.25 * (basin.length - np.sqrt(basin.length**2 - 16 * basin.area))
        length = basin.area / width
        flatness = bool(width / length < flatness_threshold)
        if graph_coverage <= max_coverage_threshold:
            flatness = False

    # apply full graph search
    return any([density, coverage, flatness])


def find_flow_routes(
    graph: nx.Graph, crossings: (shapely.Point, ...), *, use_full_graph: bool = False
) -> set[tuple[tuple, tuple]]:
    """Find all shortest routes between combinations of crossings.

    :param graph: graph
    :param crossings: (border) crossings
    :param use_full_graph: search for the shortest paths between all pairs of nodes and selecting the crossings instead
        of searching for the shortest paths between the pairs of crossings, defaults to False

    :type graph: networkx.Graph
    :type crossings: (shapely.Point, ...)
    :type use_full_graph: bool, optional

    :return: set of graph-edges that are part of at least one shortest route between crossings
    :rtype: set[tuple[tuple, tuple]]
    """
    if len(crossings) > graph.number_of_nodes():
        LOG.warning(f"More crossings ({len(crossings)}) than graph-nodes ({graph.number_of_nodes()})")
    desc = "Finding main routes"

    # initiate working variables
    flow_routes = set()
    mp_graph = shapely.MultiPoint(graph.nodes)
    set_crossings = set(crossings)
    n_combinations = int(0.5 * len(set_crossings) * (len(set_crossings) - 1))

    # no shortest paths to be found
    if n_combinations == 0:
        return flow_routes

    # find the shortest paths
    if use_full_graph:
        # shortest path between all nodes
        paths = dict(nx.shortest_path(graph, weight="weight"))

        # select shortest paths between crossings
        for c1, c2 in tqdm.tqdm(itertools.combinations(set_crossings, 2), f"{desc} (F)", n_combinations):
            try:
                # noinspection PyTypeChecker
                path = paths[crossing_to_node(mp_graph, c1)][crossing_to_node(mp_graph, c2)]
            except KeyError:
                LOG.debug(f"No path between {c1} and {c2}")
            else:
                flow_routes.update(itertools.pairwise(path))
    else:
        # loop over all combinations of (border) crossings (without order)
        for c1, c2 in tqdm.tqdm(itertools.combinations(set_crossings, 2), f"{desc} (S)", n_combinations):
            try:
                path = nx.shortest_path(
                    graph,
                    source=crossing_to_node(mp_graph, c1),
                    target=crossing_to_node(mp_graph, c2),
                    weight="weight",
                    method="dijkstra",
                )
            except nx.NetworkXNoPath as e:
                LOG.debug(e)
            else:
                flow_routes.update(itertools.pairwise(path))

    # return set of graph-edges
    return flow_routes


def label_flow_hydro_objects(
    hydro_objects: gpd.GeoDataFrame, graph: nx.Graph, flow_routes: set[tuple[tuple, tuple]]
) -> (int, ...):
    """Label hydro-objects as being part of the main flow route (or not).

    :param hydro_objects: geospatial data of hydro-objects
    :param graph: graph
    :param flow_routes: set of graph-edges constituting the main routes

    :type hydro_objects: geopandas.GeoDataFrame
    :type graph: networkx.Graph
    :type flow_routes: set[tuple[tuple, tuple]]

    :return: indices of hydro-objects on the main-route
    :rtype: (int, ...)
    """
    routing_edges = [data["geometry"] for nodes in flow_routes for data in graph.get_edge_data(*nodes).values()]
    indices = hydro_objects[hydro_objects.geometry.isin(routing_edges)].index.values
    return indices


def label_main_routes(
    basins: gpd.GeoDataFrame, hydro_objects: gpd.GeoDataFrame, crossings: gpd.GeoDataFrame, **kwargs
) -> gpd.GeoDataFrame:
    """Full pipeline function labelling the hydro-objects as main route (or not).

    This pipeline consists of:
     1. Ensure the hydro-objects form a fully connected network (if possible): `fully_connected_network()`
     2. Generate the graph of the hydro-objects: `generate_graph()`
     3. Select crossings near basin borders: `select_crossings()`
     4. Find the main flow routes in the graph, being the shortest paths between border crossings: `find_flow_routes()`
     5. Label hydro-objects as being part of the main flow route (or not): `label_flow_hydro_objects()`

    :param basins: Ribasim-basins
    :param hydro_objects: geospatial data of hydro-objects
    :param crossings: crossings-data
    :param kwargs: optional arguments

    :key buffer: buffer-radius near endpoints within which other hydro-objects are connected, and buffer-distance at basin-border in selecting crossings, defaults to 1e-2
    :key reset_multi_index: reset_multi_index: reset multi-indexing to single-indexing, defaults to True

    :return: hydro-objects with main routes labelled
    :rtype: geopandas.GeoDataFrame
    """
    # optional arguments
    buffer: float = kwargs.get("buffer", 1e-2)
    reset_multi_index: bool = kwargs.get("reset_multi_index", True)

    # prepare hydro-objects: ensure hydro-objects are connected
    hydro_objects = fully_connected_network(hydro_objects, buffer=buffer, reset_multi_index=reset_multi_index)

    # generate network graph
    graph = generate_graph(hydro_objects)

    # get crossings at basin-borders
    border_crossings = select_crossings(
        shapely.MultiPolygon(basins.explode().geometry.values), crossings, buffer=buffer
    )

    # find the shortest routes between border crossings
    edge_nodes = find_flow_routes(graph, border_crossings)
    hydro_objects = label_flow_hydro_objects(hydro_objects, graph, edge_nodes)

    # return labelled hydro-objects
    return hydro_objects
