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
import shapely
import tqdm
from shapely.ops import nearest_points, split

from peilbeheerst_model.shortest_path import connect_linestrings_within_distance

LOG = logging.getLogger(__name__)


def simplify_geodata(gdf: gpd.GeoDataFrame, tolerance: float | None = None) -> gpd.GeoDataFrame:
    """Simplify geospatial data.

    Simplification entails removing duplicates, and optionally 'almost duplicates': Removing geospatial data that is
    within a tolerance of any other entry.

    :param gdf: geospatial data
    :param tolerance: tolerance of uniqueness, defaults to None

    :type gdf: geopandas.GeoDataFrame
    :type tolerance: float, optional

    :return: simplified geospatial data
    :rtype: geopandas.GeoDataFrame
    """
    _size = len(gdf)
    if tolerance:
        temp = gdf.sjoin(gdf, how="inner", predicate="dwithin", distance=tolerance)
        gdf = gdf.iloc[temp.groupby(level=0)["index_right"].first()]

    gdf: gpd.GeoDataFrame = gdf.drop_duplicates(subset="geometry", ignore_index=True)
    LOG.info(f"Geo-data compressed: {_size} -> {len(gdf)}")
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
            hydro_objects.loc[dist < buffer, "geometry"] = shapely.MultiLineString(split(line, p))

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


def find_flow_routes(graph: nx.Graph, crossings: (shapely.Point, ...)) -> set[tuple[tuple, tuple]]:
    """Find all shortest routes between combinations of crossings.

    :param graph: graph
    :param crossings: border crossings

    :type graph: networkx.Graph
    :type crossings: (shapely.Point, ...)

    :return: set of graph-edges that are part of at least one shortest route between crossings
    :rtype: set[tuple[tuple, tuple]]
    """
    # initiate working variables
    flow_routes = set()
    mp_graph = shapely.MultiPoint(graph.nodes)

    # loop over all combinations of (border) crossings (without order)
    for c1, c2 in tqdm.tqdm(itertools.combinations(set(crossings), 2), "Finding main routes"):
        try:
            path = nx.shortest_path(
                graph, source=crossing_to_node(mp_graph, c1), target=crossing_to_node(mp_graph, c2), weight="weight"
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
