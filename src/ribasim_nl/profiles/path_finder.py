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
from shapely.ops import nearest_points

from peilbeheerst_model.shortest_path import connect_linestrings_within_distance

LOG = logging.getLogger(__name__)


def fully_connected_network(
    hydro_objects: gpd.GeoDataFrame, *, buffer: float = 1e-2, reset_multi_index: bool = True
) -> gpd.GeoDataFrame:
    # check for "real" MultiLineString-entries
    ml = hydro_objects[hydro_objects.geometry.type == "MultiLineString"]
    ml["n_linestring"] = ml.geometry.apply(lambda mls: len(mls.geoms))
    n_ml = sum(ml["n_linestring"] > 1)
    if n_ml > 0:
        LOG.warning("MultiLineString-entries found when generating a graph")

    # preprocess (Multi)LineString-entries
    hydro_objects = connect_linestrings_within_distance(hydro_objects.explode(), buffer)
    if n_ml == 0:
        hydro_objects.index = hydro_objects.index.droplevel(-1)
    elif reset_multi_index:
        hydro_objects.reset_index(drop=True, inplace=True)
    return hydro_objects


def generate_graph(hydro_objects: gpd.GeoDataFrame, **kwargs) -> nx.Graph:
    # optional arguments
    columns: (str, ...) = kwargs.get("columns", ())

    # define data included in the network
    if "geometry" not in columns:
        columns = *columns, "geometry"
    network = hydro_objects[list(columns)]

    # generate graph
    graph = momepy.gdf_to_nx(network, length="weight")
    return graph


def select_crossings(
    basin: shapely.Polygon | shapely.MultiPolygon, crossings: gpd.GeoDataFrame, *, buffer: float = 1e-2
) -> list[shapely.Point]:
    # define buffered basin border(s)
    if isinstance(basin, shapely.Polygon):
        borders = (basin.exterior.buffer(buffer),)
    elif isinstance(basin, shapely.MultiPolygon):
        borders = [p.exterior.buffer(buffer) for p in basin.geoms]
    else:
        raise TypeError
    borders = shapely.MultiPolygon(borders)

    # select crossings at border(s)
    selection = crossings[crossings.within(borders)]
    return selection.geometry.tolist()


def crossing_to_node(graph: nx.Graph | shapely.MultiPoint, crossing: shapely.Point) -> tuple[float, float]:
    if isinstance(graph, nx.Graph):
        graph = shapely.MultiPoint(graph.nodes)

    _, nearest_node = nearest_points(crossing, graph)
    return nearest_node.x, nearest_node.y


def find_flow_routes(graph: nx.Graph, crossings: (shapely.Point, ...)) -> set[tuple[tuple, tuple]]:
    flow_routes = set()

    mp_graph = shapely.MultiPoint(graph.nodes)

    for c1, c2 in itertools.combinations(set(crossings), 2):
        try:
            path = nx.shortest_path(
                graph, source=crossing_to_node(mp_graph, c1), target=crossing_to_node(mp_graph, c2), weight="weight"
            )
        except nx.NetworkXNoPath as e:
            LOG.info(e)
            continue
        edges = list(itertools.pairwise(path))
        flow_routes.update(edges)

    return flow_routes


def label_flow_hydro_objects(
    hydro_objects: gpd.GeoDataFrame,
    graph: nx.Graph,
    flow_routes: set[tuple[tuple, tuple]],
    search_column: str = "geometry",
) -> gpd.GeoDataFrame:
    routing_edges = [data[search_column] for nodes in flow_routes for data in graph.get_edge_data(*nodes).values()]
    hydro_objects["main-route"] = hydro_objects[search_column].isin(routing_edges)
    return hydro_objects


def label_main_routes(
    basins: gpd.GeoDataFrame, hydro_objects: gpd.GeoDataFrame, crossings: gpd.GeoDataFrame, **kwargs
) -> gpd.GeoDataFrame:
    # optional arguments
    buffer: float = kwargs.get("buffer", 1e-2)
    reset_multi_index: bool = kwargs.get("reset_multi_index", True)
    column_id: str = kwargs.get("column_id", "geometry")

    # prepare hydro-objects: ensure hydro-objects are connected
    hydro_objects = fully_connected_network(hydro_objects, buffer=buffer, reset_multi_index=reset_multi_index)

    # generate network graph
    graph = generate_graph(hydro_objects, columns=(column_id,))

    # get crossings at basin-borders
    border_crossings = select_crossings(shapely.MultiPolygon(basins.explode().geometry.values), crossings)

    # find the shortest routes between border crossings
    edge_nodes = find_flow_routes(graph, border_crossings)
    main_routes = label_flow_hydro_objects(hydro_objects, graph, edge_nodes, search_column=column_id)

    # return labelled hydro-objects
    return main_routes
