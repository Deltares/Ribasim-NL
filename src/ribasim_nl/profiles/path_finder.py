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
    gdf: gpd.GeoDataFrame, *, buffer: float = 1e-2, reset_multi_index: bool = True
) -> gpd.GeoDataFrame:
    # check for "real" MultiLineString-entries
    ml = gdf[gdf.geometry.type == "MultiLineString"]
    ml["n_linestring"] = ml.geometry.apply(lambda mls: len(mls.geoms))
    n_ml = sum(ml["n_linestring"] > 1)
    if n_ml > 0:
        LOG.warning("MultiLineString-entries found when generating a graph")

    # preprocess (Multi)LineString-entries
    gdf = connect_linestrings_within_distance(gdf.explode(), buffer)
    if n_ml == 0:
        gdf.index = gdf.index.droplevel(-1)
    elif reset_multi_index:
        gdf.reset_index(drop=True, inplace=True)
    return gdf


def generate_graph(gdf: gpd.GeoDataFrame, **kwargs) -> nx.Graph:
    # optional arguments
    columns: (str, ...) = kwargs.get("columns", ())

    # define data included in the network
    network = gdf[[*columns, "geometry"]]

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


def get_flow_hydro_objects(
    gdf: gpd.GeoDataFrame, graph: nx.Graph, flow_routes: set[tuple[tuple, tuple]], search_column: str = "geometry"
) -> gpd.GeoDataFrame:
    if search_column == "geometry":
        selection = [data["geometry"] for nodes in flow_routes for data in graph.get_edge_data(*nodes).values()]
    else:
        selection = [graph.get_edge_data(*nodes)[search_column] for nodes in flow_routes]

    flow_hydro_objects = gdf[gdf[search_column].isin(selection)]
    return flow_hydro_objects
