"""Snapping Ribasim-scheme onto network of hydro-objects.

Basin- and Connector-nodes as well as the Links themselves will be snapped to the hydro-objects. That is, if the hydro-
objects are sufficiently defined.

This "network-snapping" relies heavily on the network-based profile-generation: src/ribasim_nl/ribasim_nl/profiles/*.py.

NOTE: This code can be considered as duplicate of src/ribasim_nl/ribasim_nl/link_geometries.py, which is true; except
that this code is intended as a performance boost. The `fix_link_geometries`-function costs order 30+ minutes per water
authority, which is unacceptably expensive.
"""

import itertools
import logging
import pathlib
import typing

import geopandas as gpd
import networkx as nx
import shapely
import tqdm
from ribasim_nl.profiles import path_finder
from shapely.ops import unary_union

from ribasim_nl import Model

LOG = logging.getLogger(__name__)

CONNECTOR_NODE_TYPES = "ManningResistance", "Outlet", "Pump"


class NetworkSnappingError(Exception):
    pass


def get_graph(profiles_path: pathlib.Path) -> tuple[gpd.GeoDataFrame, nx.Graph]:
    fn = profiles_path / "intermediate" / "graph.gpkg"
    if not fn.exists():
        msg = f"File not found: {fn}\nRegenerate the profiles with `export_intermediate_output=True` to create {fn}"
        raise FileNotFoundError(msg)

    hydro_objects = gpd.read_file(fn, layer="lines")
    graph = path_finder.generate_graph(hydro_objects)

    return hydro_objects, graph


def relocate_nodes(model: Model, nodes: gpd.GeoDataFrame) -> Model:
    assert model.node.df is not None

    tmp = model.node.df.copy()
    tmp.loc[nodes.index, "geometry"] = nodes["geometry"]
    model.node.df = tmp.copy()  # pyrefly: ignore[bad-assignment]
    return model


def snap_basins(
    model: Model, hydro_objects: gpd.GeoDataFrame, max_distance: float | None = None, main_route_only: bool = False
) -> Model:
    # model initiation check
    assert model.basin.node.df is not None  # pyrefly: ignore[missing-attribute]
    assert model.basin.area.df is not None

    # copy datasets
    nodes = typing.cast(gpd.GeoDataFrame, model.basin.node.df).copy(deep=True)
    areas = typing.cast(gpd.GeoDataFrame, model.basin.area.df).copy(deep=True)

    # selection of non-storing basins only
    nodes = nodes[nodes["meta_categorie"] != "bergend"]
    areas = areas[areas["node_id"].isin(nodes.index)]
    areas.set_crs(nodes.crs, inplace=True)  # pyrefly: ignore[no-matching-overload]

    # selection and grouping of hydro-objects (per basin)
    if main_route_only:
        hydro_objects = hydro_objects[hydro_objects["main-route"]]
    ho_basin = gpd.sjoin(hydro_objects, areas[["node_id", "geometry"]], how="inner", predicate="within").reset_index(
        drop=True
    )[["node_id", "geometry"]]

    # merge hydro-objects to MultiLineString (per basin)
    ho_merged = ho_basin.groupby("node_id")["geometry"].apply(unary_union).rename("hydro")

    # couple grouped hydro-objects to basin-nodes
    out = typing.cast(gpd.GeoDataFrame, nodes.merge(ho_merged, how="left", left_index=True, right_index=True))
    out.set_crs(nodes.crs, inplace=True)  # pyrefly: ignore[no-matching-overload]

    # validity of hydro-objects
    valid = out["hydro"].notna()
    if not valid.any():
        msg = "No valid hydro-objects found for any basin"
        raise NetworkSnappingError(msg)

    # snap basin-nodes
    original_nodes = typing.cast(gpd.GeoSeries, out.loc[valid, "geometry"])
    shortest = gpd.GeoSeries(
        shapely.shortest_line(original_nodes.values, typing.cast(gpd.GeoSeries, out.loc[valid, "hydro"].values)),
        index=original_nodes.index,
        crs=nodes.crs,
    )
    snapped_nodes = gpd.GeoSeries(
        shapely.get_point(shortest.values, 1),
        index=original_nodes.index,
        crs=nodes.crs,
    )
    distances = original_nodes.distance(snapped_nodes)

    # maximum relocation distance
    if max_distance is not None:
        too_far = distances > max_distance
        snapped_nodes = snapped_nodes.where(~too_far, original_nodes)

    # relocate basin-nodes
    out.loc[valid, "geometry"] = snapped_nodes
    # TODO: Snapping translation

    # update Ribasim model
    return relocate_nodes(model, out)


def snap_connectors(
    model: Model,
    hydro_objects: gpd.GeoDataFrame,
    max_distance: float | None = None,
    tolerance: float = 10.0,
    main_route_only: bool = False,
) -> Model:
    """Snap connector-nodes to hydro-objects near basin-area boundaries.

    :param model: Ribasim model
    :param hydro_objects: hydro-objects geo-data
    :param max_distance: maximum distance between original location and snapping location, defaults to None
    :param tolerance: buffer distance around basin-area boundaries to search for hydro-objects, defaults to 10.0
    :param main_route_only: only include hydro-objects that are flagged as the main route, defaults to False

    :return: updated Ribasim model
    """
    # model initiation check
    assert model.node.df is not None
    assert model.basin.area.df is not None

    # copy datasets
    nodes = typing.cast(gpd.GeoDataFrame, model.node.df.copy(deep=True))
    areas = typing.cast(gpd.GeoDataFrame, model.basin.area.df.copy(deep=True))

    # selection of connector-nodes (ex. connectors between flowing and storing basin nodes)
    # TODO: Ensure that the `meta_categorie` remains set for ManningResistance-nodes of storing basins
    # nodes = nodes[(nodes["node_type"].isin(CONNECTOR_NODE_TYPES)) & (nodes.index == nodes["meta_node_id"])]
    nodes = nodes[(nodes["node_type"].isin(CONNECTOR_NODE_TYPES)) & (nodes["meta_categorie"] != "bergend")]
    if nodes.empty:
        msg = "No connector nodes found."
        raise NetworkSnappingError(msg)

    # buffered basin-area boundaries
    edges = areas.copy()
    edges["geometry"] = edges.geometry.boundary.buffer(tolerance)

    # selection of hydro-objects (near basin-area boundaries)
    if main_route_only:
        hydro_objects = hydro_objects[hydro_objects["main-route"]]
    ho_edges = gpd.sjoin(
        hydro_objects[["geometry"]], edges[["geometry"]], how="inner", predicate="intersects"
    ).reset_index(drop=True)[["geometry"]]
    ho_edges = ho_edges.drop_duplicates()

    if ho_edges.empty:
        msg = "No valid hydro-objects found near any basin-area boundary"
        raise NetworkSnappingError(msg)

    # merge all edge-hydro-objects into single geometry for snapping
    ho_merged = unary_union(ho_edges["geometry"])

    # snap connector-nodes
    original_nodes = typing.cast(gpd.GeoSeries, nodes["geometry"])
    shortest = gpd.GeoSeries(
        shapely.shortest_line(original_nodes.values, ho_merged),
        index=original_nodes.index,
        crs=nodes.crs,
    )
    snapped_nodes = gpd.GeoSeries(
        shapely.get_point(shortest.values, 1),
        index=original_nodes.index,
        crs=nodes.crs,
    )
    distances = original_nodes.distance(snapped_nodes)

    # maximum relocation distance
    if max_distance is not None:
        too_far = distances > max_distance
        snapped_nodes = snapped_nodes.where(~too_far, original_nodes)

    # relocate connector-nodes
    nodes["geometry"] = snapped_nodes

    # update Ribasim model
    return relocate_nodes(model, nodes)


def snap_links(model: Model, graph: nx.Graph) -> Model:
    """Snap link-geometries onto the hydro-object network.

    For each link, the shortest path along the hydro-objects between its from-node and to-node is found and used as
    the new link geometry. Links for which no route can be found retain their original geometry.

    :param model: Ribasim model
    :param graph: graph of the hydro-object network

    :return: updated Ribasim model
    """
    # model initiation check
    assert model.node.df is not None
    assert model.link.df is not None

    # copy datasets
    nodes = typing.cast(gpd.GeoDataFrame, model.node.df.copy(deep=True))
    links = typing.cast(gpd.GeoDataFrame, model.link.df.copy(deep=True))

    # filter Flow-links
    links = links[(links["link_type"] == "flow") & (links["meta_categorie"] != "bergend")]

    # build edge-geometry lookup (graph-edge -> LineString)
    link_graph_table: dict[tuple, shapely.LineString] = {}
    for u, v, data in graph.edges(data=True):
        geom = data.get("geometry")
        if geom is not None:
            link_graph_table[(u, v)] = geom
            link_graph_table[(v, u)] = geom

    # graph-nodes as MultiPoint (for reuse in crossing_to_node)
    graph_as_multipoint = shapely.MultiPoint(graph.nodes)

    # route each link along the hydro-object network
    # route each link along the hydro-object network
    snapped_links: dict[int, shapely.LineString] = {}
    for i, link in tqdm.tqdm(links.iterrows(), "Snapping links", len(links)):
        # locate from- and to-node on the graph
        from_point = typing.cast(shapely.Point, nodes.loc[link["from_node_id"], "geometry"])
        to_point = typing.cast(shapely.Point, nodes.loc[link["to_node_id"], "geometry"])
        from_node = path_finder.point_to_graph_node(graph_as_multipoint, from_point)
        to_node = path_finder.point_to_graph_node(graph_as_multipoint, to_point)

        # find the shortest path
        try:
            path = nx.shortest_path(graph, source=from_node, target=to_node, weight="weight")
        except (nx.NetworkXNoPath, nx.NodeNotFound) as e:
            LOG.debug(f"Could not snap link {link.name}: {e}")
            continue

        # assemble geometry from path-edges
        segments = []
        for u, v in itertools.pairwise(path):
            geom = link_graph_table.get((u, v))
            if geom is not None:
                segments.append(geom)

        if not segments:
            continue

        # merge segments into single LineString
        snapped_links[i] = shapely.ops.linemerge(shapely.MultiLineString(segments))  # pyrefly: ignore[unsupported-operation]

    # update link-geometries
    links.loc[snapped_links.keys(), "geometry"] = gpd.GeoSeries(snapped_links)

    # update Ribasim model
    tmp = model.link.df.copy()
    tmp.loc[links.index, "geometry"] = links["geometry"]
    model.link.df = tmp.copy()  # pyrefly: ignore[bad-assignment]

    return model


def relocate_link_endpoints(model: Model) -> Model:
    """Ensure link endpoints match current node locations.

    For snapped links, the first and last coordinates are updated to the current node locations while preserving
    the intermediate coordinates. For unsnapped (straight-line) links, this effectively redraws them between the
    relocated nodes.

    :param model: Ribasim model

    :return: updated Ribasim model
    """
    # model initiation check
    assert model.node.df is not None
    assert model.link.df is not None

    # copy datasets
    nodes = typing.cast(gpd.GeoDataFrame, model.node.df.copy(deep=True))
    links = typing.cast(gpd.GeoDataFrame, model.link.df.copy(deep=True))

    # current node locations per link
    from_coords = shapely.get_coordinates(links["from_node_id"].map(nodes["geometry"]).values)  # pyrefly: ignore[bad-argument-type]
    to_coords = shapely.get_coordinates(links["to_node_id"].map(nodes["geometry"]).values)  # pyrefly: ignore[bad-argument-type]

    # replace first and last coordinate of each link geometry
    new_geoms = []
    for i, (_, link) in enumerate(links.iterrows()):
        coords = shapely.get_coordinates(link.geometry)
        coords[0] = from_coords[i]
        coords[-1] = to_coords[i]
        new_geoms.append(shapely.LineString(coords))

    # update link-geometries
    links["geometry"] = gpd.GeoSeries(new_geoms, index=links.index, crs=links.crs)

    # update Ribasim model
    tmp = model.link.df.copy()
    tmp["geometry"] = links["geometry"]
    model.link.df = tmp.copy()  # pyrefly: ignore[bad-assignment]

    return model


def snap_model(
    model: Model,
    profiles_path: pathlib.Path,
    max_distance: float | None = None,
    tolerance: float = 10.0,
    main_route_only: bool = False,
) -> Model:
    hydro_objects, graph = get_graph(profiles_path)

    model = snap_basins(model, hydro_objects, max_distance=max_distance, main_route_only=main_route_only)
    model = snap_connectors(
        model, hydro_objects, max_distance=max_distance, tolerance=tolerance, main_route_only=main_route_only
    )
    model = snap_links(model, graph)
    model = relocate_link_endpoints(model)

    return model
