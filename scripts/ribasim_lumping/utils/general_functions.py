import configparser
import os
from collections import OrderedDict
from pathlib import Path
from typing import Callable, Dict, List, Tuple, TypeVar, Union

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
from pydantic import BaseModel
from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.ops import nearest_points, snap


def replace_string_in_file(file_path, string, new_string):
    with open(file_path, "r") as file:
        content = file.read()
    content = content.replace(string, new_string)
    with open(file_path, "w") as file:
        file.write(content)


def find_file_in_directory(directory, file_name, start_end='end') -> Path:
    """Find path of file in directory"""
    selected_file = None
    for root, dirs, files in os.walk(directory):
        # print(root, dirs, files)
        for file in files:
            if start_end == 'end':
                if file.endswith(file_name):
                    selected_file = os.path.join(root, file)
            elif start_end == 'start':
                if file.startswith(file_name):
                    selected_file = os.path.join(root, file)
    # print(selected_file)
    if selected_file is None:
        return None
    return Path(selected_file)


def find_directory_in_directory(directory, dir_name) -> Path:
    """Find path of subdirectory in directory"""
    selected_dir = ""
    for root, directories, files in os.walk(directory):
        for directory in directories:
            if directory.endswith(dir_name):
                selected_dir = os.path.join(root, directory)
    return Path(selected_dir)


class MultiOrderDict(OrderedDict):
    _unique = dict()
    def __setitem__(self, key, val):
        if isinstance(val, dict):
            if key not in self._unique:
                self._unique[key] = 0
            else:
                self._unique[key] += 1
            key += str(self._unique[key])
        OrderedDict.__setitem__(self, key, val)


def read_ini_file_with_similar_sections(file_path, section_name):
    config = configparser.ConfigParser(dict_type=MultiOrderDict, strict=False)
    config.read(file_path)
    section_keys = [k for k in config.keys() if k.startswith(section_name)]
    section_name_df = pd.DataFrame([config._sections[k]  for k in section_keys])
    return section_name_df


def get_point_parallel_to_line_near_point(
    line: LineString, 
    reference_point: Point, 
    side: str = 'left', 
    distance: int = 5
):
    parallel_line = line.parallel_offset(distance, 'left')
    new_point = snap(reference_point, parallel_line, distance*1.1)
    return new_point


def get_points_on_linestrings_based_on_distances(
    linestrings: gpd.GeoDataFrame, 
    linestring_id_column: str,
    points: gpd.GeoDataFrame, 
    points_linestring_id_column: str, 
    points_distance_column: str
) -> gpd.GeoDataFrame:
    """Get point location (gdf) at certain distance along linestring (gdf)"""
    points = linestrings.merge(
        points, 
        how='inner', 
        left_on=linestring_id_column, 
        right_on=points_linestring_id_column
    )
    points['geometry'] = points['geometry'].interpolate(points[points_distance_column])
    return points


def find_nearest_nodes(
    search_locations: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame, id_column: str
) -> gpd.GeoDataFrame:
    nearest_node_ids = []
    for index, row in search_locations.iterrows():
        point = row.geometry
        multipoint = nodes.drop(index, axis=0).geometry.unary_union
        _, nearest_geom = nearest_points(point, multipoint)
        nearest_node = nodes.loc[nodes["geometry"] == nearest_geom]
        nearest_node_ids.append(nearest_node[id_column].iloc[0])
    projected_points = gpd.GeoDataFrame(
        data={id_column: nearest_node_ids},
        geometry=search_locations["geometry"],
        crs=search_locations.crs,
    )
    return projected_points


def find_nearest_edges_no(
    gdf1: gpd.GeoDataFrame,
    gdf2: gpd.GeoDataFrame,
    new_column: str,
    subset: str = None
) -> gpd.GeoDataFrame: 
    if subset is not None and subset in gdf1.columns:
        gdf1_total = None
        subsets = gdf1[subset].unique()
        for i, sub in enumerate(subsets):
            gdf1_sub = gdf1[gdf1[subset]==sub].copy()
            gdf2_sub = gdf2[gdf2[subset]==sub].copy()
            ind_gdf1, ind_gdf2  = gdf2_sub['geometry'].sindex.nearest(gdf1_sub['geometry'], return_all=False)
            gdf1_sub[new_column] = gdf2_sub.index[ind_gdf2]
            if gdf1_total is None:
                gdf1_total = gdf1_sub.copy()
            else:
                gdf1_total = pd.concat([gdf1_total, gdf1_sub])
        return gdf1_total
    ind_gdf1, ind_gdf2  = gdf2['geometry'].sindex.nearest(gdf1['geometry'], return_all=False)
    gdf1[new_column] = gdf2.index[ind_gdf2]
    return gdf1


def find_nearest_edges(
    search_locations: gpd.GeoDataFrame,
    edges: gpd.GeoDataFrame,
    id_column: str,
    selection: str = None,
    tolerance: int = 100,
) -> gpd.GeoDataFrame:
    """Function to find nearest linestring including nearest location on edge"""
    bbox = search_locations.bounds + [-tolerance, -tolerance, tolerance, tolerance]
    hits = bbox.apply(lambda row: list(edges.sindex.intersection(row)), axis=1)
    tmp = pd.DataFrame({
        "split_node_i": np.repeat(hits.index, hits.apply(len)),
        "edge_no": np.concatenate(hits.values),
    })
    if tmp.empty:
        return None
    if selection is not None and selection in search_locations and selection in edges:
        tmp = tmp.merge(
            search_locations.reset_index()[selection],
            how="outer",
            left_on="split_node_i",
            right_index=True,
        ).rename(columns={selection: f"{selection}_x"})
    tmp = tmp.merge(
        edges, 
        how="inner", 
        left_on="edge_no", 
        right_on="edge_no"
    )
    tmp = tmp.join(search_locations.geometry.rename("point"), on="split_node_i")
    tmp = gpd.GeoDataFrame(tmp, geometry="geometry", crs=search_locations.crs)

    tmp["snap_dist"] = tmp.geometry.distance(gpd.GeoSeries(tmp.point))
    tmp = tmp.loc[tmp.snap_dist <= tolerance]
    tmp = tmp.sort_values(by=["snap_dist"])

    if selection is not None and selection in search_locations and selection in edges:
        tmp = tmp[tmp[selection] == tmp[f"{selection}_x"]].copy()
        tmp = tmp.drop(columns=[f"{selection}_x"])

    tmp_points = tmp.groupby("split_node_i").first()
    tmp_points["projection"] = tmp_points.apply(
        lambda x: nearest_points(x.geometry, x.point)[0], axis=1
    )
    tmp_points["projection_x"] = tmp_points["projection"].apply(lambda x: x.x)
    tmp_points["projection_y"] = tmp_points["projection"].apply(lambda x: x.y)
    tmp_points = (
        tmp_points[[id_column, "projection_x", "projection_y", "point"]]
        .rename(columns={"point": "geometry"})
        .reset_index(drop=True)
    )

    projected_points = gpd.GeoDataFrame(tmp_points, geometry="geometry", crs=search_locations.crs)
    return projected_points


def create_objects_gdf(
    data: Dict,
    xcoor: List[float],
    ycoor: List[float],
    edges_gdf: gpd.GeoDataFrame,
    selection: str = None,
    tolerance: int = 100,
):
    crs = edges_gdf.crs
    gdf = gpd.GeoDataFrame(
        data=data, geometry=gpd.points_from_xy(xcoor, ycoor), crs=crs
    )
    projected_points = find_nearest_edges(
        search_locations=gdf,
        edges=edges_gdf,
        id_column="edge_no",
        selection=selection,
        tolerance=tolerance,
    )
    if projected_points is None:
        return None
    gdf = gpd.GeoDataFrame(
        data=(
            gdf.drop(columns="geometry").merge(
                projected_points, 
                how="outer", 
                left_index=True, 
                right_index=True
            )
        ),
        geometry="geometry",
        crs=crs,
    )
    return gdf


def read_geom_file(
        filepath: Path, 
        layer_name: str = None,
        crs: int = 28992,
        explode_geoms: bool = True,
        remove_z_dim: bool = False,
        code_column: str = None
    ) -> gpd.GeoDataFrame:
    """
    Read file with geometries. If geopackage, supply layer_name.

    Parameters
    ----------
    filepath : Path
        Path to file containing geometries
    layer_name : str
        Layer name in geopackage. Needed when file is a geopackage
    crs : int
        CRS EPSG code. Default 28992 (RD New)
    explode_geoms : bool
        Explode multi-part geometries into single part. Default True
    remove_z_dim : bool
        Remove Z dimension from geometries. Only possible for single part Point and LineString.
        Default False

    Returns
    -------
    GeoDataFrame
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Could not find file {os.path.abspath(filepath)}")
    if str(filepath).lower().endswith('.gpkg'):
        if layer_name not in fiona.listlayers(filepath):
            return None
        gdf = gpd.read_file(filepath, layer=layer_name, crs=crs)
    else:
        gdf = gpd.read_file(filepath, crs=crs)
    if explode_geoms:
        gdf = gdf.explode(ignore_index=True)  # explode to transform multi-part geoms to single
    if remove_z_dim:
        gdf.geometry = [Point(g.coords[0][:2]) if isinstance(g, Point) else LineString([c[:2] for c in g.coords])
                        for g in gdf.geometry.values]  # remove possible Z dimension
    
    if code_column is not None: 
        if code_column not in gdf.columns:
            code_column = code_column.lower()
        if code_column not in gdf.columns:
            code_column = code_column.upper()
        if code_column not in gdf.columns:
            gdf = gdf.reset_index(drop=True).reset_index()
            code_column = "index"
        gdf = gdf[[code_column, "geometry"]]
        gdf.rename(columns={code_column: 'code'}, inplace=True)

    return gdf


def log_and_remove_duplicate_geoms(gdf: gpd.GeoDataFrame, colname: str = None) -> gpd.GeoDataFrame:
    """
    Log and remove duplicate geometries from GeoDataFrame

    Parameters
    ----------
    filepath : Path
        GeoDataFrame containing geometries
    colname : str
        Column name to use for logging. Default to None which will use 1st column in gdf

    Returns
    -------
    GeoDataFrame
    """
    gdf = gdf.copy()
    colname = colname if colname is not None else list(gdf.columns)[0]
    tmp = gdf.loc[gdf.duplicated('geometry', keep=False)]
    gdf = gdf.loc[~gdf.duplicated('geometry')]
    if not tmp.empty:
        for i, row in gdf.iterrows():
            tmp2 = tmp.loc[tmp.geometry.values == row.geometry]
            if not tmp2.empty:
                print(tmp)
                print(tmp2)
                print(f"  Duplicate geometries found for {colname}: {tmp2[colname].to_list()}. Keeping first entry: {row[colname]}")
    return gdf


def generate_nodes_from_edges(
        edges: gpd.GeoDataFrame
    ) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Generate start/end nodes from edges and update node information in edges GeoDataFrame.
    Return updated edges geodataframe and nodes geodataframe

    Parameters
    ----------
    edges : gpd.GeoDataFrame
        Line feature dataset containing edges

    Returns
    -------
    Tuple containing GeoDataFrame with edges and GeoDataFrame with nodes
    """
    edges['edge_no'] = range(len(edges))
    edges.index = edges['edge_no'].values

    # Generate nodes from edges and include extra information in edges
    edges[["from_node", "to_node"]] = [[g.coords[0], g.coords[-1]] for g in edges.geometry]  # generate endpoints
    _nodes = pd.unique(edges["from_node"].tolist() + edges["to_node"].tolist())  # get unique nodes
    indexer = dict(zip(_nodes, range(len(_nodes))))
    nodes = gpd.GeoDataFrame(
        data={'node_no': [indexer[x] for x in _nodes]}, 
        index=[indexer[x] for x in _nodes], 
        geometry=[Point(x) for x in _nodes],
        crs=edges.crs
    )
    edges[["from_node", "to_node"]] = edges[["from_node", "to_node"]].map(indexer.get)  # get node id instead of coords
    return edges, nodes


def snap_to_network(
        snap_type: str,
        points: gpd.GeoDataFrame, 
        edges: gpd.GeoDataFrame = None,
        nodes: gpd.GeoDataFrame = None,
        buffer_distance: float = 0.5,
        min_length_edge: float = 2.0
    ) -> gpd.GeoDataFrame:
    """
    Snap point geometries to network based on type and within buffer distance

    Parameters
    ----------
    snap_type : str
        Snap type which control how geometries will be snapped to network. Can either be "split_node" or "boundary_id".
    points : gpd.GeoDataFrame
        Point feature dataset containing points to be snapped
    edges : gpd.GeoDataFrame
        Line feature dataset containing edges of network
    nodes : gpd.GeoDataFrame
        Point feature dataset containing nodes of network
    buffer_distance: float
        Buffer distance (in meter) that is used to snap nodes to network

    Returns
    -------
    GeoDataFrame with snapped geometries that are either snapped or not (based on edge_no or node_no column value)
    """

    if snap_type == "split_node":
        print(f" - Snapping split nodes: buffer distance to nodes ({buffer_distance * 0.1:.3f} m) or edges ({buffer_distance:.3f} m)...")
        points = snap_points_to_nodes_and_edges(
            points, 
            edges=edges, 
            nodes=nodes, 
            edges_bufdist=buffer_distance,
            nodes_bufdist=buffer_distance * 0.1,
            n_edges_to_node_limit=3,
            min_length_edge=min_length_edge
        )
        # get node no or edge no on which point is located
        points = get_node_no_and_edge_no_for_points(points, edges=edges, nodes=nodes)
        # print out all non-snapped split nodes
        if ((points['node_no']==-1) & (points['edge_no']==-1)).any():
            print(f" * The following split nodes could not be snapped to nodes or edges within buffer distance:")
            for i, row in points[(points['node_no']==-1) & (points['edge_no']==-1)].iterrows():
                print(row)
                print(f"  * Split node {row['split_node']} - split_node_id {row['split_node_id']}")
        return points
    elif snap_type == "boundary":
        print(f" - Snapping boundaries within buffer distance ({buffer_distance:.3f} m) to nodes...")
        points = snap_points_to_nodes_and_edges(
            points, 
            edges=None,   # exclude edges on purpose
            nodes=nodes, 
            nodes_bufdist=buffer_distance,
            min_length_edge=min_length_edge
        )
        # get node no or edge no on which point is located
        points = get_node_no_and_edge_no_for_points(points, edges=None, nodes=nodes)
        # print out all non-snapped boundaries
        if (points['node_no']==-1).any():
            print(f"The following boundaries could not be snapped to nodes within buffer distance:")
            for i, row in points[points['node_no']==-1].iterrows():
                print(row)
                print(f"  Boundary {row['boundary_id']} - {row['boundary_name']}")
        return points
    else:
        raise ValueError('Invalid snap_type. Can either be "split_node" or "boundary_id"')


def snap_points_to_nodes_and_edges(
        points: gpd.GeoDataFrame, 
        edges: gpd.GeoDataFrame = None,
        nodes: gpd.GeoDataFrame = None,
        edges_bufdist: float = 0.5,
        nodes_bufdist: float = 0.5,
        n_edges_to_node_limit: int = 1e10,
        min_length_edge: float = 2.0
    ) -> gpd.GeoDataFrame:
    """
    Snap point geometries to network based on type and within buffer distance

    Parameters
    ----------
    points : gpd.GeoDataFrame
        Point feature dataset containing points to be snapped
    edges : gpd.GeoDataFrame
        Line feature dataset containing edges. Use None to don't snap point to edges. Note: if nodes are supplied
        too, this snapping will first try to snap to nodes and if not possible, to edges.
    nodes : gpd.GeoDataFrame
        Point feature dataset containing nodes. Use None to don't snap point to nodes
    edges_bufdist : float
        Buffer distance (in meter) that is used to snap point to edges
    nodes_bufdist : float
        Buffer distance (in meter) that is used to snap points to nodes
    n_edges_to_node_limit : int
        Limit the snapping to node by the number of edges that is connected to that node. There is no 
        snapping to node if number of connected edges to node is greater or equal than this value 
        (no snapping if n_connected_edges >= n_edges_to_node_limit). In order to use this option
        both nodes and edges need to be supplied.

    Returns
    -------
    GeoDataFrame with snapped points (whether or not it's snapped can be derived from edge_no or node_no column value)
    """

    print(f" - Snapping points to nodes and/or edges")
    new_points = points.geometry.tolist()
    for i, point in points.iterrows():
        if nodes is not None:
            check = False
            # check if point is within buffer distance of node(s)
            ix = nodes.index.values[nodes.intersects(point.geometry.buffer(nodes_bufdist))]
            if len(nodes.loc[ix]) >= 1:
                _dist_n_to_nodes = np.array([n.distance(point.geometry) for n in nodes.loc[ix].geometry.values])
                _ix = ix[np.argmin(_dist_n_to_nodes)]
                new_point = nodes.loc[_ix, 'geometry']
                node_no = nodes.loc[_ix, 'node_no']
                if edges is not None:
                    # first try with know edge/node info for speed-up, otherwise find the connected edges
                    if 'node_no' in edges.columns:
                        _edges = edges.loc[node_no == 'node_no']
                    else:
                        _edges = edges.loc[new_point.buffer(0.000001).intersects(edges.geometry.values)]
                    # if connected edges less than limit, snap to node
                    if len(_edges) < n_edges_to_node_limit:
                        check = True
                    else:
                        if 'split_node' in point:
                            point_label = f"Split node {point['split_node']} (xy={point.geometry.x:3f},{point.geometry.y:3f})"
                        else:
                            point_label = f"Point with index {i} (xy={point.geometry.x:3f},{point.geometry.y:3f})"
                        print(point)
                        print(f"  DEBUG - {point_label} can be snapped to node no {node_no} "
                              f"but number of connected edges ({len(_edges)}) is >= than limit ({n_edges_to_node_limit}).")
                else:
                    check = True
            # if check is True, a valid node for point to snap to has been found
            if check:
                new_points[i] = new_point
                continue  # no need to check snapping to edge in this case
        if edges is not None:
            # if no edge is within point combined with buffer distance, skip
            lines = edges.geometry.values[edges.geometry.intersects(point.geometry.buffer(edges_bufdist))]
            # also skip if line is shorter than 2 m
            lines = lines[[l.length > min_length_edge for l in lines]]
            if len(lines) == 0:
                continue
            # project node onto edge but make sure resulting point is some distance (0.5 meter) from start/end node of edge
            _dist_along_line = [l.project(point.geometry) for l in lines]
            _dist_along_line = [((l.length - 0.5) if (d > (l.length - 0.5)) else d) if (d > 0.5) else 0.5 
                                for l, d in zip(lines, _dist_along_line)]
            _nodes = np.array([l.interpolate(d) for l, d in zip(lines, _dist_along_line)], dtype=object)
            _dist_n_to_nodes = np.array([n.distance(point.geometry) for n in _nodes])
            # filter out nodes that is within buffer distance and use one with minimum distance
            ix = np.where(_dist_n_to_nodes <= edges_bufdist)[0]
            if len(ix) >= 1:
                # select snapped node that is closest to edge
                _nodes, _dist_n_to_nodes = _nodes[ix], _dist_n_to_nodes[ix]
                new_points[i] = _nodes[np.argmin(_dist_n_to_nodes)]
            else:
                pass   # no snapping to edge could be achieved

    # overwrite geoms with newly snapped point locations
    points['geometry'] = new_points

    return points


def get_node_no_and_edge_no_for_points(
        points: gpd.GeoDataFrame, 
        edges: gpd.GeoDataFrame = None,
        nodes: gpd.GeoDataFrame = None
    ) -> gpd.GeoDataFrame:
    """
    Get edge no or node no for point locations. If value is -1 no node and/or edge could be found for point location. 

    Parameters
    ----------
    points : gpd.GeoDataFrame
        Point feature dataset containing point locations
    nodes : gpd.GeoDataFrame
        Point feature dataset containing nodes. Note that it tries to find node first and if not possible tries to find edge
    edges : gpd.GeoDataFrame
        Line feature dataset containing edges

    Returns
    -------
    Original GeoDataFrame of split nodes with extra edge_no and node_no column
    """

    print(' - Retrieving edge no or node no for point locations...')
    prev_typs = None
    for typ, gdf in zip(['node_no', 'edge_no'], [nodes, edges]):
        if gdf is not None:
            gdf_no = np.ones(len(points), dtype=int) * -1
            gdf_ix = np.arange(len(gdf))
            gdf_buf = gdf.geometry.values.buffer(0.000001)  # for speed-up
            for i, point in enumerate(points.geometry):
                # skip if previous checked types resulted in valid result
                check = False
                if prev_typs is not None:
                    for prev_typ in prev_typs:
                        if points.iloc[i][prev_typ] != -1:
                            check = True
                if check:
                    continue
                # do below if not skipped
                ix = gdf_ix[gdf_buf.intersects(point)]
                if len(ix) >= 1:
                    gdf_no[i] = gdf.iloc[ix[0]][typ]  # only use first one found
            points[typ] = gdf_no
            prev_typs = prev_typs + [typ] if isinstance(prev_typs, list) else [typ]
        else:
            points[typ] = [None] * len(points)  # fill with None
    return points


def split_edges_by_split_nodes(
        split_nodes: gpd.GeoDataFrame, 
        edges: gpd.GeoDataFrame,
        buffer_distance: float = 0.5
    ) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Splits edges (lines) by split node locations. Split nodes should be (almost) perfectly be aligned to edges (within buffer distance).
    If not, use .snap_nodes_to_edges() before to align them to edges within a buffer distance. 
    
    If split nodes gdf contains edge_no column (which is filled with only integers), only those edges will be split. If the column is missing
    from gdf or contains None values, it will be ignored and the default (more time consuming) approach will be used.

    The start/end nodes will be regenerated after the edges are split. The edge no and node no column value in split nodes gdf will also 
    be updated because of that. Returns new (splitted) edges and new updated (start/end)nodes and split nodes for those edges

    Parameters
    ----------
    split_nodes : gpd.GeoDataFrame
        Point feature dataset containing split nodes
    edges : gpd.GeoDataFrame
        Line feature dataset containing edges
    buffer_distance: float
        Buffer distance (in meter) that is used project split nodes to edge

    Returns
    -------
    Tuple containing GeoDataFrame with split nodes, GeoDataFrame with edges and GeoDataFrame (start/end)nodes of edges
    """

    print(" - Split edges by split nodes locations...")
    split_nodes['edge_no'] = [None] * len(split_nodes)
    edge_no_col_present = 'edge_no' in split_nodes.columns
    edge_no_col_present = all([x is not None for x in split_nodes['edge_no']]) if edge_no_col_present else False
    edges_orig = edges.copy()
    # to speed-up splitting and if edge_no column is present in split nodes gdf, only
    # split those edges
    if edge_no_col_present:
        for edge_no in split_nodes['edge_no'].unique():
            if edge_no == -1:
                continue  # skip
            split_points = split_nodes.loc[split_nodes['edge_no'] == edge_no].geometry.values
            edge = edges_orig.loc[edges_orig['edge_no'] == edge_no, 'geometry'].values[0]
            splitted_edges = split_line_in_multiple(edge, distances_along_line=[edge.project(p) for p in split_points])
            if len(splitted_edges) == 0:
                continue  # skip because edge is (somehow) not splitted
            # update (original) edges gdf
            edge_row = edges_orig.loc[edges_orig['edge_no'] == edge_no]
            edges_to_add = pd.concat([edge_row]*len(splitted_edges))
            edges_to_add = gpd.GeoDataFrame(
                edges_to_add, 
                geometry=splitted_edges
            ).set_index(np.arange(len(splitted_edges)) + 1 + edges.index.max())
            edges = pd.concat([edges, edges_to_add], axis=0, ignore_index=True)
            edges.drop(index=edges_orig.loc[edges_orig['edge_no'] == edge_no].index)
    # otherwise, do default approach
    else:
        # loop over edges so we can directly split an edge with multiple split nodes in one go
        for i, edge in enumerate(edges_orig.geometry):
            # only check split nodes that are within buffer distance of edge
            split_points = split_nodes.geometry.values[split_nodes.geometry.intersects(edge.buffer(buffer_distance))]
            if len(split_points) == 0:
                continue  # skip if no split nodes found
            # also skip split nodes that are located within buffer distance from start/end nodes of edge
            nodes = np.array([edge.interpolate(x) for x in [0, edge.length]], dtype=object)
            split_points = np.array([p for p in split_points 
                                     if not any([p.intersects(n.buffer(buffer_distance)) 
                                                 for n in nodes])], 
                                    dtype=object)
            if len(split_points) == 0:
                continue  # skip if no split nodes are left
            # split edge
            splitted_edges = split_line_in_multiple(edge, distances_along_line=[edge.project(p) for p in split_points])
            if len(splitted_edges) == 0:
                continue  # skip because edge is (somehow) not splitted
            # update (original) edges gdf
            edge_row = edges_orig.loc[edges_orig.index.values[i]].to_frame().T
            edges_to_add = pd.concat([edge_row]*len(splitted_edges))
            edges_to_add = gpd.GeoDataFrame(
                edges_to_add,
                geometry=splitted_edges
            ).set_index(np.arange(len(splitted_edges)) + 1 + edges.index.max())
            edges = pd.concat([edges, edges_to_add], axis=0)
            edges.drop(index=edges_orig.index.values[i], inplace=True)

    # update edge id column if present
    if 'edge_id' in edges.columns:
        edges['edge_id'] = edges['edge_id'].fillna("dummy")
        n_max = np.max(np.unique(edges['edge_id'], return_counts=True)[1])  # max group size in groupby
        split_nrs = np.arange(start=1, stop=n_max+1)
        split_nrs = edges.groupby('edge_id')['from_node'].transform(lambda x: split_nrs[:len(x)])
        max_splits = edges.groupby('edge_id')['from_node'].transform(lambda x: len(x))
        edges['edge_id'] = [f'{b}_{s}' if m > 1 else b for b, s, m in zip(edges['edge_id'], split_nrs, max_splits)]
    # regenerate start/end nodes of edges
    edges['edge_no'] = range(len(edges))  # reset edge no
    edges, nodes = generate_nodes_from_edges(edges)
    # update edge no and node no columns in split nodes gdf
    split_nodes = get_node_no_and_edge_no_for_points(split_nodes, edges, nodes)
    return split_nodes, edges, nodes


def split_edges_by_dx(
        edges: gpd.GeoDataFrame,
        dx: float,
    ) -> gpd.GeoDataFrame:
    """
    Splits edges (lines) by a given distance (in meter) so each splitted section approximates the given distance

    Parameters
    ----------
    edges : gpd.GeoDataFrame
        Line feature dataset containing edges
    dx: float
        Distance (in meter) for new splitted sections

    Returns
    -------
    GeoDataFrame with splitted up edges
    """
    edges = edges.copy()
    edges_to_add = []
    edges_to_remove = []
    print(f' - Split up edges so each section approximates dx={dx:.3f} m...')
    for i, row in edges.iterrows():
        line = row['geometry']

        # split up line depending on the length compared to the desired length
        n = int(np.round(line.length / dx))
        if n <= 1:
            continue  # no need to split
        dx_star = line.length / n
        dists_to_split = [(i + 1) * dx_star for i in range(n - 1)]
        
        # now split and store
        new_lines = split_line_in_multiple(line, dists_to_split)
        for j, l in enumerate(new_lines):
            edges_to_add.append((i, j, l))
        edges_to_remove.append(i)
    
    # construct new geodataframe with new edges to add
    edges_to_add = gpd.GeoDataFrame(
        pd.concat(
            [
                edges.loc[[x[0] for x in edges_to_add]][[c for c in edges.columns if c != 'geometry']].reset_index(drop=True), 
                gpd.GeoDataFrame(
                    data={'suffix': [x[1] for x in edges_to_add]}, 
                    geometry=[x[2] for x in edges_to_add]
                )
            ],
            axis=1,
        )
    )
    # append branch_id column with suffix to identify splitted edges
    if 'branch_id' in edges_to_add.columns:
        edges_to_add['branch_id'] = [f"{b}___{s}" for b, s in zip(edges_to_add['branch_id'], edges_to_add['suffix'])]
    edges_to_add.drop(columns=['suffix'], inplace=True)

    # add new lines and remove old ones
    edges_new = edges.loc[edges.index[~np.isin(edges.index, edges_to_remove)]]
    edges_new = gpd.GeoDataFrame(pd.concat([edges_new, edges_to_add], ignore_index=True))
    if 'edge_no' in edges_new.columns:
        edges_new['edge_no'] = np.arange(len(edges_new))
    
    if "branch_id" in edges_new.columns:
        edges_new = edges_new.rename(columns={"branch_id": "edge_id"})
    return edges_new


def split_line_in_two(line: LineString, distance_along_line: float) -> List[LineString]:
    # Cuts a line in two at a distance from the line starting point
    if distance_along_line <= 0.0 or distance_along_line >= line.length:
        return [LineString(line)]
    coords = list(line.coords)
    for i, p in enumerate(coords):
        pd = line.project(Point(p))
        if pd == distance_along_line:
            return [LineString(coords[:i+1]), LineString(coords[i:])]
        if pd > distance_along_line:
            cp = line.interpolate(distance_along_line)
            if len(coords[:i][0]) == 2:
                return [LineString(coords[:i] + [(cp.x, cp.y)]), LineString([(cp.x, cp.y)] + coords[i:])]
            else:
                return [LineString(coords[:i] + [(cp.x, cp.y, cp.z)]), LineString([(cp.x, cp.y, cp.z)] + coords[i:])]


def split_line_in_multiple(line: LineString, distances_along_line: Union[List[Union[float, int]], np.ndarray]) -> List[LineString]:
    # Cuts a line in multiple sections at distances from the line starting point
    lines = []
    distances_along_line = sorted(distances_along_line)  # distances should by in sorted order for loop below to work
    for i, d in enumerate(distances_along_line):
        if i == 0:
            ls = split_line_in_two(line, distances_along_line[i])
            if len(distances_along_line) == 1:
                lines = ls
                break
            else:
                if len(ls) == 1:
                    new_line = ls[0]  # this can happen if distance to split is 0 which results in no splitted line
                else:
                    lines.append(ls[0])
                    new_line = ls[1]
        else:
            new_d = distances_along_line[i] - distances_along_line[i-1]
            if new_d == 0.0:
                continue
            ls = split_line_in_two(new_line, new_d)
            if i == (len(distances_along_line) - 1):
                lines.extend(ls)
            else:
                lines.append(ls[0])
                new_line = ls[1]
    return lines


def assign_unassigned_areas_to_basin_areas(
        areas: gpd.GeoDataFrame,
        basin_areas: gpd.GeoDataFrame, 
        drainage_areas: gpd.GeoDataFrame = None,
    ) -> Dict[str, gpd.GeoDataFrame]:
    """
    Assign unassigned areas to basin areas based on neighbouring basin areas within optionally the same drainage area if possible.
    Optionally, if edges are provided, the basin and basin_area columns will be updated in those gdfs for edges
    where no basin/basin_area is filled in (equals to -1). Those will be filled based on overlapping basin area after assignment
    of unassigned areas. The edges will also be used to assign unassigned areas more logically based on network connections.
    
    Parameters
    ----------
    areas : gpd.GeoDataFrame
        Areas
    basin_areas : gpd.GeoDataFrame
        Basin areas
    drainage_areas : gpd.GeoDataFrame
        Drainage areas. Optional
        
    Returns
    -------
    Dict of basin areas where unassigned areas are assigned to basin areas and areas geodataframe with updated basin codes
    It will also include edges if they are provided as input
    """
    
    areas = areas.copy()
    basin_areas = basin_areas.copy()
    drainage_areas = drainage_areas.copy() if drainage_areas is not None else None
    areas.geometry = areas.make_valid()  # dissolve can fail because of incorrect geoms. fix those first

    if drainage_areas is None:
        print(" - DEBUG: drainage areas not supplied. assigning process will only be based on areas and basin areas")
        areas["drainage_area_code"] = -1
        basin_areas["drainage_area_code"] = -1
    else:
        print(" - add drainage_area_code to areas and basin_areas")
        # add drainage_area_code to areas
        areas = areas.overlay(
            drainage_areas[["code", "geometry"]].rename(columns={"code": "drainage_area_code"}), 
            how="intersection"
        )
        areas['geom_area'] = areas.geometry.area
        areas.sort_values(by='geom_area', inplace=True)
        areas.drop_duplicates(subset='area', keep='last', inplace=True)
        areas.drop(columns=['geom_area'], inplace=True)

        # add drainage_area_code to basin_areas
        basin_areas = basin_areas.overlay(
            drainage_areas[["code", "geometry"]].rename(columns={"code": "drainage_area_code"}), 
            how="intersection"
        )
        basin_areas['geom_area'] = basin_areas.geometry.area
        basin_areas.sort_values(by='geom_area', inplace=True)
        basin_areas.drop_duplicates(subset='basin', keep='last', inplace=True)
        basin_areas.drop(columns=['geom_area'], inplace=True)
    
    # Assign unassigned areas to basin areas based on neighbouring basin areas within the same drainage area if possible
    _areas = areas.loc[areas['basin']==-1]
    nr_unassigned_areas = len(_areas)
    
    while nr_unassigned_areas != 0:
        nr_unassigned_areas_prev = nr_unassigned_areas
        print(f" - {nr_unassigned_areas}x unassigned areas", end=": ")

        # get neighbouring basin areas of areas
        tmp = _areas.copy()
        tmp["geometry"] = tmp.geometry.buffer(1)
        tmp = tmp.drop(
            columns=["basin"]
        ).overlay(
            basin_areas[["basin", "drainage_area_code", "geometry"]].rename(
                columns={"basin": "new_basin", "drainage_area_code": "new_drainage_area_code"}
            ), 
            how='intersection'
        )
        tmp = tmp[tmp["drainage_area_code"]==tmp["new_drainage_area_code"]]
        tmp["geom_area"] = tmp.geometry.area
        tmp = tmp.sort_values(by="geom_area").drop_duplicates(subset="area", keep="last")
        print(f" * assigned {len(tmp)} unassigned areas")
        
        areas = areas.merge(tmp[["area", "new_basin"]], how="left", on="area")
        areas["new_basin"] = areas["new_basin"].fillna(-1).astype(int)
        areas.loc[areas.basin==-1, "basin"] = areas.loc[areas.basin==-1, "new_basin"]
        areas.drop(columns=["new_basin"], inplace=True)
        
        # update basin areas geometries
        basin_areas = areas[areas["basin"]!=-1].dissolve(by="basin").reset_index().drop(columns=["area"])
        basin_areas["basin"] = basin_areas["basin"].astype(int)

        # update number of unassigned areas
        _areas = areas.loc[areas['basin']==-1]
        nr_unassigned_areas = len(_areas)
        
        # to stop while loop (because no new areas will be assigned anymore) but still unassigned areas remain
        if nr_unassigned_areas == nr_unassigned_areas_prev:
            nr_unassigned_areas = 0

    # update basin areas based on (updated) areas
    basin_areas["basin"] = basin_areas["basin"].fillna(-1).astype(int)
    basin_areas["area_ha"] = basin_areas.geometry.area / 10000.0
    basin_areas["color_no"] = basin_areas.basin % 50
    
    if len(areas.loc[areas['basin'].isna()]) > 0:
        print(f" - not all unassigned areas could be assigned automatically ({len(areas.loc[areas['basin']==-1])}x remaining). Please inspect manually")

    print(f" - basin areas and areas updated")
    return dict(areas=areas, basin_areas=basin_areas)


def _remove_holes(geom, min_area):
    def p(p: Polygon, min_area) -> Polygon:
        holes = [i for i in p.interiors if not Polygon(i).area>min_area]
        return Polygon(shell=p.exterior, holes=holes)

    def mp(mp: MultiPolygon, min_area) -> MultiPolygon:
        return MultiPolygon([p(i, min_area) for i in mp.geoms])

    if isinstance(geom, Polygon):
        return p(geom, min_area)
    elif isinstance(geom, MultiPolygon):
        return mp(geom, min_area)
    else:
        return geom


_Geom = TypeVar("_Geom", Polygon, MultiPolygon, gpd.GeoSeries, gpd.GeoDataFrame)


def remove_holes_from_polygons(
    geom: _Geom, min_area: float
) -> _Geom:
    """Remove all holes from a geometry that satisfy the filter function."""
    if isinstance(geom, gpd.GeoSeries):
        return geom.apply(_remove_holes, min_area=min_area)
    elif isinstance(geom, gpd.GeoDataFrame):
        geom = geom.copy()
        geom["geometry"] = remove_holes_from_polygons(geom["geometry"], min_area=min_area)
        return geom
    return _remove_holes(geom, min_area=min_area)


def extract_segment_from_linestring(line, point1, point2):
    dist1 = line.project(point1)
    dist2 = line.project(point2)
    
    start_dist, end_dist = sorted([dist1, dist2])

    coords = []
    coords.append(line.interpolate(start_dist).coords[0])
    
    for seg_start, seg_end in zip(line.coords[:-1], line.coords[1:]):
        seg_line = LineString([seg_start, seg_end])
        seg_start_dist = line.project(Point(seg_start))
        seg_end_dist = line.project(Point(seg_end))

        if seg_start_dist > end_dist:
            break

        if seg_start_dist >= start_dist and seg_end_dist <= end_dist:
            if coords[-1] != seg_start:  # Avoid duplicate points
                coords.append(seg_start)
            coords.append(seg_end)

    if coords[-1] != line.interpolate(end_dist).coords[0]:
        coords.append(line.interpolate(end_dist).coords[0])
    return LineString(coords)
