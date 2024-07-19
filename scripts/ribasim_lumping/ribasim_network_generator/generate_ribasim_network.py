from typing import List, Dict, Any, Union, Optional, Tuple
import numpy as np
import pandas as pd
import geopandas as gpd
import networkx as nx
from shapely.ops import linemerge
from shapely.geometry import Point, LineString, Polygon, MultiPolygon, GeometryCollection
from ..utils.remove_holes_from_polygons import remove_holes_from_polygons


def create_graph_based_on_nodes_edges(
        nodes: gpd.GeoDataFrame, 
        edges: gpd.GeoDataFrame,
        directional_graph: bool = True,
        add_edge_length_as_weight: bool = False,
        print_logmessage: bool = True,
    ) -> Union[nx.Graph, nx.DiGraph]:
    """
    create networkx graph based on geographic nodes and edges.
    default a directional graph.
    TODO: maybe a faster implementation possible
    """
    if directional_graph:
        graph = nx.DiGraph()
    else:
        graph = nx.Graph()
    if nodes is not None:
        for i, node in nodes.iterrows():
            graph.add_node(node.node_no, pos=(node.geometry.x, node.geometry.y))
    if edges is not None:
        for i, edge in edges.iterrows():
            if add_edge_length_as_weight:
                graph.add_edge(edge.from_node, edge.to_node, weight=edge.geometry.length)
            else:
                graph.add_edge(edge.from_node, edge.to_node)
    if print_logmessage:
        print(
            f" - create network graph from nodes ({len(nodes)}x) and edges ({len(edges)}x)"
        )
    return graph


def split_graph_based_on_split_nodes(
        graph: nx.DiGraph, 
        split_nodes: gpd.GeoDataFrame, 
        edges: gpd.GeoDataFrame
    ) -> Tuple[nx.DiGraph, gpd.GeoDataFrame]:
    """
    Split networkx graph at split_edge or split_node. It removes the original edges(s)/node(s) which are the same as split_edge and
    split_node and inserts new edges and nodes such that the graph becomes disconnected at the split point. After this edges don't
    connect to 1 node (at split point) but each end in each own new node. Because of this removing and adding edges and nodes in the
    graph, these new nodes no in graph are added to split_nodes gdf and also returned as result of this function.
    """

    split_nodes = split_nodes.copy()  # copy to make sure gdf variable is not linked
    split_nodes['graph_node_no'] = pd.Series([-1] * len(split_nodes), index=split_nodes.index, dtype=object)  # force dtype object to be able to insert tuples

    # split on edge: delete edge, create 2 nodes, create 2 edges
    # if all edge no in split nodes gdf are -1, than no splitting of edges are done
    # TODO: although edge stuff below works, it is actually better to split network at split nodes at earlier stage
    #       this will result in all edge no being -1 and only values for node no. so maybe check on that all edge_no
    #       values need to be -1 should be better.
    if "edge_no" not in split_nodes.columns:
        split_nodes["edge_no"] = -1
    split_nodes_edges = split_nodes[split_nodes.edge_no != -1].copy()

    split_edges = edges[
        edges.edge_no.isin(split_nodes_edges.edge_no.values)
    ].copy()
    # assert len(split_nodes_edges) == len(split_edges)
    split_edges = split_edges[["from_node", "to_node"]].to_dict("tight")["data"]

    split_edges = [coor for coor in split_edges if coor in graph.edges]

    split_nodes_edges["new_node_no1"] = (
        998_000_000_000 + split_nodes_edges.edge_no * 1_000 + 1
    )
    split_nodes_edges["new_node_no2"] = (
        998_000_000_000 + split_nodes_edges.edge_no * 1_000 + 2
    )
    split_nodes_edges["new_node_pos"] = split_nodes_edges.geometry.apply(
        lambda x: (x.x, x.y)
    )
    split_nodes_edges["upstream_node_no"] = [e[0] for e in split_edges]
    split_nodes_edges["downstream_node_no"] = [e[1] for e in split_edges]

    # remove splitted edges from graph and insert the newly split ones
    graph.remove_edges_from(split_edges)
    for i_edge, new in split_nodes_edges.iterrows():
        graph.add_node(new.new_node_no1, pos=new.new_node_pos)
        graph.add_node(new.new_node_no2, pos=new.new_node_pos)
        graph.add_edge(new.upstream_node_no, new.new_node_no1)
        graph.add_edge(new.new_node_no2, new.downstream_node_no)
    # update split nodes gdf with new node no
    new_graph_node_no = [(x1, x2) for x1, x2 in zip(split_nodes_edges['new_node_no1'], split_nodes_edges['new_node_no1'])]
    split_nodes.loc[split_nodes_edges.index, 'graph_node_no'] = pd.Series(new_graph_node_no, index=split_nodes_edges.index, dtype=object)

    # split_node: delete node and delete x edges, create x nodes, create x edges
    if "node_no" not in split_nodes.columns:
        split_nodes["node_no"] = -1
    split_nodes_nodes = split_nodes[split_nodes.node_no != -1]
    new_graph_node_no = []
    for split_node_id in split_nodes_nodes.node_no.values:
        if split_node_id not in graph:
            new_graph_node_no.append(-1)
            continue
        split_node_pos = graph.nodes[split_node_id]["pos"]
        split_edges = [e for e in list(graph.edges) if split_node_id in e]
        
        # remove old edges and node and insert new ones
        graph.remove_edges_from(split_edges)
        graph.remove_node(split_node_id)
        new_graph_no = []
        for i_edge, new_edge in enumerate(split_edges):
            new_node_id = 999_000_000_000 + split_node_id * 1_000 + i_edge
            graph.add_node(new_node_id, pos=split_node_pos)
            new_edge_adj = [e if e != split_node_id else new_node_id for e in new_edge]
            graph.add_edge(new_edge_adj[0], new_edge_adj[1])
            new_graph_no.append(new_node_id)
        new_graph_node_no.append(tuple(new_graph_no))
    # update split nodes gdf with new node no
    split_nodes.loc[split_nodes_nodes.index, 'graph_node_no'] = pd.Series(new_graph_node_no, index=split_nodes_nodes.index, dtype=object)
    print(f" - split network graph at split locations ({len(split_nodes)}x)")
    return graph, split_nodes


def add_basin_code_from_network_to_nodes_and_edges(
        graph: nx.DiGraph,
        nodes: gpd.GeoDataFrame,
        edges: gpd.GeoDataFrame,
        split_nodes: gpd.GeoDataFrame,
    ) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    add basin (subgraph) code to nodes and edges
    """

    edges, nodes, split_nodes = edges.copy(), nodes.copy(), split_nodes.copy()  # copy to make sure gdf variable is not linked
    subgraphs = list(nx.weakly_connected_components(graph))
    if nodes is None or edges is None:
        return None, None
    nodes["basin"] = -1
    edges["basin"] = -1
    # prepare indexer to speed-up finding original node no for graph node no
    ix = split_nodes.index[(split_nodes['graph_node_no'] == -1) | pd.isna(split_nodes['graph_node_no'])]
    split_nodes.loc[ix, 'graph_node_no'] = pd.Series([(x,) for x in split_nodes.loc[ix, 'node_no']], index=ix, dtype=object)
    orig_node_indexer = {gn: no for no, gns in zip(split_nodes['node_no'], split_nodes['graph_node_no']) for gn in list(gns)}
    for i, subgraph in enumerate(subgraphs):
        # because in the graph nodes and edges can be changed to generate subgraphs we need to find
        # the original node no for the changed nodes. this information is stored in split_nodes_gdf
        node_ids = list(subgraph)
        orig_node_ids = [orig_node_indexer[n] if n in orig_node_indexer.keys() else n for n in node_ids]

        edges.loc[edges["from_node"].isin(orig_node_ids) & edges["to_node"].isin(orig_node_ids), "basin"] = i + 1
        nodes.loc[nodes["node_no"].isin(orig_node_ids), "basin"] = i + 1
    print(f" - define numbers Ribasim-Basins ({len(subgraphs)}x) and join edges/nodes")
    return nodes, edges


def check_if_split_node_is_used(
        split_nodes: gpd.GeoDataFrame, 
        nodes: gpd.GeoDataFrame, 
        edges: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
    """
    check whether split_nodes are used, split_nodes and split_edges
    """

    split_nodes = split_nodes.copy()  # copy to make sure gdf variable is not linked
    split_nodes["status"] = True

    # check if edges connected to split_nodes have the same basin code
    split_node_ids = [v for v in split_nodes.node_no.values if v != -1]
    split_nodes_not_used = []
    for split_node_id in split_node_ids:
        from_nodes = list(edges[edges.from_node == split_node_id].to_node.values)
        to_nodes = list(edges[edges.to_node == split_node_id].from_node.values)
        neighbours = nodes[nodes.node_no.isin(from_nodes + to_nodes)]
        if len(neighbours.basin.unique()) == 1:
            split_nodes_not_used.append(split_node_id)
    split_nodes.loc[split_nodes[split_nodes.node_no.isin(split_nodes_not_used)].index, "status"] = False

    # check if nodes connected to split_edge have the same basin code
    split_edge_ids = [v for v in split_nodes.edge_no.values if v != -1]
    split_edges_not_used = []
    for split_edge_id in sorted(split_edge_ids):
        end_nodes = list(edges[edges.edge_no == split_edge_id].to_node.values)
        start_nodes = list(edges[edges.edge_no == split_edge_id].from_node.values)
        neighbours = nodes[nodes.node_no.isin(end_nodes + start_nodes)]
        if len(neighbours.basin.unique()) == 1:
            split_edges_not_used.append(split_edge_id)
    split_nodes.loc[split_nodes[split_nodes.edge_no.isin(split_edges_not_used)].index, "status"] = False

    split_nodes["object_type"] = split_nodes["object_type"].fillna("manual")
    split_nodes["split_type"] = split_nodes["object_type"]
    split_nodes.loc[~split_nodes.status, "split_type"] = "no_split"
    len_no_splits = len(split_nodes.loc[~split_nodes['status']])
    print(f" - check whether each split location results in a split ({len_no_splits}x not used)")
    if len_no_splits > 0:
        print(split_nodes.loc[~split_nodes['status']])
    return split_nodes


def create_basin_areas_based_on_drainage_unit_areas(
        edges: gpd.GeoDataFrame, 
        areas: gpd.GeoDataFrame,
        laterals: gpd.GeoDataFrame = None,
    ) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    find areas with spatial join on edges. add subgraph code to areas
    and combine all areas with certain subgraph code into one basin
    """

    if areas is None:
        return None, None
    else:
        areas = areas[["area_code", "geometry"]].copy()
    if edges is None:
        areas["basin"] = -1
        return areas, None
    else:
        edges = edges.copy()

    def get_area_code_for_lateral(laterals, areas):
        selected_areas = areas[[laterals.find(area) != -1 for area in areas["area_code"]]]
        if len(selected_areas)==0:
            return None
        else:
            return selected_areas["area_code"].values[0]

    def get_basin_code_from_lateral(area_code, laterals_join):
        basins = list(laterals_join["basin"][(laterals_join["area_code_included"]==area_code)&(laterals_join["basin"].isna()==False)].values)
        if len(basins) == 0:
            return -1
        else:
            return basins[0]

    if laterals is not None:
        laterals = laterals.copy()
        laterals["area_code_included"] = laterals["id"].apply(lambda x: get_area_code_for_lateral(x, areas))
        laterals_join = laterals.sjoin(
            gpd.GeoDataFrame(edges['basin'], geometry=edges['geometry'].buffer(1)),
            op="intersects",
            how="left",
        ).drop(columns=["index_right"])
        laterals_join["basin"] = laterals_join["basin"].fillna(-1).astype(int)
        areas["basin"] = areas.apply(lambda x: get_basin_code_from_lateral(x["area_code"], laterals_join), axis = 1)
        basin_areas = areas.dissolve(by="basin").explode().reset_index().drop(columns=["level_1"])

    else:
        edges_sel = edges.loc[edges["basin"] != -1].copy()
        # fix invalid area geometries
        areas["area"] = areas.index
        areas_orig = areas.copy()
        areas.geometry = areas.make_valid()
        # due to make_valid() GeometryCollections can be generated. Only take the (multi)polygon from those collections
        areas.geometry = [
            [g for g in gs.geoms if (isinstance(g, Polygon) or isinstance(g, MultiPolygon))][0] 
            if isinstance(gs, GeometryCollection) else gs for gs in areas.geometry
        ]
        # spatial join edges to areas that intersect
        areas = areas.sjoin(edges_sel[["basin", "geometry"]])
        # we want to select the edge which is the longest within an area to ultimately select 
        # the right basin code
        edge_lengths = [
            gpd.GeoDataFrame(edges_sel.loc[ir].to_frame().T, geometry='geometry').clip(a).geometry.length.values[0] 
            for a, ir in zip(areas.geometry, areas['index_right'])
        ]
        areas['edge_length'] = edge_lengths
        areas = areas.drop(columns=["index_right"]).reset_index(drop=True)
        areas = areas.groupby(by=["area", "basin"], as_index=False).agg({"edge_length": "sum"})
        # this sorts first such that max edge length is first item and in drop_duplicates that first item will be kept
        # effectively method to get areas with basin code based on max edge length within area
        areas = areas.sort_values(
            by=["area", "edge_length"], ascending=[True, False]
        ).drop_duplicates(subset=["area"], keep="first")
        # insert area geometries back into gdf
        areas = (
            areas[["area", "basin", "edge_length"]]
            .sort_values(by="area")
            .merge(areas_orig, how="outer", left_on="area", right_on="area")
        )
        areas = gpd.GeoDataFrame(areas, geometry="geometry", crs=edges.crs)
        areas = areas.sort_values(by="area")
        areas.geometry = areas.make_valid()  # dissolve can fail because of incorrect geoms. fix those first
        basin_areas = areas.dissolve(by="basin").reset_index().drop(columns=["area"])

    basin_areas["basin"] = basin_areas["basin"].astype(int)
    basin_areas["area_ha"] = basin_areas.geometry.area / 10000.0
    basin_areas["color_no"] = basin_areas.basin % 50
    print(
        f" - define for each Ribasim-Basin the related basin area ({len(basin_areas)}x)"
    )
    return areas, basin_areas


def create_basins_based_on_subgraphs_and_nodes(
        graph: nx.DiGraph, 
        nodes: gpd.GeoDataFrame,
        edges: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
    """
    create basin nodes based on basin_areas or nodes
    """
    connected_components = list(nx.weakly_connected_components(graph))
    centralities = {}
    for i, component in enumerate(connected_components):
        subgraph = graph.subgraph(component).to_undirected()
        centrality_subgraph = nx.closeness_centrality(subgraph)
        centralities.update(
            {node: centrality for node, centrality in centrality_subgraph.items()}
        )

    centralities = pd.DataFrame(
        dict(node_no=list(centralities.keys()), centrality=list(centralities.values()))
    )
    centralities = centralities[centralities["node_no"] < 900_000_000_000]
    basins_temp = nodes[['node_no', 'basin', 'geometry']].merge(
        centralities, 
        how="outer", 
        left_on="node_no", 
        right_on="node_no"
    )
    basins_temp = basins_temp[basins_temp["basin"] != -1].sort_values(
        by=["basin", "centrality"], ascending=[True, False]
    )
    basins = basins_temp.groupby(by="basin").first().reset_index().set_crs(nodes.crs)
    no_nodes_basin = basins_temp.groupby(by="basin")["basin"].count()
    no_nodes_basin.name = "no_nodes_basin"
    basins = basins.merge(no_nodes_basin, how='left', left_on="basin", right_index=True)
    
    basins1 = basins[basins.no_nodes_basin!=2]
    basins2 = basins[basins.no_nodes_basin==2]
    if ~basins2.empty:
        basins_temp2 = basins_temp[basins_temp.basin.isin(basins2.basin.values)]
        basins2a = basins_temp2.groupby("basin").first().reset_index().rename(columns={"node_no": "node_no1"})
        basins2b = basins_temp2.groupby("basin").last().reset_index()[["basin", "node_no"]].rename(columns={"node_no": "node_no2"})
        basins2x = basins2a.merge(basins2b, how='left', on='basin')

        basins2y = basins2x[["basin", "node_no1", "node_no2"]].merge(
            edges[["basin", "edge_no", "from_node", "to_node"]], 
            how='left', 
            left_on=["basin", "node_no1", "node_no2"], 
            right_on=["basin", "from_node", "to_node"]
        ).fillna(-1).astype(int)
        basins2z = basins2y.merge(
            edges[["basin", "edge_no", "from_node", "to_node"]], 
            how='left', 
            left_on=["basin", "node_no2", "node_no1"], 
            right_on=["basin", "from_node", "to_node"]
        ).fillna(-1).astype(int)
        basins2z.loc[basins2z["edge_no_x"]!=-1, "edge_no"] = basins2z["edge_no_x"]
        basins2z.loc[basins2z["edge_no_y"]!=-1, "edge_no"] = basins2z["edge_no_y"]
        basins2z.loc[basins2z["from_node_x"]!=-1, "from_node"] = basins2z["from_node_x"]
        basins2z.loc[basins2z["from_node_y"]!=-1, "from_node"] = basins2z["from_node_y"]
        basins2z.loc[basins2z["to_node_x"]!=-1, "to_node"] = basins2z["to_node_x"]
        basins2z.loc[basins2z["to_node_y"]!=-1, "to_node"] = basins2z["to_node_y"]
        basins2z = basins2z.fillna(-1).astype(int).merge(edges[["edge_no", "geometry"]], how="left", on="edge_no")
        basins2s = basins2z[
            ((basins2z.from_node==basins2z.node_no1) & (basins2z.to_node==basins2z.node_no2)) | 
            ((basins2z.from_node==basins2z.node_no2) & (basins2z.to_node==basins2z.node_no1))
        ]
        basins2s = gpd.GeoDataFrame(
            basins2s,
            geometry=[v.centroid for v in basins2s.geometry.values],
            crs=basins.crs
        )
        basins2_geometry = basins2["geometry"]
        basins2 = basins2.drop(columns=["geometry"]).merge(basins2s[["basin", "geometry"]], how="left", on="basin")
        basins2["geometry"] = basins2["geometry"].fillna(basins2_geometry)

    basins = pd.concat([basins1, basins2])
    basins = basins[basins.geometry != None]
    print(f" - create final locations Ribasim-Basins ({len(basins)}x)")
    return basins


def check_if_nodes_edges_within_basin_areas(nodes, edges, basin_areas):
    """
    check whether nodes assigned to a basin are also within the polygon assigned to that basin
    """
    
    edges, nodes, basin_areas = edges.copy(), nodes.copy(), basin_areas.copy()  # copy to make sure gdf variable is not linked
    if basin_areas is None:
        nodes["basin_area"] = -1
        nodes["basin_check"] = True
        edges["basin_area"] = -1
        edges["basin_check"] = True
        return nodes, edges

    nodes = nodes.drop(columns=["basin_area"], errors="ignore")
    nodes = gpd.sjoin(nodes, basin_areas[["geometry", "basin"]], how="left").drop(
        columns=["index_right"]
    )
    nodes["basin_right"] = nodes["basin_right"].fillna(-1).astype(int)
    nodes = nodes.rename(columns={"basin_left": "basin", "basin_right": "basin_area"})
    nodes["basin_check"] = nodes["basin"] == nodes["basin_area"]

    edges = edges.drop(columns=["basin_area"], errors="ignore")
    edges = gpd.sjoin(
        edges, basin_areas[["geometry", "basin"]], how="left", predicate="within"
    ).drop(columns=["index_right"])
    edges = edges.rename(columns={"basin_left": "basin", "basin_right": "basin_area"})
    edges["basin_area"] = edges["basin_area"].fillna(-1).astype(int)
    edges["basin_check"] = edges["basin"] == edges["basin_area"]
    return nodes, edges


def create_basin_connections(
        split_nodes: gpd.GeoDataFrame,
        edges: gpd.GeoDataFrame,
        nodes: gpd.GeoDataFrame,
        basins: gpd.GeoDataFrame,
        crs: int = 28992,
        option_edges_hydroobjects: bool = False,
    ) -> gpd.GeoDataFrame:
    """
    create basin connections
    """
    
    conn = split_nodes.rename(columns={"geometry": "geom_split_node"})
 
    # check if split_node is used (split_type) or is a hard cut
    conn = conn[(conn["split_type"] != "no_split") & (conn["split_type"] != "harde_knip")]

    # use different approach for: 
    # (1) splitnodes that are structures and on an edge and 
    # (2) splitnodes that are original d-hydro nodes

    # (1) splitnodes that are located on an edge
    conn_struct = conn.loc[conn["edge_no"] != -1].drop(["node_no", "from_node", "to_node"], axis=1, errors='ignore')
    # merge with edge to find us and ds nodes
    conn_struct = conn_struct.merge(
        edges[["from_node", "to_node", "edge_no"]],
        left_on="edge_no",
        right_on="edge_no"
    )
    # TODO: check for each edge the maximum absolute flow direction, in case of negative, reverse from_node/to_node
    # merge with node to find us and ds basin
    conn_struct_us = conn_struct.merge(
        nodes[["node_no", "basin"]],
        left_on="from_node",
        right_on="node_no",
    )
    conn_struct_us = conn_struct_us.drop(columns=["from_node", "to_node", "node_no", "edge_no"])
    conn_struct_ds = conn_struct.merge(
        nodes[["node_no", "basin"]],
        left_on="to_node", 
        right_on="node_no", 
    ).drop(columns=["from_node", "to_node", "node_no", "edge_no"])

    # (2) splitnodes that are original d-hydro nodes
    # merge splitnodes add connected edges
    conn_nodes = conn.loc[conn["node_no"] != -1].drop(["edge_no", "from_node", "to_node"], axis=1, errors='ignore')

    conn_nodes_ds = conn_nodes.merge(
        edges[["basin", "from_node", "to_node", "edge_no"]],
        left_on="node_no",
        right_on="from_node",
    ).drop(columns=["from_node", "to_node", "node_no", "edge_no"])
    conn_nodes_us = conn_nodes.merge(
        edges[["basin", "from_node", "to_node", "edge_no"]],
        left_on="node_no",
        right_on="to_node",
    ).drop(columns=["from_node", "to_node", "node_no", "edge_no"])

    # TODO: check for each edge the maximum absolute flow direction, in case of negative, cut and past in other dataframe.
    # Combine (1) en (2)
    conn_ds = pd.concat([conn_nodes_ds, conn_struct_ds])
    conn_us = pd.concat([conn_nodes_us, conn_struct_us])

    # merge splitnodes with basin DOWNSTREAM
    conn_ds = conn_ds.merge(
        basins[["basin", "geometry"]],
        left_on="basin",
        right_on="basin"
    ).rename(columns={"geometry": "geom_basin"})
    conn_ds["connection"] = "split_node_to_basin"
    conn_ds["geometry"] = conn_ds.apply(lambda x: LineString([x.geom_split_node, x.geom_basin]), axis=1)

    # merge splitnodes with basin UPSTREAM
    conn_us = conn_us.merge(
        basins[["basin", "geometry"]],
        left_on="basin",
        right_on="basin"
    ).rename(columns={"geometry": "geom_basin"})
    conn_us["connection"] = "basin_to_split_node"
    conn_us["geometry"] = conn_us.apply(lambda x: LineString([x.geom_basin, x.geom_split_node]), axis=1)

    basin_connections = pd.concat([conn_ds, conn_us])
    basin_connections = gpd.GeoDataFrame(
        basin_connections, 
        geometry='geometry', 
        crs=crs
    )

    if option_edges_hydroobjects:
        print(' - generate basin connections geometry from edges')
        # make undirectional graph of last updated nodes and edges including length of edges
        graph = create_graph_based_on_nodes_edges(
            nodes=nodes, 
            edges=edges, 
            directional_graph=False,
            add_edge_length_as_weight=True,
            print_logmessage=False
        )
        # get node no for split node and basin
        _basin_connections = basin_connections.copy()
        _basin_connections = _basin_connections.merge(
            split_nodes[["split_node", "node_no"]].rename(columns={"node_no": "split_node_node_no"}),
            how="left", on="split_node"
        )
        _basin_connections = _basin_connections.merge(
            basins[["basin", "node_no"]].rename(columns={"node_no": "basin_node_no"}),
            how="left", on="basin"
        )
        # get shortest paths
        _basin_connections['paths'] = [
            nx.dijkstra_path(graph, sn, bn) 
            if c == 'split_node_to_basin' else nx.dijkstra_path(graph, bn, sn)
            for sn, bn, c in zip(
                _basin_connections['split_node_node_no'], 
                _basin_connections['basin_node_no'], 
                _basin_connections['connection']
            )
        ]
        # transform shortest paths to a continuous line of the edges
        _edges = edges.copy()
        _edges['nodes1'] = [f"{n1}_{n2}" for n1, n2 in zip(_edges['from_node'], _edges['to_node'])]
        _edges['nodes2'] = [f"{n2}_{n1}" for n1, n2 in zip(_edges['from_node'], _edges['to_node'])]
        _basin_connections['geometry_from_edges'] = [
            linemerge([_edges.loc[(_edges['nodes1'] == f'{n1}_{n2}') | (_edges['nodes2'] == f'{n1}_{n2}'), 'geometry'].values[0] 
                       for n1, n2 in zip(p[:-1], p[1:])])
            for p in _basin_connections['paths']
        ]
        # fix line directions to match with connection type (split node to basin or basin to split node)
        for i, row in _basin_connections.iterrows():
            sp = row['split_node_node_no'] if row['connection'] == 'split_node_to_basin' else row['basin_node_no']
            sp = nodes.loc[nodes['node_no'] == sp, 'geometry'].values[0].buffer(0.0001)
            if 'LINESTRING' not in str(row['geometry_from_edges']):
                continue  # skip geometries that are not a line
            if Point(row['geometry_from_edges'].coords[0]).intersects(sp):
                # starting point of line matches with desired starting point based on connection type, so leave line as is
                pass
            elif Point(row['geometry_from_edges'].coords[-1]).intersects(sp):
                # last point of line matches with desired starting point based on connection type, so flip direction of line
                _basin_connections.at[i, 'geometry_from_edges'] = row['geometry_from_edges'].reverse()
            else:
                # starting point could not be found on begin or end of line. just leave line as is
                pass
        # assign new basin connections lines to original geodataframe
        basin_connections['geometry_from_edges'] = _basin_connections['geometry_from_edges']

    print(f" - create connections between Basins and split locations ({len(basin_connections)}x)")
    return basin_connections


def create_boundary_connections(
        boundaries: gpd.GeoDataFrame,
        nodes: gpd.GeoDataFrame,
        edges: gpd.GeoDataFrame,
        basins: gpd.GeoDataFrame,
        split_nodes: gpd.GeoDataFrame
    ) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    create boundary-basin connections
    """
    print(f" - create Ribasim-Edges between Boundaries and Basins")
    split_nodes = split_nodes[
        (split_nodes["split_type"] != "no_split") & 
        (split_nodes["split_type"] != "harde_knip")
    ]
    if boundaries is None or nodes is None or basins is None:
        return None, split_nodes, basins
    
    # merge boundaries with nodes and basins
    boundaries_conn = boundaries[
        ["boundary_id", "boundary_type", "geometry"]
    ].sjoin_nearest(nodes[["node_no", "geometry", "basin"]])
    
    boundaries_conn = boundaries_conn.drop(
        columns=["index_right"]
    ).rename(columns={"node_no": "boundary_node_no"})
    boundaries_conn.crs = nodes.crs
    boundaries_conn = boundaries_conn.rename(
        columns={"geometry": "geom_boundary"}
    ).merge(
        basins[["basin", "geometry"]].rename(
            columns={"geometry": "geom_basin"}
        ), 
        how="left", 
        on="basin"
    )
        
    # Discharge boundaries (1 connection, always inflow)
    dischargebnd_conn_in = boundaries_conn[boundaries_conn.boundary_type == "FlowBoundary"]
    if dischargebnd_conn_in.empty:
        dischargebnd_conn_in["geometry"] = None
    else:
        dischargebnd_conn_in.loc[:, "geometry"] = dischargebnd_conn_in.apply(
            lambda x: LineString([x["geom_boundary"], x["geom_basin"]]),
            axis=1,
        )
    dischargebnd_conn_in['connection'] = 'boundary_to_basin'
    
    # Water level boundaries (additional split_node, 2 connections)
    waterlevelbnd_conn = boundaries_conn[boundaries_conn.boundary_type == "LevelBoundary"]

    if not waterlevelbnd_conn.empty:

        # Inflow connection
        waterlevelbnd_conn_in = waterlevelbnd_conn[
            waterlevelbnd_conn.boundary_node_no.isin(edges.from_node)
        ]
        waterlevelbnd_conn_in["geometry"] = None
        if not waterlevelbnd_conn_in.empty:
            waterlevelbnd_conn_in.loc[:, "geometry"] = waterlevelbnd_conn_in.apply(
                lambda x: LineString([x["geom_boundary"], x["geom_basin"]]), axis=1
            )
        waterlevelbnd_conn_in['connection'] = 'boundary_to_basin'

        # Outflow connection
        waterlevelbnd_conn_out = waterlevelbnd_conn[
            waterlevelbnd_conn.boundary_node_no.isin(edges.to_node)
        ]
        waterlevelbnd_conn_out["geometry"] = None
        if not waterlevelbnd_conn_out.empty:
            waterlevelbnd_conn_out.loc[:, "geometry"] = waterlevelbnd_conn_out.apply(
                lambda x: LineString([x["geom_basin"], x["geom_boundary"]]),
                axis=1,
            )
        waterlevelbnd_conn_out['connection'] = 'basin_to_boundary'
        
        waterlevelbnd_conns = gpd.GeoDataFrame(
            pd.concat([
                waterlevelbnd_conn_in,
                waterlevelbnd_conn_out,
            ], ignore_index=True), 
            geometry='geometry', 
            crs=basins.crs
        )

        waterlevelbnd_conns = waterlevelbnd_conns.sort_values(['boundary_id', 'basin']).reset_index(drop=True)

        boundaries_conn = gpd.GeoDataFrame(
            pd.concat([
                dischargebnd_conn_in, 
                waterlevelbnd_conns
            ]).reset_index(drop=True),
            geometry="geometry",
            crs=28992
        )
    else:
        boundaries_conn = dischargebnd_conn_in.copy()
    
    boundaries_conn = boundaries_conn.sort_values(by="boundary_id")
    return boundaries_conn, split_nodes, basins


def remove_basins_from_boundary(boun_conn, basin_conn, basin, nodes, edges):
    boun_conn_basins = boun_conn.merge(
        basin_conn[["basin", "split_node", "geom_split_node"]],
        how="left",
        on="basin"
    )
    boun_conn_basins["split_node"] = boun_conn_basins["split_node"].fillna(-1).astype(int)
    boun_conn_basins_excl = boun_conn_basins[boun_conn_basins["split_node"]==-1]
    boun_conn_basins_out = boun_conn_basins[
        (boun_conn_basins["split_node"]!=-1) & 
        (boun_conn_basins["connection"]=="basin_to_boundary")
    ]
    if not boun_conn_basins_out.empty:
        boun_conn_basins_out["connection"] = "split_node_to_boundary"
        boun_conn_basins_out["geometry"] = boun_conn_basins_out.apply(
            lambda x: LineString([x["geom_split_node"], x["geom_boundary"]]), axis=1,
        )
        nodes.loc[nodes.basin.isin(boun_conn_basins_out.basin.values), "basin"] = -1

    boun_conn_basins_in = boun_conn_basins[
        (boun_conn_basins["split_node"]!=-1) & 
        (boun_conn_basins["connection"]=="boundary_to_basin")
    ]
    if not boun_conn_basins_in.empty:
        boun_conn_basins_in["connection"] = "boundary_to_split_node"
        boun_conn_basins_in["geometry"] = boun_conn_basins_in.apply(
            lambda x: LineString([x["geom_boundary"], x["geom_split_node"]]), axis=1,
        )
        nodes.loc[nodes.basin.isin(boun_conn_basins_in.basin.values), "basin"] = -1
    boun_conn = pd.concat([boun_conn_basins_excl, boun_conn_basins_in, boun_conn_basins_out])
    
    basin_conn = basin_conn[~basin_conn.basin.isin(boun_conn.basin.values)]
    basin = basin[~basin.basin.isin(boun_conn.basin.values)]
    return boun_conn, basin_conn, basin, nodes, edges


def remove_boundary_basins_if_not_needed(
        basins: gpd.GeoDataFrame,
        basin_connections: gpd.GeoDataFrame,
        boundary_connections: gpd.GeoDataFrame,
        nodes: gpd.GeoDataFrame,
        edges: gpd.GeoDataFrame,
        include_flow_boundary_basins: bool = True,
        include_level_boundary_basins: bool = False,
    ):
    """
    FlowBoundaries and LevelBoundaries are connected to basins using the subgraph method.
    When include_flow_boundary_basins=False or include_level_boundary_basins=False
    the basins are removed and boundaries_nodes are directly connected to the split_nodes
    """
    flow_boundary_connections = boundary_connections[boundary_connections.boundary_type=="FlowBoundary"]
    level_boundary_connections = boundary_connections[boundary_connections.boundary_type=="LevelBoundary"]

    if not include_flow_boundary_basins and not flow_boundary_connections.empty:
        flow_boundary_connections, basin_connections, basins, nodes, edges = remove_basins_from_boundary(
            flow_boundary_connections, basin_connections, basins, nodes, edges
        )
    if not include_level_boundary_basins and not level_boundary_connections.empty:
        level_boundary_connections, basin_connections, basins, nodes, edges = remove_basins_from_boundary(
            level_boundary_connections, basin_connections, basins, nodes, edges
        )
    
    boundary_connections = pd.concat([flow_boundary_connections, level_boundary_connections])
    if "split_node" in boundary_connections.columns:
        boundary_connections["split_node"] = boundary_connections["split_node"].fillna(-1).astype(int)
    else:
        boundary_connections["split_node"] = -1
    boundary_connections = boundary_connections.sort_values(by="boundary_id").reset_index(drop=True)

    boundary_connections = boundary_connections.drop(
        columns=["geom_boundary", "geom_basin", "geom_split_node"], 
        errors='ignore'
    )
    basin_connections = basin_connections.drop(
        columns=["geom_basin", "geom_split_node"], 
        errors='ignore'
    )
    return boundary_connections, basin_connections, basins, nodes, edges


def remove_holes_from_basin_areas(basin_areas: gpd.GeoDataFrame, min_area: float):
    print(f" - remove holes within basin areas with less than {min_area/10000.0:.2f}ha")
    return remove_holes_from_polygons(geom=basin_areas, min_area=min_area)


def regenerate_node_ids(
        boundaries: gpd.GeoDataFrame,
        split_nodes: gpd.GeoDataFrame, 
        basins: gpd.GeoDataFrame,
        basin_connections: gpd.GeoDataFrame,
        boundary_connections: gpd.GeoDataFrame,
        basin_areas: gpd.GeoDataFrame,
        nodes: gpd.GeoDataFrame,
        edges: gpd.GeoDataFrame,
        areas: gpd.GeoDataFrame,
    ) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Regenerate ribasim node-id for nodes and edges
    """
    boundaries, split_nodes, basins, basin_connections = boundaries.copy(), split_nodes.copy(), basins.copy(), basin_connections.copy()
    boundary_connections, basin_areas, nodes, edges, areas = boundary_connections.copy(), basin_areas.copy(), nodes.copy(), edges.copy(), areas.copy()

    print(f" - regenerate node-ids Ribasim-Nodes and Ribasim-Edges")
    # boundaries
    if boundaries is not None:
        if "boundary_node_id" in boundaries.columns:
            boundaries = boundaries.drop(columns=["boundary_node_id"])
        boundaries.insert(1, "boundary_node_id", boundaries["boundary_id"])
            
        len_boundaries = len(boundaries)
    else:
        len_boundaries = 0

    # split_nodes
    if "split_node_node_id" not in split_nodes.columns:
        split_nodes.insert(loc=1, column="split_node_node_id", value=split_nodes["split_node"] + len_boundaries)
    else:
        split_nodes["split_node_node_id"] = split_nodes["split_node"] + len_boundaries
    len_split_nodes = len(split_nodes)

    # basins
    if "basin_node_id" not in basins.columns:
        basins.insert(loc=1, column="basin_node_id", value=basins["basin"] + len_split_nodes + len_boundaries)
    else:
        basins["basin_node_id"] = basins["basin"] + len_split_nodes + len_boundaries
    len_basins = len(basins)

    # basin_connections
    basin_connections["split_node_node_id"] = basin_connections["split_node"] + len_boundaries
    basin_connections["basin_node_id"] = basin_connections["basin"] + len_split_nodes + len_boundaries
    basin_connections["from_node_id"] = basin_connections.apply(
        lambda x: x["basin_node_id"] if x["connection"]=="basin_to_split_node" else x["split_node_node_id"], 
        axis=1
    )
    basin_connections["to_node_id"] = basin_connections.apply(
        lambda x: x["basin_node_id"] if x["connection"]=="split_node_to_basin" else x["split_node_node_id"], 
        axis=1
    )
    
    # boundary_connections
    boundary_connections["boundary_node_id"] = boundary_connections["boundary_id"]
    boundary_connections["split_node_node_id"] = boundary_connections["split_node"] + len_boundaries
    boundary_connections["basin_node_id"] = boundary_connections["basin"] + len_split_nodes + len_boundaries
    boundary_connections["from_node_id"] = boundary_connections.apply(
        lambda x: x["basin_node_id"] if x["connection"].startswith("basin") else (
            x["boundary_node_id"] if x["connection"].startswith("boundary") else x["split_node_node_id"]
        ), axis=1
    ).fillna(-1).astype(int)
    boundary_connections["to_node_id"] = boundary_connections.apply(
        lambda x: x["basin_node_id"] if x["connection"].endswith("basin") else (
            x["boundary_node_id"] if x["connection"].endswith("boundary") else x["split_node_node_id"]
        ), axis=1
    ).fillna(-1).astype(int)

    basin_areas = basin_areas.merge(basins[["basin", "basin_node_id"]], on="basin")
    basin_areas["basin_node_id"] = basin_areas["basin_node_id"].astype(int)

    areas["basin_node_id"] = areas["basin"].apply(lambda x: x + len_split_nodes + len_boundaries if x>0 else -1)
    nodes["basin_node_id"] = nodes["basin"].apply(lambda x: x + len_split_nodes + len_boundaries if x>0 else -1)
    edges["basin_node_id"] = edges["basin"].apply(lambda x: x + len_split_nodes + len_boundaries if x>0 else -1)

    connections = pd.concat([
        basin_connections[["from_node_id", "to_node_id"]], 
        boundary_connections[["from_node_id", "to_node_id"]]
    ])
    split_nodes = gpd.GeoDataFrame(split_nodes.merge(
        connections.set_index("to_node_id"), 
        how="left", 
        left_on="split_node_node_id", 
        right_index=True
    ), geometry="geometry", crs=split_nodes.crs)
    split_nodes = gpd.GeoDataFrame(split_nodes.merge(
        connections.set_index("from_node_id"), 
        how="left", 
        left_on="split_node_node_id", 
        right_index=True
    ), geometry="geometry", crs=split_nodes.crs)

    # the above actions can result in duplicate entries in tables. only keep one records of those duplicates
    boundaries = boundaries.loc[~boundaries.duplicated()].copy()
    split_nodes = split_nodes.loc[~split_nodes.duplicated()].copy()
    basins = basins.loc[~basins.duplicated()].copy()
    basin_areas = basin_areas.loc[~basin_areas.duplicated()].copy()
    nodes = nodes.loc[~nodes.duplicated()].copy()
    edges = edges.loc[~edges.duplicated()].copy()
    areas = areas.loc[~areas.duplicated()].copy()

    return boundaries, split_nodes, basins, basin_areas, nodes, edges, areas, basin_connections, boundary_connections


def check_basins_connected_to_basin_areas(
        basins: gpd.GeoDataFrame, 
        basin_areas: gpd.GeoDataFrame,
        boundary_connections: gpd.GeoDataFrame,
    ):
    """
     Check if basin locations are connected with a basin area (ignoring basins generated for boundary connections)
    """
    print(' - check if basins are connected to a basin area (ignoring basins generated for boundary connections)')
    _basins = basins.loc[~np.isin(basins['basin'].values, boundary_connections['basin'].values)]
    _basins = _basins.loc[~np.isin(_basins['basin'].values, basin_areas['basin'].values)]
    if not _basins.empty:
        print('   - Following basins are not connected to a basin area (either due to incorrect network input or multiple basins in the same basin area):')
        print(f'      Basins ', end="", flush=True)
        for b in sorted(_basins['basin'].values):
            print(f"{b}, ", end="", flush=True)
    print("")


def remove_isolated_basins_and_update_administration(
        basins: gpd.GeoDataFrame,
        basin_areas: gpd.GeoDataFrame,
        areas: gpd.GeoDataFrame,
        basin_connections: gpd.GeoDataFrame,
        boundary_connections: gpd.GeoDataFrame,
        edges: gpd.GeoDataFrame,
        nodes: gpd.GeoDataFrame,
    ) -> Tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """
    Remove isolated basins (including 1-on-1 connected basin areas) based on basin and boundary connections.
    Update the basin administration in basin areas, areas, edges and nodes
    Returns basin, basin areas, areas, edges and nodes
    """
    basins, basin_areas, edges, nodes = basins.copy(), basin_areas.copy(), edges.copy(), nodes.copy()

    # get isolated basins
    connected_basins = np.unique(basin_connections['basin'].tolist() + boundary_connections['basin'].tolist())
    isolated_basins = basins.loc[~np.isin(basins['basin'].values, connected_basins), 'basin'].values
    print(f' - removing isolated basins ({len(isolated_basins)}x)')
    if len(isolated_basins) == 0:
        return basins, basin_areas, edges, nodes  # no isolated basins so no further steps needed

    # get mapping of which basin are located within a basin area
    _basin_areas = basin_areas.sjoin(basins, how='left')
    basin_area_to_basin = {k: _basin_areas.loc[[k], 'basin_right'].values for k in _basin_areas.index.unique()}
    basin_area_to_basin = {k: v for k, v in basin_area_to_basin.items() if not all(np.isnan(v))}

    # get mapping of removed isolated basins to new basin based on the location within the same basin area
    basin_to_basin_area = {_v: k for k, v in basin_area_to_basin.items() for _v in v}
    isolated_basin_to_basin = {ib: basin_area_to_basin[basin_to_basin_area[ib]] for ib in isolated_basins if ib in basin_to_basin_area.keys()}
    isolated_basin_to_basin = {k: v[np.isin(v, basins.loc[np.isin(basins['basin'].values, connected_basins)]['basin'])] for k, v in isolated_basin_to_basin.items()}
    isolated_basin_to_basin = {k: int(v[0]) if len(v) > 0 else -(1000 + k) for k, v in isolated_basin_to_basin.items()}

    # update basins, basin areas, edges and nodes. if removed basin is 1-on-1 connected to a basin area, the basin area will be removed.
    # this also means that the basin and basin area of associated nodes and edges of that removed basin will be removed
    basins = basins.loc[np.isin(basins['basin'].values, connected_basins)]
    basin_areas['basin'] = [isolated_basin_to_basin[b] if b in isolated_basin_to_basin.keys() else b for b in basin_areas['basin']]
    edges['basin'] = [isolated_basin_to_basin[b] if b in isolated_basin_to_basin.keys() else b for b in edges['basin']]
    nodes['basin'] = [isolated_basin_to_basin[b] if b in isolated_basin_to_basin.keys() else b for b in nodes['basin']]
    # remove 1-on-1 connected basin areas
    edges['basin_area'] = [-1 if b < -10 else ba for b, ba in zip(edges['basin'], edges['basin_area'])]
    nodes['basin_area'] = [-1 if b < -10 else ba for b, ba in zip(nodes['basin'], nodes['basin_area'])]
    basin_areas = basin_areas.loc[basin_areas['basin'] >= 0]  
    # update areas
    areas.loc[np.isin(areas['basin'].values, isolated_basins), 'basin'] = np.nan

    return basins, basin_areas, areas, edges, nodes


def generate_ribasim_network_using_split_nodes(
        nodes: gpd.GeoDataFrame,
        edges: gpd.GeoDataFrame,
        split_nodes: gpd.GeoDataFrame,
        areas: gpd.GeoDataFrame,
        boundaries: gpd.GeoDataFrame,
        laterals: gpd.GeoDataFrame,
        use_laterals_for_basin_area: bool,
        remove_isolated_basins: bool,
        include_flow_boundary_basins: bool = True,
        include_level_boundary_basins: bool = False,
        remove_holes_min_area: float = 10.0,
        crs: int = 28992,
        option_edges_hydroobjects: bool = False,
    ) -> Dict:
    """create basins (nodes) and basin_areas (large polygons) and connections (edges)
    based on nodes, edges, split_nodes and areas (discharge units).
    This function calls all other functions
    """
    network_graph = None
    basin_areas = None
    basins = None
    network_graph = create_graph_based_on_nodes_edges(
        nodes=nodes,
        edges=edges
    )
    network_graph, split_nodes = split_graph_based_on_split_nodes(
        graph=network_graph, 
        split_nodes=split_nodes,
        edges=edges
    )
    nodes, edges = add_basin_code_from_network_to_nodes_and_edges(
        graph=network_graph, 
        split_nodes=split_nodes,
        nodes=nodes,
        edges=edges
    )
    split_nodes = check_if_split_node_is_used(
        split_nodes=split_nodes,
        nodes=nodes,
        edges=edges
    )
    basins = create_basins_based_on_subgraphs_and_nodes(
        graph=network_graph, 
        nodes=nodes,
        edges=edges
    )
    if use_laterals_for_basin_area:
        areas, basin_areas = create_basin_areas_based_on_drainage_unit_areas(
            edges=edges, 
            areas=areas,
            laterals=laterals
        )
    else:
        areas, basin_areas = create_basin_areas_based_on_drainage_unit_areas(
            edges=edges, 
            areas=areas
        )
    nodes, edges = check_if_nodes_edges_within_basin_areas(
        nodes=nodes, 
        edges=edges, 
        basin_areas=basin_areas
    )
    basin_connections = create_basin_connections(
        split_nodes=split_nodes,
        basins=basins,
        nodes=nodes,
        edges=edges,
        crs=crs,
        option_edges_hydroobjects=option_edges_hydroobjects,
    )
    boundary_connections, split_nodes, basins = create_boundary_connections(
        boundaries=boundaries,
        split_nodes=split_nodes,
        basins=basins,
        nodes=nodes,
        edges=edges,
    )
    boundary_connections, basin_connections, basins, nodes, edges  = remove_boundary_basins_if_not_needed(
        basins=basins,
        basin_connections=basin_connections,
        boundary_connections=boundary_connections,
        include_flow_boundary_basins=include_flow_boundary_basins,
        include_level_boundary_basins=include_level_boundary_basins,
        nodes=nodes,
        edges=edges
    )
    if remove_isolated_basins:
        basins, basin_areas, areas, edges, nodes = remove_isolated_basins_and_update_administration(
            basins=basins, 
            basin_areas=basin_areas,
            areas=areas,
            basin_connections=basin_connections,
            boundary_connections=boundary_connections,
            edges=edges,
            nodes=nodes,
        )
    
    basin_areas = remove_holes_from_basin_areas(
        basin_areas=basin_areas, 
        min_area=remove_holes_min_area
    )

    results = regenerate_node_ids(
        boundaries=boundaries,
        split_nodes=split_nodes,
        basins=basins,
        basin_connections=basin_connections,
        boundary_connections=boundary_connections,
        basin_areas=basin_areas,
        nodes=nodes,
        edges=edges,
        areas=areas
    )
    boundaries, split_nodes, basins, basin_areas, \
        nodes, edges, areas, basin_connections, boundary_connections = results

    # check_basins_connected_to_basin_areas(
    #     basins=basins, 
    #     basin_areas=basin_areas,
    #     boundary_connections=boundary_connections,
    # )
    
    return dict(
        basin_areas=basin_areas,
        boundaries=boundaries,
        basins=basins,
        areas=areas,
        nodes=nodes,
        edges=edges,
        split_nodes=split_nodes,
        network_graph=network_graph,
        basin_connections=basin_connections,
        boundary_connections=boundary_connections,
    )
