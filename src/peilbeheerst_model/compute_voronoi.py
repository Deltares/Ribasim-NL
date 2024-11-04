import centerline.geometry
import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import shapely
import shapely.geometry
import tqdm.auto as tqdm

df = gpd.read_file("../../../../Data_postprocessed/Waterschappen/Wetterskip/Wetterskip.gpkg", layer="peilgebied")
df["geometry"] = df.buffer(0)
df = df[~df.is_empty].copy()
df["geometry"] = df.geometry.apply(shapely.force_2d)
df = df[df.peilgebied_cat == 1].copy()

df_crossings = gpd.read_file(
    "../../../../Data_crossings/Wetterskip/wetterskip_crossings_v05.gpkg", layer="crossings_hydroobject_filtered"
)
df_crossings = df_crossings[df_crossings.agg_links_in_use].copy()


# Merge polygons with a small buffer. Ook nodig om verbindingen te krijgen in sommige smalle watergangen.
df_merged = df.buffer(1.0).unary_union
df_merged = gpd.GeoDataFrame(geometry=list(df_merged.geoms), crs=df.crs)

# add merged id to original polygons
merged_poly_ids = []
for row in tqdm.tqdm(df.itertuples(), total=len(df)):
    idxs = df_merged.sindex.query(row.geometry, predicate="intersects")
    if len(idxs) == 0:
        raise ValueError("no matches")
    elif len(idxs) > 1:
        overlaps = []
        for idx in idxs:
            overlap = df_merged.iat[idx].intersection(row.geometry).area / row.geometry.area
            overlaps.append(overlap)
        idx = idxs.index(max(overlaps))
    else:
        idx = idxs[0]
    merged_poly_ids.append(idx)
df["merged_poly_id"] = merged_poly_ids


df_center = []
for idx, row in tqdm.tqdm(df_merged.iterrows(), total=len(df_merged)):
    geom = row.geometry
    interp_dist = 10
    if geom.area < 1000:
        interp_dist = 1
    if geom.area < 100:
        interp_dist = 0.1
    if geom.area < 10:
        interp_dist = 0.01
    if geom.area < 1:
        interp_dist = 0.001
    centerpoly = centerline.geometry.Centerline(geom, interpolation_distance=interp_dist)
    centerpoly = centerpoly.geometry
    centerpoly = centerpoly.simplify(1, preserve_topology=True)
    df_center.append(centerpoly)
df_center = gpd.GeoDataFrame(geometry=list(df_center), crs=df.crs)


df_center_single = df_center.explode(index_parts=False)
df_center_single = df_center_single.set_index(np.arange(len(df_center_single)), append=True)
df_center_single.index.set_names(["poly_id", "edge_id"], inplace=True)

df_center_single_boundary = df_center_single.copy()
df_center_single_boundary["geometry"] = df_center_single.boundary

# # Check of alles mooi verbonden is
# for i, row in tqdm.tqdm(enumerate(df_center_single_boundary.itertuples()), total=len(df_center_single_boundary), desc="check connections"):
#     idx = row.Index
#     geom = row.geometry

#     idxs, dists = df_center_single_boundary.sindex.nearest(geom, return_distance=True, return_all=True)
#     idxs = idxs[1, :]
#     dists = dists[idxs != i]
#     idxs = idxs[idxs != i]
#     if dists.min() > 0:
#         print(f"no closed connection for {idx}, {dist.min()=}")
#     elif len(idxs) == 0:
#         print(f"No connection for {idx}: {df_center_single_boundary.iloc[idxs].index}")

df_center_single_boundary_points = df_center_single_boundary.explode(index_parts=True)
df_center_single_boundary_points["node_id"] = None
df_center_single_boundary_points["connectivity"] = None

node_id = 0
idxs, node_ids, connectivity = [], [], []
for poly_id, poly_group in tqdm.tqdm(
    df_center_single_boundary_points.groupby("poly_id", sort=False), desc="assign node ids"
):
    for geom, group in tqdm.tqdm(poly_group.groupby("geometry", sort=False), desc=f"{poly_id=}", leave=False):
        idxs.append(group.index)
        node_ids.append(len(group) * [node_id])
        connectivity.append(len(group) * [len(group)])
        node_id += 1

df_center_single_boundary_points.loc[np.hstack(idxs), "node_id"] = np.hstack(node_ids)
df_center_single_boundary_points.loc[np.hstack(idxs), "connectivity"] = np.hstack(connectivity)
idxs, node_ids, connectivity = None, None, None

assert not pd.isna(df_center_single_boundary_points.node_id).any()
assert not pd.isna(df_center_single_boundary_points.connectivity).any()

df_center_single_boundary_points = df_center_single_boundary_points.droplevel(-1).set_index("node_id", append=True)
df_center_single_boundary_points


# Alleen edges proberen te mergen waarvan beide uiteindes (nodes) connectivity 2 hebben
pot_reduce = []
for edge_id, group in tqdm.tqdm(
    df_center_single_boundary_points.groupby("edge_id", sort=False), desc="Find connectivity=2"
):
    if (group.connectivity == 2).all():
        pot_reduce.append(edge_id)
pot_reduce = df_center_single_boundary.loc[pd.IndexSlice[:, pot_reduce], :].copy()

# Identify merge groups
edges_visited = {}
merge_group = 0
pot_reduce["merge_group"] = None
for poly_id, polygroup in tqdm.tqdm(pot_reduce.groupby("poly_id", sort=False), desc="group edges per polygon"):
    for edge_id, group in tqdm.tqdm(polygroup.groupby("edge_id", sort=False), leave=False, desc=f"{poly_id=}"):
        if edge_id in edges_visited:
            continue

        ivec = np.where(polygroup.index.isin(group.index))[0]
        prev_len = 0
        while len(ivec) != prev_len:
            prev_len = len(ivec)
            ivec = polygroup.sindex.query(polygroup.geometry.iloc[ivec], predicate="intersects")
            ivec = np.unique(ivec[1, :])

        lbls = polygroup.index[ivec]
        assert len(pot_reduce.loc[lbls].index.get_level_values("poly_id").unique()) == 1
        pot_reduce.loc[lbls, "merge_group"] = merge_group

        for eid in lbls.get_level_values("edge_id"):
            edges_visited[eid] = True
        merge_group += 1

# Merge
df_center_single_red = df_center_single[~df_center_single.index.isin(pot_reduce.index)].copy()
add_rows = []
for group_id, group in tqdm.tqdm(pot_reduce.groupby("merge_group", dropna=True, sort=False), desc="merge edges"):
    edges_to_merge = np.unique(group.index.get_level_values("edge_id").to_numpy())
    geoms = df_center_single.geometry.loc[pd.IndexSlice[:, edges_to_merge]].tolist()
    geom = shapely.ops.linemerge(geoms)
    assert geom.geom_type == "LineString"
    single_row = df_center_single.loc[pd.IndexSlice[:, edges_to_merge[0]], :].copy()
    single_row.loc[:, "geometry"] = geom
    add_rows.append(single_row)

# Overwrite dataframes
df_center_single = pd.concat([df_center_single_red] + add_rows)

df_center_single_boundary = df_center_single.copy()
df_center_single_boundary["geometry"] = df_center_single.boundary

df_center_single_boundary_points = df_center_single_boundary.explode(index_parts=True)
df_center_single_boundary_points["node_id"] = None
idxs, node_ids = [], []
for node_id, (geom, group) in enumerate(
    tqdm.tqdm(df_center_single_boundary_points.groupby("geometry", sort=False), desc="assign node ids")
):
    idxs.append(group.index)
    node_ids.append(len(group) * [node_id])
df_center_single_boundary_points.loc[np.hstack(idxs), "node_id"] = np.hstack(node_ids)
assert not pd.isna(df_center_single_boundary_points.node_id).any()
df_center_single_boundary_points = df_center_single_boundary_points.droplevel(-1).set_index("node_id", append=True)

# # Check of alles mooi verbonden is
# for i, row in tqdm.tqdm(enumerate(df_center_single_boundary.itertuples()), total=len(df_center_single_boundary), desc="check connections"):
#     idx = row.Index
#     geom = row.geometry

#     idxs, dists = df_center_single_boundary.sindex.nearest(geom, return_distance=True, return_all=True)
#     idxs = idxs[1, :]
#     dists = dists[idxs != i]
#     idxs = idxs[idxs != i]
#     if dists.min() > 0:
#         print(f"no closed connection for {idx}, {dist.min()=}")
#     elif len(idxs) == 0:
#         print(f"No connection for {idx}: {df_center_single_boundary.iloc[idxs].index}")

df_center_single_boundary_points


edge_lengths = dict(zip(df_center_single.index.get_level_values("edge_id"), df_center_single.length))
shortest_paths = {"poly_id": [], "start_node": [], "end_node": [], "geometry": []}
for poly_id, row in tqdm.tqdm(df_merged.iterrows(), total=len(df_merged)):
    merged_poly = row.geometry

    globalids = df.globalid.loc[df.merged_poly_id == poly_id].unique()
    df_crossings_single = df_crossings[
        df_crossings.peilgebied_from.isin(globalids) | df_crossings.peilgebied_to.isin(globalids)
    ].copy()

    # End point
    df_graph = df_center_single_boundary_points.loc[pd.IndexSlice[poly_id, :, :], :].copy()
    idx_end, distance_end = df_graph.sindex.nearest(
        merged_poly.representative_point(), return_distance=True, return_all=False
    )
    distance_end = distance_end[0]
    idx_end = idx_end[1, 0]
    idx_end = df_graph.index[idx_end]
    end_node = idx_end[-1]
    df_crossings
    # print(f"{poly_id=}, closest vertex for endpoint at {distance_end:.2f}m ({idx_end=})")

    # Starting points
    idxs, distances = df_graph.sindex.nearest(df_crossings_single.geometry, return_distance=True, return_all=False)
    idx_cross = df_crossings_single.iloc[idxs[0, :]].index
    df_crossings_single.loc[idx_cross, "start_node"] = df_graph.iloc[idxs[1, :]].index.get_level_values("node_id")
    df_crossings.loc[idx_cross, "start_node"] = df_graph.iloc[idxs[1, :]].index.get_level_values("node_id")
    start_nodes = df_crossings_single["start_node"].dropna().unique().astype(int).tolist()

    # Make network for this polygon
    node_ids = df_graph.index.get_level_values("node_id")
    graph = nx.Graph()

    # Add nodes and edges
    graph.add_nodes_from(node_ids.unique().tolist())
    for edge_id, group in df_graph.groupby("edge_id", sort=False):
        node1, node2 = group.index.get_level_values("node_id").tolist()
        graph.add_edge(node1, node2, weight=edge_lengths[edge_id])

    # Determine shortest path for each start node
    for start_node in tqdm.tqdm(start_nodes, leave=False, desc=f"{poly_id=}"):
        try:
            # node_path = nx.dijkstra_path(graph, start_node, end_node)
            node_path = nx.astar_path(graph, start_node, end_node)
            edges = df_graph.loc[pd.IndexSlice[:, :, node_path]].index.get_level_values("edge_id").to_numpy()
            geom = shapely.ops.linemerge(df_center_single.geometry.loc[pd.IndexSlice[poly_id, edges]].tolist())
            shortest_paths["poly_id"].append(poly_id)
            shortest_paths["start_node"].append(start_node)
            shortest_paths["end_node"].append(end_node)
            shortest_paths["geometry"].append(geom)
        except Exception as e:
            print(e)
            pass

df_startcrossings = df_crossings[~pd.isna(df_crossings.start_node)].copy()
shortest_paths = gpd.GeoDataFrame(shortest_paths, geometry="geometry", crs=df_crossings.crs)


df_merged.to_file("test_voronoi.gpkg", layer="merged_poly")
df_center_single.to_file("test_voronoi.gpkg", layer="edges")
df_center_single_boundary_points.to_file("test_voronoi.gpkg", layer="nodes")
shortest_paths.to_file("test_voronoi.gpkg", layer="shortest_paths")
df_startcrossings.to_file("test_voronoi.gpkg", layer="start_crossings")


# # Check of alles mooi verbonden is
# for poly_id, polygroup in tqdm.tqdm(df_center_single_boundary.groupby("poly_id", sort=False), desc="check connections"):
#     for i, row in enumerate(polygroup.itertuples()):
#         idx = row.Index
#         geom = row.geometry

#         idxs = polygroup.sindex.query(geom, predicate="intersects")
#         idxs = idxs[idxs != i]
#         dists = polygroup.geometry.iloc[idxs].distance(row.geometry)
#         if dists.min() > 0:
#             print(f"no closed connection for {idx}, {dist.min()=}")
#         if len(idxs) == 0:
#             print(f"No connection for {idx}: {polygroup.iloc[idxs].index}")
