# %%
import geopandas as gpd
import networkx as nx
import pandas as pd
import ribasim
from networkx import NetworkXNoPath
from ribasim_nl import CloudStorage, Network
from ribasim_nl.geodataframe import basins_to_points
from shapely.geometry import LineString

# %% import network
cloud = CloudStorage()
network = Network.from_network_gpkg(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk_2.gpkg")
)
network_nodes_gdf = network.nodes
PRECIPITATION = 0.005 / 86400  # m/s
EVAPORATION = 0.001 / 86400  # m/s
LEVEL = [0.0, 1.0]
AREA = [0.01, 1000.0]

# %% add flow_boundaries
flow_boundaries_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "model_user_data.gpkg"),
    engine="pyogrio",
    layer="flow_boundary",
)
data = []
for row in flow_boundaries_gdf.itertuples():
    node_id = network_nodes_gdf.geometry.distance(row.geometry).sort_values().index[0]
    flow = row.flow
    geometry = network_nodes_gdf.loc[node_id].geometry
    data += [
        {
            "unique_id": node_id,
            "flow_rate": flow,
            "geometry": geometry,
            "type": "FlowBoundary",
        }
    ]

flow_boundary_gdf = gpd.GeoDataFrame(data=data, crs=flow_boundaries_gdf.crs)

# %% add level_boundaries
level_boundaries_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "model_user_data.gpkg"),
    engine="pyogrio",
    layer="level_boundary",
)
data = []
for row in level_boundaries_gdf.itertuples():
    node_id = network_nodes_gdf.geometry.distance(row.geometry).sort_values().index[0]
    geometry = network_nodes_gdf.loc[node_id].geometry
    data += [
        {
            "unique_id": node_id,
            "level": 0,
            "geometry": geometry,
            "type": "LevelBoundary",
        }
    ]

level_boundary_gdf = gpd.GeoDataFrame(data=data, crs=level_boundaries_gdf.crs)

# %% add basins
basin_poly_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_vlakken.gpkg"),
    engine="pyogrio",
)

# mask to vaarwegen, exclude for lakes
print("create mask")
vaarwegen_mask = (
    gpd.read_file(
        cloud.joinpath(
            "Rijkswaterstaat",
            "aangeleverd",
            "rws_leggerbegrenzing_rijksvaarweg_legger.gpkg",
        )
    )
    .buffer(50)
    .unary_union
)
lakes_mask = basin_poly_gdf[
    ~basin_poly_gdf.owmident.isin(
        ["NL92_IJSSELMEER", "NL89_westsde", "NL92_MARKERMEER"]
    )
].unary_union
mask = vaarwegen_mask.intersection(lakes_mask)

print("generate basin nodes")
basin_gdf = basins_to_points(basin_poly_gdf, network, mask, buffer=5)
basin_gdf["type"] = "Basin"
basin_gdf.rename(columns={"node_id": "unique_id"}, inplace=True)

# %% do edges


def get_coordinates(node_from, node_to, links):
    # get geometries from links
    reverse = False
    geometries = links.loc[
        (links.node_from == node_from) & (links.node_to == node_to), ["geometry"]
    ]
    if geometries.empty:
        geometries = links.loc[
            (links.node_from == node_to) & (links.node_to == node_from), ["geometry"]
        ]
        if not geometries.empty:
            reverse = True
        else:
            raise ValueError(
                f"{node_from}, {node_to} not valid start and end nodes in the network"
            )

    # select geometry
    if len(geometries) > 1:
        idx = geometries.length.sort_values(ascending=False).index[0]
        geometry = geometries.loc[idx].geometry
    elif len(geometries) == 1:
        geometry = geometries.iloc[0].geometry

    # invert geometry
    if reverse:
        geometry = geometry.reverse()

    return list(geometry.coords)


def path_to_geometry(path, links):
    coords = []
    for vertice_from, vertice_to in zip(path[0:-1], path[1:]):
        coords += get_coordinates(vertice_from, vertice_to, links)
    return LineString(coords)


def find_paths(node_from, nodes_to, network, one_to_one=False):
    paths = []
    for node_to in nodes_to:
        if node_to != node_from:
            try:
                if one_to_one:
                    path = nx.shortest_path(
                        network.graph, node_from, node_to, weight="length"
                    )
                else:
                    path = nx.shortest_path(
                        network.graph_undirected, node_from, node_to, weight="length"
                    )
            except NetworkXNoPath:
                continue
            mask = [i for i in nodes_to if i != node_to]
            if not any(i in mask for i in path[1:-1]):
                if any(i in path for i in network.graph.neighbors(node_from)):
                    paths += [path]

    if one_to_one and len(paths) > 1:
        raise ValueError(f"found more than one edge from node {node_from}")
    return paths


# %% prepare links and nodes
links = network.links
nodes = network.nodes
basin_poly_gdf["unique_id"] = basin_gdf.unique_id.to_numpy()
basin_ids = gpd.overlay(
    nodes.reset_index(), basin_poly_gdf[["unique_id", "geometry"]], how="intersection"
)[["node_id", "unique_id"]]
for row in basin_ids.itertuples():
    nodes.loc[row.node_id, ["unique_id"]] = row.unique_id

data = []
edges_passed = []

# %% add boundary-edges
for node_from in flow_boundary_gdf.unique_id:
    path = find_paths(node_from, basin_gdf.unique_id, network, one_to_one=True)[0]
    geometry = path_to_geometry(path, links)
    data += [
        {"unique_id_from": node_from, "unique_id_to": path[-1], "geometry": geometry}
    ]


# %% add edges
def resistance_id(node_id, path, reversed=False):
    if reversed:
        return f"{node_id}_{path[-1]}_{path[0]}"
    else:
        return f"{node_id}_{path[0]}_{path[-1]}"


def add_resistance(nodes_select, path):
    node_id = nodes_select.index[0]
    unique_id = resistance_id(node_id, path)
    geometry = nodes_select.loc[node_id].geometry
    return [{"unique_id": unique_id, "geometry": geometry, "type": "LinearResistance"}]


def add_edges(nodes_select, path, links):
    edges = []
    node_id = nodes_select.index[0]
    unique_id = resistance_id(node_id, path)
    geometry = path_to_geometry(path[0 : path.index(node_id) + 1], links)
    edges += [
        {"unique_id_from": node_from, "unique_id_to": unique_id, "geometry": geometry}
    ]

    geometry = path_to_geometry(path[path.index(node_id) :], links)
    edges += [
        {"unique_id_from": unique_id, "unique_id_to": path[-1], "geometry": geometry}
    ]
    return edges


resistance_data = []

basin_poly_gdf.set_index("unique_id", inplace=True)
nodes_to = basin_gdf.unique_id.to_list() + level_boundary_gdf.unique_id.to_list()
for node_from in basin_gdf.unique_id:
    paths = find_paths(node_from, nodes_to, network)
    if len(paths) == 0:
        print(f"no path found for {node_from}, please fix network!")
        continue
    else:
        for path in paths:
            node_to = path[-1]

            # check if edge hasn't been passed in reversed direction
            if (node_to, node_from) in edges_passed:
                continue
            else:
                edges_passed += [(node_from, node_to)]

            nodes_select = nodes.loc[path[1:-1]]

            # if there are no extra nodes between node_from and node_to, we can't add a flow node
            if nodes_select.empty:
                raise Exception(f"no extra nodes between {node_from} and {path[-1]}")

            # else, we check if the two basins are adjacent, all nodes should either be node_from, or node_to.
            # If not, we don't have a valid path and we continue
            elif (
                not nodes_select[nodes_select.unique_id.notna()]
                .unique_id.isin([node_from, node_to])
                .all()
            ):
                continue

            elif len(nodes_select) == 1:
                resistance_data += add_resistance(nodes_select, path)
                data += add_edges(nodes_select, path, links)
                continue
            # case we have multiple nodes, we try to find the one on the node_from poly boundary
            else:
                nodes_select = nodes_select[
                    nodes_select.geometry.buffer(5).intersects(
                        basin_poly_gdf.loc[node_from].geometry.boundary
                    )
                ]
            if nodes_select.empty:
                print(
                    f"no extra nodes on poly boundary of between {node_from}, please fix network!"
                )
                continue

            elif len(nodes_select) == 1:
                resistance_data += add_resistance(nodes_select, path)
                data += add_edges(nodes_select, path, links)
            # case we have multiple nodes on node_from poly boundary, we find the closest to node_to
            else:
                if node_to in basin_poly_gdf.index:
                    geometry = basin_poly_gdf.loc[node_to].geometry
                elif node_to in level_boundary_gdf.unique_id.to_list():
                    geometry = (
                        level_boundary_gdf.set_index("unique_id").loc[node_to].geometry
                    )
                    nodes_select = nodes_select.loc[
                        [nodes_select.distance(geometry).sort_values().index[0]]
                    ]
                resistance_data += add_resistance(nodes_select, path)
                data += add_edges(nodes_select, path, links)


resistance_gdf = gpd.GeoDataFrame(resistance_data, crs=network.lines_gdf.crs)
resistance_gdf["resistance"] = float(1000)

edge_gdf = gpd.GeoDataFrame(data, crs=network.lines_gdf.crs)
edge_gdf["edge_type"] = "flow"

# %% write model
print("concatenate all nodes")
node_gdf = pd.concat(
    [basin_gdf, resistance_gdf, flow_boundary_gdf, level_boundary_gdf],
    ignore_index=True,
)

if node_gdf.unique_id.duplicated().any():
    raise Exception(
        f"node_gdf contains non-unique-ids, please fix: {node_gdf[node_gdf.unique_id.duplicated()].unique_id.to_list()}"
    )

# reset node-ids for Ribasim
node_gdf.set_index("unique_id", drop=False, inplace=True)
node_gdf["node_id"] = range(1, len(node_gdf) + 1)

# finalize basins
basin_gdf["type"] = "Basin"
basin_gdf["node_id"] = basin_gdf["unique_id"].apply(lambda x: node_gdf.loc[x].node_id)

print("create basin static")
basin_static_df = basin_gdf[["node_id"]].copy()
basin_static_df["drainage"] = 0.0
basin_static_df["potential_evaporation"] = EVAPORATION
basin_static_df["infiltration"] = 0.0
basin_static_df["precipitation"] = PRECIPITATION
basin_static_df["urban_runoff"] = 0.0


print("create profile")

if False:
    basin_profile_df = pd.read_csv(
        cloud.joinpath(
            "Rijkswaterstaat", "verwerkt", "krw_basins_vlakken_level_area.csv"
        )
    )
    basin_profile_df["node_id"] = basin_profile_df["id"].apply(
        lambda x: basin_gdf.set_index("basin_id").loc[x].node_id
    )
    basin_profile_df = basin_profile_df[["node_id", "level", "area"]].drop_duplicates()
else:
    basin_profile_df = pd.DataFrame(
        data=[
            [node, area, level]
            for node in basin_static_df["node_id"]
            for area, level in list(zip(AREA, LEVEL))
        ],
        columns=["node_id", "area", "level"],
    )

print("add ribasim basin")
basin = ribasim.Basin(profile=basin_profile_df, static=basin_static_df)

# finalize boundaries
print("add ribasim level_boundary")
level_boundary_gdf["node_id"] = level_boundary_gdf["unique_id"].apply(
    lambda x: node_gdf.loc[x].node_id
)
level_boundary = ribasim.LevelBoundary(static=level_boundary_gdf[["node_id", "level"]])

print("add ribasim flow_boundary")
flow_boundary_gdf["node_id"] = flow_boundary_gdf["unique_id"].apply(
    lambda x: node_gdf.loc[x].node_id
)
flow_boundary = ribasim.FlowBoundary(static=flow_boundary_gdf[["node_id", "flow_rate"]])

print("add ribasim resistance")
resistance_gdf["node_id"] = resistance_gdf["unique_id"].apply(
    lambda x: node_gdf.loc[x].node_id
)
linear_resistance = ribasim.LinearResistance(
    static=resistance_gdf[["node_id", "resistance"]]
)

print("add ribasim edge")
edge_gdf["from_node_id"] = edge_gdf["unique_id_from"].apply(
    lambda x: node_gdf.loc[x].node_id
)
edge_gdf["to_node_id"] = edge_gdf["unique_id_to"].apply(
    lambda x: node_gdf.loc[x].node_id
)
edge = ribasim.Edge(static=edge_gdf)

print("add ribasim node")
node_gdf.index = node_gdf["node_id"]
node_gdf.index.name = "fid"
node = ribasim.Node(static=node_gdf)

# %%
print("add all to ribasim model")
model = ribasim.Model(
    node=node,
    edge=edge,
    basin=basin,
    linear_resistance=linear_resistance,
    flow_boundary=flow_boundary,
    level_boundary=level_boundary,
    starttime="2020-01-01 00:00:00",
    endtime="2021-01-01 00:00:00",
)
# %%
print("write ribasim model")
ribasim_model_dir = cloud.joinpath("Rijkswaterstaat", "modellen", "rijkswateren")
model.write(ribasim_model_dir)

# %%
