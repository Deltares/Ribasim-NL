# %%
import geopandas as gpd
import networkx as nx
import pandas as pd
import ribasim
from networkx import NetworkXNoPath
from ribasim_nl import CloudStorage, Network, reset_index

PRECIPITATION = 0.005 / 86400  # m/s
EVAPORATION = 0.001 / 86400  # m/s

# %% read files
edge_list = []
node_list = []


cloud = CloudStorage()
network = Network.from_network_gpkg(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk.gpkg")
)

flow_boundaries_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "model_user_data.gpkg"),
    engine="pyogrio",
    layer="flow_boundary",
    fid_as_index=True,
)

basin_poly_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_vlakken.gpkg"),
    engine="pyogrio",
    fid_as_index=True,
)

structures_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "hydamo.gpkg"),
    layer="kunstwerken",
    driver="GPKG",
    fid_as_index=True,
)

level_area_df = pd.read_csv(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_vlakken_level_area.csv")
)

# %% ophalen network-nodes en links
nodes_gdf = network.nodes
links_gdf = network.links

# %% toevoegen basin_index aan nodes
basin_poly_gdf["fid"] = basin_poly_gdf.index
basin_ids = gpd.overlay(
    nodes_gdf.reset_index(),
    basin_poly_gdf[["fid", "geometry"]],
    how="intersection",
)[["node_id", "fid"]]
basin_ids = basin_ids.drop_duplicates(subset="node_id", keep="first")
basin_ids.set_index("node_id", inplace=True)

nodes_gdf["node_id_us"] = [[] for _ in range(len(nodes_gdf))]
for row in nodes_gdf.itertuples():
    if row.Index in basin_ids.index:
        nodes_gdf.loc[row.Index, ["basin_fid"]] = int(basin_ids.loc[row.Index]["fid"])
    nodes_gdf.at[row.Index, "node_id_us"] = list(network.graph.predecessors(row.Index))


# %%
def has_path(node_from, node_to):
    try:
        nx.shortest_path(network.graph, node_from, node_to)
        return True
    except NetworkXNoPath:
        return False


def is_outlet(node_id, basin_fid):
    basin_node_ids = nodes_gdf[nodes_gdf.basin_fid == basin_fid].index
    return any((has_path(i, node_id) for i in basin_node_ids))  # noqa: UP034


def get_kwk(
    df,
    basin,
    kwk_types=["Stuwen", "Spuisluizen", "Stormvloedkeringen", "keersluizen", "Gemalen"],
):
    df_select = df[df["kw_soort"].isin(kwk_types)]
    if not df_select.empty:
        kwk = df.loc[df_select.distance(network_union_lines).sort_values().index[0]]
        node_id = network.move_node(
            point=kwk.geometry, max_distance=100, allign_distance=100
        )
        outlet = is_outlet(node_id, basin)
        return {
            "type": "LinearResistance",
            "is_structure": True,
            "code": kwk.code,
            "node_id": node_id,
            "outlet": outlet,
        }
    else:
        return None


# %%
"""ToDo:
1. Detecteren boundaries en structures op de edge van de basin
2. Sub-selecten van het netwerk tussen alle in en outlets
3. Basin to point
4. edges: inlets to basin
5. edges: basin to outlets
"""


network_union_lines = network.links.unary_union

basin = basin_poly_gdf.loc[19]
connections = []
# vinden van kunstwerken
structures_select_gdf = structures_gdf[
    structures_gdf.distance(basin.geometry.boundary) < 100
]


for complex_code, df in structures_select_gdf.groupby("complex_code"):
    kwk = get_kwk(df, basin.basin_id)
    if kwk is None:
        kwk = get_kwk(df, basin.basin_id, ["Schutsluizen"])
        if kwk is None:
            raise ValueError(
                f"can't snap a structure from complex {complex_code} to basin {basin.basin_id}"
            )
    connections += [kwk]

# %% vinden van flow boundaries

flow_boundaries_select_gdf = flow_boundaries_gdf[
    flow_boundaries_gdf.distance(basin.geometry.boundary) < 1000
]
for boundary in flow_boundaries_select_gdf.itertuples():
    node_id = network.move_node(
        point=boundary.geometry,
        max_distance=100,
        allign_distance=100,
        node_types=["upstream_boundary", "downstream_boundary"],
    )
    outlet = is_outlet(node_id, basin.basin_id)
    connections += [
        {
            "type": "FlowBoundary",
            "is_structure": False,
            "fid": boundary.Index,
            "code": boundary.code,
            "node_id": node_id,
            "outlet": outlet,
        }
    ]

# %% get basin
nodes_from = [i["node_id"] for i in connections if not i["outlet"]]
nodes_to = [i["node_id"] for i in connections if i["outlet"]]

points = network.subset_nodes(nodes_from, nodes_to, inclusive=False)
basin_id = points.distance(basin.geometry.centroid).sort_values().index[0]
node_list += [
    {
        "node_id": basin_id,
        "name": basin.owmnaam,
        "type": "Basin",
        "waterbeheerder": "Rijkswaterstaat",
        "code_waterbeheerder": basin.owmident,
        "basin_id": basin.basin_id,
        "geometry": network.nodes.loc[basin_id].geometry,
    }
]
basin_node_id = points.distance(basin.geometry.centroid).sort_values().index[0]
basin_geometry = network.nodes.loc[basin_node_id].geometry

for node_from in nodes_from:
    edge_list += [
        {
            "from_node_id": node_from,
            "to_node_id": basin_id,
            "name": basin.owmnaam,
            "krw_id": basin.owmident,
            "type": "flow",
            "geometry": network.get_line(node_from, basin_id),
        }
    ]

for node_to in nodes_to:
    edge_list += [
        {
            "from_node_id": basin_id,
            "to_node_id": node_to,
            "name": basin.owmnaam,
            "krw_id": basin.owmident,
            "type": "flow",
            "geometry": network.get_line(basin_id, node_to),
        }
    ]

for connection in connections:
    node_id = connection["node_id"]
    geometry = network.nodes.at[node_id, "geometry"]
    if connection["is_structure"]:
        attributes = structures_gdf.set_index("code").loc[connection["code"]]
        attributes["code"] = attributes._name
    else:
        attributes = flow_boundaries_gdf.loc[connection["fid"]]

    node_list += [
        {
            "node_id": node_id,
            "name": attributes["naam"],
            "type": connection["type"],
            "waterbeheerder": "Rijkswaterstaat",
            "code_waterbeheerder": attributes["code"],
            "geometry": geometry,
        }
    ]

# %% define Network
node = ribasim.Node(df=gpd.GeoDataFrame(node_list, crs=28992))
node.df.set_index("node_id", drop=False, inplace=True)
node.df.index.name = "fid"
edge = ribasim.Edge(df=gpd.GeoDataFrame(edge_list, crs=28992))
network = ribasim.Network(node=node, edge=edge)

# %% define Basin
static_df = node.df[node.df["type"] == "Basin"][["node_id", "basin_id"]].set_index(
    "basin_id"
)
profile_df = level_area_df[level_area_df["id"].isin(static_df.index)]
profile_df["node_id"] = profile_df["id"].apply(lambda x: static_df.at[x, "node_id"])
profile_df = profile_df[["node_id", "area", "level"]]

static_df["precipitation"] = PRECIPITATION
static_df["potential_evaporation"] = EVAPORATION
static_df["drainage"] = 0
static_df["infiltration"] = 0
static_df["urban_runoff"] = 0

basin = ribasim.Basin(profile=profile_df, static=static_df)

# %% define Resistance
resistance_df = node.df[node.df["type"] == "LinearResistance"][["node_id"]]
resistance_df["resistance"] = 5e-4
linear_resistance = ribasim.LinearResistance(static=resistance_df)
# %% define FlowBoundary
flow_boundary_df = node.df[node.df["type"] == "FlowBoundary"]
flow_boundary_df["flow_rate"] = flow_boundary_df["code_waterbeheerder"].apply(
    lambda x: flow_boundaries_gdf.set_index("code").at[x, "flow"]
)
flow_boundary = ribasim.FlowBoundary(static=flow_boundary_df[["node_id", "flow_rate"]])
# %% write model
model = ribasim.Model(
    network=network,
    basin=basin,
    flow_boundary=flow_boundary,
    linear_resistance=linear_resistance,
    starttime="2020-01-01 00:00:00",
    endtime="2021-01-01 00:00:00",
)
print("write ribasim model")
ribasim_toml = cloud.joinpath(
    "Rijkswaterstaat", "modellen", "bovenmaas", "bovenmaas.toml"
)
model = reset_index(model)
model.write(ribasim_toml)
# %%
