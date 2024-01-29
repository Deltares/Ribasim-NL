# %% init
import geopandas as gpd
import pandas as pd
import ribasim
from ribasim_nl import CloudStorage, Network, reset_index

cloud = CloudStorage()
node_list = []
edge_list = []

boundaries_passed = []
PRECIPITATION = 0.005 / 86400  # m/s
EVAPORATION = 0.001 / 86400  # m/s

network = Network.from_network_gpkg(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk.gpkg")
)
boundary_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "model_user_data.gpkg"),
    engine="pyogrio",
    layer="boundary",
    fid_as_index=True,
)

structures_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "hydamo.gpkg"),
    layer="kunstwerken",
    engine="pyogrio",
    fid_as_index=True,
)

basin_poly_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_vlakken.gpkg"),
    engine="pyogrio",
    fid_as_index=True,
)

structures_df = pd.read_excel(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "kunstwerk_complexen.xlsx")
)

level_area_df = pd.read_csv(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "krw_basins_vlakken_level_area.csv")
)

# % define nodes and links
nodes_gdf = network.nodes
links_gdf = network.links
network_union_lines = links_gdf.unary_union
network.overlay(basin_poly_gdf[["basin_id", "geometry"]])

basin_admin = {
    i: {"nodes_from": [], "nodes_to": [], "neighbors": []} for i in basin_poly_gdf.index
}

boundary_gdf["node_id"] = boundary_gdf["geometry"].apply(
    lambda x: network.move_node(
        x,
        max_distance=10,
        allign_distance=10,
        node_types=["downstream_boundary", "connection", "upstream_boundary"],
    )
)


def get_structure_codes(structures_gdf, complex_codes, line_string):
    """Return list of nearest structure_codes from a list of complex_codes and a linestring."""
    structure_codes = []
    for complex_code, gdf in structures_gdf[
        structures_gdf.complex_code.isin(complex_codes)
    ].groupby("complex_code"):
        gdf_select = gdf[
            gdf.kw_soort.isin(
                [
                    "Stuwen",
                    "Spuisluizen",
                    "Stormvloedkeringen",
                    "keersluizen",
                    "Gemalen",
                ]
            )
        ]
        if gdf_select.empty:
            gdf_select = gdf[gdf.kw_soort == "Schutsluizen"]
            if gdf_select.empty:
                raise Exception(f"kan geen kunstwerk vinden voor {complex_code}")
        structure_codes += [
            gdf_select.at[
                gdf_select.distance(line_string).sort_values().index[0], "code"
            ]
        ]
    return structure_codes


def get_type_and_value(code, structures_gdf, structures_df):
    if code not in structures_df.code.to_numpy():
        complex_code = structures_gdf.set_index("code").loc[code].complex_code
        row = structures_df.set_index("code").loc[complex_code]
    else:
        row = structures_df.set_index("code").loc[code]
    return row.ribasim_type, row.ribasim_waarde


# %% make lijst met kunstwerk-codes
structure_codes = structures_df[structures_df.code.isin(structures_gdf.code)][
    "code"
].to_list()

complex_codes = structures_df[~structures_df.code.isin(structures_gdf.code)][
    "code"
].to_list()
structure_codes += get_structure_codes(
    structures_gdf,
    complex_codes=complex_codes,
    line_string=network_union_lines,
)


# % itereer over kunstwerken
kwk_select_gdf = structures_gdf[structures_gdf.code.isin(structure_codes)]

for kwk in kwk_select_gdf.itertuples():
    # get network_node_id
    node_id = network.move_node(
        kwk.geometry,
        max_distance=150,
        allign_distance=100,
        node_types=["connection", "upstream_boundary"],
    )
    if node_id is None:
        node_id = network.add_node(kwk.geometry, max_distance=150, allign_distance=100)

    # add to node_list
    node_type, node_value = get_type_and_value(kwk.code, structures_gdf, structures_df)

    if network.has_upstream_nodes(node_id) and network.has_downstream_nodes(
        node_id
    ):  # normal case, structure is between basins
        upstream, downstream = network.get_upstream_downstream(node_id, "basin_id")
        if upstream is not None:
            basin_admin[upstream]["nodes_to"] += [node_id]
            if downstream is not None:
                basin_admin[upstream]["neighbors"] += [downstream]
        else:
            raise ValueError(f"{kwk.naam} heeft problemen")
        if downstream is not None:
            basin_admin[downstream]["nodes_from"] += [node_id]
            if upstream is not None:
                basin_admin[downstream]["neighbors"] += [upstream]
        else:
            print(f"{kwk.naam} is een outlet")
            boundary = boundary_gdf.loc[
                boundary_gdf[boundary_gdf["type"] == "LevelBoundary"]
                .distance(kwk.geometry)
                .sort_values()
                .index[0]
            ]
            boundaries_passed += [boundary.name]
            node_list += [
                {
                    "type": "LevelBoundary",
                    "value": boundary.level,
                    "is_structure": False,
                    "code_waterbeheerder": boundary.code,
                    "waterbeheerder": "Rijkswaterstaat",
                    "name": boundary.naam,
                    "node_id": boundary.node_id,
                    "geometry": network.nodes.at[boundary.node_id, "geometry"],
                }
            ]
            print(f"Processing node_id: {node_id}")
            edge_list += [
                {
                    "from_node_id": node_id,
                    "to_node_id": boundary.node_id,
                    "edge_type": "flow",
                    "geometry": network.get_line(node_id, boundary.node_id),
                }
            ]

    elif network.has_downstream_nodes(node_id):
        first, second = network.get_downstream(node_id, "basin_id")
        if first is not None:
            basin_admin[first]["nodes_from"] += [node_id]
            if second is not None:
                basin_admin[first]["neighbors"] += [second]
        else:
            raise ValueError(f"{kwk.naam} heeft problemen")
        if second is not None:
            basin_admin[second]["nodes_from"] += [node_id]
            if first is not None:
                basin_admin[second]["neighbors"] += [first]
        else:
            raise ValueError(f"{kwk.naam} heeft problemen")

    node_list += [
        {
            "type": node_type,
            "value": node_value,
            "is_structure": True,
            "code_waterbeheerder": kwk.code,
            "waterbeheerder": "Rijkswaterstaat",
            "name": kwk.naam,
            "node_id": node_id,
            "geometry": network.nodes.at[node_id, "geometry"],
        }
    ]


# %%
for basin in basin_poly_gdf.itertuples():
    print(basin.owmnaam)
    boundary_nodes = network.nodes[
        network.nodes.distance(basin.geometry.boundary) < 0.01
    ]
    us_ds = [
        network.get_upstream_downstream(i, "basin_id") for i in boundary_nodes.index
    ]
    boundary_nodes["upstream"] = [i[0] for i in us_ds]
    boundary_nodes["downstream"] = [i[1] for i in us_ds]
    boundary_nodes = boundary_nodes.dropna(subset=["upstream", "downstream"], how="any")
    # boundary_nodes.drop_duplicates(["upstream", "downstream"], inplace=True)
    # drop duplicated nodes by max upstream/downstream nodes
    # mask = boundary_nodes["upstream"] != basin.Index
    # boundary_nodes.loc[mask, ["count"]] = [
    #     len(network.upstream_nodes(i)) for i in boundary_nodes[mask].index
    # ]

    # mask = boundary_nodes["downstream"] != basin.Index
    # boundary_nodes.loc[mask, ["count"]] = [
    #     len(network.downstream_nodes(i)) for i in boundary_nodes[mask].index
    # ]
    boundary_nodes.loc[:, ["count"]] = [
        len(network.upstream_nodes(i)) + len(network.downstream_nodes(i))
        for i in boundary_nodes.index
    ]

    boundary_nodes.sort_values(by="count", ascending=False, inplace=True)
    boundary_nodes.loc[:, ["sum"]] = (
        boundary_nodes["upstream"] + boundary_nodes["downstream"]
    )
    boundary_nodes.drop_duplicates("sum", inplace=True)
    kwk_connected = basin_admin[basin.Index]["neighbors"]
    boundary_nodes = boundary_nodes[~boundary_nodes["upstream"].isin(kwk_connected)]
    boundary_nodes = boundary_nodes[~boundary_nodes["downstream"].isin(kwk_connected)]

    for node in boundary_nodes.itertuples():
        node_list += [
            {
                "type": "ManningResistance",
                "is_structure": False,
                "node_id": node.Index,
                "geometry": network.nodes.at[node.Index, "geometry"],
            }
        ]
        if node.upstream != basin.Index:
            basin_admin[basin.Index]["nodes_from"] += [node.Index]
            basin_admin[basin.Index]["neighbors"] += [node.upstream]
        elif node.downstream != basin.Index:
            basin_admin[basin.Index]["nodes_to"] += [node.Index]
            basin_admin[basin.Index]["neighbors"] += [node.downstream]
        else:
            raise Exception(f"iets gaat fout bij {basin.Index}")

    nodes_from = basin_admin[basin.Index]["nodes_from"]
    nodes_to = basin_admin[basin.Index]["nodes_to"]

    if (len(nodes_from) > 0) and (len(nodes_to) > 0):
        gdf = network.subset_nodes(
            nodes_from,
            nodes_to,
            duplicated_nodes=True,
            directed=True,
            inclusive=False,
        )
        if gdf.empty:
            gdf = network.subset_nodes(
                nodes_from,
                nodes_to,
                duplicated_nodes=True,
                directed=False,
                inclusive=False,
            )
    elif (len(nodes_from) > 0) or (len(nodes_to) > 0):
        gdf = network.nodes[network.nodes.basin_id == basin.Index]

    gdf = gdf[gdf["basin_id"] == basin.Index]

    basin_node_id = gdf.distance(basin.geometry.centroid).sort_values().index[0]
    basin_admin[basin.Index]["node_id"] = basin_node_id
    node_list += [
        {
            "type": "Basin",
            "is_structure": False,
            "code_waterbeheerder": basin.owmident,
            "waterbeheerder": "Rijkswaterstaat",
            "name": basin.owmnaam,
            "node_id": basin_node_id,
            "basin_id": basin.Index,
            "geometry": network.nodes.at[basin_node_id, "geometry"],
        }
    ]

    for node_from in nodes_from:
        edge_list += [
            {
                "from_node_id": node_from,
                "to_node_id": basin_node_id,
                "name": basin.owmnaam,
                "krw_id": basin.owmident,
                "edge_type": "flow",
                "geometry": network.get_line(node_from, basin_node_id, directed=False),
            }
        ]

    for node_to in nodes_to:
        edge_list += [
            {
                "from_node_id": basin_node_id,
                "to_node_id": node_to,
                "name": basin.owmnaam,
                "krw_id": basin.owmident,
                "edge_type": "flow",
                "geometry": network.get_line(basin_node_id, node_to, directed=False),
            }
        ]
# %% finish boundaries
for boundary in boundary_gdf[~boundary_gdf.index.isin(boundaries_passed)].itertuples():
    if boundary.type == "FlowBoundary":
        basin_id = network.find_downstream(boundary.node_id, "basin_id")
        value = boundary.flow_rate
        edge_list += [
            {
                "from_node_id": boundary.node_id,
                "to_node_id": basin_admin[basin_id]["node_id"],
                "name": basin_poly_gdf.loc[basin_id].owmnaam,
                "krw_id": basin_poly_gdf.loc[basin_id].owmident,
                "edge_type": "flow",
                "geometry": network.get_line(
                    boundary.node_id, basin_admin[basin_id]["node_id"], directed=True
                ),
            }
        ]
        basin_admin[basin_id]["nodes_from"] += [boundary.node_id]
    else:
        basin_id = network.find_upstream(boundary.node_id, "basin_id")
        basin_node_id = basin_admin[basin_id]["node_id"]

        gdf = network.nodes[network.nodes["basin_id"] == basin_id]
        manning_node_id = (
            gdf.distance(
                network.get_line(basin_node_id, boundary.node_id).interpolate(
                    0.5, normalized=True
                )
            )
            .sort_values()
            .index[0]
        )  # get manning node, closest to half-way on the line between basin and boundary

        value = boundary.level
        node_list += [
            {
                "type": "ManningResistance",
                "is_structure": False,
                "node_id": manning_node_id,
                "geometry": network.nodes.at[manning_node_id, "geometry"],
            }
        ]
        edge_list += [
            {
                "from_node_id": basin_node_id,
                "to_node_id": manning_node_id,
                "name": basin_poly_gdf.loc[basin_id].owmnaam,
                "krw_id": basin_poly_gdf.loc[basin_id].owmident,
                "edge_type": "flow",
                "geometry": network.get_line(
                    basin_node_id, manning_node_id, directed=True
                ),
            },
            {
                "from_node_id": manning_node_id,
                "to_node_id": boundary.node_id,
                "name": basin_poly_gdf.loc[basin_id].owmnaam,
                "krw_id": basin_poly_gdf.loc[basin_id].owmident,
                "edge_type": "flow",
                "geometry": network.get_line(
                    manning_node_id, boundary.node_id, directed=True
                ),
            },
        ]
        basin_admin[basin_id]["nodes_to"] += [boundary.node_id]

    boundaries_passed += [boundary.Index]
    node_list += [
        {
            "type": boundary.type,
            "value": value,
            "is_structure": False,
            "code_waterbeheerder": boundary.code,
            "waterbeheerder": "Rijkswaterstaat",
            "name": boundary.naam,
            "node_id": boundary.node_id,
            "geometry": network.nodes.at[boundary.node_id, "geometry"],
        }
    ]

# %%
gpd.GeoDataFrame(node_list, crs=28992).to_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "ribasim_intermediate.gpkg"),
    layer="Node",
)
gpd.GeoDataFrame(edge_list, crs=28992).to_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "ribasim_intermediate.gpkg"),
    layer="Edge",
)  # %%

# %% define Network
node = ribasim.Node(df=gpd.GeoDataFrame(node_list, crs=28992).drop_duplicates())
node.df.set_index("node_id", drop=False, inplace=True)
node.df.index.name = "fid"
edge = ribasim.Edge(df=gpd.GeoDataFrame(edge_list, crs=28992))
network = ribasim.Network(node=node, edge=edge)

# %% define Basin
static_df = node.df[node.df["type"] == "Basin"][["node_id", "basin_id"]].set_index(
    "basin_id"
)
level_area_df.drop_duplicates(["level", "area", "id"], inplace=True)
profile_df = level_area_df[level_area_df["id"].isin(static_df.index)]
profile_df["node_id"] = profile_df["id"].apply(lambda x: static_df.at[x, "node_id"])
profile_df = profile_df[["node_id", "area", "level"]]

static_df["precipitation"] = PRECIPITATION
static_df["potential_evaporation"] = EVAPORATION
static_df["drainage"] = 0
static_df["infiltration"] = 0
static_df["urban_runoff"] = 0

state_df = profile_df.groupby("node_id").min()["level"].reset_index()
state_df.loc[:, ["level"]] = state_df["level"].apply(lambda x: max(x + 1, 0))

basin = ribasim.Basin(profile=profile_df, static=static_df, state=state_df)

# %% define Resistance
node.df.loc[node.df["type"] == "Outlet", ["type", "value"]] = (
    "LinearResistance",
    1000,
)  # FIXME: Nijkerkersluis als goede type meenemen

resistance_df = node.df[node.df["type"] == "LinearResistance"][["node_id", "value"]]
resistance_df.rename(columns={"value": "resistance"}, inplace=True)
linear_resistance = ribasim.LinearResistance(static=resistance_df)

# %% define Pump
pump_df = node.df[node.df["type"] == "Pump"][["node_id", "value"]]
pump_df.rename(columns={"value": "flow_rate"}, inplace=True)
pump = ribasim.Pump(static=pump_df)

# %% define Outlet
# terminal_df = node.df[node.df["type"] == "Outlet"][["node_id"]]
# # outlet_df.rename(columns={"value": "flow_rate"}, inplace=True)
# terminal = ribasim.Terminal(static=terminal_df)
# node.df.loc[node.df["type"] == "Outlet", ["type", "resistance"]] = (
#     "LinearResistance",
#     1000,
# )

# %% define Manning
manning_df = node.df[node.df["type"] == "ManningResistance"][["node_id"]]
manning_df["length"] = 10000
manning_df["manning_n"] = 0.04
manning_df["profile_width"] = 10000
manning_df["profile_slope"] = 1

manning_resistance = ribasim.ManningResistance(static=manning_df)

# %% define FlowBoundary
flow_boundary_df = node.df[node.df["type"] == "FlowBoundary"][["node_id", "value"]]
flow_boundary_df.rename(columns={"value": "flow_rate"}, inplace=True)
flow_boundary = ribasim.FlowBoundary(static=flow_boundary_df)

# %% define LevelBoundary
level_boundary_df = node.df[node.df["type"] == "LevelBoundary"][["node_id", "value"]]
level_boundary_df.rename(columns={"value": "level"}, inplace=True)
level_boundary = ribasim.LevelBoundary(static=level_boundary_df)

# %% write model
model = ribasim.Model(
    network=network,
    basin=basin,
    flow_boundary=flow_boundary,
    level_boundary=level_boundary,
    linear_resistance=linear_resistance,
    manning_resistance=manning_resistance,
    pump=pump,
    # terminal=terminal,
    starttime="2020-01-01 00:00:00",
    endtime="2021-01-01 00:00:00",
)
print("write ribasim model")
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws", "hws.toml")

# %% verwijderen Nijkerkersluis

model.network.node.df = model.network.node.df[~(model.network.node.df["node_id"] == 14)]
model.network.edge.df = model.network.edge.df[
    ~(model.network.edge.df["from_node_id"] == 14)
]
model.network.edge.df = model.network.edge.df[
    ~(model.network.edge.df["to_node_id"] == 14)
]
model.linear_resistance.static.df = model.linear_resistance.static.df[
    ~(model.linear_resistance.static.df["node_id"] == 14)
]


# %%
model = reset_index(model)
model.write(ribasim_toml)

# %%
