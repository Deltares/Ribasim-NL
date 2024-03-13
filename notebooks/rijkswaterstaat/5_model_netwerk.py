# %% init
import geopandas as gpd
import pandas as pd
import ribasim
from ribasim_nl import CloudStorage, Network, reset_index
from ribasim_nl.rating_curve import read_rating_curve
from ribasim_nl.verdeelsleutels import (
    read_verdeelsleutel,
    verdeelsleutel_to_fractions,
)

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
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "basins.gpkg"),
    layer="ribasim_basins",
    engine="pyogrio",
    fid_as_index=True,
)

structures_df = pd.read_excel(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "kunstwerk_complexen.xlsx")
)
structures_df = structures_df[structures_df.in_model]
structures_df["code"] = structures_df["code"].astype(str)

level_area_df = pd.read_csv(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "basins_level_area.csv")
)

rating_curves_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "rating_curves.gpkg"),
    engine="pyogrio",
    fid_as_index=True,
)

verdeelsleutel_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "verdeelsleutel.gpkg"),
    engine="pyogrio",
    fid_as_index=True,
)

verdeelsleutel_df = read_verdeelsleutel(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "verdeelsleutel_driel.xlsx")
)


# %% inlezen netwerk nodes en links
# define nodes and links
nodes_gdf = network.nodes
links_gdf = network.links
network_union_lines = links_gdf.unary_union
network.overlay(basin_poly_gdf[["basin_id", "geometry"]])

basin_admin: dict[str, list] = {
    i: {"nodes_from": [], "nodes_to": [], "neighbors": []} for i in basin_poly_gdf.index
}

# %% toevoegen reating-curves aan basin-admin
# toevoegen reating-curves aan basin-admin
for row in rating_curves_gdf.itertuples():
    basin_id = basin_poly_gdf[basin_poly_gdf.contains(row.geometry)].index[0]
    basin_admin[basin_id]["rating_curve"] = row.curve_id

# %% definieren boundaries
# definieren boundaries
boundary_gdf["node_id"] = boundary_gdf["geometry"].apply(
    lambda x: network.move_node(
        x,
        max_distance=10,
        align_distance=10,
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


# %% kunstwerken toekennen aan basins
# kunstwerken toekennen aan basins
# maak lijst met kunstwerk-codes
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
        align_distance=100,
        node_types=["connection", "upstream_boundary"],
    )
    if node_id is None:
        node_id = network.add_node(kwk.geometry, max_distance=150)

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
            raise ValueError(
                f"Kan geen boven en benedenstroomse knoop vinden voor {kwk.naam}"
            )
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


# %% netwerk opbouwen per basin
# netwerk opbouwen per basin
# for basin in basin_poly_gdf.itertuples():
for basin in basin_poly_gdf.itertuples():
    print(f"{basin.Index} {basin.naam}")
    # get upstream an downstream basin boundary nodes
    boundary_nodes = network.nodes[
        network.nodes.distance(basin.geometry.boundary) < 0.01
    ]
    us_ds = [
        network.get_upstream_downstream(i, "basin_id", max_iters=3, max_length=1000)
        for i in boundary_nodes.index
    ]
    boundary_nodes["upstream"] = [i[0] for i in us_ds]
    boundary_nodes["downstream"] = [i[1] for i in us_ds]
    boundary_nodes = boundary_nodes.dropna(subset=["upstream", "downstream"], how="any")
    boundary_nodes.loc[:, ["count"]] = [
        len(network.upstream_nodes(i)) + len(network.downstream_nodes(i))
        for i in boundary_nodes.index
    ]

    boundary_nodes.sort_values(by="count", ascending=False, inplace=True)
    boundary_nodes.loc[:, ["sum"]] = (
        boundary_nodes["upstream"] + boundary_nodes["downstream"]
    )
    boundary_nodes.drop_duplicates("sum", inplace=True)
    connected = basin_admin[basin.Index]["neighbors"]
    boundary_nodes = boundary_nodes[~boundary_nodes["upstream"].isin(connected)]
    boundary_nodes = boundary_nodes[~boundary_nodes["downstream"].isin(connected)]

    # add boundary nodes
    for node in boundary_nodes.itertuples():
        neighbor = next(
            i for i in [node.upstream, node.downstream] if not i == basin.Index
        )
        if basin.Index not in basin_admin[neighbor]["neighbors"]:
            ds_node = node.upstream == basin.Index
            node_type = "ManningResistance"

            # in case basin has rating curve we use FractionalFlow
            if ds_node and ("rating_curve" in basin_admin[basin.Index].keys()):
                node_type = "FractionalFlow"
            # in us_case basin has rating curve we use FractionalFlow
            elif (not ds_node) and ("rating_curve" in basin_admin[neighbor].keys()):
                node_type = "FractionalFlow"

            if node_type == "FractionalFlow":
                code = verdeelsleutel_gdf.at[
                    verdeelsleutel_gdf.distance(
                        network.nodes.at[node.Index, "geometry"]
                    )
                    .sort_values()
                    .index[0],
                    "fractie",
                ]
            else:
                code = None

            node_list += [
                {
                    "type": node_type,
                    "is_structure": False,
                    "node_id": node.Index,
                    "code_waterbeheerder": code,
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

    # add basin-node
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
            "waterbeheerder": "Rijkswaterstaat",
            "name": basin.naam,
            "node_id": basin_node_id,
            "basin_id": basin.Index,
            "geometry": network.nodes.at[basin_node_id, "geometry"],
        }
    ]

    # add rating-curve and extra edge
    if "rating_curve" in basin_admin[basin.Index].keys():
        rc_node_id = next(
            i for i in network.downstream_nodes(basin_node_id) if i in gdf.index
        )
        node_list += [
            {
                "type": "TabulatedRatingCurve",
                "is_structure": False,
                "waterbeheerder": "Rijkswaterstaat",
                "code_waterbeheerder": basin_admin[basin.Index]["rating_curve"],
                "name": basin_admin[basin.Index]["rating_curve"],
                "node_id": rc_node_id,
                "basin_id": basin.Index,
                "geometry": network.nodes.at[rc_node_id, "geometry"],
            }
        ]
        edge_list += [
            {
                "from_node_id": basin_node_id,
                "to_node_id": rc_node_id,
                "name": basin.naam,
                "edge_type": "flow",
                "geometry": network.get_line(basin_node_id, rc_node_id, directed=True),
            }
        ]
        from_id = rc_node_id
    else:
        from_id = basin_node_id

    # add edge to basin
    network.add_weight("basin_id", basin.basin_id)
    for node_from in nodes_from:
        edge_list += [
            {
                "from_node_id": node_from,
                "to_node_id": basin_node_id,
                "name": basin.naam,
                "edge_type": "flow",
                "geometry": network.get_line(
                    node_from, basin_node_id, directed=False, weight="weight"
                ),
            }
        ]

    # add from basin to edge
    for node_to in nodes_to:
        edge_list += [
            {
                "from_node_id": from_id,
                "to_node_id": node_to,
                "name": basin.naam,
                "edge_type": "flow",
                "geometry": network.get_line(
                    from_id, node_to, directed=False, weight="weight"
                ),
            }
        ]
# %% afronden boundaries
# afronden boundaries

for boundary in boundary_gdf[~boundary_gdf.index.isin(boundaries_passed)].itertuples():
    if boundary.type == "FlowBoundary":
        basin_id = network.find_downstream(boundary.node_id, "basin_id")
        value = boundary.flow_rate
        edge_list += [
            {
                "from_node_id": boundary.node_id,
                "to_node_id": basin_admin[basin_id]["node_id"],
                "name": basin_poly_gdf.loc[basin_id].naam,
                "edge_type": "flow",
                "geometry": network.get_line(
                    boundary.node_id, basin_admin[basin_id]["node_id"], directed=True
                ),
            }
        ]
        basin_admin[basin_id]["nodes_from"] += [boundary.node_id]
    else:
        basin_id = network.find_upstream(boundary.node_id, "basin_id", max_iters=50)
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
                "name": basin_poly_gdf.loc[basin_id].naam,
                "edge_type": "flow",
                "geometry": network.get_line(
                    basin_node_id, manning_node_id, directed=True
                ),
            },
            {
                "from_node_id": manning_node_id,
                "to_node_id": boundary.node_id,
                "name": basin_poly_gdf.loc[basin_id].naam,
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

# %% opslaan interim-netwerk
# opslaan interim-netwerk
gpd.GeoDataFrame(node_list, crs=28992).to_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "ribasim_intermediate.gpkg"),
    layer="Node",
)
gpd.GeoDataFrame(edge_list, crs=28992).to_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "ribasim_intermediate.gpkg"),
    layer="Edge",
)  # %%

# %% conversie naar ribasim
# conversie naar ribasim
node_df = gpd.GeoDataFrame(node_list, crs=28992).drop_duplicates()
node_df.set_index("node_id", drop=False, inplace=True)
node_df.index.name = "fid"
node = ribasim.Node(
    df=node_df[
        ["node_id", "name", "type", "waterbeheerder", "code_waterbeheerder", "geometry"]
    ].rename(
        columns={
            "type": "node_type",
            "code_waterbeheerder": "meta_code_waterbeheerder",
            "waterbeheerder": "meta_waterbeheerder",
        }
    )
)

edge_df = gpd.GeoDataFrame(edge_list, crs=28992)
edge_df.loc[:, "from_node_type"] = edge_df.from_node_id.apply(
    lambda x: node_df.at[x, "type"]
)

edge_df.loc[:, "to_node_type"] = edge_df.to_node_id.apply(
    lambda x: node_df.at[x, "type"]
)

edge = ribasim.Edge(df=edge_df)
network = ribasim.Network(node=node, edge=edge)

# define Basin
static_df = node_df[node_df["type"] == "Basin"][["node_id", "basin_id"]].set_index(
    "basin_id"
)

# Area
area_df = node_df[node_df["type"] == "Basin"][["basin_id", "node_id"]]
area_df.loc[:, "geometry"] = area_df.basin_id.apply(
    lambda x: basin_poly_gdf.at[x, "geometry"]
)
area_df = area_df[["node_id", "geometry"]]

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

basin = ribasim.Basin(
    profile=profile_df, static=static_df, state=state_df, area=area_df
)

# % define Resistance
# node_df.loc[node_df["type"] == "Outlet", ["type", "value"]] = (
#     "LinearResistance",
#     1000,
# )  # FIXME: Nijkerkersluis als goede type meenemen

resistance_df = node_df[node_df["type"] == "LinearResistance"][["node_id", "value"]]
resistance_df.rename(columns={"value": "resistance"}, inplace=True)
linear_resistance = ribasim.LinearResistance(static=resistance_df)

# % define Pump
pump_df = node_df[node_df["type"] == "Pump"][["node_id", "value"]]
pump_df.rename(columns={"value": "flow_rate"}, inplace=True)
pump = ribasim.Pump(static=pump_df)


# % define Outlet
outlet_df = node_df[node_df["type"] == "Outlet"][["node_id", "value"]]
outlet_df.rename(columns={"value": "flow_rate"}, inplace=True)
outlet = ribasim.Outlet(static=outlet_df)

# % define fraction
node_index = node_df[node_df["type"] == "FractionalFlow"][
    ["code_waterbeheerder", "node_id"]
].set_index("code_waterbeheerder")["node_id"]

fractional_flow_df = verdeelsleutel_to_fractions(verdeelsleutel_df, node_index)
fractional_flow = ribasim.FractionalFlow(static=fractional_flow_df)

# %
node_index = node_df[node_df["type"] == "TabulatedRatingCurve"][
    ["code_waterbeheerder", "node_id"]
].set_index("code_waterbeheerder")["node_id"]
tabulated_rating_curve_df = read_rating_curve(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "rating_curves.xlsx"),
    node_index,
)
tabulated_rating_curve = ribasim.TabulatedRatingCurve(
    static=tabulated_rating_curve_df.rename(
        columns={"code_waterbeheerder": "meta_code_waterbeheerder"}
    )
)

# % define Manning
manning_df = node_df[node_df["type"] == "ManningResistance"][["node_id"]]
manning_df["length"] = 10000
manning_df["manning_n"] = 0.04
manning_df["profile_width"] = 10000
manning_df["profile_slope"] = 1

manning_resistance = ribasim.ManningResistance(static=manning_df)

# % define FlowBoundary
flow_boundary_df = node_df[node_df["type"] == "FlowBoundary"][["node_id", "value"]]
flow_boundary_df.rename(columns={"value": "flow_rate"}, inplace=True)
flow_boundary = ribasim.FlowBoundary(static=flow_boundary_df)

# % define LevelBoundary
level_boundary_df = node_df[node_df["type"] == "LevelBoundary"][["node_id", "value"]]
level_boundary_df.rename(columns={"value": "level"}, inplace=True)
level_boundary = ribasim.LevelBoundary(static=level_boundary_df)


# % write model
model = ribasim.Model(
    network=network,
    basin=basin,
    flow_boundary=flow_boundary,
    level_boundary=level_boundary,
    linear_resistance=linear_resistance,
    manning_resistance=manning_resistance,
    tabulated_rating_curve=tabulated_rating_curve,
    fractional_flow=fractional_flow,
    pump=pump,
    outlet=outlet,
    # terminal=terminal,
    starttime="2020-01-01 00:00:00",
    endtime="2021-01-01 00:00:00",
)

#
model = reset_index(model)

#  verwijderen Nijkerkersluis

nijkerk_idx = model.network.node.df[
    model.network.node.df["meta_code_waterbeheerder"] == "32E-001-04"
].index

# model.network.node.df = model.network.node.df[
#     ~(model.network.node.df["meta_node_id"].isin(nijkerk_idx))
# ]

# model.network.edge.df = model.network.edge.df[
#     ~(model.network.edge.df["from_node_id"].isin(nijkerk_idx))
# ]

# model.network.edge.df = model.network.edge.df[
#     ~(model.network.edge.df["to_node_id"].isin(nijkerk_idx))
# ]

# model.linear_resistance.static.df = model.linear_resistance.static.df[
#     ~model.linear_resistance.static.df["node_id"].isin(nijkerk_idx)
# ]

# model = reset_index(model)

model.linear_resistance.static.df.loc[
    model.linear_resistance.static.df["node_id"].isin(nijkerk_idx), ["active"]
] = False

model.linear_resistance.static.df.loc[
    ~model.linear_resistance.static.df["node_id"].isin(nijkerk_idx), ["active"]
] = True


#
model.solver.algorithm = "RK4"
model.solver.dt = 10.0
model.solver.saveat = 360

# %% wegschrijven model
# wegschrijven model
print("write ribasim model")
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_network", "hws.toml")
model.write(ribasim_toml)

# %%
