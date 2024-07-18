# %%
import warnings

import geopandas as gpd
import openpyxl
import pandas as pd
from ribasim import Node
from ribasim.nodes import (
    basin,
    discrete_control,
    flow_boundary,
    fractional_flow,
    level_boundary,
    manning_resistance,
    outlet,
    pid_control,
    pump,
    tabulated_rating_curve,
)
from ribasim_nl import CloudStorage, Model, Network
from ribasim_nl import discrete_control as dc
from ribasim_nl.case_conversions import pascal_to_snake_case
from shapely.geometry import LineString, MultiLineString

warnings.filterwarnings(
    action="ignore",
    module="openpyxl",
)

KWK_INVERSE_FLOW_DIRECTION = ["750028686", "44D-002-03"]
VERDEELSLEUTELS = ["Lobith", "Monsin"]
RVW_IJSSELMEER = ["KOBU", "OEBU"]
AS_PUMP = ["Gemaal Ternaaien4"]

# %% functies
boundary_node_ids = []


def get_profile(line, point, profile_length):
    distance = line.project(point, normalized=True)
    offsets = [
        line.parallel_offset(profile_length / 2, side=i) for i in ["left", "right"]
    ]
    points = [i.interpolate(distance, normalized=True) for i in offsets]
    return LineString(points)


def clip_profile(line, point, poly):
    line = line.intersection(poly)
    if isinstance(line, MultiLineString):
        distances = [i.distance(point) for i in line.geoms]
        line = line.geoms[distances.index(min(distances))]
    return line


def read_rating_curve(kwk_df):
    qh_df = kwk_df.loc[kwk_df.Eigenschap.to_list().index("Q(h) relatie") + 2 :][
        ["Eigenschap", "Waarde"]
    ].rename(columns={"Eigenschap": "level", "Waarde": "flow_rate"})
    qh_df.dropna(inplace=True)
    return tabulated_rating_curve.Static(**qh_df.to_dict(orient="list"))


def read_qq_curve(kwk_df):
    return kwk_df.loc[kwk_df.Eigenschap.to_list().index("QQ relatie") + 2 :][
        ["Eigenschap", "Waarde"]
    ].rename(columns={"Eigenschap": "condition_flow_rate", "Waarde": "flow_rate"})


def read_qhq(verdeling_df):
    df = verdeling_df.loc[verdeling_df.Eigenschap.to_list().index("QHQ relatie") + 2 :]
    df.columns = ["control_flow_rate", "min_crest_level", "flow_rate_1", "flow_rate_2"]


def read_kwk_properties(kwk_df):
    properties = (
        kwk_df[0:12][["Eigenschap", "Waarde"]]
        .dropna()
        .set_index("Eigenschap")["Waarde"]
    )

    if "Kunstwerkcode" in properties.keys():
        properties["Kunstwerkcode"] = str(properties["Kunstwerkcode"])
    return properties


def read_flow_kwargs(kwk_properties, include_crest_level=False):
    mapper = {
        "Capaciteit (m3/s)": "flow_rate",
        "minimale capaciteit (m3/s)": "min_flow_rate",
    }
    if include_crest_level:
        mapper["Streefpeil (m +NAP)"] = "min_crest_level"

    kwargs = kwk_properties.rename(mapper).to_dict()
    if "flow_rate" in kwargs.keys():
        kwargs["max_flow_rate"] = kwargs["flow_rate"]
    kwargs = {
        k: [v]
        for k, v in kwargs.items()
        if k in ["flow_rate", "min_flow_rate", "max_flow_rate", "min_crest_level"]
    }
    # kwargs["flow_rate"] = [kwk_properties["Capaciteit (m3/s)"]]
    return kwargs


def read_outlet(kwk_df, name=None):
    if "QQ relatie" in kwk_df["Eigenschap"].to_numpy():
        qq_properties = read_qq_curve(kwk_df)
        outlet_df = dc.node_table(
            values=qq_properties["flow_rate"].to_list(),
            variable="flow_rate",
            name=name,
            node_id=node_id,
        )
        return outlet.Static(
            flow_rate=outlet_df.flow_rate.to_list(),
            control_state=outlet_df.control_state.to_list(),
        )
    else:
        return outlet.Static(
            **read_flow_kwargs(read_kwk_properties(kwk_df), include_crest_level=True)
        )


def read_pump(kwk_properties):
    kwargs = read_flow_kwargs(kwk_properties)
    return pump.Static(**kwargs)


def read_pid(control_properties, control_basin_id):
    if control_properties["Controle benedenstrooms"]:
        p = 500000
        i = 1e-07
    else:
        p = -500000
        i = -1e-07

    return [
        pid_control.Static(
            listen_node_id=[control_basin_id],
            target=[control_properties["Streefpeil (m+NAP)"]],
            listen_node_type="Basin",
            proportional=[p],
            integral=[i],
            derivative=[0.0],
        )
    ]


cloud = CloudStorage()
# %% read files

model_user_data_gpkg = cloud.joinpath(
    "Rijkswaterstaat", "verwerkt", "model_user_data.gpkg"
)

network = Network.from_network_gpkg(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "netwerk.gpkg")
)
nodes_gdf = network.nodes

boundary_gdf = gpd.read_file(
    model_user_data_gpkg,
    engine="pyogrio",
    layer="boundary",
    fid_as_index=True,
)

verdeelpunten_gdf = gpd.read_file(
    model_user_data_gpkg,
    engine="pyogrio",
    layer="verdeelpunten",
    fid_as_index=True,
).set_index("verdeelpunt")

verdeelsleutel_gdf = gpd.read_file(
    model_user_data_gpkg,
    engine="pyogrio",
    layer="verdeelsleutels",
    fid_as_index=True,
).set_index("verdeelsleutel")

basin_poly_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "basins.gpkg"),
    layer="ribasim_basins",
    engine="pyogrio",
    fid_as_index=True,
)

basin_profile_df = pd.read_csv(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "basins_level_area.csv")
)[["level", "area", "id"]].set_index("id")

# %% kunstwerken inlezen

kwk_dir = cloud.joinpath("Rijkswaterstaat", "verwerkt", "kunstwerken")
kwk_xlsx = kwk_dir.joinpath("kunstwerken.xlsx")
verdeelsleutels_xlsx = kwk_dir.joinpath("verdeelsleutels.xlsx")
kwks_df = pd.read_excel(kwk_xlsx, sheet_name="kunstwerken")


kwks_df.loc[:, "code"] = kwks_df["code"].astype(str)
# kwks_df = kwks_df[kwks_df.in_model]

kwks_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "hydamo.gpkg"),
    layer="kunstwerken",
    engine="pyogrio",
    fid_as_index=True,
)

# %% extra dingen toevoegen

network.overlay(
    basin_poly_gdf[["basin_id", "geometry"]]
)  # basin_id toekennen aan netwerk

# het netwerk om afstanden te bepalen tot
links_gdf = network.links
network_union_lines = links_gdf.unary_union

basin_poly_gdf.set_index("basin_id", inplace=True)  # basin_id is Index

# maken van lijstjes waarmee we later het netwerk kunnen tekenen per basin
basin_poly_gdf["nodes_from"] = [[] for _ in range(len(basin_poly_gdf))]
basin_poly_gdf["nodes_to"] = [[] for _ in range(len(basin_poly_gdf))]
basin_poly_gdf["neighbor_basins"] = [[] for _ in range(len(basin_poly_gdf))]
basin_poly_gdf["node_id"] = pd.Series(dtype="int")
basin_poly_gdf["outlet_type"] = pd.Series(dtype="str")
basin_poly_gdf["outlet_data"] = None


# %% init model
model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")


# %% boundaries bouwen

# Toevoegen Boundaries
# We voegen de boundaries toe aan het netwerk

level_ijsselmeer_df = pd.read_excel(
    kwk_dir / "IJsselmeer-Markermeer.xlsx", sheet_name="IJsselmeer", skiprows=4
)
level_ijsselmeer_df.index = [i.day_of_year for i in level_ijsselmeer_df.datum]


def get_level(timestamp, level_ijsselmeer_df):
    return level_ijsselmeer_df.at[
        level_ijsselmeer_df.index[
            level_ijsselmeer_df.index <= timestamp.dayofyear
        ].max(),
        "level",
    ]


time = pd.date_range(model.starttime, model.endtime).to_list()
level = [get_level(i, level_ijsselmeer_df) for i in time]

for row in boundary_gdf.itertuples():
    # uitlezen dichtsbijzijnde netwerk-knoop
    node_id = nodes_gdf.distance(row.geometry).idxmin()
    point = nodes_gdf.at[node_id, "geometry"]

    # uitlezen naam als beschikbaar
    if pd.isna(row.naam):
        name = ""
    else:
        name = row.naam

    # toevoegen knoop per type: FlowBoundary, LevelBoundary of Terminal
    if row.type == "FlowBoundary":
        table = model.flow_boundary
        table.add(
            Node(node_id, point, name=name),
            [flow_boundary.Static(flow_rate=[row.flow_rate])],
        )
        # zoeken aangrenzende basin
        basin_id = network.find_downstream(node_id, "basin_id", max_iters=60)
        basin_poly_gdf.loc[basin_id, "nodes_from"].append(node_id)
    elif row.type == "LevelBoundary":
        table = model.level_boundary
        if row.meetlocatie_code in RVW_IJSSELMEER:
            data = [level_boundary.Time(level=level, time=time)]
        else:
            data = [level_boundary.Static(level=[row.level])]
        table.add(Node(node_id, point, name=name), data)
    elif row.type == "Terminal":
        table = model.terminal
        table.add(Node(node_id, point, name=name))

    # toevoegen meta_code
    table.node.df.loc[table.node.df.node_id == node_id, "meta_meetlocatie_code"] = (
        row.meetlocatie_code
    )

# %% kunsterken toevoegen

# Toevoegen kunstwerken uit de Excel
mask = kwks_df.code.isin(kwks_gdf.code) | kwks_df.code.isin(kwks_gdf.complex_code)
mask = mask & kwks_df.in_model

kwk_topology = {}

for gebied, flow_kwk_df in kwks_df[mask].groupby(by="gebied"):
    print(f"{gebied}")
    # get sheet-names
    file_name = kwk_dir / f"{gebied}.xlsx"
    workbook = openpyxl.open(file_name)
    sheet_names = workbook.sheetnames
    workbook.close()
    for row in flow_kwk_df.itertuples():
        print(f" {row.naam}")
        name = row.naam

        # haal de geometrie op
        if row.code in kwks_gdf.code.to_numpy():
            point = kwks_gdf.set_index("code").at[row.code, "geometry"]
        else:
            point = kwks_gdf.at[
                kwks_gdf[kwks_gdf.complex_code == row.code]
                .distance(network_union_lines)
                .idxmin(),
                "geometry",
            ]

        # verplaats het netwerk richting het punt en haal de node_id op
        node_id = network.move_node(
            point,
            max_distance=30,
            align_distance=100,
            node_types=["connection"],
        )

        if node_id is None:
            node_id = network.add_node(point, max_distance=150)
        if node_id is None:
            raise ValueError("kunstwerk ligt te ver bij netwerk vandaan")

        # definieer de node
        node = Node(node_id, point, name=row.naam, meta_code_waterbeheerder=row.code)

        # haal alle eigenschappen op
        kwk_df = pd.read_excel(file_name, sheet_name=row.naam)
        kwk_properties = read_kwk_properties(kwk_df)

        # check if code-value is the same in both Excels
        if kwk_properties["Kunstwerkcode"] != row.code:
            raise ValueError(
                f"code for {row.naam} do not match in `{file_name.name}` and `{kwk_xlsx.name}`: {kwk_properties["Kunstwerkcode"]} != {row.code}"
            )

        # prepare static-data for updating node
        node_type = kwk_properties["Ribasim type"]
        if node_type == "TabulatedRatingCurve":
            data = [read_rating_curve(kwk_df)]
        elif node_type == "Outlet":
            data = [read_outlet(kwk_df, name=row.naam)]
        elif node_type == "Pump":
            data = [read_pump(kwk_properties)]
        else:
            raise ValueError(f"node-type {node_type} not yet implemented")

        # toevoegen van de knoop aan het netwerk
        node_table = getattr(model, pascal_to_snake_case(node_type))
        node_table.add(node, data)

        # zoeken naar aangrenzende basins
        us_basin_id, ds_basin_id = network.get_upstream_downstream(
            node_id, "basin_id", max_iters=4
        )

        # soms modelleren we een gemaal bij een sluis en moeten we de tekenrichting omdraaien
        if row.code in KWK_INVERSE_FLOW_DIRECTION:
            us_basin_id, ds_basin_id = ds_basin_id, us_basin_id

        if us_basin_id is None:
            print(" FOUT: de basins zijn hier niet goed geknipt!")
            continue
        # bijhouden basin-connecties
        basin_poly_gdf.loc[us_basin_id, "nodes_to"].append(node_id)

        if ds_basin_id is None:
            print("  verbinden naar dichtsbijzijnde LevelBoundary of Terminal")
            dfs = (getattr(model, i).node.df for i in ["level_boundary", "terminal"])
            dfs = (i.set_index("node_id") for i in dfs if i is not None)
            df = pd.concat(list(dfs))
            boundary = df.loc[df.distance(point).idxmin()]
            boundary_node_id = boundary.name
            boundary_node_ids += [boundary_node_id]
            edge_geom = network.get_line(node_id, boundary_node_id)
            model.edge.add(
                node_table[node_id],
                getattr(model, pascal_to_snake_case(boundary.node_type))[
                    boundary_node_id
                ],
                geometry=edge_geom,
            )
        else:
            # bijhouden basin-connecties
            basin_poly_gdf.loc[ds_basin_id, "nodes_from"].append(node_id)

            # bijhouden buur-basins
            basin_poly_gdf.loc[ds_basin_id, "neighbor_basins"].append(us_basin_id)
            basin_poly_gdf.loc[us_basin_id, "neighbor_basins"].append(ds_basin_id)

        kwk_topology[row.code] = {
            "node_id": node_id,
            "upstream": us_basin_id,
            "downstream": ds_basin_id,
        }

# %% verdeelsleutels toevoegen

outlets_gdf = gpd.read_file(
    cloud.joinpath("Rijkswaterstaat", "verwerkt", "outlets.gpkg")
)
verdeelsleutel_node_id = 1000001
# itereren per verdelsleutel
for verdeelsleutel in VERDEELSLEUTELS:
    print(f"verdeelsleutel: {verdeelsleutel}")

    verdeelsleutel_df = pd.read_excel(
        verdeelsleutels_xlsx, sheet_name=f"Verdeelsleutel {verdeelsleutel}"
    )

    verdeelsleutel_properties = read_kwk_properties(verdeelsleutel_df)

    # add control node
    control_node = Node(
        verdeelsleutel_node_id,
        verdeelsleutel_gdf.at[verdeelsleutel, "geometry"],
        name=f"Verdeelsleutel {verdeelsleutel}",
    )

    control_flow_rate = verdeelsleutel_df.loc[
        verdeelsleutel_df.Eigenschap.to_list().index("Q") + 2 :
    ]["Eigenschap"].to_list()

    control_state = [
        f"{verdeelsleutel}_{idx+1:03d}" for idx in range(len(control_flow_rate))
    ]

    truth_state = [
        "".join(["T"] * i + ["F"] * len(control_flow_rate))[0 : len(control_flow_rate)]
        for i in range(len(control_flow_rate))
    ]

    listen_node_id = (
        model.node_table()
        .df.set_index("meta_meetlocatie_code")
        .at[verdeelsleutel_properties["Meetlocatiecode"], "node_id"]
    )
    data = [
        discrete_control.Variable(
            compound_variable_id=1,
            listen_node_id=[listen_node_id],
            listen_node_type=[model.get_node_type(listen_node_id)],
            variable=["flow_rate"],
        ),
        discrete_control.Condition(
            compound_variable_id=1,
            greater_than=control_flow_rate,
            meta_control_state=control_state,
        ),
        discrete_control.Logic(
            truth_state=truth_state,
            control_state=control_state,
        ),
    ]

    model.discrete_control.add(control_node, data)

    verdeelsleutel_node = model.discrete_control[verdeelsleutel_node_id]
    verdeelsleutel_node_id += 1

    # add all verdelingen as Outlets
    for verdeling in [
        verdeelsleutel_properties[i]
        for i in verdeelsleutel_properties.keys()
        if i.startswith("Verdeling")
    ]:
        print(f"verdeling: {verdeling}")
        verdeling_df = pd.read_excel(verdeelsleutels_xlsx, sheet_name=verdeling)
        verdeling_properties = read_kwk_properties(verdeling_df)
        waterlichamen = [
            i for i in verdeling_properties.keys() if i.startswith("waterlichaam")
        ]
        qhq_df = verdeling_df.loc[
            verdeling_df.Eigenschap.to_list().index("QHQ relatie") + 2 :
        ].iloc[:, : len(waterlichamen) + 2]
        qhq_df.columns = ["control_flow_rate", "min_crest_level"] + waterlichamen

        min_crest_level = qhq_df.min_crest_level.to_list()

        for waterlichaam in waterlichamen:
            index = waterlichaam[-1]
            if f"kunstwerk {index}" in verdeling_properties.keys():
                name = verdeling_properties[f"kunstwerk {index}"]
                kwk = kwks_df.set_index("naam").loc[name]
                code_waterbeheerder = kwk.code
                point = kwks_gdf.set_index("code").at[kwk.code, "geometry"]
            elif f"outlet_naam {index}" in verdeling_properties.keys():
                name = verdeling_properties[f"outlet_naam {index}"]
                point = outlets_gdf.set_index("kunstwerkcode").at[name, "geometry"]
                code_waterbeheerder = None

            # add outlet node
            node_id = network.move_node(
                point,
                max_distance=200,
                align_distance=100,
            )

            node = Node(
                node_id,
                point,
                name=name,
                meta_code_waterbeheerder=code_waterbeheerder,
            )

            if name in AS_PUMP:
                model.pump.add(
                    node,
                    [
                        pump.Static(
                            flow_rate=qhq_df[waterlichaam].to_list(),
                            control_state=control_state,
                        )
                    ],
                )
                node = model.pump[node_id]
            else:
                model.outlet.add(
                    node,
                    [
                        outlet.Static(
                            flow_rate=qhq_df[waterlichaam].to_list(),
                            min_crest_level=min_crest_level,
                            control_state=control_state,
                        )
                    ],
                )
                node = model.outlet[node_id]

            # toevoegen edge tussen control-node en fractie
            model.edge.add(
                verdeelsleutel_node,
                node,
                name=verdeling_properties[waterlichaam],
            )

            # zoeken naar aangrenzende basins
            us_basin_id, ds_basin_id = network.get_upstream_downstream(
                node_id, "basin_id", max_iters=4
            )

            # soms modelleren we een gemaal bij een sluis en moeten we de tekenrichting omdraaien
            if code_waterbeheerder in KWK_INVERSE_FLOW_DIRECTION:
                us_basin_id, ds_basin_id = ds_basin_id, us_basin_id

            if us_basin_id is None:
                print(" FOUT: de basins zijn hier niet goed geknipt!")
                continue
            # bijhouden basin-connecties
            basin_poly_gdf.loc[us_basin_id, "nodes_to"].append(node_id)

            if ds_basin_id is None:
                print("  verbinden naar dichtsbijzijnde LevelBoundary of Terminal")
                dfs = (
                    getattr(model, i).node.df for i in ["level_boundary", "terminal"]
                )
                dfs = (i.set_index("node_id") for i in dfs if i is not None)
                df = pd.concat(list(dfs))
                boundary = df.loc[df.distance(point).idxmin()]
                boundary_node_id = boundary.name
                boundary_node_ids += [boundary_node_id]
                edge_geom = network.get_line(node_id, boundary_node_id)
                model.edge.add(
                    model.outlet[node_id],
                    getattr(model, pascal_to_snake_case(boundary.node_type))[
                        boundary_node_id
                    ],
                    geometry=edge_geom,
                )
            else:
                # bijhouden basin-connecties
                basin_poly_gdf.loc[ds_basin_id, "nodes_from"].append(node_id)

                # bijhouden buur-basins
                basin_poly_gdf.loc[ds_basin_id, "neighbor_basins"].append(us_basin_id)
                basin_poly_gdf.loc[us_basin_id, "neighbor_basins"].append(ds_basin_id)

            kwk_topology[row.code] = {
                "node_id": node_id,
                "upstream": us_basin_id,
                "downstream": ds_basin_id,
            }


# %% ignore edges with "circular structures combinations"
ignore_links = []
kwk_topology = pd.DataFrame.from_dict(kwk_topology, orient="index")
for kwk in kwk_topology.itertuples():
    condition = kwk_topology["upstream"] == kwk.downstream
    condition = condition & (kwk_topology["downstream"] == kwk.upstream)
    df = kwk_topology[condition]
    for row in df.itertuples():
        ignore_links += [(row.node_id, kwk.node_id)]

# %% basins
# for row in basin_poly_gdf[basin_poly_gdf.index == 1].itertuples():

profile_geometries = []
for row in basin_poly_gdf.itertuples():
    # row = next(i for i in basin_poly_gdf.itertuples() if i.Index == 1)
    print(f"{row.Index} {row.naam}")
    # get upstream an downstream basin boundary nodes
    basin_boundary_nodes = network.nodes[
        network.nodes.distance(row.geometry.boundary) < 0.01
    ]
    us_ds = [
        network.get_upstream_downstream(i, "basin_id", max_iters=3, max_length=1000)
        for i in basin_boundary_nodes.index
    ]
    basin_boundary_nodes["upstream"] = [i[0] for i in us_ds]
    basin_boundary_nodes["downstream"] = [i[1] for i in us_ds]
    basin_boundary_nodes = basin_boundary_nodes.dropna(
        subset=["upstream", "downstream"], how="any"
    )
    basin_boundary_nodes.loc[:, ["count"]] = [
        len(network.upstream_nodes(i)) + len(network.downstream_nodes(i))
        for i in basin_boundary_nodes.index
    ]

    basin_boundary_nodes.sort_values(by="count", ascending=False, inplace=True)
    basin_boundary_nodes.loc[:, ["sum"]] = (
        basin_boundary_nodes["upstream"] + basin_boundary_nodes["downstream"]
    )
    basin_boundary_nodes.drop_duplicates("sum", inplace=True)
    connected = basin_poly_gdf.at[row.Index, "neighbor_basins"]
    basin_boundary_nodes = basin_boundary_nodes[
        ~basin_boundary_nodes["upstream"].isin(connected)
    ]
    basin_boundary_nodes = basin_boundary_nodes[
        ~basin_boundary_nodes["downstream"].isin(connected)
    ]

    if model.manning_resistance.node.df is not None:
        basin_boundary_nodes = basin_boundary_nodes[
            ~basin_boundary_nodes.index.isin(model.manning_resistance.node.df.node_id)
        ]

    # overige basin_boundary_nodes voorzien van een ManningResistance
    for bbn_row in basin_boundary_nodes.reset_index().itertuples():
        node = Node(bbn_row.node_id, bbn_row.geometry)
        poly = (
            basin_poly_gdf.loc[[bbn_row.upstream, bbn_row.downstream]]
            .buffer(1)
            .unary_union.buffer(-1)
        )
        line = LineString(
            [
                network.nodes.at[bbn_row.node_id, "geometry"],
                network.nodes.at[
                    network.upstream_nodes(bbn_row.node_id)[0], "geometry"
                ],
            ]
        )

        geometry = clip_profile(
            get_profile(line, bbn_row.geometry, 50000), bbn_row.geometry, poly
        )
        profile_geometries += [{"node_id": bbn_row.node_id, "geometry": geometry}]

        data = [
            manning_resistance.Static(
                length=[999],
                manning_n=[0.04],
                profile_width=[geometry.length],
                profile_slope=[1],
            )
        ]

        model.manning_resistance.add(node, data)

        # bijhouden basin-connecties
        basin_poly_gdf.loc[bbn_row.downstream, "nodes_from"].append(bbn_row.node_id)
        basin_poly_gdf.loc[bbn_row.upstream, "nodes_to"].append(bbn_row.node_id)

    # toevoegen basin

    nodes_from = list(set(row.nodes_from))
    nodes_to = list(set(row.nodes_to))

    # add basin-node
    if (len(nodes_from) > 0) and (len(nodes_to) > 0):
        gdf = network.subset_nodes(
            nodes_from,
            nodes_to,
            duplicated_nodes=True,
            directed=True,
            inclusive=False,
            ignore_links=ignore_links,
        )
        if gdf.empty:
            gdf = network.subset_nodes(
                nodes_from,
                nodes_to,
                duplicated_nodes=True,
                directed=False,
                inclusive=False,
                ignore_links=ignore_links,
            )
        if gdf.empty:
            gdf = network.nodes[network.nodes.basin_id == row.Index]
    elif (len(nodes_from) > 0) or (len(nodes_to) > 0):
        gdf = network.nodes[network.nodes.basin_id == row.Index]

    gdf = gdf[gdf["basin_id"] == row.Index]

    basin_node_id = gdf.distance(row.geometry.centroid).idxmin()
    basin_node = Node(
        basin_node_id, network.nodes.at[basin_node_id, "geometry"], name=row.naam
    )
    basin_poly_gdf.loc[row.Index, "node_id"] = basin_node_id
    level = basin_profile_df.loc[row.Index].level.to_list()
    area = basin_profile_df.loc[row.Index].area.to_list()
    data = [
        basin.Profile(
            area=area,
            level=level,
        ),
        basin.Static(
            drainage=[0.0],
            potential_evaporation=[0.0],
            infiltration=[0.0],
            precipitation=[0.0],
            urban_runoff=[0.0],
        ),
        basin.State(
            level=[max(max(level), 0)]
        ),  # meter waterdiepte, maar minimaal dan NAP
        basin.Area(geometry=[basin_poly_gdf.at[row.Index, "geometry"]]),
    ]

    model.basin.add(basin_node, data)

    nodes_series = (
        model.node_table()
        .df.set_index("node_id")["node_type"]
        .apply(pascal_to_snake_case)
    )

    # connect all nodes_from to basin
    for node_id in nodes_from:
        model.edge.add(
            from_node=getattr(model, nodes_series[node_id])[node_id],
            to_node=model.basin[basin_node_id],
            geometry=network.get_line(node_id, basin_node_id),
            name=row.naam,
        )
    # connect all nodes_to to basin
    for node_id in nodes_to:
        model.edge.add(
            from_node=model.basin[basin_node_id],
            to_node=getattr(model, nodes_series[node_id])[node_id],
            geometry=network.get_line(basin_node_id, node_id),
            name=row.naam,
        )


# %% Verbinden overgebleven levelBoundaries zonder kunstwerken (bijv. Westerschelde en Nieuwe Maas)
for boundary_node_id in model.level_boundary.node.df[
    ~model.level_boundary.node.df.node_id.isin(boundary_node_ids)
].node_id.to_list():
    level = model.level_boundary.static.df.set_index("node_id").at[
        boundary_node_id, "level"
    ]
    us_basin_id = network.find_upstream(boundary_node_id, "basin_id", max_iters=50)

    basin_node_id = int(basin_poly_gdf.at[us_basin_id, "node_id"])
    node_id = (
        network.nodes.loc[network.get_path(basin_node_id, boundary_node_id)]
        .distance(basin_poly_gdf.at[us_basin_id, "geometry"].boundary)
        .idxmin()
    )
    # toevoegen outlet met oneindig capaciteit en een crest_level op boundary level
    model.outlet.add(
        Node(node_id, network.nodes.at[node_id, "geometry"]),
        [outlet.Static(flow_rate=[99999], min_crest_level=[level])],
    )

    model.edge.add(
        model.basin[basin_node_id],
        model.outlet[node_id],
        geometry=network.get_line(basin_node_id, node_id),
    )

    model.edge.add(
        model.outlet[node_id],
        model.level_boundary[boundary_node_id],
        geometry=network.get_line(node_id, boundary_node_id),
    )

    boundary_node_ids += [boundary_node_id]

# %%updaten manningResistances
for row in model.manning_resistance.static.df.itertuples():
    edge_to = model.edge.df[model.edge.df["to_node_id"] == row.node_id].iloc[0]
    edge_from = model.edge.df[model.edge.df["from_node_id"] == row.node_id].iloc[0]

    # length = sum of both lengths
    length = edge_to.geometry.length + edge_from.geometry.length

    model.manning_resistance.static.df.loc[
        model.manning_resistance.static.df.node_id == row.node_id,
        ["length"],
    ] = length

# %%wegschrijven model
print("write ribasim model")
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_netwerk", "hws.toml")
model.write(ribasim_toml)
database_gpkg = ribasim_toml.with_name("database.gpkg")

gpd.GeoDataFrame(profile_geometries, crs=28992).to_file(
    database_gpkg, layer="ManningResistance / profile"
)

# %%
