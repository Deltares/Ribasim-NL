# %%
import inspect

import geopandas as gpd
import pandas as pd
from networkx import all_shortest_paths, shortest_path
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet, pump
from ribasim_nl.geometry import split_line
from ribasim_nl.gkw import get_data_from_gkw
from ribasim_nl.model import default_tables
from ribasim_nl.reset_static_tables import reset_static_tables
from ribasim_nl.sanitize_node_table import sanitize_node_table
from shapely.geometry import MultiLineString, Point
from shapely.ops import snap, split

from ribasim_nl import CloudStorage, Model, Network, NetworkValidator

cloud = CloudStorage()

authority = "Noorderzijlvest"
short_name = "nzv"

he_shp = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/20241113/HydrologischeEenheden_v45.shp")
he_snap_shp = cloud.joinpath(authority, "verwerkt/1_ontvangen_data/20241113/HE_v45_snappingpoints.shp")
lines_shp = cloud.joinpath(authority, "verwerkt/5_D_HYDRO_export/hydroobjecten/Noorderzijlvest_hydroobjecten.shp")
model_edits_path = cloud.joinpath(authority, "verwerkt/model_edits.gpkg")

ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3")

cloud.synchronize(filepaths=[he_shp, he_snap_shp, lines_shp, model_edits_path, ribasim_dir])
ribasim_toml = ribasim_dir / "model.toml"

# %% read model
model = Model.read(ribasim_toml)
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{short_name}.toml")
network_validator = NetworkValidator(model)

# read hydrologische eenheden
he_df = gpd.read_file(he_shp)
he_df.loc[:, "node_id"] = pd.Series()

he_snap_df = gpd.read_file(he_snap_shp)

lines_gdf = gpd.read_file(
    lines_shp,
    fid_as_index=True,
)

points = (
    model.node_table().df[model.node_table().df.node_type.isin(["TabulatedRatingCurve", "Outlet", "Pump"])].geometry
)

for row in lines_gdf.itertuples():
    line = row.geometry
    snap_points = points[points.distance(line) < 0.1]
    snap_points = snap_points[snap_points.distance(line.boundary) > 0.1]
    if not snap_points.empty:
        snap_point = snap_points.union_all()
        line = snap(line, snap_point, 1e-8)
        split_lines = split(line, snap_point)

        lines_gdf.loc[row.Index, ["geometry"]] = split_lines

lines_gdf = lines_gdf.explode(index_parts=False, ignore_index=True)
lines_gdf.crs = 28992
network = Network(lines_gdf.copy())
network.to_file(cloud.joinpath(authority, "verwerkt/network.gpkg"))


# %% some stuff we'll need again
manning_data = manning_resistance.Static(length=[100], manning_n=[0.04], profile_width=[10], profile_slope=[1])
level_data = level_boundary.Static(level=[0])

basin_data = [
    basin.Profile(level=[0.0, 1.0], area=[0.01, 1000.0]),
    basin.Static(
        drainage=[0.0],
        potential_evaporation=[0.001 / 86400],
        infiltration=[0.0],
        precipitation=[0.005 / 86400],
    ),
    basin.State(level=[0]),
]
outlet_data = outlet.Static(flow_rate=[100])
pump_data = pump.Static(flow_rate=[10])

# %%
# %% https://github.com/Deltares/Ribasim-NL/issues/155#issuecomment-2454955046

# 76 links bij opgeheven nodes verwijderen
mask = model.link.df.to_node_id.isin(model.node_table().df.index) & model.link.df.from_node_id.isin(
    model.node_table().df.index
)
missing_links_df = model.link.df[~mask]

model.link.df = model.link.df[~model.link.df.index.isin(missing_links_df.index)]

# %% Reset static tables

# Reset static tables
model = reset_static_tables(model)

# %% add snap_point to he_df

# Purpose is to define 1 point for every 1 he polygon
# We can use that later in finding he over the network

# drop duplicated kwk points
he_snap_df = he_snap_df[~(he_snap_df["Kunstwerk"].duplicated() & he_snap_df["Kunstwerk"].notna())]

# strip GPG from Kunstwerk column
mask = he_snap_df["Kunstwerk"].notna() & he_snap_df["Kunstwerk"].str.startswith("GPG")
he_snap_df.loc[mask, "Kunstwerk"] = he_snap_df[mask].Kunstwerk.str[3:]

# we define 1 outlet for every he
he_outlet_df = gpd.GeoDataFrame(geometry=gpd.GeoSeries(), crs=he_snap_df.crs, index=he_df.index)

mask = he_df["KWKuit"].notna()
he_outlet_df.loc[mask, "geometry"] = he_df[mask]["KWKuit"].apply(
    lambda x: he_snap_df.set_index("Kunstwerk").at[x, "geometry"]
)
he_outlet_df.loc[~mask, "geometry"] = he_df[~mask]["HEIDENT"].apply(
    lambda x: he_snap_df.set_index("HEIDENT").at[x, "geometry"]
)

he_outlet_df.loc[:, "HEIDENT"] = he_df["HEIDENT"]
he_outlet_df.set_index("HEIDENT", inplace=True)
he_df.set_index("HEIDENT", inplace=True)

# niet altijd ligt de coordinaat goed
he_outlet_df.loc["GPGKST0470", ["geometry"]] = model.manning_resistance[892].geometry

he_outlet_df.to_file(cloud.joinpath(authority, "verwerkt/HydrologischeEenheden_v45_outlets.gpkg"))

# %% Edit network

# We modify the network:
# merge basins in Lauwersmeer

for action in [
    "merge_basins",
    "remove_node",
    "reverse_link",
    "move_node",
    "add_basin",
    "connect_basins",
    "update_node",
    "remove_link",
    "update_node",
]:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_path, layer=action, fid_as_index=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)


model.merge_basins(basin_id=1231, to_basin_id=1280)
model.merge_basins(basin_id=1179, to_basin_id=1184)
model.merge_basins(basin_id=1184, to_basin_id=1280)
model.merge_basins(basin_id=1034, to_basin_id=1280)
model.merge_basins(basin_id=1279, to_basin_id=1182)
model.merge_basins(basin_id=1181, to_basin_id=1182)
model.merge_basins(basin_id=1408, to_basin_id=1182)
model.merge_basins(basin_id=1028, to_basin_id=1378)
model.merge_basins(basin_id=1373, to_basin_id=1378)
model.merge_basins(basin_id=1032, to_basin_id=1182)

# # Van Starkenborghkanaal mergen Manning knopen weg!
model.merge_basins(basin_id=1223, to_basin_id=1307)
model.merge_basins(basin_id=1307, to_basin_id=1244)
model.merge_basins(basin_id=1244, to_basin_id=1186)
model.merge_basins(basin_id=1292, to_basin_id=1186)


# %% assign Basin / Area using KWKuit

node_df = model.node_table().df


# we find Basin area if we kan find KWKuit in the model
def find_basin_id(kwk_code):
    kwk_node_id = node_df[node_df.node_type != "Basin"].reset_index().set_index("name").at[kwk_code, "node_id"]
    basin_node_id = model.upstream_node_id(kwk_node_id)
    return basin_node_id


# only works if names are not duplicated
mask = node_df[node_df["name"].isin(he_df["KWKuit"])].duplicated()
if mask.any():
    raise ValueError(f"kwk codes duplicated in node table {node_df[mask].name.to_list()}")

mask = he_df["KWKuit"].isin(node_df.name)

he_df.loc[mask, "node_id"] = he_df[mask]["KWKuit"].apply(lambda x: find_basin_id(x))


# %% find he on network within basin


# We find all hydrologische eenheden using outlets between basin and it's connector-nodes
def get_network_node(point):
    node = network.move_node(point, max_distance=1, align_distance=10)
    if node is None:
        node = network.add_node(point, max_distance=1, align_distance=10)
    return node


for node_id in model.basin.node.df.index:
    print(node_id)
    # get basin_node_id
    network_basin_node = get_network_node(node_df.at[node_id, "geometry"])
    network._graph.nodes[network_basin_node]["node_id"] = node_id

    upstream_node_ids = model.upstream_node_id(node_id)
    if isinstance(upstream_node_ids, pd.Series):
        upstream_node_ids = upstream_node_ids.to_list()
    else:
        upstream_node_ids = [upstream_node_ids]

    downstream_node_ids = model.downstream_node_id(node_id)
    if isinstance(downstream_node_ids, pd.Series):
        downstream_node_ids = downstream_node_ids.to_list()
    else:
        downstream_node_ids = [downstream_node_ids]

    upstream_nodes = [get_network_node(node_df.at[i, "geometry"]) for i in upstream_node_ids if i is not None]
    downstream_nodes = [get_network_node(node_df.at[i, "geometry"]) for i in downstream_node_ids]

    # empty list of LineStrings
    data = []

    # draw links from upstream nodes
    for idx, network_node in enumerate(upstream_nodes):
        all_paths = list(all_shortest_paths(network.graph_undirected, source=network_node, target=network_basin_node))
        if len(all_paths) > 1:
            all_nodes = [i for i in upstream_nodes + downstream_nodes if i != network_node]
            all_paths = [i for i in all_paths if not any(_node_id in all_nodes for _node_id in i)]
        if len(all_paths) != 1:
            all_paths = [shortest_path(network.graph_undirected, source=network_node, target=network_basin_node)]
        else:
            link = network.path_to_line(all_paths[0])
            if link.length > 0:
                data += [link]

                mask = (model.link.df["from_node_id"] == upstream_node_ids[idx]) & (
                    model.link.df["to_node_id"] == node_id
                )
                model.link.df.loc[mask, ["geometry"]] = link

    # draw links to downstream nodes
    for idx, network_node in enumerate(downstream_nodes):
        all_paths = list(all_shortest_paths(network.graph_undirected, target=network_node, source=network_basin_node))
        if len(all_paths) > 1:
            all_nodes = [i for i in upstream_nodes + downstream_nodes if i != network_node]
            all_paths = [i for i in all_paths if not any(_node_id in all_nodes for _node_id in i)]
        if len(all_paths) != 1:
            all_paths = [shortest_path(network.graph_undirected, target=network_node, source=network_basin_node)]
        else:
            link = network.path_to_line(all_paths[0])
            if link.length > 0:
                data += [link]

                mask = (model.link.df["to_node_id"] == downstream_node_ids[idx]) & (
                    model.link.df["from_node_id"] == node_id
                )
                model.link.df.loc[mask, ["geometry"]] = link

    mask = he_df.node_id.isna() & (he_outlet_df.distance(MultiLineString(data)) < 0.75)
    he_df.loc[mask, ["node_id"]] = node_id

# %% add last missings

# We add last missing hydrologische eenheden on downstream basin
for row in he_df[he_df["node_id"].isna()].itertuples():
    # row = next(i for i in he_df.itertuples() if i.Index == "GFE04712")
    print(row.Index)
    point = he_outlet_df.at[row.Index, "geometry"]

    network_node = get_network_node(point)

    basin_node_id = network.find_downstream(network_node, attribute="node_id")
    he_df.loc[row.Index, ["node_id"]] = basin_node_id


data = []
for node_id, df in he_df[he_df["node_id"].notna()].groupby("node_id"):
    geometry = df.union_all()
    df.sort_values("OPVAFWZP", inplace=True)
    streefpeil = df.iloc[0].OPVAFWZP
    code = df.iloc[0].GPGIDENT

    data += [
        {"node_id": node_id, "meta_streefpeil": streefpeil, "meta_code_waterbeheerder": code, "geometry": geometry}
    ]

df = gpd.GeoDataFrame(data, crs=model.crs)
df.loc[:, "geometry"] = df.buffer(0.1).buffer(-0.1)
df.index.name = "fid"
model.basin.area.df = df

for action in ["remove_basin_area", "add_basin_area"]:
    print(action)
    # get method and args
    method = getattr(model, action)
    keywords = inspect.getfullargspec(method).args
    df = gpd.read_file(model_edits_path, layer=action, fid_as_index=True)
    for row in df.itertuples():
        # filter kwargs by keywords
        kwargs = {k: v for k, v in row._asdict().items() if k in keywords}
        method(**kwargs)

model.remove_unassigned_basin_area()


# %% TabulatedRatingCurve to Outlet

# TabulatedRatingCurve to Outlet
for row in model.node_table().df[model.node_table().df.node_type == "TabulatedRatingCurve"].itertuples():
    node_id = row.Index
    model.update_node(node_id=node_id, node_type="Outlet")

# get a name series from GKW-data
df = get_data_from_gkw(authority=authority, layers=["gemaal", "stuw", "sluis"])
df.set_index("code", inplace=True)
names = df["naam"]
names.loc["KSL011"] = "R.J. Cleveringensluizen"

sanitize_node_table(
    model,
    meta_columns=["meta_code_waterbeheerder", "meta_categorie"],
    copy_map=[
        {"node_types": ["Outlet", "Pump"], "columns": {"name": "meta_code_waterbeheerder"}},
        {"node_types": ["LevelBoundary", "FlowBoundary", "Basin", "ManningResistance"], "columns": {"name": ""}},
    ],
    names=names,
)

# %% set meta_gestuwd. Omdat er geen duikers in dit model zitten mogen alle outlets en pumps op True
model.basin.node.df["meta_gestuwd"] = False
model.outlet.node.df["meta_gestuwd"] = True
model.pump.node.df["meta_gestuwd"] = True

# en dan de basis bovenstrooms van deze objecten
upstream_node_ids = [model.upstream_node_id(i) for i in node_df.index]
basin_mask = model.basin.node.df.index.isin(upstream_node_ids)
model.basin.node.df.loc[basin_mask, "meta_gestuwd"] = True

# %% set flow-boundaries to level-boundaries (plus outlet)
for row in model.flow_boundary.node.df.itertuples():
    node_id = row.Index
    basin_node_id = model.downstream_node_id(node_id)

    # get link geometry and remove link
    link_id = model.link.df[model.link.df["from_node_id"] == node_id].index[0]
    link_geometry = model.link.df.at[link_id, "geometry"]
    model.link.df = model.link.df[model.link.df.index != link_id]

    # outlet node.geometry 10m from upstream or at 10% of link.geometry
    if link_geometry.length > 20:
        outlet_node_geometry = link_geometry.interpolate(10)
    else:
        outlet_node_geometry = link_geometry.interpolate(0.1, normalized=True)

    # change flow_boundary to level_boundary and add outlet_node
    model.update_node(node_id, node_type="LevelBoundary")
    outlet_node = model.outlet.add(
        node=Node(geometry=outlet_node_geometry, name=row.name), tables=default_tables.outlet
    )

    # remove old links and add 2 new
    left_link_geometry, right_link_geometry = list(split_line(link_geometry, outlet_node_geometry).geoms)
    model.link.add(model.level_boundary[node_id], outlet_node, geometry=left_link_geometry)
    model.link.add(outlet_node, model.basin[basin_node_id], geometry=right_link_geometry)

# Moved t Noord-Willemskanaal so it connects properly with Hunze and Aa's model
model.move_node(geometry=Point(233237, 559975), node_id=14)

# %% reverse links before junctionfy
for link_id in [224, 1178, 7, 991, 213, 1152, 519, 1491, 2033, 2032, 12, 997]:
    model.reverse_link(link_id=link_id)

# Lijst met link_ids die omgedraaid moeten worden nav review Vincent
reverse_link_ids = [
    111,
    1057,  # behorende bij knoop 81 (Inlaat Oldenoord)
    42,
    1141,  # behorende bij knoop 1141 (inlaat Oude Badweg)
    283,
    1198,  # behorende bij knoop 186 (inlaat Lettelberterbergboezem)
    37,
    1022,  # behorende bij knoop 48 (inlaat gemaal De Verbetering)
    1192,
    282,  # behorende bij knoop 182 (inlaat gemaal Lettelbert)
    131,
    1067,  # behorende bij knoop 1067 (inlaat gemaal Tilburg)
    73,
    1064,  # behorende bij knoop 88 (inlaat gemaal De Dijken)
    30,
    1015,  # behorende bij knoop 43 (inlaat gemaal Nieuwe Robbengat)
    290,
    1196,  # behorende bij knoop 185 (inlaat Stadspark)
    32,
    1018,  # behorende bij knoop 45 (inlaat gemaal De Slokkert)
]

# Draai de richting van alle genoemde links om
for link_id in reverse_link_ids:
    model.reverse_link(link_id=link_id)

model.update_node(node_id=837, node_type="Outlet")  # duiker wordt outlet, was manning
model.remove_node(node_id=1749, remove_links=True)
model.remove_node(node_id=12, remove_links=True)
model.remove_node(node_id=1750, remove_links=True)
model.remove_node(node_id=13, remove_links=True)

# voeg inlaat Meerweg toe (n.a.v communicatie met Vincent)
boundary_node = model.level_boundary.add(Node(geometry=Point(234937, 575821)))
outlet_node = model.outlet.add(Node(name="Meerweg", geometry=Point(234893, 575872)))
model.link.add(boundary_node, outlet_node)
model.link.add(outlet_node, model.basin[1192])

# %% Create junctions
# model = junctionify(model)


#  %% write model
model.use_validation = True
model.write(ribasim_toml)
model.report_basin_area()
model.report_internal_basins()

# %%
