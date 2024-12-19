# %% Import Libraries and Initialize Variables
import inspect

import geopandas as gpd
import pandas as pd
from networkx import all_shortest_paths, shortest_path
from shapely.geometry import MultiLineString
from shapely.ops import snap, split

from ribasim_nl import CloudStorage, Model, Network

# Initialize cloud storage and set authority/model parameters
cloud_storage = CloudStorage()
authority_name = "Noorderzijlvest"
model_short_name = "nzv"

# Define the path to the Ribasim model configuration file
ribasim_model_dir = cloud_storage.joinpath(authority_name, "modellen", f"{authority_name}_fix_model_network")
ribasim_model_path = ribasim_model_dir / f"{model_short_name}.toml"
model = Model.read(ribasim_model_path)

# read hydrologische eenheden
he_df = gpd.read_file(
    cloud_storage.joinpath(authority_name, "verwerkt", "1_ontvangen_data", "20241113", "HydrologischeEenheden_v45.shp"),
)
he_df.loc[:, "node_id"] = pd.Series()

he_snap_df = gpd.read_file(
    cloud_storage.joinpath(authority_name, "verwerkt", "1_ontvangen_data", "20241113", "HE_v45_snappingpoints.shp"),
)

lines_gdf = gpd.read_file(
    cloud_storage.joinpath(
        authority_name, "verwerkt", "5_D_HYDRO_export", "hydroobjecten", "Noorderzijlvest_hydroobjecten.shp"
    ),
    use_fid_as_indes=True,
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
network.to_file(cloud_storage.joinpath(authority_name, "verwerkt", "network.gpkg"))

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

he_outlet_df.to_file(cloud_storage.joinpath(authority_name, "verwerkt", "HydrologischeEenheden_v45_outlets.gpkg"))

# %% Edit network

# We modify the network:
# merge basins in Lauwersmeer

# Load node edit data
model_edits_url = cloud_storage.joinurl(authority_name, "verwerkt", "model_edits.gpkg")
model_edits_path = cloud_storage.joinpath(authority_name, "verwerkt", "model_edits.gpkg")
if not model_edits_path.exists():
    cloud_storage.download_file(model_edits_url)


for action in [
    "merge_basins",
    "remove_node",
    "update_node",
    "reverse_edge",
    "connect_basins",
    "move_node",
    "add_basin",
    "remove_edge",
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

    # draw edges from upstream nodes
    for idx, network_node in enumerate(upstream_nodes):
        all_paths = list(all_shortest_paths(network.graph_undirected, source=network_node, target=network_basin_node))
        if len(all_paths) > 1:
            all_nodes = [i for i in upstream_nodes + downstream_nodes if i != network_node]
            all_paths = [i for i in all_paths if not any(_node_id in all_nodes for _node_id in i)]
        if len(all_paths) != 1:
            all_paths = [shortest_path(network.graph_undirected, source=network_node, target=network_basin_node)]
        else:
            edge = network.path_to_line(all_paths[0])
            if edge.length > 0:
                data += [edge]

                mask = (model.edge.df["from_node_id"] == upstream_node_ids[idx]) & (
                    model.edge.df["to_node_id"] == node_id
                )
                model.edge.df.loc[mask, ["geometry"]] = edge

    # draw edges to downstream nodes
    for idx, network_node in enumerate(downstream_nodes):
        all_paths = list(all_shortest_paths(network.graph_undirected, target=network_node, source=network_basin_node))
        if len(all_paths) > 1:
            all_nodes = [i for i in upstream_nodes + downstream_nodes if i != network_node]
            all_paths = [i for i in all_paths if not any(_node_id in all_nodes for _node_id in i)]
        if len(all_paths) != 1:
            all_paths = [shortest_path(network.graph_undirected, target=network_node, source=network_basin_node)]
        else:
            edge = network.path_to_line(all_paths[0])
            if edge.length > 0:
                data += [edge]

                mask = (model.edge.df["to_node_id"] == downstream_node_ids[idx]) & (
                    model.edge.df["from_node_id"] == node_id
                )
                model.edge.df.loc[mask, ["geometry"]] = edge

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
    streefpeil = df["OPVAFWZP"].min()

    data += [{"node_id": node_id, "meta_streefpeil": streefpeil, "geometry": geometry}]

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

# %%

model.write(ribasim_model_dir.with_stem(f"{authority_name}_fix_model_area") / f"{model_short_name}.toml")
model.report_basin_area()
model.report_internal_basins()
# %%
