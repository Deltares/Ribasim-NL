# %%
import geopandas as gpd
import pandas as pd
import ribasim
from ribasim_nl import CloudStorage

cloud = CloudStorage()

# dummy values
DEFAULT_PROFILE = {
    "area": [0.01, 1000.0],
    "level": [0.0, 1.0],
}
DEFAULT_PRECIPITATION = 0.005 / 86400  # m/s
DEFAULT_EVAPORATION = 0.001 / 86400  # m/s
DEFAULT_FLOW_RATE = 0.1  # m3/s
DEFAULT_MANNING = {
    "length": 100,
    "manning_n": 0.04,
    "profile_width": 10,
    "profile_slope": 1,
}
DEFAULT_RESISTANCE = 0.005
DEFAULT_LEVEL = 0
DEFAULT_RATING_CURVE = {
    "level": [0, 5],
    "flow_rate": [0.0, DEFAULT_FLOW_RATE],
}

waterschap = "Noorderzijlvest"

in_gpkg = cloud.joinpath(waterschap, "verwerkt", "ribasim_model.gpkg")

# read-tables
node_in_df = gpd.read_file(in_gpkg, layer="Node", engine="pyogrio")
edge_in_df = gpd.read_file(in_gpkg, layer="Edge", engine="pyogrio")


# TODO: remove duplicate node_ids in input-data
node_in_df.drop_duplicates("node_id", inplace=True)

# %% define ribasim-node table
node_df = node_in_df.rename(columns={"type": "node_type"})[
    ["node_id", "name", "node_type", "geometry"]
]
node_df.set_index("node_id", drop=False, inplace=True)
node_df.index.name = "fid"
node = ribasim.Node(df=node_df)

# %% define ribasim-edge table

edge_df = edge_in_df[["geometry"]]
edge_df.loc[:, ["from_node_id", "to_node_id", "from_node_type", "to_node_type"]] = (
    None,
    None,
    None,
    None,
)

# add from/to node id/type to edge_df
for row in edge_in_df.itertuples():
    point_from, point_to = row.geometry.boundary.geoms

    from_node_id = node_df.geometry.distance(point_from).sort_values().index[0]
    from_node_type = node_df.at[from_node_id, "node_type"]

    to_node_id = node_df.geometry.distance(point_to).sort_values().index[0]
    to_node_type = node_df.at[to_node_id, "node_type"]

    edge_df.loc[
        [row.Index], ["from_node_id", "from_node_type", "to_node_id", "to_node_type"]
    ] = from_node_id, from_node_type, to_node_id, to_node_type

# TODO: remove duplicated edges from input-data
edge_df.drop_duplicates(["from_node_id", "to_node_id"], inplace=True)

# convert to edge-table
edge = ribasim.Edge(df=edge_df)
# %% define ribasim network-table
network = ribasim.Network(node=node, edge=edge)

# %% define ribasim basin-table
profile_df = pd.concat(
    [
        pd.DataFrame({"node_id": [i] * len(DEFAULT_PROFILE["area"]), **DEFAULT_PROFILE})
        for i in node_df[node_df.node_type == "Basin"].node_id
    ],
    ignore_index=True,
)

static_df = node_df[node_df.node_type == "Basin"][["node_id"]]
static_df.loc[:, ["precipitation"]] = DEFAULT_PRECIPITATION
static_df.loc[:, ["potential_evaporation"]] = DEFAULT_EVAPORATION
static_df.loc[:, ["drainage"]] = 0
static_df.loc[:, ["infiltration"]] = 0
static_df.loc[:, ["urban_runoff"]] = 0

state_df = profile_df.groupby("node_id").min()["level"].reset_index()

basin = ribasim.Basin(profile=profile_df, static=static_df, state=state_df)

# %% define ribasim pump-table
static_df = node_df[node_df.node_type == "Pump"][["node_id"]]
static_df.loc[:, ["flow_rate"]] = DEFAULT_FLOW_RATE

pump = ribasim.Pump(static=static_df)

# %% define ribasim outlet-table
static_df = node_df[node_df.node_type == "Outlet"][["node_id"]]
static_df.loc[:, ["flow_rate"]] = DEFAULT_FLOW_RATE

outlet = ribasim.Outlet(static=static_df)
