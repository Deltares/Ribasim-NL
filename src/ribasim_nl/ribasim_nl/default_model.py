import logging

import geopandas as gpd
import pandas as pd
import ribasim

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
DEFAULT_START_TIME = "2020-01-01 00:00:00"
DEFAULT_END_TIME = "2021-01-01 00:00:00"
DEFAULTS = {
    "profile": DEFAULT_PROFILE,
    "precipitation": DEFAULT_PRECIPITATION,
    "evaporation": DEFAULT_EVAPORATION,
    "flow_rate": DEFAULT_FLOW_RATE,
    "manning": DEFAULT_MANNING,
    "resistance": DEFAULT_RESISTANCE,
    "level": DEFAULT_LEVEL,
    "rating_curve": DEFAULT_RATING_CURVE,
    "start_time": DEFAULT_START_TIME,
    "end_time": DEFAULT_END_TIME,
}

logger = logging.getLogger(__name__)


def default_model(
    node_in_df: gpd.GeoDataFrame,
    edge_in_df: gpd.GeoDataFrame,
    profile: dict,
    precipitation: float,
    evaporation: float,
    flow_rate: float,
    manning: dict,
    resistance: float,
    level: float,
    rating_curve: dict,
    start_time: str,
    end_time: str,
) -> ribasim.Model:
    """Model with default settings.

    Parameters
    ----------
    node_in_df : gpd.GeoDataFrame
        GeoDataFrame with nodes. Should contain a `node_id` and `geometry` column
    edge_in_df : gpd.GeoDataFrame
        GeoDataFrame with edges. Should contain LineStrings connecting nodes
    profile : dict
        default profile to use for all basins, e.g.:
            {
                "area": [0.01, 1000.0],
                "level": [0.0, 1.0],
            }
    precipitation : float
        default precipitation value
    evaporation : float
        default precipitation value
    flow_rate : float
        default flow_rate
    manning : dict
        default manning values, e.g.:
        {
            "length": 100,
            "manning_n": 0.04,
            "profile_width": 10,
            "profile_slope": 1,
        }
    resistance : float
        default resistance to apply on LinearResistance nodes
    level : float
        default level value
    rating_curve : dict
        default rating curve, e.g.:
        {
            "level": [0, 5],
            "flow_rate": [0.0, 0.1],
        }
    start_time : str
        default start-time, e.g. `2020-01-01 00:00:00`
    end_time : str
        default end-time, e.g. `2021-01-01 00:00:00`

    Returns
    -------
    ribasim.Model
        Ribasim model with default tables
    """

    # check and drop duplicated node ides
    if node_in_df.node_id.duplicated().any():
        logger.warning("node_in_df contains duplicated node_ids that get dropped")
        node_in_df.drop_duplicates("node_id", inplace=True)

    # define ribasim-node table
    node_df = node_in_df.rename(columns={"type": "node_type"})[
        ["node_id", "name", "node_type", "geometry"]
    ]
    node_df.set_index("node_id", drop=False, inplace=True)
    node_df.index.name = "fid"
    node = ribasim.Node(df=node_df)

    # define ribasim-edge table
    edge_df = edge_in_df[["geometry"]]
    edge_df.loc[:, ["from_node_id", "to_node_id", "from_node_type", "to_node_type"]] = (
        None,
        None,
        None,
        None,
    )

    # add from/to node id/type to edge_df
    for row in edge_in_df.itertuples():
        try:
            point_from, point_to = row.geometry.boundary.geoms

            from_node_id = node_df.geometry.distance(point_from).sort_values().index[0]
            from_node_type = node_df.at[from_node_id, "node_type"]

            to_node_id = node_df.geometry.distance(point_to).sort_values().index[0]
            to_node_type = node_df.at[to_node_id, "node_type"]

            edge_df.loc[
                [row.Index],
                ["from_node_id", "from_node_type", "to_node_id", "to_node_type"],
            ] = from_node_id, from_node_type, to_node_id, to_node_type
        except Exception as e:
            logging.error(f"Exception in edge {row.Index}")
            raise e

    # check and drop duplicated edges
    if edge_df.duplicated(subset=["from_node_id", "to_node_id"]).any():
        logger.warning("edge_in_df contains duplicated node_ids that get dropped")
        edge_df.drop_duplicates(["from_node_id", "to_node_id"], inplace=True)

    # convert to edge-table
    edge = ribasim.Edge(df=edge_df)
    # define ribasim network-table
    network = ribasim.Network(node=node, edge=edge)

    # define ribasim basin-table
    profile_df = pd.concat(
        [
            pd.DataFrame({"node_id": [i] * len(profile["area"]), **profile})
            for i in node_df[node_df.node_type == "Basin"].node_id
        ],
        ignore_index=True,
    )

    static_df = node_df[node_df.node_type == "Basin"][["node_id"]]
    static_df.loc[:, ["precipitation"]] = precipitation
    static_df.loc[:, ["potential_evaporation"]] = evaporation
    static_df.loc[:, ["drainage"]] = 0
    static_df.loc[:, ["infiltration"]] = 0
    static_df.loc[:, ["urban_runoff"]] = 0

    state_df = profile_df.groupby("node_id").min()["level"].reset_index()

    basin = ribasim.Basin(profile=profile_df, static=static_df, state=state_df)

    # define ribasim pump-table
    static_df = node_df[node_df.node_type == "Pump"][["node_id"]]
    static_df.loc[:, ["flow_rate"]] = flow_rate

    pump = ribasim.Pump(static=static_df)

    # define ribasim outlet-table
    static_df = node_df[node_df.node_type == "Outlet"][["node_id"]]
    static_df.loc[:, ["flow_rate"]] = flow_rate

    outlet = ribasim.Outlet(static=static_df)

    # define ribasim manning resistance
    static_df = node_df[node_df.node_type == "ManningResistance"][["node_id"]]
    static_df.loc[:, ["length"]] = manning["length"]
    static_df.loc[:, ["manning_n"]] = manning["manning_n"]
    static_df.loc[:, ["profile_width"]] = manning["profile_width"]
    static_df.loc[:, ["profile_slope"]] = manning["profile_slope"]

    manning_resistance = ribasim.ManningResistance(static=static_df)

    # define ribasim linear resistance
    static_df = node_df[node_df.node_type == "LinearResistance"][["node_id"]]
    static_df.loc[:, ["resistance"]] = resistance

    linear_resistance = ribasim.LinearResistance(static=static_df)

    # define ribasim tabulated reatingcurve
    static_df = pd.concat(
        [
            pd.DataFrame(
                {
                    "node_id": [i] * len(rating_curve["level"]),
                    **rating_curve,
                }
            )
            for i in node_df[node_df.node_type == "TabulatedRatingCurve"].node_id
        ],
        ignore_index=True,
    )

    tabulated_rating_curve = ribasim.TabulatedRatingCurve(static=static_df)

    # define ribasim flow boundary
    static_df = node_df[node_df.node_type == "FlowBoundary"][["node_id"]]
    static_df.loc[:, ["flow_rate"]] = flow_rate

    flow_boundary = ribasim.FlowBoundary(static=static_df)

    # define ribasim level boundary
    static_df = node_df[node_df.node_type == "LevelBoundary"][["node_id"]]
    static_df.loc[:, ["level"]] = level

    level_boundary = ribasim.LevelBoundary(static=static_df)

    # write model
    model = ribasim.Model(
        network=network,
        basin=basin,
        flow_boundary=flow_boundary,
        level_boundary=level_boundary,
        linear_resistance=linear_resistance,
        manning_resistance=manning_resistance,
        tabulated_rating_curve=tabulated_rating_curve,
        pump=pump,
        outlet=outlet,
        starttime=start_time,
        endtime=end_time,
    )

    return model
