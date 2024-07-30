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
    node_df: gpd.GeoDataFrame,
    edge_df: gpd.GeoDataFrame,
    basin_areas_df: gpd.GeoDataFrame,
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
    crs: int = "28992",
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
    model = ribasim.Model(starttime=start_time, endtime=end_time, crs=crs)

    # check and drop duplicated node ides
    if node_df.node_id.duplicated().any():
        logger.warning("node_in_df contains duplicated node_ids that get dropped")
        node_df.drop_duplicates("node_id", inplace=True)

    # correction column names ribasim Nodes
    if "split_type" in node_df.columns:
        rename_columns = {
            "object_type": "meta_object_type",
            "split_node_id": "meta_split_node_id",
            "split_type": "meta_split_type",
            "boundary": "meta_boundary",
            "split_node": "meta_split_node",
            "basin": "meta_basin",
            "connection": "meta_connection",
        }
        node_df = node_df.rename(columns=rename_columns)

    # define all node types
    model.basin.node.df = node_df[node_df.node_type == "Basin"]
    model.basin.area.df = basin_areas_df
    model.pump.node.df = node_df[node_df.node_type == "Pump"]
    model.outlet.node.df = node_df[node_df.node_type == "Outlet"]
    model.tabulated_rating_curve.node.df = node_df[node_df.node_type == "TabulatedRatingCurve"]
    model.manning_resistance.node.df = node_df[node_df.node_type == "ManningResistance"]
    model.linear_resistance.node.df = node_df[node_df.node_type == "LinearResistance"]
    model.level_boundary.node.df = node_df[node_df.node_type == "LevelBoundary"]
    model.flow_boundary.node.df = node_df[node_df.node_type == "FlowBoundary"]

    # check and drop duplicated edges
    if "split_type" in edge_df.columns:
        rename_columns = {
            "object_type": "meta_object_type",
            "split_node_id": "meta_split_node_id",
            "split_type": "meta_split_type",
            "boundary": "meta_boundary",
            "split_node": "meta_split_node",
            "basin": "meta_basin",
            "connection": "meta_connection",
        }
        edge_df = edge_df.rename(columns=rename_columns)
        edge_df["meta_boundary"] = edge_df["meta_boundary"].fillna(-1).astype(int)
    if "from_node_id" not in edge_df.columns:
        nodes = node_df[["node_id", "node_type", "meta_basin", "meta_boundary", "meta_split_node", "meta_categorie"]]
        node_basin = nodes[nodes.meta_basin != -1]
        node_boundary = nodes[nodes.meta_boundary != -1]
        node_split_node = nodes[nodes.meta_split_node != -1]
        rename_from_nodes = {
            "node_id": "from_node_id",
            "node_type": "from_node_type",
            "meta_categorie": "meta_from_categorie",
        }
        rename_to_nodes = {"node_id": "to_node_id", "node_type": "to_node_type", "meta_categorie": "meta_to_categorie"}
        from_node_basin = node_basin.rename(columns=rename_from_nodes)[
            ["from_node_id", "from_node_type", "meta_from_categorie", "meta_basin"]
        ]
        to_node_basin = node_basin.rename(columns=rename_to_nodes)[
            ["to_node_id", "to_node_type", "meta_to_categorie", "meta_basin"]
        ]
        from_node_boundary = node_boundary.rename(columns=rename_from_nodes)[
            ["from_node_id", "from_node_type", "meta_from_categorie", "meta_boundary"]
        ]
        to_node_boundary = node_boundary.rename(columns=rename_to_nodes)[
            ["to_node_id", "to_node_type", "meta_to_categorie", "meta_boundary"]
        ]
        from_node_split_node = node_split_node.rename(columns=rename_from_nodes)[
            ["from_node_id", "from_node_type", "meta_from_categorie", "meta_split_node"]
        ]
        to_node_split_node = node_split_node.rename(columns=rename_to_nodes)[
            ["to_node_id", "to_node_type", "meta_to_categorie", "meta_split_node"]
        ]

        edge_split_node_to_basin = edge_df[edge_df.meta_connection == "split_node_to_basin"]
        edge_basin_to_split_node = edge_df[edge_df.meta_connection == "basin_to_split_node"]
        edge_split_node_to_boundary = edge_df[edge_df.meta_connection == "split_node_to_boundary"]
        edge_boundary_to_split_node = edge_df[edge_df.meta_connection == "boundary_to_split_node"]

        edge_split_node_to_basin = to_node_basin.merge(edge_split_node_to_basin, how="inner", on="meta_basin")
        edge_split_node_to_basin = from_node_split_node.merge(
            edge_split_node_to_basin, how="inner", on="meta_split_node"
        )
        edge_basin_to_split_node = to_node_split_node.merge(edge_basin_to_split_node, how="inner", on="meta_split_node")
        edge_basin_to_split_node = from_node_basin.merge(edge_basin_to_split_node, how="inner", on="meta_basin")
        edge_split_node_to_boundary = to_node_boundary.merge(
            edge_split_node_to_boundary, how="inner", on="meta_boundary"
        )
        edge_split_node_to_boundary = from_node_split_node.merge(
            edge_split_node_to_boundary, how="inner", on="meta_split_node"
        )
        edge_boundary_to_split_node = to_node_split_node.merge(
            edge_boundary_to_split_node, how="inner", on="meta_split_node"
        )
        edge_boundary_to_split_node = from_node_boundary.merge(
            edge_boundary_to_split_node, how="inner", on="meta_boundary"
        )

        edge_df = pd.concat(
            [
                edge_split_node_to_basin,
                edge_basin_to_split_node,
                edge_split_node_to_boundary,
                edge_boundary_to_split_node,
            ]
        )
        edge_df = edge_df.reset_index(drop=True)
        edge_df.index.name = "fid"
        edge_df = gpd.GeoDataFrame(edge_df, geometry="geometry", crs=crs)
        edge_df["meta_categorie"] = "doorgaand"
        edge_df.loc[
            (edge_df["meta_from_categorie"] == "hoofdwater") & (edge_df["meta_to_categorie"] == "hoofdwater"),
            "meta_categorie",
        ] = "hoofdwater"

    if edge_df.duplicated(subset=["from_node_id", "from_node_type", "to_node_id", "to_node_type"]).any():
        logger.warning("edge_df contains duplicated node_ids that get dropped")
        edge_df.drop_duplicates(["from_node_id", "from_node_type", "to_node_id", "to_node_type"], inplace=True)

    # convert to edge-table
    model.edge.df = edge_df

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

    model.basin.profile.df = profile_df
    model.basin.static.df = static_df
    model.basin.state.df = state_df

    # define ribasim pump-table
    static_df = node_df[node_df.node_type == "Pump"][["node_id"]]
    static_df.loc[:, ["flow_rate"]] = flow_rate

    model.pump.static.df = static_df

    # define ribasim outlet-table
    static_df = node_df[node_df.node_type == "Outlet"][["node_id"]]
    static_df.loc[:, ["flow_rate"]] = flow_rate

    model.outlet.static.df = static_df

    # define ribasim manning resistance
    static_df = node_df[node_df.node_type == "ManningResistance"][["node_id"]]
    static_df.loc[:, ["length"]] = manning["length"]
    static_df.loc[:, ["manning_n"]] = manning["manning_n"]
    static_df.loc[:, ["profile_width"]] = manning["profile_width"]
    static_df.loc[:, ["profile_slope"]] = manning["profile_slope"]

    model.manning_resistance.static.df = static_df

    # define ribasim linear resistance
    static_df = node_df[node_df.node_type == "LinearResistance"][["node_id"]]
    static_df.loc[:, ["resistance"]] = resistance

    model.linear_resistance.static.df = static_df

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

    model.tabulated_rating_curve.static.df = static_df

    # define ribasim flow boundary
    static_df = node_df[node_df.node_type == "FlowBoundary"][["node_id"]]
    static_df.loc[:, ["flow_rate"]] = flow_rate

    model.flow_boundary.static.df = static_df

    # define ribasim level boundary
    static_df = node_df[node_df.node_type == "LevelBoundary"][["node_id"]]
    static_df.loc[:, ["level"]] = level

    model.level_boundary.static.df = static_df

    return model
