# %%
# import pathlib
import datetime
import json
import logging
import os
import shutil
import subprocess
import sys
import typing
import warnings
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import ribasim
import shapely
import tqdm.auto as tqdm
from ribasim.nodes import continuous_control
from shapely.geometry import LineString

from peilbeheerst_model import supply
from peilbeheerst_model.ribasim_feedback_processor import RibasimFeedbackProcessor
from ribasim_nl import CloudStorage, settings


# FIXME: Seems to be giving already used node IDs due to inconsistent node ID definitions
#  (e.g., the used 'meta_node_id'-column contains many `<NA>`-values).
def get_current_max_node_id(ribasim_model: ribasim.Model) -> int:
    with warnings.catch_warnings():
        warnings.simplefilter(action="ignore", category=FutureWarning)
        df_all_nodes = ribasim_model.node_table().df

    if len(df_all_nodes) == 0:
        max_id = 1
    else:
        max_id = int(df_all_nodes.meta_node_id.max())

    return max_id


def set_initial_basin_state(ribasim_model):
    if "meta_peilgebied_cat" in list(ribasim_model.basin.node.df.keys()):
        basin_state_df = ribasim_model.basin.node.df[["node_id", "meta_peilgebied_cat"]]
        basin_state_df["meta_categorie"] = basin_state_df["meta_peilgebied_cat"]
    else:
        basin_state_df = ribasim_model.basin.node.df[["node_id", "meta_categorie"]]

    basin_state_df["level"] = ribasim_model.basin.area.df["meta_streefpeil"].to_numpy()
    ribasim_model.basin.state.df = basin_state_df
    return


def insert_standard_profile(
    ribasim_model, unknown_streefpeil, regular_percentage=10, boezem_percentage=90, depth_profile=2
):
    profile = ribasim_model.basin.area.df.copy()
    profile.node_id, profile.meta_streefpeil = (
        profile.meta_node_id.astype(int),
        profile.meta_streefpeil.astype(float),
    )  # convert to numbers

    # determine the profile area, which is also used for the profile
    profile["area"] = profile["geometry"].area * (regular_percentage / 100)
    profile = profile[["node_id", "meta_streefpeil", "area"]]
    profile = profile.rename(columns={"meta_streefpeil": "level"})

    # now overwrite the area for the nodes which are the boezem, as these peilgebieden consist mainly out of water
    node_id_boezem = ribasim_model.basin.state.df.loc[
        ribasim_model.basin.state.df.meta_categorie == "hoofdwater", "node_id"
    ].to_numpy()
    profile.loc[profile.node_id.isin(node_id_boezem), "area"] *= (
        boezem_percentage / regular_percentage
    )  # increase the size of the area

    # define the profile at the bottom of the bakje
    profile_bottom = profile.copy()
    profile_bottom["area"] = 0.1
    profile_bottom["level"] -= depth_profile

    # define the profile slightly above the bottom of the bakje
    profile_slightly_above_bottom = profile.copy()
    profile_slightly_above_bottom["level"] -= depth_profile - 0.01  # remain one centimeter above the bottom

    # define the profile at the top of the bakje.
    profile_top = profile.copy()  # it seems that the profile stops at the streefpeil. Ribasim will extrapolate this however. By keeping it this way, it remains clear what the streefpeil of the particular node_id is.

    # combine all profiles by concatenating them, and sort on node_id, level and area.
    profile_total = pd.concat([profile_bottom, profile_slightly_above_bottom, profile_top])
    profile_total = profile_total.sort_values(by=["node_id", "level", "area"], ascending=True).reset_index(drop=True)

    # the profiles of the bergende basins are not the same as the doorgaande basins. Fix this.
    profile_total["meta_categorie"] = profile_total.merge(right=ribasim_model.basin.state.df, on="node_id")[
        "meta_categorie"
    ]

    # find the node_id of the bergende nodes with the doorgaande nodes
    bergende_nodes = profile_total.loc[profile_total.meta_categorie == "bergend"][["node_id"]].reset_index(drop=True)
    bergende_nodes["from_MR_node"] = bergende_nodes.merge(
        right=ribasim_model.link.df, left_on="node_id", right_on="from_node_id", how="left"
    )["to_node_id"]

    bergende_nodes["doorgaande_node"] = bergende_nodes.merge(
        right=ribasim_model.link.df, left_on="from_MR_node", right_on="from_node_id", how="left"
    )["to_node_id"]

    # find the profiles
    bergende_nodes = bergende_nodes.drop_duplicates(subset="node_id")
    bergende_nodes = bergende_nodes.merge(
        right=ribasim_model.basin.profile.df,
        left_on="doorgaande_node",
        right_on="node_id",
        how="inner",
        suffixes=("", "doorgaand"),
    )
    bergende_nodes["meta_categorie"] = "bergend"

    # add the found profiles in the table
    profile_total = profile_total.loc[profile_total.meta_categorie != "bergend"].reset_index(drop=True)
    bergende_nodes[["node_id", "level", "area", "meta_categorie"]]

    # drop unused columns to avoid a warning
    profile_total = profile_total.dropna(axis=1, how="all")
    bergende_nodes = bergende_nodes.dropna(axis=1, how="all")

    # remove bergende profiles, as they will be added here below
    profile_total = pd.concat([profile_total, bergende_nodes])
    profile_total = profile_total.sort_values(by=["node_id", "level"]).reset_index(drop=True)

    # insert the new tables in the model
    ribasim_model.basin.profile = profile_total

    # due to the bergende basin, the surface area has been doubled. Correct this.
    ribasim_model.basin.profile.df.area /= 2
    return


def convert_mm_day_to_m_sec(mm_per_day: float) -> float:
    """Convert a rate from millimeters per day to meters per second.

    Parameters
    ----------
    mm_per_day : float
        The rate in millimeters per day.

    Returns
    -------
    float
        The rate converted to meters per second.
    """
    seconds_per_day = 86400  # 24 hours * 60 minutes * 60 seconds
    meters_per_second = mm_per_day / 1000 / seconds_per_day

    return meters_per_second


def set_static_forcing(timesteps: int, timestep_size: str, start_time: str, forcing_dict: dict, ribasim_model: object):
    """Generate static forcing data for a Ribasim-NL model

    Generate static forcing data for a Ribasim-NL model simulation, assigning
    hydrological inputs to each node in a basin based on specified parameters.
    Modifies the ribasim_model object in place by updating its basin static
    DataFrame with the new forcing data.

    Parameters
    ----------
    timesteps : int
        Number of timesteps to generate data for.
    timestep_size : str
        Frequency of timesteps, formatted as a pandas date_range frequency string (e.g., 'D' for daily).
    start_time : str
        Start date for the data range, formatted as 'YYYY-MM-DD'.
    forcing_dict : dict
        Containing a dictionary of a single value in m/s for precipitation, potential_evaporation, drainage, infiltration, urban_runoff.
    ribasim_model : object
        A model object containing the basin node data for assigning forcing inputs.
    """
    """_summary_

    Parameters
    ----------
    timesteps : int
        _description_
    timestep_size : str
        _description_
    start_time : str
        _description_
    forcing_dict : dict
        _description_
    ribasim_model : object
        _description_
    """
    # set time range
    time_range = pd.date_range(start=start_time, periods=timesteps, freq=timestep_size)

    # set forcing conditions
    all_node_forcing_data = ribasim_model.basin.node.df[["meta_node_id"]].copy()
    for col_name, col_value in forcing_dict.items():
        all_node_forcing_data[col_name] = col_value

    # update model
    ribasim_model.basin.static = all_node_forcing_data.reset_index()
    ribasim_model.starttime = time_range[0].to_pydatetime()
    ribasim_model.endtime = time_range[-1].to_pydatetime()


def set_dynamic_forcing(ribasim_model: ribasim.Model, time: typing.Sequence[datetime.datetime], forcing: dict) -> None:
    """Set dynamic forcing conditions.

    :param ribasim_model: ribasim model
    :param time: time series/array
    :param forcing: forcing conditions

    :type ribasim_model: ribasim.Model
    :type time: sequence[datetime]
    :type forcing: dict

    :raise ValueError: if values in `forcing`-dictionary are neither float/int, nor of equal size as `time`
    """
    # validate forcing conditions
    for k, v in forcing.items():
        if not isinstance(v, float | int) and len(v) != len(time):
            msg = f"Forcing must be a single-value or its size must equal the time range; {k} has {len(v)} (=/={len(time)})"
            raise ValueError(msg)

    # set forcing conditions
    basins_ids = ribasim_model.basin.node.df[["meta_node_id"]].to_numpy(dtype=int)
    basin_time = pd.DataFrame({"node_id": np.repeat(basins_ids, len(time)), "time": np.tile(time, len(basins_ids))})
    for k, v in forcing.items():
        if isinstance(v, float | int):
            basin_time[k] = v
        else:
            basin_time[k] = np.tile(v, len(basins_ids))

    # update model
    ribasim_model.basin.time.df = basin_time.reset_index(drop=True)
    ribasim_model.starttime = time[0]
    ribasim_model.endtime = time[-1]


def set_hypothetical_dynamic_forcing(
    ribasim_model: ribasim.Model, start_time: datetime.datetime, end_time: datetime.datetime, value: float = 10
) -> None:
    """Set a basic hypothetical dynamic forcing.

    The hypothetical forcing consists of a period of precipitation followed by a period of evaporation. These periods
    are equally divided over the total model duration.

    :param ribasim_model: ribasim model
    :param start_time: start time of simulation
    :param end_time: end time of simulation
    :param value: value for precipitation and evaporation in mm per day, defaults to 10

    :type ribasim_model: ribasim.Model
    :type start_time: datetime.datetime
    :type end_time: datetime.datetime
    :type value: float, optional
    """
    # define time-variables
    halftime = start_time + (end_time - start_time) // 3
    time = start_time, halftime, end_time

    # define forcing time-series
    v = convert_mm_day_to_m_sec(value)
    precipitation = v, 0, 0
    evaporation = 0, v, v

    # set forcing conditions
    forcing = {
        "precipitation": precipitation,
        "potential_evaporation": evaporation,
        "drainage": 0,
        "infiltration": 0,
    }
    set_dynamic_forcing(ribasim_model, time, forcing)


def set_dynamic_level_boundaries(
    ribasim_model: ribasim.Model, time: typing.Sequence[datetime.datetime], levels: typing.Sequence[float]
) -> None:
    """Set dynamic level boundary water levels.

    :param ribasim_model: ribasim model
    :param time: time-series/array
    :param levels: water levels

    :type ribasim_model: ribasim.Model
    :type time: sequence[datetime]
    :type levels: sequence[float]

    :raise AssertionError: if `time` and `levels` are not of the same size
    """
    # validate conditions
    assert len(time) == len(levels), f"Size of `time` ({len(time)}) and `levels` ({len(levels)}) must be equal."

    # set level time-series
    lb_ids = ribasim_model.level_boundary.node.df[["meta_node_id"]].to_numpy(dtype=int)
    lb_time = pd.DataFrame(
        {
            "node_id": np.repeat(lb_ids, len(time)),
            "time": np.tile(time, len(lb_ids)),
            "level": np.tile(levels, len(lb_ids)),
        }
    )

    # update model
    ribasim_model.level_boundary.time.df = lb_time.reset_index(drop=True)


def set_hypothetical_dynamic_level_boundaries(
    ribasim_model: ribasim.Model,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    low: float,
    high: float,
    DYNAMIC_CONDITIONS: bool,
) -> None:
    """Set basic hypothetical dynamic level boundaries.

    :param ribasim_model: ribasim model
    :param start_time: start time of simulation
    :param end_time: end time of simulation
    :param low: low water level ("waterafvoer")
    :param high: high water level ("wateraanvoer")

    :type ribasim_model: ribasim.Model
    :type start_time: datetime.datetime
    :type end_time: datetime.datetime
    :type low: float
    :type high: float
    """
    # define time-series
    if DYNAMIC_CONDITIONS:
        end_winter = datetime.datetime(start_time.year, 4, 1)
        end_winter_1 = end_winter + datetime.timedelta(days=1)
        end_summer = datetime.datetime(start_time.year, 10, 1)
        end_summer_1 = end_summer + datetime.timedelta(days=1)

        time = start_time, end_winter, end_winter_1, end_summer, end_summer_1, end_time
        level = low, low, high, high, low, low

    else:
        halftime = start_time + (end_time - start_time) // 3
        halftime_1 = halftime + datetime.timedelta(days=1)
        time = start_time, halftime, halftime_1, end_time
        level = low, low, high, high

    # set dynamic level boundaries
    set_dynamic_level_boundaries(ribasim_model, time, level)


def Terminals_to_LevelBoundaries(ribasim_model, default_level=0):
    # all Terminals need to be replaced by LevelBoundaries for the integration of the NHWS

    # first, locate all the Terminals
    nodes_Terminals = ribasim_model.terminal.node.df.copy(deep=True)

    # second, implement the LevelBoundary nodes and change the node_type
    nodes_Terminals.node_type = "LevelBoundary"

    ribasim_model.level_boundary.node.df = pd.concat([ribasim_model.level_boundary.node.df, nodes_Terminals])
    ribasim_model.level_boundary.node.df = ribasim_model.level_boundary.node.df.sort_values(by="meta_node_id")

    # third, implement the LevelBoundary static
    nodes_Terminals = nodes_Terminals.reset_index()
    LB_static = nodes_Terminals[["node_id"]]
    LB_static.loc[:, "level"] = default_level
    LB_combined = pd.concat([ribasim_model.level_boundary.static.df, LB_static])
    LB_combined = LB_combined.drop_duplicates(subset="node_id").sort_values(by="node_id").reset_index(drop=True)
    ribasim_model.level_boundary.static = LB_combined

    # fourth, update the links table.
    ribasim_model.link.df.replace(to_replace="Terminal", value="LevelBoundary", inplace=True)

    # fifth, remove all node rows with Terminals
    if len(ribasim_model.terminal.node.df) > 0:
        ribasim_model.terminal.node.df = ribasim_model.terminal.node.df.iloc[0:0]
        # ribasim_model.terminal.static.df = ribasim_model.terminal.static.df.iloc[0:0]
    return


def FlowBoundaries_to_LevelBoundaries(ribasim_model, default_level=0):
    # all FlowBoundaries need to be replaced by LevelBoundaries for the integration of the NHWS

    # first, locate all the FlowBoundaries
    nodes_FlowBoundary = ribasim_model.flow_boundary.node.df.copy(deep=True)

    # FlowBoundaries do not yet have a TRC between the FB and the basin. Change the FlowBoundary to a TRC, and add a LevelBoundary next to it
    nodes_TRC_FlowBoundary = nodes_FlowBoundary.copy(deep=True)
    nodes_TRC_FlowBoundary["node_type"] = "TabulatedRatingCurve"

    # supplement the TRC.node table
    new_TRC_node = pd.concat([ribasim_model.tabulated_rating_curve.node.df, nodes_TRC_FlowBoundary]).reset_index(
        drop=True
    )
    new_TRC_node["node_id"] = new_TRC_node["meta_node_id"].copy()
    new_TRC_node = new_TRC_node.set_index("node_id")

    ribasim_model.tabulated_rating_curve.node.df = new_TRC_node

    # ribasim_model.tabulated_rating_curve.node.df = pd.concat(
    #     [ribasim_model.tabulated_rating_curve.node.df, nodes_TRC_FlowBoundary]
    # ).reset_index(drop=True)

    # Also supplement the TRC.static table. Create dummy Q(h)-relations
    TRC_LB1 = nodes_FlowBoundary[["meta_node_id"]].copy()
    TRC_LB1.loc[:, "level"] = 0
    TRC_LB1.loc[:, "flow_rate"] = 0

    TRC_LB2 = nodes_FlowBoundary[["meta_node_id"]].copy()
    TRC_LB2.loc[:, "level"] = 1
    TRC_LB2.loc[:, "flow_rate"] = 1

    TRC_LB = pd.concat([TRC_LB1, TRC_LB2])
    TRC_LB = TRC_LB.sort_values(by=["node_id", "level"]).reset_index(drop=True)
    ribasim_model.tabulated_rating_curve.static = pd.concat(
        [ribasim_model.tabulated_rating_curve.static.df, TRC_LB]
    ).reset_index(drop=True)

    # change the FlowBoundaries to TRC in the links
    ribasim_model.link.df.replace(to_replace="FlowBoundary", value="TabulatedRatingCurve", inplace=True)

    # remove all node rows with FlowBoundaries
    if len(ribasim_model.flow_boundary.node.df) > 0:
        ribasim_model.flow_boundary.node.df = ribasim_model.flow_boundary.node.df.iloc[0:0]
        ribasim_model.flow_boundary.static.df = ribasim_model.flow_boundary.static.df.iloc[0:0]

    # up till this point, all FlowBoundaries have been converted to TRC's. Now the actual LevelBoundaries needs to be created
    max_id = get_current_max_node_id(ribasim_model)
    nodes_FlowBoundary["meta_old_node_id"] = nodes_FlowBoundary.meta_node_id  # store for later
    nodes_FlowBoundary["node_id"] = max_id + nodes_FlowBoundary.index + 1  # implement new id's
    # nodes_FlowBoundary["node_id"] = nodes_FlowBoundary.meta_node_id.copy()
    nodes_FlowBoundary["geometry"] = nodes_FlowBoundary.geometry.translate(
        xoff=-1, yoff=-1
    )  # move the points 1 meter to the lower left (diagonally)
    nodes_FlowBoundary["node_type"] = "LevelBoundary"

    # set the new node_id; overrule the old
    nodes_FlowBoundary = nodes_FlowBoundary.set_index("node_id", drop=True)
    nodes_FlowBoundary = nodes_FlowBoundary[["node_type", "geometry", "meta_old_node_id"]]

    nodes_LevelBoundary = nodes_FlowBoundary.copy(deep=True)  # switch for clarity from Flow to Level

    # supplement the LB.node table
    new_LB_node = pd.concat(
        [ribasim_model.level_boundary.node.df, nodes_LevelBoundary[["node_type", "geometry", "meta_old_node_id"]]]
    )  # .reset_index(drop=True)
    new_LB_node["meta_node_id"] = new_LB_node.index.copy()

    ribasim_model.level_boundary.node.df = new_LB_node

    # supplement the LB.static table
    # ribasim_model.level_boundary.static.df = pd.concat(
    #     [ribasim_model.level_boundary.static.df, nodes_LevelBoundary[["node_id", "level"]]]
    # ).reset_index(drop=True)

    # the nodes have been created. Now add the links
    links_LB = pd.DataFrame()
    links_LB["from_node_id"] = (
        nodes_LevelBoundary.index.copy()
    )  # nodes_LevelBoundary["meta_node_id"].copy()  # as these nodes were initially FlowBoundaries, they always flow into the model, not out. Thus, is always the starting point (=from_node_id)
    links_LB["meta_from_node_type"] = "LevelBoundary"
    links_LB["to_node_id"] = nodes_LevelBoundary["meta_old_node_id"].to_numpy()
    links_LB["meta_to_node_type"] = "TabulatedRatingCurve"
    links_LB["meta_categorie"] = "doorgaand"

    # find the geometries, based on the from and to points
    lines_LB = pd.DataFrame()
    lines_LB["from_point"] = nodes_FlowBoundary.geometry
    lines_LB["to_point"] = nodes_FlowBoundary.geometry.translate(xoff=1, yoff=1)  # = original coordinates

    def create_linestring(row):
        return LineString([row["from_point"], row["to_point"]])

    # create the linestrings, and plug them into the df of links_LB
    if len(lines_LB) > 0:
        lines_LB["geometry"] = lines_LB.apply(create_linestring, axis=1)
        # links_LB["geometry"] = lines_LB["line"]

        # merge the geometries to the newtemp
        links_LB = links_LB.merge(right=lines_LB[["geometry"]], left_on="from_node_id", right_index=True, how="left")

    # concat the original links with the newly created links of the LevelBoundaries
    new_links = pd.concat([ribasim_model.link.df, links_LB]).reset_index(drop=True)
    new_links["link_id"] = new_links.index.copy() + 1
    new_links = new_links[
        [
            "link_id",
            "from_node_id",
            "to_node_id",
            "link_type",
            "name",
            "geometry",
            "meta_from_node_type",
            "meta_to_node_type",
            "meta_categorie",
        ]
    ]
    new_links = new_links.set_index("link_id")
    ribasim_model.link.df = new_links

    # replace all 'FlowBoundaries' with 'LevelBoundaries' in the link table
    # ribasim_model.link.df.replace(to_replace='FlowBoundary', value='LevelBoundary', inplace=True)

    # create the static table for the
    static_LevelBoundary = nodes_LevelBoundary.reset_index().copy()[["node_id"]]
    static_LevelBoundary = static_LevelBoundary.rename(columns={"meta_node_id": "node_id"})
    static_LevelBoundary["level"] = default_level
    new_static_LevelBoundary = pd.concat([ribasim_model.level_boundary.static.df, static_LevelBoundary]).reset_index(
        drop=True
    )
    ribasim_model.level_boundary.static = new_static_LevelBoundary

    return


def add_outlets(ribasim_model, delta_crest_level=0.10):
    # TRC_naar_OL = ribasim_model.tabulated_rating_curve.static.df.copy() #aanpassing RB 11 oktober
    TRC_naar_OL = ribasim_model.tabulated_rating_curve.node.df.copy()
    TRC_naar_OL = TRC_naar_OL.reset_index()  # convert the node_id index to a regular column
    TRC_naar_OL = TRC_naar_OL.drop_duplicates(subset="node_id", keep="first")
    TRC_naar_OL = TRC_naar_OL[["node_id"]]

    # fill the tables for the outlet
    outlet = pd.DataFrame(columns=["node_id", "control_state", "active", "flow_rate", "min_flow_rate", "max_flow_rate"])
    outlet["node_id"] = TRC_naar_OL
    outlet["max_flow_rate"] = 25

    # find the min_crest_level
    # to do so, find the target levels of the (boezem) connected basins. This has to be done by looking within the links
    target_level = TRC_naar_OL.merge(ribasim_model.link.df, left_on="node_id", right_on="to_node_id", how="left")

    # the basins of which the target_levels should be retrieved, are stored in the column of from_node_id
    target_level = target_level.merge(
        ribasim_model.basin.state.df, left_on="from_node_id", right_on="node_id", how="left"
    )

    # clean the df for clarity. Next, add the levels to the outlet df
    target_level = target_level[["node_id_x", "level"]]
    target_level.rename(columns={"level": "meta_min_crest_level", "node_id_x": "node_id"}, inplace=True)
    target_level = target_level.sort_values(by=["node_id"])

    outlet = target_level.copy(deep=True)
    outlet["meta_min_crest_level"] -= (
        delta_crest_level  # the peil of the boezem is allowed to lower with this much before no water will flow through the outlet, to prevent
    )
    get_outlet_geometries = ribasim_model.tabulated_rating_curve.node.df.loc[
        ribasim_model.tabulated_rating_curve.node.df.meta_node_id.isin(outlet.node_id.to_numpy())
    ]
    outlet = outlet.merge(
        get_outlet_geometries[["meta_node_id", "geometry"]], left_on="node_id", right_on="meta_node_id"
    )
    outlet["node_type"] = "Outlet"
    outlet["flow_rate"] = 0  # default setting
    outlet["meta_categorie"] = "Inlaat"

    outlet_node = outlet[["node_id", "meta_node_id", "node_type", "geometry"]]
    outlet_node = outlet_node.set_index("node_id")
    outlet_static = outlet[["node_id", "flow_rate", "meta_min_crest_level", "meta_categorie"]]

    # add the outlets to the model
    ribasim_model.outlet.node.df = outlet_node
    ribasim_model.outlet.static = outlet_static

    # remove the TRC's nodes
    ribasim_model.tabulated_rating_curve.node.df = ribasim_model.tabulated_rating_curve.node.df.loc[
        ~ribasim_model.tabulated_rating_curve.node.df.meta_node_id.isin(outlet.meta_node_id)
    ]
    ribasim_model.tabulated_rating_curve.static = ribasim_model.tabulated_rating_curve.static.df.loc[
        ribasim_model.tabulated_rating_curve.static.df.node_id.isin(
            ribasim_model.tabulated_rating_curve.node.df.index.to_numpy()
        )
    ].reset_index(drop=True)

    # replace the from_node_type and the to_node_type in the link table
    ribasim_model.link.df = ribasim_model.link.df.replace(to_replace="TabulatedRatingCurve", value="Outlet")

    return


def add_discrete_control_nodes(ribasim_model):
    if len(ribasim_model.discrete_control.node.df) != 0:
        print("Model bevat al discrete controls! Dan voegen we geen extra controls toe...")
        return

    control_states = ["off", "on"]
    dfs_pump = ribasim_model.pump.static.df
    if "control_state" not in dfs_pump.columns.tolist() or pd.isna(dfs_pump.control_state).all():
        dfs_pump = []
        for control_state in control_states:
            df_pump = ribasim_model.pump.static.df.copy()
            df_pump["control_state"] = control_state
            if control_state == "off":
                df_pump["flow_rate"] = 0.0
            dfs_pump.append(df_pump)
        dfs_pump = pd.concat(dfs_pump, ignore_index=True)
        ribasim_model.pump.static.df = dfs_pump

    for i, row in enumerate(ribasim_model.pump.node.df.itertuples()):
        # Get max nodeid and iterate
        cur_max_node_id = get_current_max_node_id(ribasim_model)
        if cur_max_node_id < 90000:
            new_nodeid = 90000 + cur_max_node_id + 1  # aanpassen loopt vanaf 90000 +1
        else:
            new_nodeid = cur_max_node_id + 1
        # print(new_nodeid, end="\r")

        # @TODO Ron aangeven in geval van meerdere matches welke basin gepakt moet worden
        # basin is niet de beste variable name
        # Kan ook level boundary of terminal, dus na & weghalen
        # ["Basin", "LevelBoundary", "Terminal"]
        basin = ribasim_model.link.df[
            ((ribasim_model.link.df.to_node_id == row.node_id) | (ribasim_model.link.df.from_node_id == row.node_id))
            & ((ribasim_model.link.df.from_node_type == "Basin") | (ribasim_model.link.df.to_node_type == "Basin"))
        ]
        assert len(basin) >= 1  # In principe altijd 2 (check)
        # Hier wordt hardcoded de eerste gepakt (aanpassen adhv meta_aanvoerafvoer kolom)
        basin = basin.iloc[0, :].copy()
        if basin.from_node_type == "Basin":
            compound_variable_id = basin.from_node_id
            listen_node_id = basin.from_node_id
        else:
            compound_variable_id = basin.to_node_id
            listen_node_id = basin.to_node_id

        df_streefpeilen = ribasim_model.basin.state.df.set_index("node_id")
        assert df_streefpeilen.index.is_unique

        ribasim_model.discrete_control.add(
            ribasim.Node(new_nodeid, row.geometry),
            [
                ribasim.nodes.discrete_control.Variable(
                    compound_variable_id=compound_variable_id,
                    listen_node_type=["Basin"],
                    listen_node_id=listen_node_id,
                    variable=["level"],
                ),
                ribasim.nodes.discrete_control.Condition(
                    compound_variable_id=compound_variable_id,
                    greater_than=[df_streefpeilen.at[listen_node_id, "level"]],  # streefpeil
                ),
                ribasim.nodes.discrete_control.Logic(
                    truth_state=["F", "T"],  # aan uit wanneer groter dan streefpeil
                    control_state=control_states,  # Werkt nu nog niet!
                ),
            ],
        )

        ribasim_model.link.add(ribasim_model.discrete_control[new_nodeid], ribasim_model.pump[row.node_id])

    return


def set_tabulated_rating_curves(ribasim_model, level_increase=1.0, flow_rate=4, LevelBoundary_level=0):
    """Create the Q(h)-relations for each TRC. It starts passing water from target level onwards."""
    # find the originating basin of each TRC
    target_level = ribasim_model.link.df.loc[
        ribasim_model.link.df.to_node_type == "TabulatedRatingCurve"
    ]  # select all TRC's. Do this from the link table, so we can look the basins easily up afterwards

    # find the target level
    target_level = pd.merge(
        left=target_level,
        right=ribasim_model.basin.state.df[["node_id", "level"]],
        left_on="from_node_id",
        right_on="node_id",
        how="left",
    )

    target_level.level.fillna(value=LevelBoundary_level, inplace=True)

    # zero flow rate on target level
    Qh_table0 = target_level[["to_node_id", "level"]]
    Qh_table0 = Qh_table0.rename(columns={"to_node_id": "node_id"})
    Qh_table0["flow_rate"] = 0

    # pre defined flow rate on target level + level increase
    Qh_table1 = Qh_table0.copy()
    Qh_table1["level"] += level_increase
    Qh_table1["flow_rate"] = flow_rate

    # combine tables, sort, reset index
    Qh_table = pd.concat([Qh_table0, Qh_table1])
    Qh_table.sort_values(by=["node_id", "level", "flow_rate"], inplace=True)
    Qh_table.reset_index(drop=True, inplace=True)

    ribasim_model.tabulated_rating_curve.static.df = Qh_table

    # remove all redundand TRC nodes
    ribasim_model.tabulated_rating_curve.node.df = ribasim_model.tabulated_rating_curve.node.df.loc[
        ribasim_model.tabulated_rating_curve.node.df.node_id.isin(Qh_table.node_id)
    ]
    ribasim_model.tabulated_rating_curve.node.df.sort_values(by="node_id", inplace=True)
    ribasim_model.tabulated_rating_curve.node.df.reset_index(drop=True, inplace=True)

    return


def set_tabulated_rating_curves_boundaries(ribasim_model, level_increase=0.1, flow_rate=40):
    # select the TRC which flow to a Terminal
    TRC_ter = ribasim_model.link.df.copy()
    TRC_ter = TRC_ter.loc[
        (TRC_ter.from_node_type == "TabulatedRatingCurve") & (TRC_ter.to_node_type == "Terminal")
    ]  # select the correct nodes

    # not all TRC_ter's should be changed, as not all these nodes are in a boezem. Some are just regular peilgebieden. Filter these nodes for numerical stability
    # first, check where they originate from
    basins_to_TRC_ter = ribasim_model.link.df.loc[ribasim_model.link.df.to_node_id.isin(TRC_ter.from_node_id)]
    basins_to_TRC_ter = basins_to_TRC_ter.loc[
        basins_to_TRC_ter.from_node_type == "Basin"
    ]  # just to be sure its a basin

    # check which basins are the boezem
    node_id_boezem = ribasim_model.basin.state.df.loc[
        ribasim_model.basin.state.df.meta_categorie == "hoofdwater", "node_id"
    ].to_numpy()

    # now, only select the basins_to_TRC_ter which are a boezem
    boezem_basins_to_TRC_ter = basins_to_TRC_ter.loc[basins_to_TRC_ter.from_node_id.isin(node_id_boezem)]

    # plug these values in TRC_ter again to obtain the selection
    TRC_ter = TRC_ter.loc[TRC_ter.from_node_id.isin(boezem_basins_to_TRC_ter.to_node_id)]

    for i in range(len(TRC_ter)):
        node_id = TRC_ter.from_node_id.iloc[i]  # retrieve node_id of the boundary TRC's

        # adjust the Q(h)-relation in the ribasim_model.tabulated_rating_curve.static.df
        original_h = ribasim_model.tabulated_rating_curve.static.df.loc[
            ribasim_model.tabulated_rating_curve.static.df.node_id == node_id, "level"
        ].iloc[0]
        last_index = ribasim_model.tabulated_rating_curve.static.df.loc[
            ribasim_model.tabulated_rating_curve.static.df.node_id == node_id
        ].index[-1]  # this is the row with the highest Qh value, which should be changed

        # change the Qh relation on the location of the last index, for each node_id in TRC_ter
        ribasim_model.tabulated_rating_curve.static.df.loc[
            ribasim_model.tabulated_rating_curve.static.df.index == last_index, "level"
        ] = original_h + level_increase
        ribasim_model.tabulated_rating_curve.static.df.loc[
            ribasim_model.tabulated_rating_curve.static.df.index == last_index, "flow_rate"
        ] = flow_rate

    return


def create_sufficient_Qh_relation_points(ribasim_model):
    """There are more TRC nodes than defined in the static table. Identify the nodes which occur less than twice in the table, and create a (for now) dummy relation. Also delete the TRC in the static table if it doesnt occur in the node table"""
    # get rid of all TRC's static rows which do not occur in the node table (assuming the node table is the groundtruth)
    TRC_nodes = ribasim_model.tabulated_rating_curve.node.df.node_id.values
    ribasim_model.tabulated_rating_curve.static.df = ribasim_model.tabulated_rating_curve.static.df.loc[
        ribasim_model.tabulated_rating_curve.static.df.node_id.isin(TRC_nodes)
    ]

    # each node_id should occur at least three times in the pile (once because of the node, twice because of the Qh relation)
    node_id_counts = ribasim_model.tabulated_rating_curve.static.df["node_id"].value_counts()

    # select all nodes which occur less than 3 times
    unique_node_ids = node_id_counts[node_id_counts < 3].index

    # create new Qh relations
    zero_flow = ribasim_model.tabulated_rating_curve.static.df[
        ribasim_model.tabulated_rating_curve.static.df["node_id"].isin(unique_node_ids)
    ]
    one_flow = zero_flow.copy()
    zero_flow.flow_rate = 0  # set flow rate to 0 if on target level
    one_flow.level += 1  # set level 1 meter higher where it discharges 1 m3/s

    # remove old Qh points
    ribasim_model.tabulated_rating_curve.static.df = ribasim_model.tabulated_rating_curve.static.df.loc[
        ~ribasim_model.tabulated_rating_curve.static.df["node_id"].isin(unique_node_ids)
    ]

    # add the new Qh points back in the df
    ribasim_model.tabulated_rating_curve.static.df = pd.concat(
        [ribasim_model.tabulated_rating_curve.static.df, zero_flow, one_flow]
    )
    # drop duplicates, sort and reset index
    ribasim_model.tabulated_rating_curve.static.df.drop_duplicates(subset=["node_id", "level"], inplace=True)
    ribasim_model.tabulated_rating_curve.static.df.sort_values(by=["node_id", "level", "flow_rate"], inplace=True)
    ribasim_model.tabulated_rating_curve.node.df.sort_values(by=["node_id"], inplace=True)
    ribasim_model.tabulated_rating_curve.static.df.reset_index(drop=True, inplace=True)

    print(len(TRC_nodes))
    print(len(ribasim_model.tabulated_rating_curve.static.df.node_id.unique()))

    return


def write_ribasim_model_Zdrive(ribasim_model, path_ribasim_toml):
    # Write Ribasim model to the Z drive
    if not os.path.exists(path_ribasim_toml):
        os.makedirs(path_ribasim_toml)

    ribasim_model.write(path_ribasim_toml)


def write_ribasim_model_GoodCloud(ribasim_model, work_dir, waterschap, include_results=True):
    """Write Ribasim model locally and to the GoodCloud.

    Copy the work_dir to the "modellen" dir, as it is required to maintain the same folder structure locally as well as the GoodCloud.
    Also clear the directory of modellen/parametereized, as there may be old results in it.
    The log file of the feedback form is not included to avoid cluttering.'
    """
    destination_path = os.path.join(
        settings.ribasim_nl_data_dir, waterschap, "modellen", f"{waterschap}_parameterized/"
    )

    # clear the modellen/parameterized dir
    if os.path.exists(destination_path):
        shutil.rmtree(destination_path)  # Remove the entire directory
    os.makedirs(destination_path)  # Recreate the empty folder

    # copy the work_dir to the "modellen" dir to maintain the same folder structure locally as well as on the 'GoodCloud'
    shutil.copytree(work_dir, destination_path, dirs_exist_ok=True)

    # it is not necessary to inlcude the log file of the feedback forms. Delete it
    for file in os.listdir(destination_path):
        file_path = os.path.join(destination_path, file)
        if file.endswith(".log") and os.path.isfile(file_path):
            os.remove(file_path)

    cloud_storage = CloudStorage()

    # Upload to waterschap/modellen/model_name instead of waterschap/verwerkt
    cloud_storage.upload_model(
        authority=waterschap, model=waterschap + "_parameterized", include_results=include_results
    )

    print(f"The model of waterboard {waterschap} has been uploaded to the goodcloud!")
    return


def index_reset(ribasim_model):
    ribasim_model.basin.node.df = ribasim_model.basin.node.df.reset_index(drop=True)
    ribasim_model.basin.node.df.index += 1

    ribasim_model.discrete_control.node.df = ribasim_model.discrete_control.node.df.reset_index(drop=True)
    ribasim_model.discrete_control.node.df.index += 1

    ribasim_model.flow_boundary.node.df = ribasim_model.flow_boundary.node.df.reset_index(drop=True)
    ribasim_model.flow_boundary.node.df.index += 1

    ribasim_model.level_boundary.node.df = ribasim_model.level_boundary.node.df.reset_index(drop=True)
    ribasim_model.level_boundary.node.df.index += 1

    ribasim_model.manning_resistance.node.df = ribasim_model.manning_resistance.node.df.reset_index(drop=True)
    ribasim_model.manning_resistance.node.df.index += 1

    ribasim_model.pump.node.df = ribasim_model.pump.node.df.reset_index(drop=True)
    ribasim_model.pump.node.df.index += 1

    ribasim_model.tabulated_rating_curve.node.df = ribasim_model.tabulated_rating_curve.node.df.reset_index(drop=True)
    ribasim_model.tabulated_rating_curve.node.df.index += 1

    return


def tqdm_subprocess(cmd, suffix=None, print_other=True, leave=True):
    desc = "Simulating"
    if suffix is not None:
        desc = f"{desc} {suffix}"

    with tqdm.tqdm(total=100, desc=desc, leave=leave) as pbar:
        process = subprocess.Popen(cmd, shell=False, bufsize=1, universal_newlines=True, stderr=subprocess.PIPE)
        for line in process.stderr:
            if line.startswith("Simulating"):
                cur_perc = int(line.split("%")[0].lower().replace("simulating", ""))
                pbar.update(cur_perc - pbar.n)
            elif print_other:
                print(line.strip())
            sys.stderr.flush()
        process.stderr.close()

        return_code = process.wait()
        if return_code != 0:
            raise subprocess.CalledProcessError(return_code, cmd)


def iterate_TRC(
    ribasim_param, allowed_tolerance, max_iter, expected_difference, max_adjustment, cmd, output_dir, path_ribasim_toml
):
    # Do initial calculation
    ribasim_param.tqdm_subprocess(cmd, print_other=False, suffix="init")

    # Read results of initial calculation
    df_basin = pd.read_feather(output_dir.joinpath("basin.arrow"))
    df_basin = df_basin.set_index("time")

    stored_trc = {}
    converged = False
    iteration = 0
    debug_data = {"iteration": [], "basin_nodeid": [], "trc_nodeid": [], "basin_diff": [], "trc_adjustment": []}
    with tqdm.tqdm(total=max_iter) as pbar:
        while not converged:
            iteration += 1

            # Read model
            with warnings.catch_warnings():
                warnings.simplefilter(action="ignore", category=FutureWarning)
                ribasim_model = ribasim.Model(filepath=path_ribasim_toml)

            # assert that all upstream nodes of a tabulated_rating_curve are basins.
            df_trc = ribasim_model.link.df[ribasim_model.link.df.to_node_type == "TabulatedRatingCurve"]
            assert (df_trc.from_node_type == "Basin").all()

            df_trc_static = ribasim_model.tabulated_rating_curve.static.df
            basins_converged = []
            for basin_node, trc_group in df_trc.groupby("from_node_id"):
                # Check for a single streefpeil for all connected tabulated rating curves
                trc_streefpeilen = []
                for row in trc_group.itertuples():
                    trc_streefpeilen.append(df_trc_static.loc[df_trc_static.node_id == row.to_node_id, "level"].min())
                assert len(set(trc_streefpeilen)) == 1

                # Assert that we have a single unique basin
                basin = ribasim_model.basin.node.df[ribasim_model.basin.node.df.node_id == basin_node]
                assert len(basin) == 1

                # Find streefpeil
                fid = ribasim_model.basin.node.df[ribasim_model.basin.node.df.node_id == basin_node].index[0]
                streefpeil = float(ribasim_model.basin.area.df.meta_streefpeil.at[fid])

                # Find water levels for this basin.
                df_basin = pd.read_feather(output_dir.joinpath("basin.arrow"))
                df_basin = df_basin.set_index("time")
                basin_result = df_basin[df_basin.node_id == basin_node]
                last_wl = basin_result.sort_index().level.iat[-1]
                basin_diff = last_wl - streefpeil

                # Determine if the rating curves have resulted in convergence
                if abs(basin_diff) < allowed_tolerance:
                    basins_converged.append(True)
                else:
                    # Adjust tabulated_rating_curve(s). Divide the total adjustment equally
                    # over multiple rating curves.Also limit the adjustment to a maximum
                    # adjustment.
                    adjustment = -basin_diff / float(len(trc_group))
                    adjustment = np.sign(adjustment) * min(max_adjustment, abs(adjustment))
                    for row in trc_group.itertuples():
                        # Get index of first and last point in tabulated rating curve
                        idx_firstpoint = df_trc_static.loc[df_trc_static.node_id == row.to_node_id, "level"].index[0]
                        idx_lastpoint = df_trc_static.loc[df_trc_static.node_id == row.to_node_id, "level"].index[-1]

                        # Enforce a positive tabulated rating curve
                        trc_adjust = adjustment
                        if (df_trc_static.at[idx_lastpoint, "level"] + trc_adjust) < df_trc_static.at[
                            idx_firstpoint, "level"
                        ]:
                            trc_adjust = (
                                df_trc_static.at[idx_lastpoint, "level"]
                                - df_trc_static.at[idx_firstpoint, "level"]
                                - 1e-2
                            )

                        # Adjust last point of tabulated rating curve
                        df_trc_static.at[idx_lastpoint, "level"] += trc_adjust

                        # Save debug data
                        debug_data["iteration"].append(iteration)
                        debug_data["basin_nodeid"].append(basin_node)
                        debug_data["trc_nodeid"].append(row.to_node_id)
                        debug_data["basin_diff"].append(basin_diff)
                        debug_data["trc_adjustment"].append(trc_adjust)
                    basins_converged.append(False)

            converged = all(basins_converged)

            # Store the tabulated rating curve in the dictionary
            stored_trc[str(iteration)] = ribasim_model.tabulated_rating_curve.static.df
            if iteration == max_iter or converged:
                pbar.update(max_iter - pbar.n)
                break
            else:
                # Do new calculation
                ribasim_param.write_ribasim_model_Zdrive(ribasim_model, path_ribasim_toml)
                tqdm_subprocess(cmd, print_other=False, leave=False, suffix=str(iteration).rjust(2))
                pbar.update(1)


def validate_basin_area(model, threshold_area=45000):
    """
    Validate the area of basins in the model.

    :param model: The ribasim model to validate
    :param threshold_area: The area threshold for validation
    :return: None
    """
    too_small_basins = []
    error = False
    for index, row in model.basin.node.df.iterrows():
        basin_id = int(row["meta_node_id"])
        basin_geometry = model.basin.area.df.loc[model.basin.area.df["meta_node_id"] == basin_id, "geometry"]
        if not basin_geometry.empty:
            basin_area = basin_geometry.iloc[0].area
            if basin_area < threshold_area:
                error = True
                print(f"Basin with Node ID {basin_id} has an area smaller than {threshold_area} m²: {basin_area} m²")
                too_small_basins.append(basin_id)
    if not error:
        print(f"All basins are larger than {threshold_area} m²")

    return


def validate_manning_basins(model):
    manning_nodes = model.manning_resistance.node.df.reset_index()[["node_id"]]
    basins_downstream_manning_nodes = model.link.df.loc[
        model.link.df.from_node_id.isin(manning_nodes.to_numpy().flatten())
    ][["from_node_id", "to_node_id"]].copy()  # select the basins downstream of the manning_nodes
    manning_nodes = manning_nodes.merge(
        right=basins_downstream_manning_nodes,
        left_on="node_id",
        right_on="from_node_id",
        how="left",
        suffixes=("", "_y"),
    )  # merge to the manning_nodes df
    manning_nodes = manning_nodes.rename(columns={"to_node_id": "downstream_basin"})
    manning_nodes = manning_nodes.merge(
        right=model.basin.area.df[["node_id", "meta_streefpeil"]],
        left_on="downstream_basin",
        right_on="node_id",
        how="left",
        suffixes=("", "_y"),
    )  # add the streefpeilen
    manning_nodes = manning_nodes.rename(columns={"meta_streefpeil": "downstream_streefpeil"})
    manning_nodes = manning_nodes[["node_id", "downstream_basin", "downstream_streefpeil"]]

    # repeat for the upstream basins of each manning node
    basins_upstream_manning_nodes = model.link.df.loc[
        model.link.df.to_node_id.isin(manning_nodes.to_numpy().flatten())
    ][["from_node_id", "to_node_id"]].copy()  # select the basins downstream of the manning_nodes

    manning_nodes = manning_nodes.merge(
        right=basins_upstream_manning_nodes, left_on="node_id", right_on="to_node_id", how="left", suffixes=("", "_y")
    )  # merge to the manning_nodes df

    manning_nodes = manning_nodes.rename(columns={"from_node_id": "upstream_basin"})
    manning_nodes = manning_nodes.merge(
        right=model.basin.area.df[["node_id", "meta_streefpeil"]],
        left_on="upstream_basin",
        right_on="node_id",
        how="left",
        suffixes=("", "_y"),
    )  # add the streefpeilen
    manning_nodes = manning_nodes.rename(columns={"meta_streefpeil": "upstream_streefpeil"})
    manning_nodes = manning_nodes.drop(columns=["to_node_id", "node_id_y"])

    # round streefpeilen
    for col in ["downstream_streefpeil", "upstream_streefpeil"]:
        manning_nodes[col] = pd.to_numeric(manning_nodes[col], errors="coerce").round(2)

    if not manning_nodes.empty:
        print("Warning! The streefpeilen on both sides of following Manning Nodes are not equal!")
        print(manning_nodes.loc[manning_nodes.downstream_streefpeil != manning_nodes.upstream_streefpeil])


def identify_node_meta_categorie(ribasim_model: ribasim.Model, **kwargs):
    """
    Identify the meta_categorie of each Outlet, Pump and LevelBoundary.

    It checks whether they are inlaten en uitlaten from a boezem, buitenwater or just regular peilgebieden.
    This will determine the rules of the control nodes.
    """
    # # optional arguments
    # aanvoer_enabled: bool = kwargs.get("aanvoer_enabled", True)

    # create new columsn to store the meta categorie of each node
    ribasim_model.outlet.static.df["meta_categorie"] = np.nan
    ribasim_model.pump.static.df["meta_categorie"] = np.nan

    # select all basins which are not "bergend"
    basin_nodes = ribasim_model.basin.state.df.copy()
    # peilgebied_basins = basin_nodes.loc[basin_nodes.meta_categorie == "doorgaand", "node_id"]
    boezem_basins = basin_nodes.loc[basin_nodes.meta_categorie == "hoofdwater", "node_id"]

    # select the nodes which originate from a boezem, and the ones which go to a boezem. Use the link table for this.
    nodes_from_boezem = ribasim_model.link.df.loc[ribasim_model.link.df.from_node_id.isin(boezem_basins), "to_node_id"]
    nodes_to_boezem = ribasim_model.link.df.loc[ribasim_model.link.df.to_node_id.isin(boezem_basins), "from_node_id"]

    # select the nodes which originate from, and go to a boundary
    nodes_from_boundary = ribasim_model.link.df.loc[
        ribasim_model.link.df.meta_from_node_type == "LevelBoundary", "to_node_id"
    ]
    nodes_to_boundary = ribasim_model.link.df.loc[
        ribasim_model.link.df.meta_to_node_type == "LevelBoundary", "from_node_id"
    ]

    # some pumps do not have a function yet, as they may have been changed due to the feedback forms. Set it to afvoer.
    # Check for rows where all three specified columns are NaN and set 'meta_func_afvoer' to 1
    ribasim_model.pump.static.df.loc[
        ribasim_model.pump.static.df[["meta_func_afvoer", "meta_func_aanvoer", "meta_func_circulatie"]]
        .isna()
        .all(axis=1),
        "meta_func_afvoer",
    ] = 1

    # fill in the nan values
    ribasim_model.pump.static.df.fillna({"meta_func_afvoer": 0}, inplace=True)
    ribasim_model.pump.static.df.fillna({"meta_func_aanvoer": 0}, inplace=True)
    ribasim_model.pump.static.df.fillna({"meta_func_circulatie": 0}, inplace=True)

    # Convert the column to string type
    ribasim_model.outlet.static.df["meta_categorie"] = ribasim_model.outlet.static.df["meta_categorie"].astype("string")
    ribasim_model.pump.static.df["meta_categorie"] = ribasim_model.outlet.static.df["meta_categorie"].astype("string")

    # Identify the INlaten from the boezem, both stuwen (outlets) and gemalen (pumps)
    inlet_outlet_nodes = ribasim_model.outlet.static.df.node_id.isin(nodes_from_boezem)
    inlet_pump_nodes_afvoer = ribasim_model.pump.static.df.node_id.isin(nodes_from_boezem) & (
        ribasim_model.pump.static.df.meta_func_aanvoer == 0
    )
    inlet_pump_nodes_aanvoer = ribasim_model.pump.static.df.node_id.isin(nodes_from_boezem) & (
        ribasim_model.pump.static.df.meta_func_aanvoer != 0
    )

    ribasim_model.outlet.static.df.loc[inlet_outlet_nodes, "meta_categorie"] = "Inlaat boezem, stuw"
    ribasim_model.pump.static.df.loc[inlet_pump_nodes_afvoer, "meta_categorie"] = "Inlaat boezem, afvoer gemaal"
    ribasim_model.pump.static.df.loc[inlet_pump_nodes_aanvoer, "meta_categorie"] = "Inlaat boezem, aanvoer gemaal"

    # Identify the UITlaten from the boezem, both stuwen (outlets) and gemalen (pumps)
    outlet_outlet_nodes = ribasim_model.outlet.static.df.node_id.isin(nodes_to_boezem)
    outlet_pump_nodes_afvoer = ribasim_model.pump.static.df.node_id.isin(nodes_to_boezem) & (
        ribasim_model.pump.static.df.meta_func_aanvoer == 0
    )
    outlet_pump_nodes_aanvoer = ribasim_model.pump.static.df.node_id.isin(nodes_to_boezem) & (
        ribasim_model.pump.static.df.meta_func_aanvoer != 0
    )

    ribasim_model.outlet.static.df.loc[outlet_outlet_nodes, "meta_categorie"] = "Uitlaat boezem, stuw"
    ribasim_model.pump.static.df.loc[outlet_pump_nodes_afvoer, "meta_categorie"] = "Uitlaat boezem, afvoer gemaal"
    ribasim_model.pump.static.df.loc[outlet_pump_nodes_aanvoer, "meta_categorie"] = "Uitlaat boezem, aanvoer gemaal"

    # identify the outlets and pumps at the regular peilgebieden
    ribasim_model.outlet.static.df.loc[
        ~(
            (ribasim_model.outlet.static.df.node_id.isin(nodes_from_boezem))
            | (ribasim_model.outlet.static.df.node_id.isin(nodes_to_boezem))
        ),
        "meta_categorie",
    ] = "Reguliere stuw"

    ribasim_model.pump.static.df.loc[
        ~(
            (ribasim_model.pump.static.df.node_id.isin(nodes_from_boezem))
            | (ribasim_model.pump.static.df.node_id.isin(nodes_to_boezem))
        ),
        "meta_categorie",
    ] = "Regulier gemaal"  # differentiate between afvoer and aanvoer below

    # differentiate between reguliere afvoer and regulieren aanvoer gemalen
    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.meta_categorie == "Regulier gemaal")
        & (ribasim_model.pump.static.df.meta_func_aanvoer == 0),
        "meta_categorie",
    ] = "Regulier afvoer gemaal"

    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.meta_categorie == "Regulier gemaal")
        & (ribasim_model.pump.static.df.meta_func_aanvoer != 0),
        "meta_categorie",
    ] = "Regulier aanvoer gemaal"

    # repeat for the boundary nodes
    # identify the buitenwater uitlaten and inlaten. A part will be overwritten later, if its a boundary & boezem.
    ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.node_id.isin(nodes_to_boundary), "meta_categorie"
    ] = "Uitlaat buitenwater peilgebied, stuw"
    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_to_boundary))
        & (ribasim_model.pump.static.df.meta_func_aanvoer == 0),
        "meta_categorie",
    ] = "Uitlaat buitenwater peilgebied, afvoer gemaal"
    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_to_boundary))
        & (ribasim_model.pump.static.df.meta_func_aanvoer != 0),
        "meta_categorie",
    ] = "Uitlaat buitenwater peilgebied, aanvoer gemaal"

    ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.node_id.isin(nodes_from_boundary), "meta_categorie"
    ] = "Inlaat buitenwater peilgebied, stuw"
    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_from_boundary))
        & (ribasim_model.pump.static.df.meta_func_aanvoer == 0),
        "meta_categorie",
    ] = "Inlaat buitenwater peilgebied, afvoer gemaal"

    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_from_boundary))
        & (ribasim_model.pump.static.df.meta_func_aanvoer != 0),
        "meta_categorie",
    ] = "Inlaat buitenwater peilgebied, aanvoer gemaal"

    # boundary & boezem. This is the part where a portion of the already defined meta_categorie will be overwritten by the code above.
    ribasim_model.outlet.static.df.loc[
        (ribasim_model.outlet.static.df.node_id.isin(nodes_to_boundary))
        & (ribasim_model.outlet.static.df.node_id.isin(nodes_from_boezem)),  # to
        "meta_categorie",
    ] = "Uitlaat buitenwater boezem, stuw"

    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_to_boundary))
        & (ribasim_model.pump.static.df.node_id.isin(nodes_from_boezem))
        & (ribasim_model.pump.static.df.meta_func_aanvoer == 0),  # to
        "meta_categorie",
    ] = "Uitlaat buitenwater boezem, afvoer gemaal"

    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_to_boundary))
        & (ribasim_model.pump.static.df.node_id.isin(nodes_from_boezem))
        & (ribasim_model.pump.static.df.meta_func_aanvoer != 0),  # to
        "meta_categorie",
    ] = "Uitlaat buitenwater boezem, aanvoer gemaal"

    ribasim_model.outlet.static.df.loc[
        (ribasim_model.outlet.static.df.node_id.isin(nodes_from_boundary))
        & (ribasim_model.outlet.static.df.node_id.isin(nodes_to_boezem)),  # from
        "meta_categorie",
    ] = "Inlaat buitenwater boezem, stuw"
    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_from_boundary))
        & (ribasim_model.pump.static.df.node_id.isin(nodes_to_boezem))
        & (ribasim_model.pump.static.df.meta_func_aanvoer == 0),  # from
        "meta_categorie",
    ] = "Inlaat buitenwater boezem, afvoer gemaal"

    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_from_boundary))
        & (ribasim_model.pump.static.df.node_id.isin(nodes_to_boezem))
        & (ribasim_model.pump.static.df.meta_func_aanvoer != 0),  # from
        "meta_categorie",
    ] = "Inlaat buitenwater boezem, aanvoer gemaal"

    # boezem & boezem.
    ribasim_model.outlet.static.df.loc[
        (ribasim_model.outlet.static.df.node_id.isin(nodes_from_boezem))
        & (ribasim_model.outlet.static.df.node_id.isin(nodes_to_boezem)),
        "meta_categorie",
    ] = "Boezem boezem, stuw"

    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_from_boezem))
        & (ribasim_model.pump.static.df.node_id.isin(nodes_to_boezem))
        & (ribasim_model.pump.static.df.meta_func_aanvoer == 0),
        "meta_categorie",
    ] = "Boezem boezem, afvoer gemaal"

    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_from_boezem))
        & (ribasim_model.pump.static.df.node_id.isin(nodes_to_boezem))
        & (ribasim_model.pump.static.df.meta_func_aanvoer != 0),
        "meta_categorie",
    ] = "Boezem boezem, aanvoer gemaal"

    # some pumps have been added due to the feedback form. Assume all these nodes are afvoer gemalen
    ribasim_model.pump.static.df.fillna({"meta_func_afvoer": 1}, inplace=True)
    ribasim_model.pump.static.df.fillna({"meta_func_aanvoer": 0}, inplace=True)
    ribasim_model.pump.static.df.fillna({"meta_func_circulatie": 0}, inplace=True)


def set_aanvoer_flags(
    ribasim_model: str | ribasim.Model,
    aanvoer_regions: str | gpd.GeoDataFrame,
    processor: RibasimFeedbackProcessor = None,
    **kwargs,
) -> ribasim.Model:
    """
    Set the 'aanvoer'-flags for both basins and outlets, grouping the whole pipeline in a single method.

    :param ribasim_model: Ribasim model, or file/path to a Ribasim model
    :param aanvoer_regions: geometry data of 'aanvoergebieden', or file/path to this geometry data
    :param processor: Ribasim feedback processor object, defaults to None

    :key aanvoer_enabled: 'aanvoer'-settings are enabled, defaults to True
    :key basin_aanvoer_on: basin node-IDs to manually set 'aanvoer' to True, defaults to None
    :key basin_aanvoer_off: basin node-IDs to manually set 'aanvoer' to False, defaults to None
    :key outlet_aanvoer_on: outlet node-IDs to manually set 'aanvoer' to True, defaults to None
    :key outlet_aanvoer_off: outlet node-IDs to manually set 'aanvoer' to False, defaults to None
    :key overruling_enabled: in case a basin can be supplied directly from the 'hoofdwatersysteem', other supply-routes
        are "overruled", i.e., removed, defaults to True

    :type ribasim_model: str, ribasim.Model
    :type aanvoer_regions: str, geopandas.GeoDataFrame
    :type processor: RibasimFeedbackProcessor, optional
    :type aanvoer_enabled: bool, optional
    :type basin_aanvoer_on: tuple, optional
    :type basin_aanvoer_off: tuple, optional
    :type outlet_aanvoer_on: tuple, optional
    :type outlet_aanvoer_off: tuple, optional
    :type overruling_enabled: bool, optional
    """
    # manual 'aanvoer'-flagging
    _aanvoer_keys = "basin_aanvoer_on", "basin_aanvoer_off", "outlet_aanvoer_on", "outlet_aanvoer_off"
    for k, v in kwargs.items():
        if k in _aanvoer_keys and isinstance(v, int):
            kwargs[k] = (v,)  # added flexibility of optional input

    # optional arguments
    aanvoer_enabled: bool = kwargs.get("aanvoer_enabled", True)
    basin_aanvoer_on: tuple = kwargs.get("basin_aanvoer_on", ())
    basin_aanvoer_off: tuple = kwargs.get("basin_aanvoer_off", ())
    outlet_aanvoer_on: tuple = kwargs.get("outlet_aanvoer_on", ())
    outlet_aanvoer_off: tuple = kwargs.get("outlet_aanvoer_off", ())
    overruling_enabled: bool = kwargs.get("overruling_enabled", True)
    load_geometry_kw: dict = kwargs.get("load_geometry_kw", {})

    # skip 'aanvoer'-flagging
    if not aanvoer_enabled:
        logging.info("Aanvoer-flagging skipped.")
        ribasim_model.basin.area.df["meta_aanvoer"] = False
        ribasim_model.outlet.static.df["meta_aanvoer"] = False
        return ribasim_model

    # all is 'aanvoergebied'
    if aanvoer_regions is None:
        logging.warning(
            f'With aanvoer_regions={aanvoer_regions}, the whole region is considered an "aanvoergebied". '
            f"This is a temporary catch and will be deprecated in the future: "
            f'Make sure that all water boards have a geometry-file from which the "aanvoergebieden" can be deduced.'
        )
        aanvoer_regions = ribasim_model.basin.area.df.reset_index()

    # include 'aanvoer'-settings from feedback form
    if processor is not None:
        basin_aanvoer_on = set(basin_aanvoer_on) | set(processor.basin_aanvoer_on)
        basin_aanvoer_off = set(basin_aanvoer_off) | set(processor.basin_aanvoer_off)
        outlet_aanvoer_on = set(outlet_aanvoer_on) | set(processor.outlet_aanvoer_on)
        outlet_aanvoer_off = set(outlet_aanvoer_off) | set(processor.outlet_aanvoer_off)

    # label basins as 'aanvoergebied'
    sb = supply.SupplyBasin(ribasim_model, aanvoer_regions, **load_geometry_kw)
    sb.exec()

    if basin_aanvoer_on:
        sb.set_aanvoer_on(*basin_aanvoer_on)

    if basin_aanvoer_off:
        sb.set_aanvoer_off(*basin_aanvoer_off)

    # label outlets as 'aanvoerkunstwerk'
    so = supply.SupplyOutlet(sb.model)
    so.exec(overruling_enabled=overruling_enabled)

    if outlet_aanvoer_on:
        so.set_aanvoer_on(*outlet_aanvoer_on)

    if outlet_aanvoer_off:
        so.set_aanvoer_off(*outlet_aanvoer_off)

    # reset ribasim model
    ribasim_model = so.model
    return ribasim_model


def load_model_settings(file_path):
    script_path = Path(__file__)  # Get the path to the current python file
    file_path = script_path.parent.parent / "Parametrize" / file_path  # Correct the path to the JSON file

    with open(file_path) as file:
        settings = json.load(file)
    return settings


def determine_min_upstream_max_downstream_levels(ribasim_model: ribasim.Model, waterschap: str, **kwargs) -> None:
    # optional arguments
    aanvoer_upstream_offset: float = kwargs.get("aanvoer_upstream_offset", 0.04)
    aanvoer_downstream_offset: float = kwargs.get("aanvoer_downstream_offset", 0)
    afvoer_upstream_offset: float = kwargs.get("afvoer_upstream_offset", 0)
    afvoer_downstream_offset: float = kwargs.get("afvoer_downstream_offset", 0.04)
    max_flow_rate: float = kwargs.get("default_max_flow_rate", 20)

    # read sturing, if available
    parametrization_path = Path(__file__)  # path to current script
    sturing_location = (
        parametrization_path.parent.parent / "Parametrize" / f"sturing_{waterschap}.json"
    )  # path to the sturing
    try:
        sturing = load_model_settings(sturing_location)  # load the waterschap specific sturing
    except FileNotFoundError:
        sturing = None

    # create empty columns for the sturing
    ribasim_model.outlet.static.df["min_upstream_level"] = np.nan
    ribasim_model.outlet.static.df["max_downstream_level"] = np.nan
    ribasim_model.outlet.static.df["max_flow_rate"] = np.nan
    ribasim_model.outlet.static.df["flow_rate"] = np.nan

    ribasim_model.pump.static.df["min_upstream_level"] = np.nan
    ribasim_model.pump.static.df["max_downstream_level"] = np.nan

    # make a temp copy to reduce line length, place it later again in the model
    outlet = ribasim_model.outlet.static.df.copy()
    pump = ribasim_model.pump.static.df.copy()

    # create 'sturing'-dictionary, if none is provided
    if sturing is None:
        keys = outlet["meta_categorie"].unique().tolist() + pump["meta_categorie"].unique().tolist()
        values = {
            "upstream_level_offset": afvoer_upstream_offset,
            "downstream_level_offset": afvoer_downstream_offset,
            "max_flow_rate": max_flow_rate,
        }
        sturing = dict.fromkeys(keys, values)

    # check 'aanvoer'-flagging of outlets
    if "meta_aanvoer" not in outlet.columns:
        msg = 'Outlets are missing the "aanvoer"-flag. Please execute the `supply`-based preparations first.'
        raise KeyError(msg)

    # for each different outlet and pump type, determine the min and max upstream and downstream level
    for types, sturing_settings in sturing.items():
        # Extract values for each setting
        upstream_level_offset = sturing_settings["upstream_level_offset"]
        downstream_level_offset = sturing_settings["downstream_level_offset"]
        max_flow_rate = sturing_settings["max_flow_rate"]

        # Update the min_upstream_level and max_downstream_level in the OUTLET dataframe
        outlet.loc[(outlet.meta_categorie == types) & (~outlet["meta_aanvoer"]), "min_upstream_level"] = (
            outlet.meta_from_level - upstream_level_offset
        )
        outlet.loc[(outlet.meta_categorie == types) & (outlet["meta_aanvoer"]), "min_upstream_level"] = (
            outlet.meta_from_level - aanvoer_upstream_offset
        )
        outlet.loc[(outlet.meta_categorie == types) & (~outlet["meta_aanvoer"]), "max_downstream_level"] = (
            outlet.meta_to_level + downstream_level_offset
        )
        outlet.loc[(outlet.meta_categorie == types) & (outlet["meta_aanvoer"]), "max_downstream_level"] = (
            outlet.meta_to_level + aanvoer_downstream_offset
        )
        outlet.loc[outlet.meta_categorie == types, "flow_rate"] = max_flow_rate

        # Update the min_upstream_level and max_downstream_level in the PUMP dataframe. can be done within the same loop, as the meta_categorie is different for the outlet and pump
        pump.loc[pump.meta_categorie == types, "min_upstream_level"] = pump.meta_from_level - upstream_level_offset
        pump.loc[pump.meta_categorie == types, "max_downstream_level"] = pump.meta_to_level + downstream_level_offset
        pump.loc[pump.meta_categorie == types, "flow_rate"] = max_flow_rate

    # raise warning if there are np.nan in the columns
    def check_for_nans_in_columns(df: pd.DataFrame, outlet_or_pump: str, columns_to_check: list = None) -> None:
        columns_to_check = columns_to_check or ["min_upstream_level", "max_downstream_level", "flow_rate"]
        assert outlet_or_pump in ("outlet", "pump")

        if df[columns_to_check].isnull().values.any():
            warnings.warn(
                f"Warning: NaN values found in the following columns of the {outlet_or_pump} dataframe: "
                f"{', '.join([col for col in columns_to_check if df[col].isnull().any()])}"
            )

    check_for_nans_in_columns(outlet, "outlet")
    check_for_nans_in_columns(pump, "pump")

    if pump["flow_rate"].isna().any():
        print("Warning! Some pumps do not have a flow rate yet. Dummy value of 0.1234 m3/s has been taken.")
        pump.fillna({"flow_rate": 0.1234}, inplace=True)

    # place the df's back in the ribasim_model
    ribasim_model.outlet.static.df = outlet
    ribasim_model.pump.static.df = pump


def set_dynamic_min_upstream_max_downstream(ribasim_model: ribasim.Model) -> None:
    """Set the upstream/downstream bounding levels to `None` if they are based on dynamic `LevelBoundary`-nodes.

    With dynamic `LevelBoundary`-nodes, the `min_upstream_level` and `max_downstream_level` of both `Outlet`- and
    `Pump`-nodes are no longer valid, as they are static while the `LevelBoundary`-nodes are dynamic. To remove this
    constrain, the values of `min_upstream_level` and `max_downstream_level` that are based on `LevelBoundary`-nodes are
    set to `None` to prevent incorrect flows to and from `LevelBoundary`-nodes.

    :param ribasim_model: ribasim model
    :type ribasim_model: ribasim.Model
    """
    level_boundary_node_ids = ribasim_model.level_boundary.node.df["meta_node_id"].values

    for structure in ("outlet", "pump"):
        data = getattr(ribasim_model, structure)
        df = data.static.df
        df.loc[df["meta_from_node_id"].isin(level_boundary_node_ids), "min_upstream_level"] = None
        df.loc[df["meta_to_node_id"].isin(level_boundary_node_ids), "max_downstream_level"] = None
        setattr(ribasim_model, structure, data)


def add_discrete_control(ribasim_model, waterschap, default_level):
    """Add discrete control nodes to the network. The rules are based on the meta_categorie of each node."""
    # load in the sturing which is defined in the json files
    sturing = load_model_settings(f"sturing_{waterschap}.json")

    # Remove all Discrete Control nodes and links if its present
    ribasim_model.discrete_control.node.df = ribasim_model.discrete_control.node.df.iloc[0:0]
    if ribasim_model.discrete_control.condition.df is not None:
        ribasim_model.discrete_control.condition.df = ribasim_model.discrete_control.condition.df.iloc[0:0]
        ribasim_model.discrete_control.logic.df = ribasim_model.discrete_control.logic.df.iloc[0:0]
        ribasim_model.discrete_control.variable.df = ribasim_model.discrete_control.variable.df.iloc[0:0]
        ribasim_model.link.df = ribasim_model.link.df.loc[ribasim_model.link.df.link_type != "control"]

    # start assigning sturing to outlets/weirs
    # find the nodes to change
    inlaat_boezem_stuw = ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.meta_categorie == "Inlaat boezem, stuw", "node_id"
    ]
    uitlaat_boezem_stuw = ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.meta_categorie == "Uitlaat boezem, stuw", "node_id"
    ]
    reguliere_stuw = ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.meta_categorie == "Reguliere stuw", "node_id"
    ]
    inlaat_buitenwater_peilgebied_stuw = ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.meta_categorie == "Inlaat buitenwater peilgebied, stuw", "node_id"
    ]
    uitlaat_buitenwater_peilgebied_stuw = ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.meta_categorie == "Uitlaat buitenwater peilgebied, stuw", "node_id"
    ]
    boezem_boezem_stuw = ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.meta_categorie == "Boezem boezem, stuw", "node_id"
    ]

    # assign the sturing for the weirs/outlets.
    nodes_to_control_list_stuw = [
        inlaat_boezem_stuw,
        uitlaat_boezem_stuw,
        reguliere_stuw,
        inlaat_buitenwater_peilgebied_stuw,
        uitlaat_buitenwater_peilgebied_stuw,
        boezem_boezem_stuw,
    ]

    category_list_stuw = [
        "Inlaat boezem, stuw",
        "Uitlaat boezem, stuw",
        "Reguliere stuw",
        "Inlaat buitenwater peilgebied, stuw",
        "Uitlaat buitenwater peilgebied, stuw",
        "Boezem boezem, stuw",
    ]

    # fill the discrete control. Do this table by tables, where the condition table is determined by the meta_categorie
    for nodes_to_control, category in zip(nodes_to_control_list_stuw, category_list_stuw):
        if len(nodes_to_control) > 0:
            print(f"Sturing has been added for the category {category}")
            add_discrete_control_partswise(
                ribasim_model=ribasim_model,
                nodes_to_control=nodes_to_control,
                category=category,
                sturing=sturing,
                default_level=default_level,
            )
        else:
            print(f"No stuwen are found in the category of {category}")

    # repeat for the pumps
    # find the nodes to change
    inlaat_boezem_gemaal = ribasim_model.pump.static.df.loc[
        ribasim_model.pump.static.df.meta_categorie == "Inlaat boezem, gemaal", "node_id"
    ]
    uitlaat_boezem_gemaal = ribasim_model.pump.static.df.loc[
        ribasim_model.pump.static.df.meta_categorie == "Uitlaat boezem, gemaal", "node_id"
    ]

    regulier_gemaal_afvoer = ribasim_model.pump.static.df.loc[
        (
            (ribasim_model.pump.static.df.meta_categorie == "Regulier gemaal")
            & (ribasim_model.pump.static.df.meta_func_afvoer != 0)
        ),
        "node_id",
    ]
    regulier_gemaal_aanvoer = ribasim_model.pump.static.df.loc[
        (
            (ribasim_model.pump.static.df.meta_categorie == "Regulier gemaal")
            & (ribasim_model.pump.static.df.meta_func_afvoer != 1)
        ),
        "node_id",
    ]

    uitlaat_buitenwater_peilgebied_gemaal_afvoer = ribasim_model.pump.static.df.loc[
        (
            (ribasim_model.pump.static.df.meta_categorie == "Uitlaat buitenwater peilgebied, gemaal")
            & (ribasim_model.pump.static.df.meta_func_afvoer != 0)
        ),
        "node_id",
    ]
    uitlaat_buitenwater_peilgebied_gemaal_aanvoer = ribasim_model.pump.static.df.loc[
        (
            (ribasim_model.pump.static.df.meta_categorie == "Uitlaat buitenwater peilgebied, gemaal")
            & (ribasim_model.pump.static.df.meta_func_afvoer != 1)
        ),
        "node_id",
    ]

    inlaat_buitenwater_peilgebied_gemaal_afvoer = ribasim_model.pump.static.df.loc[
        (
            (ribasim_model.pump.static.df.meta_categorie == "Inlaat buitenwater peilgebied, gemaal")
            & (ribasim_model.pump.static.df.meta_func_afvoer != 0)
        ),
        "node_id",
    ]
    inlaat_buitenwater_peilgebied_gemaal_aanvoer = ribasim_model.pump.static.df.loc[
        (
            (ribasim_model.pump.static.df.meta_categorie == "Inlaat buitenwater peilgebied, gemaal")
            & (ribasim_model.pump.static.df.meta_func_afvoer != 1)
        ),
        "node_id",
    ]

    boezem_boezem_gemaal_afvoer = ribasim_model.outlet.static.df.loc[
        (
            (ribasim_model.outlet.static.df.meta_categorie == "Boezem boezem, gemaal")
            & (ribasim_model.pump.static.df.meta_func_afvoer != 0)
        ),
        "node_id",
    ]
    boezem_boezem_gemaal_aanvoer = ribasim_model.outlet.static.df.loc[
        (
            (ribasim_model.outlet.static.df.meta_categorie == "Boezem boezem, gemaal")
            & (ribasim_model.pump.static.df.meta_func_afvoer != 1)
        ),
        "node_id",
    ]

    # assign the sturing for the gemalen/pumps.
    nodes_to_control_list_gemaal = [
        inlaat_boezem_gemaal,
        uitlaat_boezem_gemaal,
        regulier_gemaal_afvoer,
        regulier_gemaal_aanvoer,
        uitlaat_buitenwater_peilgebied_gemaal_afvoer,
        uitlaat_buitenwater_peilgebied_gemaal_aanvoer,
        inlaat_buitenwater_peilgebied_gemaal_afvoer,  #
        inlaat_buitenwater_peilgebied_gemaal_aanvoer,  #
        boezem_boezem_gemaal_afvoer,
        boezem_boezem_gemaal_aanvoer,
    ]

    category_list_gemaal = [
        "Inlaat boezem, gemaal",
        "Uitlaat boezem, gemaal",
        "Regulier afvoer gemaal",
        "Regulier aanvoer gemaal",
        "Uitlaat buitenwater peilgebied, afvoer gemaal",
        "Uitlaat buitenwater peilgebied, aanvoer gemaal",
        "Inlaat buitenwater peilgebied, afvoer gemaal",  #
        "Inlaat buitenwater peilgebied, aanvoer gemaal",  #
        "Boezem boezem, afvoer gemaal",
        "Boezem boezem, aanvoer gemaal",
    ]

    # fill the discrete control. Do this table by tables, where the condition table is determined by the meta_categorie
    for nodes_to_control, category in zip(nodes_to_control_list_gemaal, category_list_gemaal):
        if len(nodes_to_control) > 0:
            print(f"Sturing has been added for the category {category}")
            add_discrete_control_partswise(
                ribasim_model=ribasim_model,
                nodes_to_control=nodes_to_control,
                category=category,
                sturing=sturing,
                default_level=default_level,
            )
        else:
            print(f"No gemalen are found in the category of {category}")

    # # fill the discrete control. Do this table by tables, where the condition table is determined by the meta_categorie. Start with the outlets/stuwen
    # add_discrete_control_partswise(
    #     ribasim_model=ribasim_model,
    #     nodes_to_control=inlaat_boezem_stuw,
    #     category='Inlaat boezem, stuw',
    #     sturing = sturing,
    #     default_level = default_level)

    # many duplicate values have been created. Discard those.
    # ribasim_model.outlet.static.df = ribasim_model.outlet.static.df.drop_duplicates().reset_index(drop=True)
    # ribasim_model.pump.static.df = ribasim_model.pump.static.df.drop_duplicates().reset_index(drop=True)

    # a DC node occures twice in the table of teh nodes at case of AGV, while this node is not present at all in the DC tables. REmove it
    DC_nodes = pd.concat(
        [
            ribasim_model.discrete_control.logic.df.node_id,
            ribasim_model.discrete_control.variable.df.node_id,
            ribasim_model.discrete_control.condition.df.node_id,
        ]
    )

    DC_nodes = DC_nodes.drop_duplicates().reset_index(drop=True)

    # add meta_downstream to the DC variable
    ribasim_model.discrete_control.variable.df["meta_downstream"] = ribasim_model.discrete_control.variable.df.merge(
        right=ribasim_model.discrete_control.condition.df,
        left_on=["compound_variable_id", "listen_node_id"],
        right_on=["compound_variable_id", "meta_listen_node_id"],
        how="left",
    )[["meta_downstream"]]

    ribasim_model.discrete_control.node.df = ribasim_model.discrete_control.node.df.loc[
        ribasim_model.discrete_control.node.df.node_id.isin(DC_nodes.values)
    ].reset_index(drop=True)
    ribasim_model.discrete_control.node.df = ribasim_model.discrete_control.node.df.drop_duplicates(
        subset="node_id"
    ).reset_index(drop=True)
    ribasim_model.discrete_control.condition.df = (
        ribasim_model.discrete_control.condition.df.drop_duplicates()
        .sort_values(by=["node_id", "meta_downstream"])
        .reset_index(drop=True)
    )
    ribasim_model.discrete_control.variable.df = (
        ribasim_model.discrete_control.variable.df.drop_duplicates()
        .sort_values(by=["node_id", "meta_downstream"])
        .reset_index(drop=True)
    )
    ribasim_model.discrete_control.logic.df = (
        ribasim_model.discrete_control.logic.df.drop_duplicates()
        .sort_values(by=["node_id", "truth_state"])
        .reset_index(drop=True)
    )

    return


def add_discrete_control_partswise(ribasim_model, nodes_to_control, category, sturing, default_level):
    # define the sturing parameters in variables
    upstream_level_offset = sturing[category]["upstream_level_offset"]
    truth_state = sturing[category]["truth_state"]
    control_state = sturing[category]["control_state"]
    flow_rate_block = sturing[category]["flow_rate_block"]
    flow_rate_pass = sturing[category]["flow_rate_pass"]
    node_type = sturing[category]["node_type"]

    ### node ####################################################
    # add the discrete control .node table. The node_ids are the same as the node_id of the outlet/pump, but 80.000 is added
    DC_nodes = pd.DataFrame()
    DC_nodes["node_id"] = nodes_to_control.astype(int) + 80000

    # trace back the node_id which the DiscreteControl controls, including the compoun_variable_id which is set the same as the node_id
    DC_nodes["meta_control_node_id"] = nodes_to_control
    DC_nodes["meta_compound_variable_id"] = DC_nodes["node_id"]
    DC_nodes["node_type"] = "DiscreteControl"
    DC_nodes = DC_nodes.sort_values(by="node_id").reset_index(drop=True)

    # retrieve the geometries of the DiscreteControl. Put it at the same location, so they may be stored in either the Outlets or the Pumps, so check both    if
    if node_type == "outlet" or node_type == "Outlet":
        DC_nodes["geometry"] = DC_nodes.merge(
            right=ribasim_model.outlet.node.df[["node_id", "geometry"]],
            left_on="meta_control_node_id",
            right_on="node_id",
            how="left",
        )["geometry"]
    elif node_type == "pump" or node_type == "Pump":
        DC_nodes["geometry"] = DC_nodes.merge(
            right=ribasim_model.pump.node.df[["node_id", "geometry"]],
            left_on="meta_control_node_id",
            right_on="node_id",
            how="left",
        )["geometry"]

    DC_nodes = DC_nodes[["node_id", "node_type", "meta_control_node_id", "meta_compound_variable_id", "geometry"]]

    # concat the DC_nodes to the ribasim model
    ribasim_model.discrete_control.node.df = (
        pd.concat([ribasim_model.discrete_control.node.df, DC_nodes]).sort_values(by="node_id").reset_index(drop=True)
    )
    ribasim_model.discrete_control.node.df = gpd.GeoDataFrame(
        ribasim_model.discrete_control.node.df, geometry="geometry"
    )

    ### node OUTLET static ###
    if node_type == "outlet" or node_type == "Outlet":
        # df when water is blocked
        outlet_static_block = ribasim_model.outlet.static.df.copy()
        outlet_static_block["control_state"] = "block"
        outlet_static_block["flow_rate"] = flow_rate_block
        outlet_static_block["meta_min_crest_level"] = (
            np.nan
        )  # min crest level is redundant, as control is defined for both upstream as well as downstream levels

        # df when water is passed
        outlet_static_pass = ribasim_model.outlet.static.df.copy()
        outlet_static_pass["control_state"] = "pass"
        outlet_static_pass["flow_rate"] = flow_rate_pass
        outlet_static_pass["meta_min_crest_level"] = (
            np.nan
        )  # min crest level is redundant, as control is defined for both upstream as well as downstream levels

        outlet_static = (
            pd.concat([outlet_static_block, outlet_static_pass])
            .sort_values(by=["node_id", "control_state"])
            .reset_index(drop=True)
        )
        ribasim_model.outlet.static.df = outlet_static

    ### node PUMP static ###
    if node_type == "pump" or node_type == "Pump":
        # df when water is blocked
        pump_static_block = ribasim_model.pump.static.df.copy()
        pump_static_block["control_state"] = "block"
        pump_static_block["flow_rate"] = flow_rate_block

        # df when water is passed
        pump_static_pass = ribasim_model.pump.static.df.copy()
        pump_static_pass["control_state"] = "pass"
        pump_static_pass["flow_rate"] = flow_rate_pass

        pump_static = (
            pd.concat([pump_static_block, pump_static_pass])
            .sort_values(by=["node_id", "control_state"])
            .reset_index(drop=True)
        )
        ribasim_model.pump.static.df = pump_static

    ### condition ####################################################
    # create the DiscreteControl condition table
    DC_condition_us = pd.DataFrame()
    DC_condition_us["node_id"] = ribasim_model.discrete_control.node.df["node_id"]
    DC_condition_us["meta_control_node_id"] = ribasim_model.discrete_control.node.df["meta_control_node_id"]
    DC_condition_us["compound_variable_id"] = ribasim_model.discrete_control.node.df["meta_compound_variable_id"]
    DC_condition_ds = DC_condition_us.copy(deep=True)

    # find the greather_than value by looking the corresponding UPstream basin up in the link table
    basin_to_control_node_us = ribasim_model.link.df.loc[
        ribasim_model.link.df.to_node_id.isin(nodes_to_control.values)
    ]  # ['from_node_id']
    basin_to_control_node_us = basin_to_control_node_us.merge(
        right=ribasim_model.basin.state.df, left_on="from_node_id", right_on="node_id", how="left"
    )[["to_node_id", "to_node_type", "level", "from_node_id", "from_node_type"]]
    basin_to_control_node_us["meta_to_control_node_id"] = basin_to_control_node_us["to_node_id"]
    basin_to_control_node_us["meta_to_control_node_type"] = basin_to_control_node_us["to_node_type"]

    DC_condition_us = DC_condition_us.merge(
        right=basin_to_control_node_us, left_on="meta_control_node_id", right_on="to_node_id"
    )
    DC_condition_us["level"] -= upstream_level_offset

    # formatting
    DC_condition_us.rename(
        columns={
            "level": "greater_than",
            "from_node_id": "meta_listen_node_id",
            "from_node_type": "meta_listen_node_type",
        },
        inplace=True,
    )
    DC_condition_us = DC_condition_us[
        [
            "node_id",
            "compound_variable_id",
            "greater_than",
            "meta_listen_node_id",
            "meta_listen_node_type",
            "meta_to_control_node_id",
            "meta_to_control_node_type",
        ]
    ]
    DC_condition_us["meta_downstream"] = 0  # add a column to sort it later on

    # for each row, there is (incorrectly) another row added where the listen node is the DiscreteControl. This should not be the case. Remove it
    DC_condition_us = DC_condition_us.loc[DC_condition_us.meta_listen_node_type != "DiscreteControl"]

    # the upstream node which is listened to is found. Now, find the downstream listen node.
    # basically repeat the same lines as above
    basin_to_control_node_ds = ribasim_model.link.df.loc[
        ribasim_model.link.df.from_node_id.isin(nodes_to_control.values)
    ]
    basin_to_control_node_ds = basin_to_control_node_ds.merge(
        right=ribasim_model.basin.state.df, left_on="to_node_id", right_on="node_id", how="left"
    )[["from_node_id", "from_node_type", "level", "to_node_id", "to_node_type"]]

    DC_condition_ds = DC_condition_ds.merge(
        right=basin_to_control_node_ds, left_on="meta_control_node_id", right_on="from_node_id"
    )
    DC_condition_ds["level"] -= upstream_level_offset

    # formatting
    DC_condition_ds.rename(
        columns={"level": "greater_than", "to_node_id": "meta_listen_node_id", "to_node_type": "meta_listen_node_type"},
        inplace=True,
    )
    DC_condition_ds = DC_condition_ds[
        ["node_id", "compound_variable_id", "greater_than", "meta_listen_node_id", "meta_listen_node_type"]
    ]
    DC_condition_ds["meta_downstream"] = 1  # add a column to sort it later on

    # add some more columns so the downstream table matches the upstream table. Not sure why this is not created
    DC_condition_ds["meta_to_control_node_id"] = DC_condition_ds.merge(
        right=DC_condition_us, on="compound_variable_id", how="left"
    )["meta_to_control_node_id"]
    DC_condition_ds["meta_to_control_node_type"] = DC_condition_ds.merge(
        right=DC_condition_us, on="compound_variable_id", how="left"
    )["meta_to_control_node_type"]

    # concat the upstream and the downstream condition table
    DC_condition = pd.concat([DC_condition_us, DC_condition_ds])

    # every basin should have a target level by this part of the code. However, LevelBoundaries may not. Implement it
    DC_condition.greater_than.fillna(value=default_level, inplace=True)

    # concat the entire DC_condition to the ribasim model
    ribasim_model.discrete_control.condition.df = pd.concat([ribasim_model.discrete_control.condition.df, DC_condition])
    ribasim_model.discrete_control.condition.df = ribasim_model.discrete_control.condition.df.sort_values(
        by=["node_id", "meta_downstream"]
    ).reset_index(drop=True)

    ### logic ####################################################
    DC_logic = pd.DataFrame()
    for i in range(len(truth_state)):
        DC_logic_temp = DC_condition.copy()[["node_id"]].drop_duplicates()
        DC_logic_temp["truth_state"] = truth_state[i]
        DC_logic_temp["control_state"] = control_state[i]

        DC_logic = pd.concat([DC_logic, DC_logic_temp])

    # concat the DC_condition to the ribasim model
    ribasim_model.discrete_control.logic.df = pd.concat([ribasim_model.discrete_control.logic.df, DC_logic])
    ribasim_model.discrete_control.logic.df = ribasim_model.discrete_control.logic.df.sort_values(
        by=["node_id", "truth_state"]
    ).reset_index(drop=True)

    ### variable ####################################################
    DC_variable = DC_condition.copy()[
        ["node_id", "compound_variable_id", "meta_listen_node_id", "meta_listen_node_type"]
    ]
    DC_variable.rename(
        columns={"meta_listen_node_id": "listen_node_id", "meta_listen_node_type": "listen_node_type"}, inplace=True
    )
    DC_variable["variable"] = "level"

    # concat the DC_variable to the ribasim model
    ribasim_model.discrete_control.variable.df = pd.concat([ribasim_model.discrete_control.variable.df, DC_variable])
    ribasim_model.discrete_control.variable.df = ribasim_model.discrete_control.variable.df.sort_values(
        by=["node_id", "listen_node_id"]
    ).reset_index(drop=True)

    ### link ####################################################
    DC_link = DC_condition.copy()[["node_id", "meta_to_control_node_id", "meta_to_control_node_type"]]

    # as the DC listens to both the upstream as well as the downstream nodes, it contains twice the node_ids. Only select one.
    DC_link = DC_link.drop_duplicates(subset="node_id")
    DC_link.rename(
        columns={
            "node_id": "from_node_id",
            "meta_to_control_node_id": "to_node_id",
            "meta_to_control_node_type": "to_node_type",
        },
        inplace=True,
    )

    # DC_link['to_node_type'] = ribasim_model.link.df.loc[ribasim_model.link.df.to_node_id == DC_link.node_id, 'to_node_type']
    DC_link["from_node_type"] = "DiscreteControl"
    DC_link["link_type"] = "control"
    DC_link["meta_categorie"] = "DC_control"

    # retrieve the FROM geometry from the DC_nodes. The TO is the same, as the DiscreteControl is on the same location
    DC_link["from_coord"] = DC_nodes["geometry"]
    DC_link["to_coord"] = DC_nodes["geometry"]

    def create_linestring(row):
        return LineString([row["from_coord"], row["to_coord"]])

    DC_link["geometry"] = DC_link.apply(create_linestring, axis=1)

    DC_link = DC_link[
        ["from_node_id", "from_node_type", "to_node_id", "to_node_type", "link_type", "meta_categorie", "geometry"]
    ]
    ribasim_model.link.df = pd.concat([ribasim_model.link.df, DC_link]).reset_index(drop=True)


def add_continuous_control_node(
    ribasim_model: ribasim.Model, connection_node: ribasim.geometry.link.NodeData, listen_nodes: list[int], **kwargs
) -> ribasim.Node | None:
    """Add a continuous control node to `Pump`-/`Outlet`-nodes that are for both 'aanvoer' and 'afvoer'.

    :param ribasim_model: Ribasim model
    :param connection_node: `Pump`-/`Outlet`-node with dual 'sturing'
    :param listen_nodes: up- and downstream nodes to "listen to"

    :key dx: spatial distance between `connection_node` and continuous control node (x-direction), defaults to 0
    :key dy: spatial distance between `connection_node` and continuous control node (y-direction), defaults to 0
    :key capacity: capacity of controlled output, defaults to 20
    :key control_variable: variable of controlled output, defaults to 'flow_rate'
    :key listen_targets: target variable values of the `listen_nodes`, defaults to None
    :key listen_variable: variable of listened to input, defaults to 'level'
    :key node_id_raiser: node-ID of `ContinuousControl`-node is based on node-ID of `connection_node` as follows:
        `continuous_control_node.node_id = connection_node.node_id + node_id_raiser`
        defaults to 10000
    :key numerical_tolerance: listened to input at which full capacity is reached, defaults to 0.01
    :key weights: weights of `listen_nodes` to determine control, defaults to [1, -1]

    :type ribasim_model: ribasim.Model
    :type connection_node: ribasim.geometry.link.NodeData
    :type listen_nodes: list[int]
    :type dx: float, optional
    :type dy: float, optional
    :type capacity: float, optional
    :type control_variable: str, optional
    :type listen_targets: list[float], optional
    :type listen_variable: str, optional
    :type node_id_raiser: int, optional
    :type numerical_tolerance: float, optional
    :type weights: list[float], optional

    :raise AssertionError: if `control_variable` is unknown
    :raise AssertionError: if `listen_variable` is unknown
    :raise AssertionError: if lengths of `listen_nodes`, `weights` and `listen_targets` are not 2

    :return: continuous control node (if created)
    :rtype: ribasim.Node, None
    """
    # optional arguments
    dx: float = kwargs.get("dx", 0)
    dy: float = kwargs.get("dy", 0)
    capacity: float = kwargs.get("capacity", 20)
    control_variable: str = kwargs.get("control_variable", "flow_rate")
    listen_targets: list[float] = kwargs.get("listen_targets")
    listen_variable: str = kwargs.get("listen_variable", "level")
    node_id_raiser: int = kwargs.get("node_id_raiser", 10000)
    numerical_tolerance: float = kwargs.get("numerical_tolerance", 0.01)
    weights: list[float] = kwargs.get("weights", [1, -1])

    # input validation
    VARIABLES = "level", "flow_rate"
    assert control_variable in VARIABLES, f"`control_variable` must be in {VARIABLES}, {control_variable} given."
    assert listen_variable in VARIABLES, f"`listen_variable` must be in {VARIABLES}, {listen_variable} given."
    assert len(listen_nodes) == len(weights) == 2, (
        f"Continuous control node requires two `listen_nodes` ({len(listen_nodes)}) and two `weights` ({len(weights)})"
    )

    # get listen targets
    if listen_targets is None:
        try:
            listen_targets = [
                float(
                    ribasim_model.basin.area.df.loc[
                        ribasim_model.basin.area.df["meta_node_id"] == i, "meta_streefpeil"
                    ].values[0]
                )
                for i in listen_nodes
            ]
        except IndexError:
            listen_targets = []
            node_types = [ribasim_model.node_table().df.loc[i, "node_type"] for i in listen_nodes]
            for i, t in zip(listen_nodes, node_types):
                match t.lower():
                    case "basin":
                        value = ribasim_model.basin.area.df.loc[
                            ribasim_model.basin.area.df["meta_node_id"] == i, "meta_streefpeil"
                        ]
                    case "levelboundary":
                        value = ribasim_model.level_boundary.static.df.loc[
                            ribasim_model.level_boundary.static.df["node_id"] == i, "level"
                        ]
                    case _:
                        msg = (
                            f"Unknown node-type ({t.lower()}) for implementation of `ContinuousControl`-node for "
                            f"{connection_node.node_type} #{connection_node.node_id}."
                        )
                        raise NotImplementedError(msg)

                listen_targets.append(float(value.values[0]))

    assert len(listen_targets) == 2, (
        f"Continuous control node requires two `listen_targets` ({len(listen_targets)}) "
        f"corresponding to the number of `listen_nodes` ({len(listen_nodes)})"
    )

    # set ON-switch for continuous control node
    margin = 2 * numerical_tolerance
    on_switch = sum(t * w for t, w in zip(listen_targets, weights))

    # add continuous control
    point = connection_node.geometry
    node_id = connection_node.node_id + node_id_raiser
    continuous_control_node = ribasim_model.continuous_control.add(
        ribasim.Node(node_id=node_id, geometry=shapely.Point(point.x + dx, point.y + dy)),
        [
            continuous_control.Variable(
                listen_node_id=listen_nodes,
                weight=weights,
                variable=listen_variable,
            ),
            continuous_control.Function(
                input=[on_switch - margin, on_switch, on_switch + numerical_tolerance, on_switch + margin],
                output=[0, 0, capacity, capacity],
                controlled_variable=control_variable,
            ),
        ],
    )

    # update connection node
    static_table = getattr(ribasim_model, connection_node.node_type.lower()).static.df
    static_table.loc[static_table["node_id"] == connection_node.node_id, "min_upstream_level"] = np.nan
    static_table.loc[static_table["node_id"] == connection_node.node_id, "max_downstream_level"] = np.nan

    # add control link
    ribasim_model.link.add(continuous_control_node, connection_node)

    # return control node
    return continuous_control_node


def add_continuous_control(ribasim_model: ribasim.Model, **kwargs) -> None:
    # optional arguments
    apply_on_outlets: bool = kwargs.pop("apply_on_outlets", True)
    apply_on_pumps: bool = kwargs.pop("apply_on_pumps", True)

    # collect nodes that are part of the 'hoofdwatersysteem'
    main_water_system_node_ids = ribasim_model.basin.state.df.loc[
        ribasim_model.basin.state.df["meta_categorie"] == "hoofdwater", "node_id"
    ].to_list()
    level_boundaries = ribasim_model.level_boundary.node.df.index.to_list()

    # add continuous control nodes to outlets
    if apply_on_outlets:
        outlet = ribasim_model.outlet.static.df.copy()
        selection = outlet[
            outlet["meta_aanvoer"].astype(bool)  # make sure this is boolean and not int
            & (
                (~outlet["meta_from_node_id"].isin(main_water_system_node_ids))
                | (
                    outlet["meta_from_node_id"].isin(main_water_system_node_ids)
                    & outlet["meta_to_node_id"].isin(main_water_system_node_ids)
                )
            )
            & (~outlet["meta_from_node_id"].isin(level_boundaries) & ~outlet["meta_to_node_id"].isin(level_boundaries))
        ]
        if len(selection) > 0:
            selection.apply(
                lambda r: add_continuous_control_node(
                    ribasim_model,
                    ribasim_model.outlet[r["node_id"]],
                    [r["meta_from_node_id"], r["meta_to_node_id"]],
                    **kwargs,
                ),
                axis=1,
            )

    # add continuous control nodes to pumps
    if apply_on_pumps:
        pump = ribasim_model.pump.static.df.copy()
        selection = pump[
            (pump["meta_func_aanvoer"] == 1)
            & (pump["meta_func_afvoer"] == 1)
            & (~pump["meta_from_node_id"].isin(level_boundaries) & ~pump["meta_to_node_id"].isin(level_boundaries))
        ]
        if len(selection) > 0:
            selection.apply(
                lambda r: add_continuous_control_node(
                    ribasim_model,
                    ribasim_model.pump[r["node_id"]],
                    [r["meta_from_node_id"], r["meta_to_node_id"]],
                    **kwargs,
                ),
                axis=1,
            )

    # add "meta_node_id"-column to continuous control nodes
    ribasim_model.continuous_control.node.df["meta_node_id"] = ribasim_model.continuous_control.node.df.index


def clean_tables(ribasim_model: ribasim.Model, waterschap: str):
    """Only retain node_id's which are present in the .node table."""
    # Basin
    basin_ids = ribasim_model.basin.node.df.loc[
        ribasim_model.basin.node.df.node_type == "Basin", "meta_node_id"
    ].to_numpy()
    ribasim_model.basin.area = ribasim_model.basin.area.df.loc[
        ribasim_model.basin.area.df.node_id.isin(basin_ids)
    ].reset_index(drop=True)
    ribasim_model.basin.profile = ribasim_model.basin.profile.df.loc[
        ribasim_model.basin.profile.df.node_id.isin(basin_ids)
    ].reset_index(drop=True)
    ribasim_model.basin.state = ribasim_model.basin.state.df.loc[
        ribasim_model.basin.state.df.node_id.isin(basin_ids)
    ].reset_index(drop=True)

    if ribasim_model.basin.static.df is not None:
        ribasim_model.basin.static.df = ribasim_model.basin.static.df.loc[
            ribasim_model.basin.static.df.node_id.isin(basin_ids)
        ].reset_index(drop=True)
    else:
        ribasim_model.basin.time.df = ribasim_model.basin.time.df.loc[
            ribasim_model.basin.time.df.node_id.isin(basin_ids)
        ].reset_index(drop=True)

    # Outlet
    outlet_ids = ribasim_model.outlet.node.df.loc[
        ribasim_model.outlet.node.df.node_type == "Outlet", "meta_node_id"
    ].to_numpy()
    ribasim_model.outlet.static = ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.node_id.isin(outlet_ids)
    ].reset_index(drop=True)

    # Pump
    pump_ids = ribasim_model.pump.node.df.loc[ribasim_model.pump.node.df.node_type == "Pump", "meta_node_id"].to_numpy()
    ribasim_model.pump.static = ribasim_model.pump.static.df.loc[
        ribasim_model.pump.static.df.node_id.isin(pump_ids)
    ].reset_index(drop=True)

    # ManningResistance
    manningresistance_ids = ribasim_model.manning_resistance.node.df.loc[
        ribasim_model.manning_resistance.node.df.node_type == "ManningResistance", "meta_node_id"
    ].to_numpy()
    ribasim_model.manning_resistance.static = ribasim_model.manning_resistance.static.df.loc[
        ribasim_model.manning_resistance.static.df.node_id.isin(manningresistance_ids)
    ].reset_index(drop=True)

    # LevelBoundary
    levelboundary_ids = ribasim_model.level_boundary.node.df.loc[
        ribasim_model.level_boundary.node.df.node_type == "LevelBoundary", "meta_node_id"
    ].to_numpy()
    ribasim_model.level_boundary.static = ribasim_model.level_boundary.static.df.loc[
        ribasim_model.level_boundary.static.df.node_id.isin(levelboundary_ids)
    ].reset_index(drop=True)

    # identify empty static tables
    # Basin
    if ribasim_model.basin.static.df is not None:
        basin_static_missing = ribasim_model.basin.node.df.loc[
            ~ribasim_model.basin.node.df.index.isin(ribasim_model.basin.static.df.node_id)
        ]
        if len(basin_static_missing) > 0:
            print(
                "\nFollowing node_id's in the Basin.static table are missing:\n", basin_static_missing.index.to_numpy()
            )

    else:
        basin_time_missing = ribasim_model.basin.node.df.loc[
            ~ribasim_model.basin.node.df.index.isin(ribasim_model.basin.time.df.node_id)
        ]
        if len(basin_time_missing) > 0:
            print("\nFollowing node_id's in the Basin.time table are missing:\n", basin_time_missing.index.to_numpy())

    basin_state_missing = ribasim_model.basin.node.df.loc[
        ~ribasim_model.basin.node.df.index.isin(ribasim_model.basin.state.df.node_id)
    ]  # .index.to_numpy()
    if len(basin_state_missing) > 0:
        print("\nFollowing node_id's in the Basin.state table are missing:\n", basin_state_missing.index.to_numpy())

    basin_profile_missing = ribasim_model.basin.node.df.loc[
        ~ribasim_model.basin.node.df.index.isin(ribasim_model.basin.profile.df.node_id)
    ]  # .index.to_numpy()
    if len(basin_profile_missing) > 0:
        print("\nFollowing node_id's in the Basin.profile table are missing:\n", basin_profile_missing.index.to_numpy())

    basin_area_missing = ribasim_model.basin.node.df.loc[
        ~ribasim_model.basin.node.df.index.isin(ribasim_model.basin.area.df.node_id)
    ]  # .index.to_numpy()
    if len(basin_area_missing) > 0:
        print("\nFollowing node_id's in the Basin.area table are missing:\n", basin_area_missing.index.to_numpy())

    # Outlet
    outlet_missing = ribasim_model.outlet.node.df.loc[
        ~ribasim_model.outlet.node.df.index.isin(ribasim_model.outlet.static.df.node_id)
    ]  # .index.to_numpy()
    if len(outlet_missing) > 0:
        print("\nFollowing node_id's in the Outlet.static table are missing:\n", outlet_missing.index.to_numpy())

    # Pump
    pump_missing = ribasim_model.pump.node.df.loc[
        ~ribasim_model.pump.node.df.index.isin(ribasim_model.pump.static.df.node_id)
    ]  # .index.to_numpy()
    if len(pump_missing) > 0:
        print("\nFollowing node_id's in the pump.static table are missing:\n", pump_missing.index.to_numpy())

    # ManningResistance
    manning_resistance_missing = ribasim_model.manning_resistance.node.df.loc[
        ~ribasim_model.manning_resistance.node.df.index.isin(ribasim_model.manning_resistance.static.df.node_id)
    ]  # .index.to_numpy()
    if len(manning_resistance_missing) > 0:
        print(
            "\nFollowing node_id's in the manning_resistance.static table are missing\n:",
            manning_resistance_missing.index.to_numpy(),
        )

    # LevelBoundary
    level_boundary_missing = ribasim_model.level_boundary.node.df.loc[
        ~ribasim_model.level_boundary.node.df.index.isin(ribasim_model.level_boundary.static.df.node_id)
    ]  # .index.to_numpy()
    if len(level_boundary_missing) > 0:
        print(
            "\nFollowing node_id's in the level_boundary.static table are missing:\n",
            level_boundary_missing.index.to_numpy(),
        )

    # check for duplicated indexes in all the node tables
    # reating individual DataFrames for each node type
    basin_ids_df = pd.DataFrame({"Type": "Basin", "node_id": basin_ids})
    outlet_ids_df = pd.DataFrame({"Type": "Outlet", "node_id": outlet_ids})
    pump_ids_df = pd.DataFrame({"Type": "Pump", "node_id": pump_ids})
    manningresistance_ids_df = pd.DataFrame({"Type": "ManningResistance", "node_id": manningresistance_ids})
    levelboundary_ids_df = pd.DataFrame({"Type": "LevelBoundary", "node_id": levelboundary_ids})

    # Concatenating all DataFrames into one
    combined_df = pd.concat(
        [basin_ids_df, outlet_ids_df, pump_ids_df, manningresistance_ids_df, levelboundary_ids_df], ignore_index=True
    )
    duplicated_ids = combined_df[combined_df.duplicated(subset="node_id", keep=False)]

    if len(duplicated_ids) > 0:
        print("\nThe following node_ids are duplicates: \n", duplicated_ids)

    if ribasim_model.basin.static.df is not None:
        # check for duplicated indexes in the basin static tables
        duplicated_static_basin = ribasim_model.basin.static.df.loc[
            ribasim_model.basin.static.df.duplicated(subset="node_id")
        ]
        if len(duplicated_static_basin) > 0:
            print("\nFollowing indexes are duplicated in the basin.static table:\n", duplicated_static_basin)

    # check for duplicated indexes in the outlet static tables
    duplicated_static_outlet = ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.duplicated(subset="node_id")
    ]
    if len(duplicated_static_outlet) > 0:
        print("\nFollowing indexes are duplicated in the outlet.static table:\n", duplicated_static_outlet)

    # check for duplicated indexes in the pump static tables
    duplicated_static_pump = ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.duplicated(subset="node_id")]
    if len(duplicated_static_pump) > 0:
        print("\nFollowing indexes are duplicated in the pump.static table:\n", duplicated_static_pump)

    # check for duplicated indexes in the manning_resistance static tables
    duplicated_static_manning_resistance = ribasim_model.manning_resistance.static.df.loc[
        ribasim_model.manning_resistance.static.df.duplicated(subset="node_id")
    ]
    if len(duplicated_static_manning_resistance) > 0:
        print(
            "\nFollowing indexes are duplicated in the manning_resistance.static table:\n",
            duplicated_static_manning_resistance,
        )

    # check for duplicated indexes in the level_boundary static tables
    duplicated_static_level_boundary = ribasim_model.level_boundary.static.df.loc[
        ribasim_model.level_boundary.static.df.duplicated(subset="node_id")
    ]
    if len(duplicated_static_level_boundary) > 0:
        print(
            "\nFollowing indexes are duplicated in the level_boundary.static table:\n", duplicated_static_level_boundary
        )

    # check if node_ids in the link table are not present in the node table
    link = ribasim_model.link.df.copy()
    missing_from_node_id = link.loc[~link.from_node_id.isin(combined_df.node_id.to_numpy())]
    missing_to_node_id = link.loc[~link.to_node_id.isin(combined_df.node_id.to_numpy())]
    missing_links = combined_df.loc[
        (~combined_df.node_id.isin(link.from_node_id)) & (~combined_df.node_id.isin(link.to_node_id))
    ]

    if len(missing_from_node_id) > 0:
        print("\nFollowing from_node_id's in the link table do not exist:\n", missing_from_node_id)
    if len(missing_to_node_id) > 0:
        print("\nFollowing to_node_id's in the link table do not exist:\n", missing_to_node_id)
    if len(missing_links) > 0:
        print("\nFollowing node_ids are not connected to any links:\n", missing_links)

    # set crs
    ribasim_model.basin.node.df = gpd.GeoDataFrame(ribasim_model.basin.node.df.set_crs(crs="EPSG:28992"))
    ribasim_model.tabulated_rating_curve.node.df = gpd.GeoDataFrame(
        ribasim_model.tabulated_rating_curve.node.df.set_crs(crs="EPSG:28992")
    )
    ribasim_model.outlet.node.df = gpd.GeoDataFrame(ribasim_model.outlet.node.df.set_crs(crs="EPSG:28992"))
    ribasim_model.pump.node.df = gpd.GeoDataFrame(ribasim_model.pump.node.df.set_crs(crs="EPSG:28992"))
    ribasim_model.manning_resistance.node.df = gpd.GeoDataFrame(
        ribasim_model.manning_resistance.node.df.set_crs(crs="EPSG:28992")
    )
    ribasim_model.level_boundary.node.df = gpd.GeoDataFrame(
        ribasim_model.level_boundary.node.df.set_crs(crs="EPSG:28992")
    )
    ribasim_model.flow_boundary.node.df = gpd.GeoDataFrame(
        ribasim_model.flow_boundary.node.df.set_crs(crs="EPSG:28992")
    )
    ribasim_model.terminal.node.df = gpd.GeoDataFrame(ribasim_model.terminal.node.df.set_crs(crs="EPSG:28992"))

    # section below as asked by D2HYDRO

    # #add category in the .node table
    basin_node = ribasim_model.basin.node.df.merge(
        right=ribasim_model.basin.state.df[["node_id", "meta_categorie"]],
        how="left",
        left_on="meta_node_id",
        right_on="node_id",
    )
    basin_node = basin_node.set_index("node_id")  # change index
    ribasim_model.basin.node.df = basin_node  # replace the df

    # add waterschap name, remove meta_node_id
    ribasim_model.basin.node.df["meta_waterbeheerder"] = waterschap
    ribasim_model.basin.node.df = ribasim_model.basin.node.df.drop(columns="meta_node_id")


def find_upstream_downstream_target_levels(ribasim_model: ribasim.Model, node: str) -> None:
    """Find the target levels upstream and downstream from each outlet, and add it as meta data to the outlet.static table."""
    if node.lower() == "outlet":
        structure_static = ribasim_model.outlet.static.df.copy(deep=True)
        structure_static = structure_static[
            [
                "node_id",
                "active",
                "flow_rate",
                "min_flow_rate",
                "max_flow_rate",
                "meta_min_crest_level",
                "control_state",
                "meta_categorie",
            ]
        ]  # prevent errors if the function is ran before
    elif node.lower() == "pump":
        structure_static = ribasim_model.pump.static.df.copy(deep=True)
        structure_static = structure_static[
            [
                "node_id",
                "active",
                "flow_rate",
                "min_flow_rate",
                "max_flow_rate",
                "control_state",
                "meta_func_afvoer",
                "meta_func_aanvoer",
                "meta_func_circulatie",
                "meta_type_verbinding",
                "meta_categorie",
            ]
        ]  # prevent errors if the function is ran before
    else:
        msg = f'Only the following node types are implemented: ("Outlet", "Pump"); {node} given.'
        raise NotImplementedError(msg)
    # find upstream basin node_id
    structure_static = structure_static.merge(
        right=ribasim_model.link.df[["from_node_id", "to_node_id"]],
        left_on="node_id",
        right_on="to_node_id",
        how="left",
    )
    structure_static = structure_static.drop(columns="to_node_id")  # remove redundant column

    # find downstream basin node_id
    structure_static = structure_static.merge(
        right=ribasim_model.link.df[["to_node_id", "from_node_id"]],
        left_on="node_id",
        right_on="from_node_id",
        how="left",
        suffixes=("", "_remove"),
    )
    structure_static = structure_static.drop(columns="from_node_id_remove")  # remove redundant column

    # filter the basin state table, in case basins have been converted to Level Boundaries in the FF
    basin_state = ribasim_model.basin.state.df[["node_id", "level"]].copy()
    basin_state = basin_state.loc[basin_state.node_id.isin(ribasim_model.basin.node.df.index.values)]

    # merge upstream target level to the outlet static table by using the Basins
    structure_static = structure_static.merge(
        right=basin_state,
        left_on="to_node_id",
        right_on="node_id",
        how="left",
        suffixes=("", "_remove"),
    )
    structure_static = structure_static.rename(columns={"level": "to_basin_level"})
    structure_static = structure_static.drop(columns="node_id_remove")  # remove redundant column

    structure_static = structure_static.merge(
        right=ribasim_model.basin.state.df[["node_id", "level"]],
        left_on="from_node_id",
        right_on="node_id",
        how="left",
        suffixes=("", "_remove"),
    )
    structure_static = structure_static.rename(columns={"level": "from_basin_level"})
    structure_static = structure_static.drop(columns="node_id_remove")  # remove redundant column

    # merge upstream target level to the outlet static table by using the LevelBoundaries
    structure_static = structure_static.merge(
        right=ribasim_model.level_boundary.static.df[["node_id", "level"]],
        left_on="to_node_id",
        right_on="node_id",
        how="left",
        suffixes=("", "_remove"),
    )
    structure_static = structure_static.rename(columns={"level": "to_LevelBoundary_level"})
    structure_static = structure_static.drop(columns="node_id_remove")  # remove redundant column

    structure_static = structure_static.merge(
        right=ribasim_model.level_boundary.static.df[["node_id", "level"]],
        left_on="from_node_id",
        right_on="node_id",
        how="left",
        suffixes=("", "_remove"),
    )
    structure_static = structure_static.rename(columns={"level": "from_LevelBoundary_level"})
    structure_static = structure_static.drop(columns="node_id_remove")  # remove redundant column

    # fill new columns with the upstream target levels of both Basins as well as LevelBoundaries
    structure_static["from_level"] = structure_static["from_basin_level"].fillna(
        structure_static["from_LevelBoundary_level"]
    )
    structure_static["to_level"] = structure_static["to_basin_level"].fillna(structure_static["to_LevelBoundary_level"])

    # drop the redundant columns, and prepare column names for Ribasim
    structure_static = structure_static.drop(
        columns=["to_basin_level", "from_basin_level", "to_LevelBoundary_level", "from_LevelBoundary_level"]
    )
    structure_static = structure_static.rename(
        columns={
            "from_node_id": "meta_from_node_id",
            "to_node_id": "meta_to_node_id",
            "from_level": "meta_from_level",
            "to_level": "meta_to_level",
        }
    )

    # replace the old ribasim_model.____.static.df with the updated structure_static df
    if "utlet" in node:
        ribasim_model.outlet.static = structure_static
    elif "ump" in node:
        ribasim_model.pump.static = structure_static


def change_func(ribasim_model: ribasim.Model, node_id: int, node_type: str, func: str, value: int) -> None:
    """Change the 'meta_func_{func}' of a `Outlet`- or `Pump`-node.

    :param ribasim_model: Ribasim model
    :param node_id: node ID
    :param node_type: node type, options are {'outlet', 'pump'}
    :param func: function to change, options are {'aanvoer', 'afvoer'}
    :param value: value to set the function to, options are {0, 1}

    :type ribasim_model: ribasim.Model
    :type node_id: int
    :type node_type: str
    :type func: str
    :type value: int
    """
    assert node_type in ("outlet", "pump")
    assert func in ("aanvoer", "afvoer")
    assert value in (0, 1)
    getattr(ribasim_model, node_type).static.df.loc[
        getattr(ribasim_model, node_type).static.df["node_id"] == node_id, f"meta_func_{func}"
    ] = value


def change_pump_func(ribasim_model: ribasim.Model, node_id: int, func: str, value: int) -> None:
    """Change the 'meta_func_{func}' of a `Pump`-node.

    :param ribasim_model: Ribasim model
    :param node_id: pump node ID
    :param func: function to change, options are {'aanvoer', 'afvoer'}
    :param value: value to set the function to, options are {0, 1}

    :type ribasim_model: ribasim.Model
    :type node_id: int
    :type func: str
    :type value: int
    """
    change_func(ribasim_model, node_id, "pump", func, value)


def change_outlet_func(ribasim_model: ribasim.Model, node_id: int, func: str, value: int) -> None:
    """Change the 'meta_func_{func}' of a `Outlet`-node.

    :param ribasim_model: Ribasim model
    :param node_id: outlet node ID
    :param func: function to change, options are {'aanvoer', 'afvoer'}
    :param value: value to set the function to, options are {0, 1}

    :type ribasim_model: ribasim.Model
    :type node_id: int
    :type func: str
    :type value: int
    """
    change_func(ribasim_model, node_id, "outlet", func, value)
