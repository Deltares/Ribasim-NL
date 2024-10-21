# import pathlib
import json
import os
import shutil
import subprocess
import sys
import warnings

import geopandas as gpd
import numpy as np
import pandas as pd
import ribasim
import tqdm.auto as tqdm
from ribasim_nl import CloudStorage
from shapely.geometry import LineString


def get_current_max_nodeid(ribasim_model):
    # max_ids = [1]
    # for k, v in ribasim_model.__dict__.items():
    #     if hasattr(v, 'node') and "node_id" in v.node.df.columns.tolist():
    #         if len(v.node.df.node_id) > 0:
    #             mid = int(v.node.df.node_id.max())
    #             max_ids.append(mid)
    # max_id = max(max_ids)

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
        right=ribasim_model.edge.df, left_on="node_id", right_on="from_node_id", how="left"
    )["to_node_id"]

    bergende_nodes["doorgaande_node"] = bergende_nodes.merge(
        right=ribasim_model.edge.df, left_on="from_MR_node", right_on="from_node_id", how="left"
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
    profile_total = profile_total.loc[profile_total.meta_categorie != "bergend"].reset_index(
        drop=True
    )  # remove bergende profiles, as they will be added here below
    profile_total = pd.concat([profile_total, bergende_nodes[["node_id", "level", "area", "meta_categorie"]]])
    profile_total = profile_total.sort_values(by=["node_id", "level"]).reset_index(drop=True)

    # insert the new tables in the model
    ribasim_model.basin.profile = profile_total

    # due to the bergende basin, the surface area has been doubled. Correct this.
    ribasim_model.basin.profile.df.area /= 2

    # # The newly created (storage) basins do not have a correct initial level yet. Fix this as well.
    # initial_level = ribasim_model.basin.profile.df.copy()
    # initial_level = initial_level.drop_duplicates(subset="node_id", keep="last")
    # ribasim_model.basin.state.df["level"] = ribasim_model.basin.state.df.merge(right=initial_level, on="node_id")[
    #     "level_y"
    # ]
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

    time_range = pd.date_range(start=start_time, periods=timesteps, freq=timestep_size)

    # Create forcing_data single Node
    # forcing_data = {
    #     "time": time_range,
    #     "precipitation": [forcing_dict['precipitation']] * timesteps,
    #     "potential_evaporation": [forcing_dict['potential_evaporation']] * timesteps,
    #     "drainage": [forcing_dict['drainage']] * timesteps,
    #     "infiltration": [forcing_dict['infiltration']] * timesteps,
    #     "urban_runoff": [forcing_dict['urban_runoff']] * timesteps
    # }
    # forcing_data_df = pd.DataFrame(forcing_data)

    # # Create forcing_data for all Nodes
    # node_ids = ribasim_model.basin.node.df['node_id'].to_list()

    # all_node_forcing_data = pd.concat([
    #     pd.DataFrame({
    #         "node_id": node_id,
    #         "precipitation": forcing_data_df['precipitation'],
    #         "potential_evaporation": forcing_data_df['potential_evaporation'],
    #         "drainage": forcing_data_df['drainage'],
    #         "infiltration": forcing_data_df['infiltration'],
    #         "urban_runoff": forcing_data_df['urban_runoff']
    #     }) for node_id in node_ids], ignore_index=True)

    all_node_forcing_data = ribasim_model.basin.node.df[["meta_node_id"]].copy()
    for col_name, col_value in forcing_dict.items():
        all_node_forcing_data[col_name] = col_value

    # Update Model
    ribasim_model.basin.static = all_node_forcing_data.reset_index()
    ribasim_model.starttime = time_range[0].to_pydatetime()
    ribasim_model.endtime = time_range[-1].to_pydatetime()

    return


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
    LB_static["level"] = default_level
    LB_combined = pd.concat([ribasim_model.level_boundary.static.df, LB_static])
    LB_combined = LB_combined.drop_duplicates(subset="node_id").sort_values(by="node_id").reset_index(drop=True)
    ribasim_model.level_boundary.static = LB_combined

    # fourth, update the edges table.
    ribasim_model.edge.df.replace(to_replace="Terminal", value="LevelBoundary", inplace=True)

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

    # also supplement the TRC.static table. Create dummy Q(h)-relations
    TRC_LB1 = nodes_FlowBoundary[["meta_node_id"]]
    TRC_LB1["level"] = 0
    TRC_LB1["flow_rate"] = 0

    TRC_LB2 = nodes_FlowBoundary[["meta_node_id"]]
    TRC_LB2["level"] = 1
    TRC_LB2["flow_rate"] = 1

    TRC_LB = pd.concat([TRC_LB1, TRC_LB2])
    TRC_LB = TRC_LB.sort_values(by=["node_id", "level"]).reset_index(drop=True)
    ribasim_model.tabulated_rating_curve.static = pd.concat(
        [ribasim_model.tabulated_rating_curve.static.df, TRC_LB]
    ).reset_index(drop=True)

    # change the FlowBoundaries to TRC in the edges
    ribasim_model.edge.df.replace(to_replace="FlowBoundary", value="TabulatedRatingCurve", inplace=True)

    # remove all node rows with FlowBoundaries
    if len(ribasim_model.flow_boundary.node.df) > 0:
        ribasim_model.flow_boundary.node.df = ribasim_model.flow_boundary.node.df.iloc[0:0]
        ribasim_model.flow_boundary.static.df = ribasim_model.flow_boundary.static.df.iloc[0:0]

    # up till this point, all FlowBoundaries have been converted to TRC's. Now the actual LevelBoundaries needs to be created
    max_id = get_current_max_nodeid(ribasim_model)

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

    nodes_LevelBoundary = nodes_FlowBoundary.copy(deep=True)  # for clarity

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

    # the nodes have been created. Now add the edges
    edges_LB = pd.DataFrame()
    edges_LB["from_node_id"] = (
        nodes_LevelBoundary.index.copy()
    )  # nodes_LevelBoundary["meta_node_id"].copy()  # as these nodes were initially FlowBoundaries, they always flow into the model, not out. Thus, is always the starting point (=from_node_id)
    edges_LB["meta_from_node_type"] = "LevelBoundary"
    edges_LB["to_node_id"] = nodes_LevelBoundary["meta_old_node_id"].values.to_numpy()
    edges_LB["meta_to_node_type"] = "TabulatedRatingCurve"
    edges_LB["meta_categorie"] = "doorgaand"

    # find the geometries, based on the from and to points
    lines_LB = pd.DataFrame()
    lines_LB["from_point"] = nodes_FlowBoundary.geometry
    lines_LB["to_point"] = nodes_FlowBoundary.geometry.translate(xoff=1, yoff=1)  # = original coordinates

    def create_linestring(row):
        return LineString([row["from_point"], row["to_point"]])

    # create the linestrings, and plug them into the df of edges_LB
    if len(lines_LB) > 0:
        lines_LB["geometry"] = lines_LB.apply(create_linestring, axis=1)
        # edges_LB["geometry"] = lines_LB["line"]

        # merge the geometries to the newtemp
        edges_LB = edges_LB.merge(right=lines_LB[["geometry"]], left_on="from_node_id", right_index=True, how="left")

    # concat the original edges with the newly created edges of the LevelBoundaries
    new_edges = pd.concat([ribasim_model.edge.df, edges_LB]).reset_index(drop=True)
    new_edges["edge_id"] = new_edges.index.copy() + 1
    new_edges = new_edges[
        [
            "edge_id",
            "from_node_id",
            "to_node_id",
            "edge_type",
            "name",
            "subnetwork_id",
            "geometry",
            "meta_from_node_type",
            "meta_to_node_type",
            "meta_categorie",
        ]
    ]
    new_edges = new_edges.set_index("edge_id")
    ribasim_model.edge.df = new_edges

    # replace all 'FlowBoundaries' with 'LevelBoundaries' in the edge table
    # ribasim_model.edge.df.replace(to_replace='FlowBoundary', value='LevelBoundary', inplace=True)

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
    # select all TRC's which are inlaten
    # display(ribasim_model.tabulated_rating_curve.static.df)
    # TRC_naar_OL = ribasim_model.tabulated_rating_curve.static.df.loc[
    #     ribasim_model.tabulated_rating_curve.static.df.meta_type_verbinding == "Inlaat"
    # ]

    # update: change all TRC's to Outlets
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
    # to do so, find the target levels of the (boezem) connected basins. This has to be done by looking within the edges
    target_level = TRC_naar_OL.merge(ribasim_model.edge.df, left_on="node_id", right_on="to_node_id", how="left")

    # the basins of which the target_levels should be retrieved, are stored in the column of from_node_id
    target_level = target_level.merge(
        ribasim_model.basin.state.df, left_on="from_node_id", right_on="node_id", how="left"
    )

    # clean the df for clarity. Next, add the levels to the outlet df
    target_level = target_level[["node_id_x", "level"]]
    target_level.rename(columns={"level": "meta_min_crest_level", "node_id_x": "node_id"}, inplace=True)

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
    ]  # .reset_index(drop=True)
    ribasim_model.tabulated_rating_curve.static = ribasim_model.tabulated_rating_curve.static.df.loc[
        ribasim_model.tabulated_rating_curve.static.df.node_id.isin(
            ribasim_model.tabulated_rating_curve.node.df.index.to_numpy()
        )
    ].reset_index(drop=True)

    # replace the from_node_type and the to_node_type in the edge table
    ribasim_model.edge.df = ribasim_model.edge.df.replace(to_replace="TabulatedRatingCurve", value="Outlet")

    # ribasim_model.edge.df.loc[ribasim_model.edge.df.from_node_id.isin(outlet.node_id), "from_node_type"] = "Outlet"
    # ribasim_model.edge.df.loc[ribasim_model.edge.df.to_node_id.isin(outlet.node_id), "to_node_type"] = "Outlet"

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
        cur_max_nodeid = get_current_max_nodeid(ribasim_model)
        if cur_max_nodeid < 90000:
            new_nodeid = 90000 + cur_max_nodeid + 1  # aanpassen loopt vanaf 90000 +1
        else:
            new_nodeid = cur_max_nodeid + 1
        # print(new_nodeid, end="\r")

        # @TODO Ron aangeven in geval van meerdere matches welke basin gepakt moet worden
        # basin is niet de beste variable name
        # Kan ook level boundary of terminal, dus na & weghalen
        # ["Basin", "LevelBoundary", "Terminal"]
        basin = ribasim_model.edge.df[
            ((ribasim_model.edge.df.to_node_id == row.node_id) | (ribasim_model.edge.df.from_node_id == row.node_id))
            & ((ribasim_model.edge.df.from_node_type == "Basin") | (ribasim_model.edge.df.to_node_type == "Basin"))
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

        ribasim_model.edge.add(ribasim_model.discrete_control[new_nodeid], ribasim_model.pump[row.node_id])

    return


def set_tabulated_rating_curves(ribasim_model, level_increase=1.0, flow_rate=4, LevelBoundary_level=0):
    """Create the Q(h)-relations for each TRC. It starts passing water from target level onwards."""
    # find the originating basin of each TRC
    target_level = ribasim_model.edge.df.loc[
        ribasim_model.edge.df.to_node_type == "TabulatedRatingCurve"
    ]  # select all TRC's. Do this from the edge table, so we can look the basins easily up afterwards

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
    TRC_ter = ribasim_model.edge.df.copy()
    TRC_ter = TRC_ter.loc[
        (TRC_ter.from_node_type == "TabulatedRatingCurve") & (TRC_ter.to_node_type == "Terminal")
    ]  # select the correct nodes

    # not all TRC_ter's should be changed, as not all these nodes are in a boezem. Some are just regular peilgebieden. Filter these nodes for numerical stability
    # first, check where they originate from
    basins_to_TRC_ter = ribasim_model.edge.df.loc[ribasim_model.edge.df.to_node_id.isin(TRC_ter.from_node_id)]
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
    # #get rid of all TRC's static rows which do not occur in the node table (assuming the node table is the groundtruth)
    # TRC_nodes = ribasim_model.tabulated_rating_curve.node.df.node_id.values
    # ribasim_model.tabulated_rating_curve.static.df = ribasim_model.tabulated_rating_curve.static.df.loc[ribasim_model.tabulated_rating_curve.static.df.node_id.isin(TRC_nodes)]

    # #put all TRC's nodes on one pile. So both the static as well as the node df
    # TRC_pile = pd.concat([ribasim_model.tabulated_rating_curve.static.df['node_id'],
    #                       ribasim_model.tabulated_rating_curve.node.df['node_id']])

    # #each node_id should occur at least three times in the pile (once because of the node, twice because of the Qh relation)
    # node_id_counts = ribasim_model.tabulated_rating_curve.static.df['node_id'].value_counts()

    # #select all nodes which occur less than 3 times
    # unique_node_ids = node_id_counts[node_id_counts < 3].index

    # #create new Qh relations
    # zero_flow = ribasim_model.tabulated_rating_curve.static.df[ribasim_model.tabulated_rating_curve.static.df['node_id'].isin(unique_node_ids)]
    # one_flow = zero_flow.copy()
    # zero_flow.flow_rate = 0 #set flow rate to 0 if on target level
    # one_flow.level += 1 #set level 1 meter higher where it discharges 1 m3/s

    # #remove old Qh points
    # ribasim_model.tabulated_rating_curve.static.df = ribasim_model.tabulated_rating_curve.static.df.loc[~ribasim_model.tabulated_rating_curve.static.df['node_id'].isin(unique_node_ids)]

    # #add the new Qh points back in the df
    # ribasim_model.tabulated_rating_curve.static.df = pd.concat([ribasim_model.tabulated_rating_curve.static.df,
    #                                                             zero_flow,
    #                                                             one_flow])
    # #drop duplicates, sort and reset index
    # ribasim_model.tabulated_rating_curve.static.df.drop_duplicates(subset = ['node_id', 'level'], inplace = True)
    # ribasim_model.tabulated_rating_curve.static.df.sort_values(by=['node_id', 'level', 'flow_rate'], inplace = True)
    # ribasim_model.tabulated_rating_curve.node.df.sort_values(by=['node_id'], inplace = True)
    # ribasim_model.tabulated_rating_curve.static.df.reset_index(drop = True, inplace = True)

    # print(len(TRC_nodes))
    # print(len(ribasim_model.tabulated_rating_curve.static.df.node_id.unique()))

    return


def write_ribasim_model_Zdrive(ribasim_model, path_ribasim_toml):
    # Write Ribasim model to the Z drive
    if not os.path.exists(path_ribasim_toml):
        os.makedirs(path_ribasim_toml)

    ribasim_model.write(path_ribasim_toml)


def write_ribasim_model_GoodCloud(
    ribasim_model, path_ribasim_toml, waterschap, modeltype="boezemmodel", include_results=True
):
    # copy the results folder from the "updated" folder to the "Ribasim_networks" folder
    results_source = f"../../../../../Ribasim_updated_models/{waterschap}/modellen/{waterschap}_parametrized/results"
    parametrized_location = (
        f"../../../../../Ribasim_networks/Waterschappen/{waterschap}/modellen/{waterschap}_parametrized"
    )

    if not os.path.exists(parametrized_location):
        os.makedirs(parametrized_location)

    # If the destination folder of the results already exists, remove it
    print(os.path.join(parametrized_location, "results"))
    if os.path.exists(os.path.join(parametrized_location, "results")):
        shutil.rmtree(os.path.join(parametrized_location, "results"))

    # copy the results to the Ribasim_networks folder
    shutil.copytree(results_source, os.path.join(parametrized_location, "results"))

    # copy the model to the Ribasim_networks folder
    parametrized_location = os.path.join(parametrized_location, "ribasim.toml")
    ribasim_model.write(
        parametrized_location
    )  # write to the "Ribasim_networks" folder (will NOT be overwritten at each upload)

    path_goodcloud_password = "../../../../../Data_overig/password_goodcloud.txt"
    with open(path_goodcloud_password) as file:
        password = file.read()

    cloud_storage = CloudStorage(
        password=password,
        data_dir=r"../../../../../Ribasim_networks/Waterschappen/",
    )

    cloud_storage.upload_model(
        authority=waterschap, model=waterschap + "_parametrized", include_results=include_results
    )

    print(f"The model of waterboard {waterschap} has been uploaded to the goodcloud in the directory of {modeltype}!")
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
            df_trc = ribasim_model.edge.df[ribasim_model.edge.df.to_node_type == "TabulatedRatingCurve"]
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


#### New ####
def validate_basin_area(model):
    """
    Validate the area of basins in the model.

    :param model: The ribasim model to validate
    :return: None
    """
    too_small_basins = []
    error = False
    for index, row in model.basin.node.df.iterrows():
        basin_id = int(row["meta_node_id"])
        basin_geometry = model.basin.area.df.loc[model.basin.area.df["meta_node_id"] == basin_id, "geometry"]
        if not basin_geometry.empty:
            basin_area = basin_geometry.iloc[0].area
            if basin_area < 100:
                error = True
                print(f"Basin with Node ID {basin_id} has an area smaller than 100 m²: {basin_area} m²")
                too_small_basins.append(basin_id)
    if not error:
        print("All basins are larger than 100 m²")

    return


def identify_node_meta_categorie(ribasim_model):
    """
    Identify the meta_categorie of each Outlet, Pump and LevelBoundary.

    It checks whether they are inlaten en uitlaten from a boezem, buitenwater or just regular peilgebieden.
    This will determine the rules of the control nodes.
    """
    # create new columsn to store the meta categorie of each node
    ribasim_model.outlet.static.df["meta_categorie"] = np.nan
    ribasim_model.pump.static.df["meta_categorie"] = np.nan

    # select all basins which are not "bergend"
    basin_nodes = ribasim_model.basin.state.df.copy()
    # peilgebied_basins = basin_nodes.loc[basin_nodes.meta_categorie == "doorgaand", "node_id"]
    boezem_basins = basin_nodes.loc[basin_nodes.meta_categorie == "hoofdwater", "node_id"]

    # select the nodes which originate from a boezem, and the ones which go to a boezem. Use the edge table for this.
    nodes_from_boezem = ribasim_model.edge.df.loc[ribasim_model.edge.df.from_node_id.isin(boezem_basins), "to_node_id"]
    nodes_to_boezem = ribasim_model.edge.df.loc[ribasim_model.edge.df.to_node_id.isin(boezem_basins), "from_node_id"]

    # select the nodes which originate from, and go to a boundary
    nodes_from_boundary = ribasim_model.edge.df.loc[
        ribasim_model.edge.df.meta_from_node_type == "LevelBoundary", "to_node_id"
    ]
    nodes_to_boundary = ribasim_model.edge.df.loc[
        ribasim_model.edge.df.meta_to_node_type == "LevelBoundary", "from_node_id"
    ]

    # identify the INlaten from the boezem, both stuwen (outlets) and gemalen (pumps)
    ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.node_id.isin(nodes_from_boezem), "meta_categorie"
    ] = "Inlaat boezem, stuw"
    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_from_boezem))
        & (ribasim_model.pump.static.df.meta_func_aanvoer == 0),
        "meta_categorie",
    ] = "Inlaat boezem, afvoer gemaal"
    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_from_boezem))
        & (ribasim_model.pump.static.df.meta_func_aanvoer != 0),
        "meta_categorie",
    ] = "Inlaat boezem, aanvoer gemaal"

    # identify the UITlaten from the boezem, both stuwen (outlets) and gemalen (pumps)
    ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.node_id.isin(nodes_to_boezem), "meta_categorie"
    ] = "Uitlaat boezem, stuw"
    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_to_boezem))
        & (ribasim_model.pump.static.df.meta_func_aanvoer == 0),
        "meta_categorie",
    ] = "Uitlaat boezem, afvoer gemaal"
    ribasim_model.pump.static.df.loc[
        (ribasim_model.pump.static.df.node_id.isin(nodes_to_boezem))
        & (ribasim_model.pump.static.df.meta_func_aanvoer != 0),
        "meta_categorie",
    ] = "Uitlaat boezem, aanvoer gemaal"

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
    ribasim_model.pump.static.df.meta_func_afvoer.fillna(value=1.0, inplace=True)
    ribasim_model.pump.static.df.meta_func_aanvoer.fillna(value=0.0, inplace=True)
    ribasim_model.pump.static.df.meta_func_circulatie.fillna(value=0.0, inplace=True)

    return


def load_model_settings(file_path):
    with open(file_path) as file:
        settings = json.load(file)
    return settings


def determine_min_upstream_max_downstream_levels(ribasim_model, waterschap):
    sturing = load_model_settings(f"sturing_{waterschap}.json")  # load the waterschap specific sturing

    # create empty columns for the sturing
    ribasim_model.outlet.static.df["min_upstream_level"] = np.nan
    ribasim_model.outlet.static.df["max_downstream_level"] = np.nan
    ribasim_model.outlet.static.df["max_flow_rate"] = np.nan
    ribasim_model.outlet.static.df["flow_rate"] = np.nan

    ribasim_model.pump.static.df["min_upstream_level"] = np.nan
    ribasim_model.pump.static.df["max_downstream_level"] = np.nan
    ribasim_model.pump.static.df["max_flow_rate"] = np.nan
    ribasim_model.pump.static.df["flow_rate"] = np.nan

    # make a temp copy to reduce line length, place it later again in the model
    outlet = ribasim_model.outlet.static.df.copy()
    pump = ribasim_model.pump.static.df.copy()

    # for each different outlet and pump type, determine the min an max upstream and downstream level
    for types, settings in sturing.items():
        # Extract values for each setting
        upstream_level_offset = settings["upstream_level_offset"]
        downstream_level_offset = settings["downstream_level_offset"]
        max_flow_rate = settings["max_flow_rate"]

        # Update the min_upstream_level and max_downstream_level in the OUTLET dataframe
        outlet.loc[outlet.meta_categorie == types, "min_upstream_level"] = (
            outlet.meta_from_level - upstream_level_offset
        )
        outlet.loc[outlet.meta_categorie == types, "max_downstream_level"] = (
            outlet.meta_to_level + downstream_level_offset
        )
        outlet.loc[outlet.meta_categorie == types, "flow_rate"] = max_flow_rate

        # Update the min_upstream_level and max_downstream_level in the PUMP dataframe. can be done within the same loop, as the meta_categorie is different for the outlet and pump
        pump.loc[pump.meta_categorie == types, "min_upstream_level"] = pump.meta_from_level - upstream_level_offset
        pump.loc[pump.meta_categorie == types, "max_downstream_level"] = pump.meta_to_level + downstream_level_offset
        pump.loc[pump.meta_categorie == types, "flow_rate"] = max_flow_rate

    # outlet['flow_rate'] = outlet['max_flow_rate']
    # pump['flow_rate'] = pump['max_flow_rate']

    # raise warning if there are np.nan in the columns
    def check_for_nans_in_columns(
        df, outlet_or_pump, columns_to_check=["min_upstream_level", "max_downstream_level", "flow_rate", "flow_rate"]
    ):
        if df[columns_to_check].isnull().values.any():
            warnings.warn(
                f"Warning: NaN values found in the following columns of the {outlet_or_pump} dataframe: "
                f"{', '.join([col for col in columns_to_check if df[col].isnull().any()])}"
            )

    check_for_nans_in_columns(outlet, "outlet")
    check_for_nans_in_columns(pump, "pump")

    # place the df's back in the ribasim_model
    ribasim_model.outlet.static.df = outlet
    ribasim_model.pump.static.df = pump

    return


def add_discrete_control(ribasim_model, waterschap, default_level):
    """Add discrete control nodes to the network. The rules are based on the meta_categorie of each node."""
    # load in the sturing which is defined in the json files
    sturing = load_model_settings(f"sturing_{waterschap}.json")

    # Remove all Discrete Control nodes and edges if its present
    ribasim_model.discrete_control.node.df = ribasim_model.discrete_control.node.df.iloc[0:0]
    if ribasim_model.discrete_control.condition.df is not None:
        ribasim_model.discrete_control.condition.df = ribasim_model.discrete_control.condition.df.iloc[0:0]
        ribasim_model.discrete_control.logic.df = ribasim_model.discrete_control.logic.df.iloc[0:0]
        ribasim_model.discrete_control.variable.df = ribasim_model.discrete_control.variable.df.iloc[0:0]
        ribasim_model.edge.df = ribasim_model.edge.df.loc[ribasim_model.edge.df.edge_type != "control"]

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

    # find the greather_than value by looking the corresponding UPstream basin up in the edge table
    basin_to_control_node_us = ribasim_model.edge.df.loc[
        ribasim_model.edge.df.to_node_id.isin(nodes_to_control.values)
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
    basin_to_control_node_ds = ribasim_model.edge.df.loc[
        ribasim_model.edge.df.from_node_id.isin(nodes_to_control.values)
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

    ### edge ####################################################
    DC_edge = DC_condition.copy()[["node_id", "meta_to_control_node_id", "meta_to_control_node_type"]]

    # as the DC listens to both the upstream as well as the downstream nodes, it contains twice the node_ids. Only select one.
    DC_edge = DC_edge.drop_duplicates(subset="node_id")
    DC_edge.rename(
        columns={
            "node_id": "from_node_id",
            "meta_to_control_node_id": "to_node_id",
            "meta_to_control_node_type": "to_node_type",
        },
        inplace=True,
    )

    # DC_edge['to_node_type'] = ribasim_model.edge.df.loc[ribasim_model.edge.df.to_node_id == DC_edge.node_id, 'to_node_type']
    DC_edge["from_node_type"] = "DiscreteControl"
    DC_edge["edge_type"] = "control"
    DC_edge["meta_categorie"] = "DC_control"

    # retrieve the FROM geometry from the DC_nodes. The TO is the same, as the DiscreteControl is on the same location
    DC_edge["from_coord"] = DC_nodes["geometry"]
    DC_edge["to_coord"] = DC_nodes["geometry"]

    def create_linestring(row):
        return LineString([row["from_coord"], row["to_coord"]])

    DC_edge["geometry"] = DC_edge.apply(create_linestring, axis=1)

    DC_edge = DC_edge[
        ["from_node_id", "from_node_type", "to_node_id", "to_node_type", "edge_type", "meta_categorie", "geometry"]
    ]
    ribasim_model.edge.df = pd.concat([ribasim_model.edge.df, DC_edge]).reset_index(drop=True)

    return


def clean_tables(ribasim_model):
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
    ribasim_model.basin.static = ribasim_model.basin.static.df.loc[
        ribasim_model.basin.static.df.node_id.isin(basin_ids)
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
    basin_static_missing = ribasim_model.basin.node.df.loc[
        ~ribasim_model.basin.node.df.index.isin(ribasim_model.basin.static.df.node_id)
    ]  # .index.to_numpy()
    if len(basin_static_missing) > 0:
        print("\nFollowing node_id's in the Basin.static table are missing:\n", basin_static_missing.index.to_numpy())

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

    # Manning resistance
    manning_resistance_missing = ribasim_model.manning_resistance.node.df.loc[
        ~ribasim_model.manning_resistance.node.df.index.isin(ribasim_model.manning_resistance.static.df.node_id)
    ]  # .index.to_numpy()
    if len(manning_resistance_missing) > 0:
        print(
            "\nFollowing node_id's in the manning_resistance.static table are missing\n:",
            manning_resistance_missing.index.to_numpy(),
        )

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

    # check for duplicated indexes in the basin static tables
    duplicated_static_basin = ribasim_model.basin.static.df.loc[
        ribasim_model.basin.static.df.duplicated(subset="node_id")
    ]
    if len(duplicated_static_basin) > 0:
        print("\nFollowing indexes are duplicated in the basin.static table:", duplicated_static_basin)

    # check for duplicated indexes in the outlet static tables
    duplicated_static_outlet = ribasim_model.outlet.static.df.loc[
        ribasim_model.outlet.static.df.duplicated(subset="node_id")
    ]
    if len(duplicated_static_outlet) > 0:
        print("\nFollowing indexes are duplicated in the outlet.static table:", duplicated_static_outlet)

    # check for duplicated indexes in the pump static tables
    duplicated_static_pump = ribasim_model.pump.static.df.loc[ribasim_model.pump.static.df.duplicated(subset="node_id")]
    if len(duplicated_static_pump) > 0:
        print("\nFollowing indexes are duplicated in the pump.static table:", duplicated_static_pump)

    # check for duplicated indexes in the manning_resistance static tables
    duplicated_static_manning_resistance = ribasim_model.manning_resistance.static.df.loc[
        ribasim_model.manning_resistance.static.df.duplicated(subset="node_id")
    ]
    if len(duplicated_static_manning_resistance) > 0:
        print(
            "\nFollowing indexes are duplicated in the manning_resistance.static table:",
            duplicated_static_manning_resistance,
        )

    # check for duplicated indexes in the level_boundary static tables
    duplicated_static_level_boundary = ribasim_model.level_boundary.static.df.loc[
        ribasim_model.level_boundary.static.df.duplicated(subset="node_id")
    ]
    if len(duplicated_static_level_boundary) > 0:
        print(
            "\nFollowing indexes are duplicated in the level_boundary.static table:", duplicated_static_level_boundary
        )

    return


def find_upstream_downstream_target_levels(ribasim_model, node):
    """Find the target levels upstream and downstream from each outlet, and add it as meta data to the outlet.static table."""
    if "utlet" in node:
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
    elif "ump" in node:
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
    # find upstream basin node_id
    structure_static = structure_static.merge(
        right=ribasim_model.edge.df[["from_node_id", "to_node_id"]],
        left_on="node_id",
        right_on="to_node_id",
        how="left",
    )
    structure_static = structure_static.drop(columns="to_node_id")  # remove redundant column

    # find downstream basin node_id
    structure_static = structure_static.merge(
        right=ribasim_model.edge.df[["to_node_id", "from_node_id"]],
        left_on="node_id",
        right_on="from_node_id",
        how="left",
        suffixes=("", "_remove"),
    )
    structure_static = structure_static.drop(columns="from_node_id_remove")  # remove redundant column

    # merge upstream target level to the outlet static table by using the Basins
    structure_static = structure_static.merge(
        right=ribasim_model.basin.state.df[["node_id", "level"]],
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

    return


##################### Recycle bin ##########################
# def calculate_update_basin_area(ribasim_model, percentage):
#     """
#     Calculates the area of each basin in the provided GeoDataFrame and adds this data as a new column.

#     Parameters:
#     geo_df (geopandas.GeoDataFrame): A GeoDataFrame containing a column of geometry data of each basin.
#     percentage (int): A percentage of the basin area that will be used in the Ribasim model.
#     ribasim_model (object): Ribasim model object that contains the model data.

#     Returns:
#     Updated Ribasim model
#     """
#     # Calculate area and multiple by percentage fraction
#     area = (ribasim_model.basin.area.df['geometry'].area * (percentage/100)).to_list()

#     # Add 0.1 as first min level profile area
#     basin_area = [0.1]

#     # Loop through the original list and add each element and 0.1
#     for item in area:
#         if item < 0.1:
#             item = 1
#             print(item)
#         basin_area.append(item)
#         basin_area.append(0.1)
#         # basin_area.append(item)


#     basin_area.pop()

#     # Update basin profile area
#     ribasim_model.basin.profile.df['area'] = basin_area

#     return ribasim_model
