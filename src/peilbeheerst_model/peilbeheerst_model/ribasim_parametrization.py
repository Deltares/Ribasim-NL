# import pathlib
import subprocess
import sys
import warnings

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
        max_id = int(df_all_nodes.node_id.max())

    return max_id


def set_initial_basin_state(ribasim_model):
    # display(ribasim_model.basin.state.df)
    if "meta_peilgebied_cat" in list(ribasim_model.basin.node.df.keys()):
        basin_state_df = ribasim_model.basin.node.df[["node_id", "meta_peilgebied_cat"]]
        basin_state_df["meta_categorie"] = basin_state_df["meta_peilgebied_cat"]
    else:
        basin_state_df = ribasim_model.basin.node.df[["node_id", "meta_categorie"]]

    basin_state_df["level"] = ribasim_model.basin.area.df["meta_streefpeil"].to_numpy()
    ribasim_model.basin.state.df = basin_state_df
    return


def insert_standard_profile(ribasim_model, regular_percentage=10, boezem_percentage=90, depth_profile=2):
    profile = ribasim_model.basin.area.df.copy()
    profile.node_id, profile.meta_streefpeil = (
        profile.node_id.astype(int),
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

    # insert the new tables in the model
    ribasim_model.basin.profile.df = profile_total

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

    all_node_forcing_data = ribasim_model.basin.node.df[["node_id"]].copy()
    for col_name, col_value in forcing_dict.items():
        all_node_forcing_data[col_name] = col_value

    # Update Model
    ribasim_model.basin.static.df = all_node_forcing_data
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

    # third, implement the LevelBoundary static
    LB_static = nodes_Terminals[["node_id"]]
    LB_static["level"] = default_level
    ribasim_model.level_boundary.static.df = pd.concat([ribasim_model.level_boundary.static.df, LB_static])

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
    ribasim_model.tabulated_rating_curve.node.df = pd.concat(
        [ribasim_model.tabulated_rating_curve.node.df, nodes_TRC_FlowBoundary]
    ).reset_index(drop=True)

    # also supplement the TRC.static table. Create dummy Q(h)-relations
    TRC_LB1 = nodes_FlowBoundary[["node_id"]]
    TRC_LB1["level"] = 0
    TRC_LB1["flow_rate"] = 0

    TRC_LB2 = nodes_FlowBoundary[["node_id"]]
    TRC_LB2["level"] = 1
    TRC_LB2["flow_rate"] = 1

    TRC_LB = pd.concat([TRC_LB1, TRC_LB2])
    TRC_LB = TRC_LB.sort_values(by=["node_id", "level"]).reset_index(drop=True)
    ribasim_model.tabulated_rating_curve.static.df = pd.concat(
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
    nodes_FlowBoundary["node_id_old"] = nodes_FlowBoundary.node_id  # store for later
    nodes_FlowBoundary["node_id"] = max_id + nodes_FlowBoundary.index + 1  # implement new id's
    nodes_FlowBoundary["geometry"] = nodes_FlowBoundary.geometry.translate(
        xoff=-1, yoff=-1
    )  # move the points 1 meter to the lower left (diagonally)
    nodes_FlowBoundary["node_type"] = "LevelBoundary"
    nodes_FlowBoundary["level"] = default_level

    nodes_LevelBoundary = nodes_FlowBoundary.copy(deep=True)  # for clarity

    # supplement the LB.node table
    ribasim_model.level_boundary.node.df = pd.concat(
        [ribasim_model.level_boundary.node.df, nodes_LevelBoundary[["node_id", "node_type", "geometry"]]]
    ).reset_index(drop=True)
    # supplement the LB.static table
    ribasim_model.level_boundary.static.df = pd.concat(
        [ribasim_model.level_boundary.static.df, nodes_LevelBoundary[["node_id", "level"]]]
    ).reset_index(drop=True)

    # the nodes have been created. Now add the edges
    edges_LB = pd.DataFrame()
    edges_LB["from_node_id"] = nodes_LevelBoundary[
        "node_id"
    ]  # as these nodes were initially FlowBoundaries, they always flow into the model, not out. Thus, is always the starting point (=from_node_id)
    edges_LB["from_node_type"] = "LevelBoundary"
    edges_LB["to_node_id"] = nodes_LevelBoundary["node_id_old"]
    edges_LB["to_node_type"] = "TabulatedRatingCurve"
    edges_LB["meta_categorie"] = "doorgaand"

    # find the geometries, based on the from and to points
    lines_LB = pd.DataFrame()
    lines_LB["from_point"] = nodes_FlowBoundary.geometry
    lines_LB["to_point"] = nodes_FlowBoundary.geometry.translate(xoff=1, yoff=1)  # = original coordinates

    def create_linestring(row):
        return LineString([row["from_point"], row["to_point"]])

    # create the linestrings, and plug them into the df of edges_LB
    if len(nodes_FlowBoundary) > 0:
        lines_LB["line"] = lines_LB.apply(create_linestring, axis=1)
        edges_LB["geometry"] = lines_LB["line"]

    # concat the original edges with the newly created edges of the LevelBoundaries
    ribasim_model.edge.df = pd.concat([ribasim_model.edge.df, edges_LB]).reset_index(drop=True)

    # replace all 'FlowBoundaries' with 'LevelBoundaries' in the edge table
    # ribasim_model.edge.df.replace(to_replace='FlowBoundary', value='LevelBoundary', inplace=True)

    return


def add_outlets(ribasim_model, delta_crest_level=0.10):
    # select all TRC's which are inlaten
    # display(ribasim_model.tabulated_rating_curve.static.df)
    TRC_naar_OL = ribasim_model.tabulated_rating_curve.static.df.loc[
        ribasim_model.tabulated_rating_curve.static.df.meta_type_verbinding == "Inlaat"
    ]
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
    target_level.rename(columns={"level": "min_crest_level", "node_id_x": "node_id"}, inplace=True)

    outlet = target_level.copy(deep=True)
    outlet["min_crest_level"] -= (
        delta_crest_level  # the peil of the boezem is allowed to lower with this much before no water will flow through the outlet, to prevent
    )
    get_outlet_geometries = ribasim_model.tabulated_rating_curve.node.df.loc[
        ribasim_model.tabulated_rating_curve.node.df.node_id.isin(outlet.node_id.to_numpy())
    ]
    outlet = outlet.merge(get_outlet_geometries[["node_id", "geometry"]], on="node_id")
    outlet["node_type"] = "Outlet"
    outlet["flow_rate"] = 0  # default setting
    outlet["meta_categorie"] = "Inlaat"

    # add the outlets to the model
    ribasim_model.outlet.node.df = outlet[["node_id", "node_type", "geometry"]]
    ribasim_model.outlet.static.df = outlet[["node_id", "flow_rate", "meta_categorie"]]

    # remove the TRC's nodes
    ribasim_model.tabulated_rating_curve.node = ribasim_model.tabulated_rating_curve.node.df.loc[
        ~ribasim_model.tabulated_rating_curve.node.df.node_id.isin(outlet.node_id)
    ].reset_index(drop=True)
    ribasim_model.tabulated_rating_curve.static = ribasim_model.tabulated_rating_curve.static.df.loc[
        ~ribasim_model.tabulated_rating_curve.static.df.node_id.isin(outlet.node_id)
    ].reset_index(drop=True)

    # replace the from_node_type and the to_node_type in the edge table
    ribasim_model.edge.df.loc[ribasim_model.edge.df.from_node_id.isin(outlet.node_id), "from_node_type"] = "Outlet"
    ribasim_model.edge.df.loc[ribasim_model.edge.df.to_node_id.isin(outlet.node_id), "to_node_type"] = "Outlet"

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


def set_tabulated_rating_curves(ribasim_model, level_increase=1.0, flow_rate=4):
    df_edge = ribasim_model.edge.df
    df_edge_tab = ribasim_model.tabulated_rating_curve.static.df.merge(
        df_edge, left_on="node_id", right_on="to_node_id", how="inner"
    )
    df_tab = ribasim_model.basin.state.df.merge(df_edge_tab, left_on="node_id", right_on="from_node_id", how="inner")
    df_tab = df_tab[["to_node_id", "active", "level_x", "flow_rate", "control_state", "meta_type_verbinding"]]
    df_tab = df_tab.rename(columns={"to_node_id": "node_id", "level_x": "level"})

    def adjust_rows(df):
        df["row_num"] = df.groupby("node_id").cumcount() + 1
        df.loc[df["row_num"] == 2, "level"] += level_increase
        df.loc[df["row_num"] == 2, "flow_rate"] = flow_rate
        df.drop(columns="row_num", inplace=True)

        return df

    # Apply the function
    df_tab.node_id = df_tab.node_id.astype(int)
    df_tab.level = df_tab.level.astype(float)
    df_tab.flow_rate = df_tab.flow_rate.astype(float)

    ribasim_model.tabulated_rating_curve.static.df = adjust_rows(df_tab)

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


def write_ribasim_model_Zdrive(ribasim_model, path_ribasim_toml):
    # Write Ribasim model to the Z drive
    ribasim_model.write(path_ribasim_toml)


def write_ribasim_model_GoodCloud(
    ribasim_model, path_ribasim_toml, waterschap, modeltype="boezemmodel", include_results=True
):
    # Write Ribasim model to the Z drive again, as we want to store the results as well
    ribasim_model.write(path_ribasim_toml)

    # Write Ribasim model to the GoodCloud
    path_goodcloud_password = "../../../../../Data_overig/password_goodcloud.txt"
    with open(path_goodcloud_password) as file:
        password = file.read()

    # Gain access to the goodcloud
    cloud_storage = CloudStorage(password=password, data_dir=r"../../../../../Ribasim_networks/Waterschappen/")

    # Upload the model
    cloud_storage.upload_model(
        authority=waterschap, model=waterschap + "_" + modeltype, include_results=include_results
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
    error = False
    for index, row in model.basin.node.df.iterrows():
        basin_id = int(row["node_id"])
        basin_geometry = model.basin.area.df.loc[model.basin.area.df["node_id"] == basin_id, "geometry"]
        if not basin_geometry.empty:
            basin_area = basin_geometry.iloc[0].area
            if basin_area < 100:
                error = True
                print(f"Basin with Node ID {basin_id} has an area smaller than 100 m²: {basin_area} m²")
    if not error:
        print("All basins are larger than 100 m²")


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
