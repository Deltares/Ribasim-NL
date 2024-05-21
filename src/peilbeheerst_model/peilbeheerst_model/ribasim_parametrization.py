import pandas as pd
import pathlib
import warnings

import ribasim



def get_current_max_nodeid(ribasim_model):
    # max_ids = [1]
    # for k, v in ribasim_model.__dict__.items():
    #     if hasattr(v, 'node') and "node_id" in v.node.df.columns.tolist():
    #         if len(v.node.df.node_id) > 0:
    #             mid = int(v.node.df.node_id.max())
    #             max_ids.append(mid)
    # max_id = max(max_ids)
    
    with warnings.catch_warnings():
        warnings.simplefilter(action='ignore', category=FutureWarning)
        df_all_nodes = ribasim_model.node_table().df
    if len(df_all_nodes) == 0:
        max_id = 1
    else:
        max_id = int(df_all_nodes.node_id.max())

    return max_id

def set_initial_basin_state(ribasim_model):
    basin_state_df = ribasim_model.basin.node.df[['node_id', 'meta_peilgebied_cat']]
    basin_state_df['level'] = ribasim_model.basin.area.df['meta_streefpeil'].values
    ribasim_model.basin.state.df = basin_state_df
    return
    
def insert_standard_profile(ribasim_model, regular_percentage = 10, boezem_percentage = 90, depth_profile = 2):
    profile = ribasim_model.basin.area.df.copy()
    profile.node_id, profile.meta_streefpeil = profile.node_id.astype(int), profile.meta_streefpeil.astype(float) #convert to numbers

    #determine the profile area, which is also used for the profile
    profile['area'] = profile['geometry'].area * (regular_percentage/100)
    profile = profile[['node_id', 'meta_streefpeil', 'area']]
    profile = profile.rename(columns={'meta_streefpeil':'level'})

    #now overwrite the area for the nodes which are the boezem, as these peilgebieden consist mainly out of water
    node_id_boezem = ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df.meta_peilgebied_cat == 1, 'node_id'].values
    profile.loc[profile.node_id.isin(node_id_boezem), 'area'] *= (boezem_percentage/regular_percentage) #increase the size of the area
    
    #define the profile at the bottom of the bakje
    profile_bottom = profile.copy()
    profile_bottom['area'] = 0.1
    profile_bottom['level'] -= depth_profile

    #define the profile slightly above the bottom of the bakje
    profile_slightly_above_bottom = profile.copy()
    profile_slightly_above_bottom['level'] -= (depth_profile - 0.01) #remain one centimeter above the bottom 

    #define the profile at the top of the bakje. 
    profile_top = profile.copy() #it seems that the profile stops at the streefpeil. Ribasim will extrapolate this however. By keeping it this way, it remains clear what the streefpeil of the particular node_id is. 

    #combine all profiles by concatenating them, and sort on node_id, level and area.
    profile_total = pd.concat([profile_bottom, profile_slightly_above_bottom, profile_top])
    profile_total = profile_total.sort_values(by=['node_id', 'level', 'area'], ascending = True).reset_index(drop=True)

    #insert the new tables in the model
    ribasim_model.basin.profile.df = profile_total
    
    return 
    
    
def convert_mm_day_to_m_sec(mm_per_day):
    """
    Converts a rate from millimeters per day to meters per second.

    Parameters:
    mm_per_day (float): The rate in millimeters per day.

    Returns:
    float: The rate converted to meters per second.
    """
    seconds_per_day = 86400  # 24 hours * 60 minutes * 60 seconds
    meters_per_second = mm_per_day / 1000 / seconds_per_day
    
    return meters_per_second

def set_static_forcing(timesteps, timestep_size, start_time, forcing_dict, ribasim_model):
    """
    Generates static forcing data for the Ribasim-NL model simulation, assigning
    hydrological inputs to each node in a basin based on specified parameters.

    Parameters:
    timesteps (int): Number of timesteps to generate data for.
    timestep_size (str): Frequency of timesteps, formatted as a pandas date_range frequency string (e.g., 'D' for daily).
    start_time (str): Start date for the data range, formatted as 'YYYY-MM-DD'.
    forcing_dict (dict): Containing a dictionary of a single value in m/s for precipitation, potential_evaporation, drainage, infiltration, urban_runoff.
    ribasim_model (object): A model object containing the basin node data for assigning forcing inputs.

    Returns:
    None: Modifies the ribasim_model object in place by updating its basin static DataFrame with the new forcing data.
    return 
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





def add_discrete_control_nodes(ribasim_model):
    dfs_pump = ribasim_model.pump.static.df
    if "control_state" not in dfs_pump.columns.tolist() or pd.isnull(dfs_pump.control_state).all():
        control_states =["off", "on"]
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
            new_nodeid = 90000 + cur_max_nodeid + 1 # aanpassen loopt vanaf 90000 +1
        else:
            new_nodeid = cur_max_nodeid + 1
        # print(new_nodeid, end="\r")
        
        # @TODO Ron aangeven in geval van meerdere matches welke basin gepakt moet worden
        # basin is niet de beste variable name
        # Kan ook level boundary of terminal, dus na & weghalen
        # ["Basin", "LevelBoundary", "Terminal"]
        basin = ribasim_model.edge.df[((ribasim_model.edge.df.to_node_id == row.node_id) | (ribasim_model.edge.df.from_node_id == row.node_id)) & ((ribasim_model.edge.df.from_node_type == "Basin") | (ribasim_model.edge.df.to_node_type == "Basin"))]
        assert len(basin) >= 1 # In principe altijd 2 (check)
        # Hier wordt hardcoded de eerste gepakt (aanpassen adhv meta_aanvoerafvoer kolom)
        basin = basin.iloc[0, :].copy()
        if basin.from_node_type == "Basin":
            compound_variable_id = basin.from_node_id
            listen_node_id = basin.from_node_id
        else:
            compound_variable_id = basin.to_node_id
            listen_node_id = basin.to_node_id

        df_streefpeilen = ribasim_model.basin.area.df.set_index("node_id")
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
                    greater_than=[df_streefpeilen.at[listen_node_id, 'meta_streefpeil']], # streefpeil
                ),
                ribasim.nodes.discrete_control.Logic(
                    truth_state=["F", "T"], # aan uit wanneer groter dan streefpeil
                    control_state=control_states, # Werkt nu nog niet!
                ),
            ],
        )
        
        ribasim_model.edge.add(ribasim_model.discrete_control[new_nodeid], ribasim_model.pump[row.node_id])

    return



def set_tabulated_rating_curves(ribasim_model, level_increase=1.0, flow_rate=4):
    df_edge = ribasim_model.edge.df
    df_edge_tab = pd.merge(ribasim_model.tabulated_rating_curve.static.df,df_edge, left_on='node_id',right_on='to_node_id', how='inner')
    df_tab =  pd.merge(ribasim_model.basin.area.df,df_edge_tab, left_on='node_id', right_on='from_node_id', how='inner')
    df_tab = df_tab[['to_node_id', 'active', 'meta_streefpeil', 'flow_rate', 'control_state']]
    df_tab = df_tab.rename(columns={'meta_streefpeil':'level',
                                    'to_node_id':'node_id'})


    def adjust_rows(df):
        df['row_num'] = df.groupby('node_id').cumcount() + 1
        df.loc[df['row_num'] == 2, 'level'] += level_increase
        df.loc[df['row_num'] == 2, 'flow_rate'] = flow_rate
        df.drop(columns='row_num', inplace=True)
        
        return df

    # Apply the function
    df_tab.node_id = df_tab.node_id.astype(int)
    df_tab.level = df_tab.level.astype(float)
    df_tab.flow_rate = df_tab.flow_rate.astype(float)

    ribasim_model.tabulated_rating_curve.static.df = adjust_rows(df_tab)
    
    return

def set_tabulated_rating_curves_boundaries(ribasim_model, level_increase = 0.1, flow_rate = 40):
    # select the TRC which flow to a Terminal
    TRC_ter = ribasim_model.edge.df.copy()
    TRC_ter = TRC_ter.loc[(TRC_ter.from_node_type == 'TabulatedRatingCurve') & (TRC_ter.to_node_type == 'Terminal')] #select the correct nodes
    
    #not all TRC_ter's should be changed, as not all these nodes are in a boezem. Some are just regular peilgebieden. Filter these nodes for numerical stability
    #first, check where they originate from
    basins_to_TRC_ter = ribasim_model.edge.df.loc[ribasim_model.edge.df.to_node_id.isin(TRC_ter.from_node_id)]
    basins_to_TRC_ter = basins_to_TRC_ter.loc[basins_to_TRC_ter.from_node_type == 'Basin'] #just to be sure its a basin
    
    #check which basins are the boezem
    node_id_boezem = ribasim_model.basin.state.df.loc[ribasim_model.basin.state.df.meta_peilgebied_cat == 1, 'node_id'].values
    
    #now, only select the basins_to_TRC_ter which are a boezem
    boezem_basins_to_TRC_ter = basins_to_TRC_ter.loc[basins_to_TRC_ter.from_node_id.isin(node_id_boezem)]

    #plug these values in TRC_ter again to obtain the selection
    TRC_ter = TRC_ter.loc[TRC_ter.from_node_id.isin(boezem_basins_to_TRC_ter.to_node_id)]
    

    for i in range(len(TRC_ter)):
        node_id = TRC_ter.from_node_id.iloc[i] #retrieve node_id of the boundary TRC's        
        
        #adjust the Q(h)-relation in the ribasim_model.tabulated_rating_curve.static.df
        original_h = ribasim_model.tabulated_rating_curve.static.df.loc[ribasim_model.tabulated_rating_curve.static.df.node_id == node_id, 'level'].iloc[0]
        last_index = ribasim_model.tabulated_rating_curve.static.df.loc[ribasim_model.tabulated_rating_curve.static.df.node_id == node_id].index[-1] #this is the row with the highest Qh value, which should be changed

        #change the Qh relation on the location of the last index, for each node_id in TRC_ter
        ribasim_model.tabulated_rating_curve.static.df.loc[ribasim_model.tabulated_rating_curve.static.df.index == last_index, 'level'] = original_h + level_increase
        ribasim_model.tabulated_rating_curve.static.df.loc[ribasim_model.tabulated_rating_curve.static.df.index == last_index, 'flow_rate'] = flow_rate
        
    return
    
def write_ribasim_model(ribasim_model, path_ribasim_toml):
    """
    Writes the Ribasim model data and configuration to a directory.

    Parameters:
    ribasim_model (object): The Ribasim model object that contains the configuration to be saved.
    outputdir (str or Path): The base directory where the model configuration should be saved.
    modelcase (str): A string identifier for the use case, which will be used to create a subdirectory under the output directory.

    Returns:
    None
    """
    
    # # Create path objects
    # outputdir = pathlib.Path(outputdir)
    # modelcase_dir = pathlib.Path(modelcase)
    
    # # Create the full path if it does not exist
    # full_path = outputdir / modelcase_dir
    # full_path.mkdir(parents=True, exist_ok=True)

    # Write the Ribasim model configuration to the TOML file
    # ribasim_model.saveat(60)
    ribasim_model.write(path_ribasim_toml)
    
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
