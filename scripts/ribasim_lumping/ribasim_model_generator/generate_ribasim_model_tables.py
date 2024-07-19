import geopandas as gpd
import numpy as np
import pandas as pd
import random
import datetime


def generate_basin_profile_table(
    basin_h: pd.DataFrame, 
    basin_a: pd.DataFrame, 
    basins: pd.DataFrame, 
    set_name: int, 
    decimals=3, 
    dummy_model=False
):
    if dummy_model or basin_h is None:
        return pd.DataFrame(
            dict(
                node_id=np.repeat(basins.basin_node_id.values, 2), 
                level=[0.0, 1.0]*len(basins), 
                area=[1000.0, 1000.0]*len(basins)
            )
        )
    
    # take only basin node ids from basins
    basin_node_ids = basins.basin_node_id.values

    basin_profile = pd.DataFrame(columns=['node_id', 'level', 'area'])
    for basin_node_id in basin_node_ids:
        basin_profile_col = basin_h.loc[set_name, [basin_node_id]].reset_index(drop=True)
        basin_profile_col.columns = ['level']
        basin_profile_col["node_id"] = basin_node_id
        basin_profile_col['area'] = basin_a.loc[set_name, [basin_node_id]].reset_index(drop=True)
        basin_profile = pd.concat([basin_profile, basin_profile_col])
    basin_profile = basin_profile[basin_profile['level'].notna()]
    basin_profile["area"] = basin_profile["area"].replace(0.0, 0.001)
    basin_profile = basin_profile.reset_index(drop=True)
    basin_profile["level_diff"] = basin_profile["level"].diff()
    
    for node_id in basin_node_ids:
        basin_profile.loc[(basin_profile['node_id'] == node_id).idxmax(), 'level_diff'] = np.nan

    basin_profile = basin_profile[
        (basin_profile["level_diff"].diff(1) > 0.0001) | (basin_profile["level_diff"].isna())
    ].reset_index(drop=True)
    basin_profile = basin_profile.sort_values(
        by=["node_id", "level", "area"]
    ).drop_duplicates(
        subset=["node_id", "level"], 
        keep="first"
    )
    basin_profile["level"] = basin_profile["level"].round(4)

    new_basin_profile = pd.DataFrame()
    for basin_node_id, profile in basin_profile.groupby("node_id"):
        if len(profile) == 1:
            print(f" * Profile of Basin {basin_node_id} has only 1 record. one record is added.")
            profile.loc[len(basin_profile)] = profile.iloc[0]
            profile.loc[len(basin_profile), "level"] = profile.iloc[-1]["level"] + 1.0
            profile.loc[len(basin_profile), "area"] = profile.iloc[-1]["area"] * 2.0
        new_basin_profile = pd.concat([new_basin_profile, profile])
    new_basin_profile = new_basin_profile.sort_values(
        by=["node_id", "level", "area"]
    ).reset_index(drop=True)
    return new_basin_profile.drop(columns=["level_diff"])


def generate_basin_time_table_laterals(basins, basin_areas, laterals, laterals_data, saveat):
    laterals_basins = (
        laterals[["id", "geometry"]]
        .sjoin(basin_areas[["basin_node_id", "geometry"]]).drop(columns=["index_right"])
        [["id", "basin_node_id"]]
    )
    if saveat is not None:
        laterals_data = laterals_data.resample(f"{saveat}S").interpolate()
    
    # take only basin node ids from basins
    basin_node_ids = basins.basin_node_id.values

    timeseries = pd.DataFrame()
    for basin_node_id in basin_node_ids:
        if basin_node_id in basin_areas["basin_node_id"].to_numpy():
            laterals_basin = laterals_basins[laterals_basins["basin_node_id"]==basin_node_id]["id"].to_numpy()
            laterals_basin = [l for l in laterals_basin if l in laterals_data.columns]
            timeseries_basin = laterals_data[laterals_basin].sum(axis=1)
            timeseries_basin.name = "drainage"
            timeseries_basin = timeseries_basin.to_frame()
        else:
            timeseries_basin = pd.DataFrame(index=laterals_data.index, columns=["drainage"])
            timeseries_basin["drainage"] = 0.0
        timeseries_basin.index.name = "time"
        timeseries_basin = timeseries_basin.reset_index()

        timeseries_basin["potential_evaporation"] = 0.0
        timeseries_basin["precipitation"] = 0.0
        timeseries_basin["infiltration"] = 0.0
        timeseries_basin["urban_runoff"] = 0.0
        timeseries_basin["node_id"] = basin_node_id
        
        timeseries = pd.concat([
            timeseries,
            timeseries_basin
        ])
    timeseries = timeseries.sort_values(["time", "node_id"]).reset_index(drop=True)
    timeseries = timeseries[["time", "node_id", "precipitation", "potential_evaporation", "drainage", "infiltration", "urban_runoff"]]

    return timeseries


def generate_basin_time_table_laterals_areas_data(basins, areas, laterals_areas_data):
    timeseries = pd.DataFrame()

    # take only basin node ids from basins
    basin_node_ids = basins.basin_node_id.values

    for basin_node_id in basin_node_ids:
        areas_basin = list(areas[areas['basin_node_id'] == basin_node_id]['area_code'].unique())
        timeseries_basin = laterals_areas_data[areas_basin].sum(axis=1).to_frame().rename(columns={0: 'Netto_flux'}).reset_index()
        
        timeseries_basin["drainage"] = timeseries_basin["Netto_flux"][timeseries_basin["Netto_flux"]>0]
        timeseries_basin["drainage"] = timeseries_basin["drainage"].fillna(0.0)

        timeseries_basin["infiltration"] = timeseries_basin["Netto_flux"][timeseries_basin["Netto_flux"]<0]
        timeseries_basin["infiltration"] = timeseries_basin["infiltration"].fillna(0.0).abs()
        
        timeseries_basin["potential_evaporation"] = 0.0
        timeseries_basin["precipitation"] = 0.0
        timeseries_basin["urban_runoff"] = 0.0
        timeseries_basin["node_id"] = basin_node_id

        timeseries = pd.concat([
            timeseries,
            timeseries_basin
        ])
    
    timeseries = timeseries.sort_values(["time", "node_id"]).reset_index(drop=True)

    timeseries_variables = ["precipitation", "potential_evaporation", "drainage", "infiltration", "urban_runoff"]
    timeseries = timeseries[["time", "node_id"] + timeseries_variables]
    timeseries[timeseries_variables] = timeseries[timeseries_variables].round(6)

    return timeseries


def generate_basin_time_table_laterals_drainage_per_ha(basins, basin_areas, laterals_drainage_per_ha):
    laterals_drainage_per_ha.name = "drainage"
    laterals_drainage_per_ha = laterals_drainage_per_ha.resample("H").interpolate()
    drainage_m3_s_ha = laterals_drainage_per_ha.to_frame() / 1000.0
    drainage_m3_s_ha.index.name = "time"

    basin_areas_ha = basin_areas.set_index('basin_node_id')

    timeseries = pd.DataFrame()
    for basin_no in basins["basin_node_id"].values:
        timeseries_basin = drainage_m3_s_ha.reset_index()
        timeseries_basin["potential_evaporation"] = 0.0
        timeseries_basin["precipitation"] = 0.0
        timeseries_basin["infiltration"] = 0.0
        timeseries_basin["urban_runoff"] = 0.0
        timeseries_basin["node_id"] = basin_no

        if basin_no in basin_areas_ha.index:
            area = basin_areas_ha.loc[basin_no, 'area_ha']
            timeseries_basin["drainage"] = timeseries_basin["drainage"] * area
        else:
            timeseries_basin["drainage"] = 0.0

        timeseries = pd.concat([
            timeseries,
            timeseries_basin
        ])
    timeseries = timeseries.sort_values(["time", "node_id"]).reset_index(drop=True)
    timeseries = timeseries[["time", "node_id", "precipitation", "potential_evaporation", "drainage", "infiltration", "urban_runoff"]]

    return timeseries


def generate_basin_time_table(
    method_laterals: int,
    basins: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame,
    dummy_model: bool,
    basin_state: pd.DataFrame,
    basin_areas: gpd.GeoDataFrame,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    saveat: int,
    laterals=None,
    laterals_data=None,
    laterals_areas_data=None,
    laterals_drainage_per_ha=None,
):
    basin_node_ids = basins.basin_node_id.values

    if dummy_model:
        basin_time = pd.concat([
            pd.DataFrame(
                dict(
                    time=pd.date_range(start_time, end_time),
                    node_id=node_id,
                    precipitation=random.uniform(0.0, 1.0) / 365.0 / 24.0 / 3600.0,
                    potential_evaporation=random.uniform(0, 500) / 365.0 / 24.0 / 3600.0,
                    drainage=0.0,
                    infiltration=0.0,
                    urban_runoff=0.0
                )
            )
            for node_id in basin_node_ids
        ]).sort_values(by=["time", "node_id"]).reset_index(drop=True)
        print('laterals: dummy model, period 2020-01-01 - 2021-01-01')
    elif method_laterals == 1:
        if laterals is None or laterals_data is None:
            raise ValueError("method_laterals = 1 requires laterals and laterals_areas_data")
        basin_time = generate_basin_time_table_laterals(
            basins=basins, 
            basin_areas=basin_areas, 
            laterals=laterals,
            laterals_data=laterals_data,
            saveat=saveat
        )
        print('laterals: based on lateral inflow according to dhydro network')
    elif method_laterals == 2:
        if laterals_areas_data is None:
            raise ValueError("method_laterals = 2 requires laterals_areas_data")
        basin_time = generate_basin_time_table_laterals_areas_data(
            basins=basins, 
            areas=areas, 
            laterals_areas_data=laterals_areas_data
        )
        print('laterals: based on lateral inflow (timeseries) per area')
    elif method_laterals == 3:
        if laterals_drainage_per_ha is None:
            raise ValueError("method_laterals = 3 requires laterals_drainage_per_ha")
        basin_time = generate_basin_time_table_laterals_drainage_per_ha(
            basins=basins, 
            basin_areas=basin_areas, 
            laterals_drainage_per_ha=laterals_drainage_per_ha
        )
        print('laterals: based on homogeneous lateral inflow timeseries')
    else:
        raise ValueError('method_laterals should be 1, 2 or 3')
    
    return basin_time


def generate_tabulated_rating_curve(
    basins_outflows, tabulated_rating_curves, basin_h, \
    edge_q_df, weir_q_df, uniweir_q_df, orifice_q_df, culvert_q_df, bridge_q_df, pump_q_df, \
    set_name, dummy_model=False
):
    basins_outflows_sel = basins_outflows.copy()
    basins_outflows_sel.columns = ["__".join(c) for c in basins_outflows_sel.columns]
    basins_outflows_sel = basins_outflows_sel[["general__split_node", "structure__crestwidth", set_name + "__crestlevel"]]
    basins_outflows_sel.columns = ["split_node", "crestwidth", "crestlevel"]
    
    tabulated_rating_curves = tabulated_rating_curves.merge(
        basins_outflows_sel, 
        how="left", 
        left_on="split_node", 
        right_on="split_node", 
    )

    curves = pd.DataFrame()
    for i, trc in tabulated_rating_curves.iterrows():
        if dummy_model:
            curve = pd.DataFrame(
                dict(
                    node_id=trc["split_node_node_id"],
                    level=[0.0, 1.0, 1.1],
                    flow_rate=[0.0, 0.0, 1.0]
                )
            )
            curves = pd.concat([curves, curve])
            continue

        def weir_formula(crestlevel, crestwidth, waterlevel):
            return 2.0/3.0 * max(0.0, waterlevel - crestlevel)**(3.0/2.0) * (2*9.81)**0.5 * crestwidth

        basin_node_id = trc["from_node_id"]
        water_levels_basin = basin_h.loc[set_name][basin_node_id]
        targetlevel = trc["crestlevel"]
        
        split_type = trc["split_type"]
        split_node_name = trc["split_node_id"]
        
        discharges_list = dict(
            weir=weir_q_df,
            universalWeir=uniweir_q_df,
            orifice=orifice_q_df,
            culvert=culvert_q_df,
            bridge=bridge_q_df,
            pump=pump_q_df,
            openwater=edge_q_df,
        )
        discharges = discharges_list[split_type]
        if discharges is None:
            curve = pd.DataFrame()
            curve["level"] = water_levels_basin
            curve["flow_rate"] = curve.apply(lambda x: weir_formula(trc.crestlevel, trc.crestwidth, x["level"]), axis=1).fillna(0.0)
            curve["node_id"] = trc["split_node_node_id"]
            curves = pd.concat([curves, curve[["node_id", "level", "flow_rate"]]])
            continue
        else:
            discharges = discharges.loc[set_name][split_node_name]
            curve = pd.concat([water_levels_basin, discharges.replace(-999.0, np.nan)], axis=1)

        curve.columns = ["level", "flow_rate"]
        curve.iloc[0]["flow_rate"] = 0.0
        curve.iloc[1]["flow_rate"] = 0.0
        curve.iloc[2]["flow_rate"] = 0.0
        curve.iloc[3]["flow_rate"] = 0.0
        curve["node_id"] = trc["split_node_node_id"]
        curve = curve.sort_values(by=["level", "flow_rate"])

        curve = curve.interpolate().bfill().reset_index(drop=True).round(4)
        curve["meta_condition"] = curve.index

        # flow_rate can not be negative (?)
        curve.loc[curve["flow_rate"] < 0.0, "flow_rate"] = 0.0
        # sort on level and flow rate and keep first occurence of level
        
        # check whether flow_rate is decreasing, remove points and count number of occurences
        no_negative_total = 0
        no_total = len(curve)
        for i in range(len(curve)):
            curve["flow_rate_prev"] = curve["flow_rate"].diff()
            curve.loc[curve["flow_rate_prev"] < 0.0, "flow_rate"] = np.nan
            no_negative = len(curve[curve["flow_rate"].isna()])
            if no_negative == 0:
                break
            no_negative_total += no_negative
            curve = curve[~curve["flow_rate"].isna()]
        if no_negative_total > 0:
            print(f" x {trc.split_node_id} ({split_type}): {no_negative_total}/{no_total} records with decreasing discharge")
        curve = curve.drop(columns="flow_rate_prev").reset_index(drop=True)

        # check whether flow_rate is equal to zero
        if curve["flow_rate"].max() < 0.001:
            print(f" x basin_node_id {basin_node_id}: no discharge over split_node {trc.split_node_id} ({split_type})")
            curve.loc[0, "level"] = trc.crestlevel - 0.01
            curve.loc[0, "flow_rate"] = 0.0
            curve.loc[1:, "level"] = [i*0.05 + trc.crestlevel for i in range(0, len(curve)-1)]
            curve["flow_rate"] = curve.apply(lambda x: weir_formula(trc.crestlevel, trc.crestwidth, x["level"]), axis=1).fillna(0.0)
        
        curves = pd.concat([curves, curve.drop_duplicates(subset="level", keep="first")])
    return curves.drop_duplicates().reset_index(drop=True)


def generate_manning_resistances(manningresistance, set_name):
    return pd.DataFrame(
        dict(
            node_id=manningresistance["split_node_node_id"],
            length=[750.0]*len(manningresistance),
            manning_n=[0.04]*len(manningresistance),
            profile_width=[5.0]*len(manningresistance),
            profile_slope=[3.0]*len(manningresistance),
        )
    )


def generate_linear_resistances(linearresistance, set_name):
    return pd.DataFrame(
        dict(
            node_id=linearresistance["split_node_node_id"],
            resistance=[0.02]*len(linearresistance),
            max_flow_rate=[10000.0]*len(linearresistance),
        )
    )


def generate_boundary_time_table(boundaries, boundaries_data, boundary_type):
    if boundaries.empty:
        return None

    # boundary table from csv with timeseries per boundary_node
    timeseries = pd.DataFrame()
    for i_boundary, boundary in boundaries.iterrows():
        # print(boundary)
        boundary_no = boundary["name"]

        # timeseries_boundary = boundaries_csv_data[boundary_no].sum(axis=1).to_frame().rename(columns={0: 'flow'}).reset_index()
        timeseries_boundary = boundaries_data[[boundary_no]].reset_index()
        timeseries_boundary= timeseries_boundary.rename(columns={boundary_no: boundary_type})
        timeseries_boundary[boundary_type] = timeseries_boundary[boundary_type][timeseries_boundary[boundary_type]>0]
        timeseries_boundary[boundary_type] = timeseries_boundary[boundary_type].fillna(0.0)

        timeseries_boundary["node_id"] = boundary["boundary_node_id"]

        timeseries = pd.concat([
            timeseries,
            timeseries_boundary
        ])
    timeseries = timeseries.sort_values(["time", "node_id"]).reset_index(drop=True)
    timeseries = timeseries[["time","node_id", boundary_type]]
    return timeseries


def generate_boundary_static_data(boundaries, boundaries_static_data):
    # test
    dummyvalue = 999
    boundary_data = pd.DataFrame()
    for boundary in boundaries:
        boundary_no = boundary["name"]

        boundary_data_boundary = boundaries_static_data[boundary_no].sum(axis=1).to_frame().rename(columns={0: 'flow'}).reset_index()
        boundary_data_boundary["flow"] = boundary_data_boundary["flow"].fillna(dummyvalue)
        boundary_data_boundary["node_id"] = boundary_no

        boundary_data = pd.concat([
            boundary_data,
            boundary_data_boundary
        ])
    boundary_data = boundary_data.sort_values(["node_id"]).reset_index(drop=True)
    boundary_data = boundary_data[["node_id", "flow"]]
    return boundary_data


def generate_basin_state_table(basin_h_initial, basin_profile, basin_h, set_name=None, dummy_model=False):
    if dummy_model:
        basin_state = basin_profile.groupby("node_id").max()[["level"]].reset_index()
        return basin_state

    if basin_h_initial is None:
        basin_state = basin_h.loc[(set_name, "targetlevel")].rename("level").reset_index().rename(columns={"basin_node_id": "node_id"})
        print('basin-state: generated based on targetlevel')
    else:                            
        basin_state = basin_h_initial.rename("level").reset_index().rename(columns={"basin_node_id": "node_id"})
        print('basin-state: generated as defined')
    
    # check whether initial water level is higher than bedlevel, if not: use bedlevel
    bedlevel = basin_profile.groupby("node_id").min()[["level"]].rename(columns={"level": "bedlevel"})
    state = basin_state.set_index("node_id")[["level"]]
    state = bedlevel.merge(state, how='left', left_index=True, right_index=True).max(axis=1)
    state.name = "level"
    basin_state = basin_state.drop(columns=["level"]).merge(state, how='left', left_on="node_id", right_index=True)
    basin_state["level"] = basin_state["level"].round(4)
    return basin_state


def generate_basin_subgrid_table(basins_nodes_h_relation, set_name, dummy_model=False, basins=None):
    if basins_nodes_h_relation is None:
        if not dummy_model: 
            raise ValueError("basins_nodes_h_relation not OK")
        elif basins is None:
            raise ValueError("basins_nodes_h_relation not OK")
        
        no_subgrid_per_basin = 3
        levels_basins = [0.0, 1.0, 2.0]
        levels_subgrids = [0.0, 1.0, 2.0]
        return pd.DataFrame(
            dict(
                subgrid_id=np.repeat(range(len(basins) * len(levels_basins)), no_subgrid_per_basin),
                node_id=np.repeat(basins.basin_node_id, no_subgrid_per_basin * len(levels_basins)),
                basin_level=levels_basins * no_subgrid_per_basin * len(basins),
                subgrid_level=levels_subgrids * no_subgrid_per_basin * len(basins)
            )
        )
    
    basin_subgrid = basins_nodes_h_relation[
        (basins_nodes_h_relation["set"] == set_name) &
        (basins_nodes_h_relation["basin_node_id"] != -1)
    ]

    no_subgrid_nodes = len(basin_subgrid["node_no"].unique())
    no_conditions = len(basin_subgrid["condition"].unique())
    basin_subgrid["meta_condition"] = np.repeat(list(range(no_conditions)), no_subgrid_nodes)

    basin_subgrid = basin_subgrid[["node_no", "basin_node_id", "basin_h", "node_no_h", "meta_condition", "geometry"]]
    basin_subgrid["meta_x"] = basin_subgrid.geometry.x
    basin_subgrid["meta_y"] = basin_subgrid.geometry.y

    basin_subgrid = basin_subgrid.rename(columns={
        "node_no": "subgrid_id", 
        "basin_node_id": "node_id", 
        "basin_h": "basin_level", 
        "node_no_h": "subgrid_level"
    }).drop(columns="geometry")
    basin_subgrid = basin_subgrid.round(5).sort_values(by=["subgrid_id", "node_id", "basin_level"])

    for i in range(3):
        basin_subgrid["basin_level_diff"] = basin_subgrid["basin_level"].diff(1)
        basin_subgrid["subgrid_level_diff"] = basin_subgrid["subgrid_level"].diff(1)
        for node_id in basin_subgrid.node_id.unique():
            basin_subgrid.loc[(basin_subgrid['node_id'] == node_id).idxmin(), ['basin_level_diff', 'subgrid_level_diff']] = np.nan
        basin_subgrid = basin_subgrid[
            ((basin_subgrid["basin_level_diff"] > 0.0001) | basin_subgrid["basin_level_diff"].isna()) & 
            ((basin_subgrid["subgrid_level_diff"] > 0.0001) | basin_subgrid["subgrid_level_diff"].isna())
        ].reset_index(drop=True)
    
    basin_subgrid = basin_subgrid.drop(columns=["basin_level_diff", "subgrid_level_diff"])
    basin_subgrid = basin_subgrid.sort_values(by=["node_id", "subgrid_id", "meta_condition"])
    basin_subgrid = basin_subgrid.reset_index(drop=True)
    return basin_subgrid


def generate_ribasim_model_tables(dummy_model, basin_h, basin_a, basins, areas, basin_areas, basins_nodes_h_relation,
    laterals, laterals_data, boundaries, boundaries_data, split_nodes, basins_outflows, set_name, 
    method_boundaries, boundaries_timeseries_data, 
    method_laterals, laterals_areas_data, laterals_drainage_per_ha, basin_h_initial, 
    saveat, edge_q_df, weir_q_df, uniweir_q_df, orifice_q_df, culvert_q_df, bridge_q_df, pump_q_df):

    start_time = pd.Timestamp(2020, 1, 1, 0, 0, 0)
    end_time = pd.Timestamp(2021, 1, 1, 0, 0, 0)

    # create tables for BASINS
    tables = dict()
    print('basin-profile: generated')
    tables['basin_profile'] = generate_basin_profile_table(
        basin_h=basin_h, 
        basin_a=basin_a, 
        basins=basins, 
        decimals=3, 
        set_name=set_name,
        dummy_model=dummy_model
    )

    # create tables for INITIAL STATE
    print('basin-state: generated')
    tables["basin_state"] = generate_basin_state_table(
        basin_h_initial=basin_h_initial, 
        basin_profile=tables["basin_profile"], 
        basin_h=basin_h, 
        set_name=set_name, 
        dummy_model=dummy_model
    )
    
    # create tables for BASIN AREAS
    print('basin-areas: generated')
    basin_areas = basin_areas[basin_areas.basin_node_id.isin(basins.basin_node_id)]
    tables["basin_areas"] = basin_areas[["basin_node_id", "geometry"]].rename(columns={"basin_node_id": "node_id"})
    tables["basin_areas"]["meta_color_code"] = tables["basin_areas"].index % 50
    
    # create subgrid
    print("subgrid: based on water level relation basin and nodes")
    tables['basin_subgrid'] = generate_basin_subgrid_table(
        basins_nodes_h_relation=basins_nodes_h_relation,
        set_name=set_name,
        dummy_model=dummy_model,
        basins=basins
    )

    # create laterals table
    tables['basin_time'] = generate_basin_time_table(
        method_laterals=method_laterals,
        basins=basins,
        areas=areas,
        dummy_model=dummy_model,
        basin_state=tables["basin_state"],
        basin_areas=basin_areas,
        start_time=start_time,
        end_time=end_time,
        saveat=saveat,
        laterals=laterals,
        laterals_data=laterals_data,
        laterals_areas_data=laterals_areas_data,
        laterals_drainage_per_ha=laterals_drainage_per_ha,
    )

    # create tables for BOUNDARIES
    flow_boundaries = boundaries[boundaries['boundary_type']=="FlowBoundary"]
    level_boundaries = boundaries[boundaries['boundary_type']=="LevelBoundary"]

    if dummy_model:
        tables['flow_boundary_time'] = pd.concat([
            pd.DataFrame(
                dict(
                    time=pd.date_range(start_time, end_time),
                    node_id=node_id,
                    flow_rate=1.0
                )
            )
            for node_id in flow_boundaries.boundary_node_id.values
        ]).sort_values(by=["time", "node_id"]).reset_index(drop=True)
        tables['flow_boundary_static'] = None
        print(f'flowboundaries: dummy_model --> flow_rate=1.0m3/s')

        tables['level_boundary_time'] = pd.concat([
            pd.DataFrame(
                dict(
                    time=pd.date_range(start_time, end_time),
                    node_id=node_id,
                    level=0.0
                )
            )
            for node_id in level_boundaries.boundary_node_id.values
        ]).sort_values(by=["time", "node_id"]).reset_index(drop=True)
        tables['level_boundary_static'] = None
        print(f'levelboundaries: dummy_model --> level=1.0m')
    
    elif method_boundaries and boundaries_timeseries_data is not None:
        print(f'flowboundaries: based on timeseries ({len(flow_boundaries)} flowboundaries)')
        tables['flow_boundary_time'] = generate_boundary_time_table(
            flow_boundaries, 
            boundaries_timeseries_data,
            boundary_type='flow_rate',
        )
        tables['flow_boundary_static'] = None

        print(f'levelboundaries: based on timeseries ({len(level_boundaries)} levelboundaries)')
        tables['level_boundary_time'] = generate_boundary_time_table(
            level_boundaries, 
            boundaries_timeseries_data,
            boundary_type='level',
        )  
        tables['level_boundary_static'] = None

    else:
        # oud
        tables['flow_boundary_static'] = pd.DataFrame(
            dict(
                node_id=flow_boundaries["boundary_node_id"],
                flow_rate=[0.0] * len(flow_boundaries),
            )
        )
        tables['flow_boundary_time'] = None

        tables['level_boundary_static'] = pd.DataFrame(
            dict(
                node_id=level_boundaries["boundary_node_id"],
                level=[7.15] * len(level_boundaries),
            )
        )
        tables['level_boundary_time'] = None
    
    # create tables for PUMPS
    pumps = split_nodes[split_nodes['ribasim_type'] == 'Pump']
    print(f"pumps: generated ({len(pumps)} pumps)")
    if len(pumps) == 0:
        tables['pump_static'] = None
    elif dummy_model:
        tables['pump_static'] = pd.DataFrame(
            dict(
                node_id=pumps["split_node_node_id"],
                flow_rate=[0.0] * len(pumps),
            )
        )
    else:
        tables['pump_static'] = pd.DataFrame(
            dict(
                node_id=pumps["split_node_node_id"],
                flow_rate=[0.0] * len(pumps),
            )
        )

    # create tables for OUTLETS
    outlets = split_nodes[split_nodes['ribasim_type'] == 'Outlet']
    print(f"outlets: generated ({len(outlets)} outlets)")
    if len(outlets) == 0:
        tables['outlet_static'] = None
    elif dummy_model:
        tables['outlet_static'] = pd.DataFrame(
            dict(
                node_id=outlets["split_node_node_id"],
                flow_rate=[0.0] * len(outlets),
            )
        )
    else:
        tables['outlet_static'] = pd.DataFrame(
            dict(
                node_id=outlets["split_node_node_id"],
                flow_rate=[0.0] * len(outlets),
            )
        )

    # create tables for TABULATED RATING CURVES
    tabulated_rating_curves = split_nodes[split_nodes['ribasim_type'] == 'TabulatedRatingCurve']
    print(f"tabulated_rating_curves: generated ({len(tabulated_rating_curves)} tabulated_rating_curves)")
    tables['tabulated_rating_curve_static'] = generate_tabulated_rating_curve(
        basins_outflows, tabulated_rating_curves, 
        basin_h, edge_q_df, weir_q_df, uniweir_q_df, 
        orifice_q_df, culvert_q_df, bridge_q_df, pump_q_df, set_name, dummy_model
    )
    
    # create tables for MANNING RESISTANCE
    manningresistance = split_nodes[split_nodes['ribasim_type'] == 'ManningResistance']
    print(f"manningresistance: generated ({len(manningresistance)} manningresistance)")
    tables['manningresistance_static'] = generate_manning_resistances(manningresistance, set_name)

    # create tables for LINEAR RESISTANCE
    linearresistance = split_nodes[split_nodes['ribasim_type'] == 'LinearResistance']
    print(f"linearresistance: generated ({len(linearresistance)} linearresistance)")
    tables['linearresistance_static'] = generate_linear_resistances(linearresistance, set_name)

    return tables
