import math

import geopandas as gpd
import numpy as np
import pandas as pd
import scipy


def generate_ribasim_types_for_all_split_nodes(
        boundaries: gpd.GeoDataFrame, 
        split_nodes: gpd.GeoDataFrame, 
        basins: gpd.GeoDataFrame, 
        split_node_type_conversion: dict, 
        split_node_id_conversion: dict,
    ) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """"
    Generate Ribasim Types for all split nodes
    """

    print(f" - define Ribasim-Nodes types based on input conversion table(s)")
    # Basins
    basins["ribasim_type"] = "Basin"
    basins["name"] = "Basin"

    # Boundaries
    boundaries["ribasim_type"] = boundaries["boundary_type"]
    boundaries["name"] = boundaries["boundary_name"]

    # Split nodes
    removed_split_nodes = None
    if not split_nodes[~split_nodes.status].empty:
        removed_split_nodes = split_nodes[~split_nodes.status].copy()
        print(f"   * {len(removed_split_nodes)} split_nodes resulting in no_split")
        split_nodes = split_nodes[split_nodes.status]

    split_nodes["ribasim_type"] = "TabulatedRatingCurve" 
    split_nodes_conversion = {
        "weir": "TabulatedRatingCurve",
        "uniweir": "TabulatedRatingCurve",
        "pump": "Pump",
        "culvert":"ManningResistance",
        "manual": "ManningResistance",
        "orifice": "TabulatedRatingCurve",
    }
    if isinstance(split_node_type_conversion, dict):
        for key, value in split_node_type_conversion.items():
            split_nodes_conversion[key] = value
    split_nodes["ribasim_type"] = split_nodes["split_type"].replace(split_nodes_conversion)

    if isinstance(split_node_id_conversion, dict):
        for key, value in split_node_id_conversion.items():
            if len(split_nodes[split_nodes["split_node_id"] == key]) == 0:
                print(f"   * split_node type conversion id={key} (type={value}) does not exist")
            split_nodes.loc[split_nodes["split_node_id"] == key, "ribasim_type"] = value

    # add removed split nodes back into gdf
    if removed_split_nodes is not None:
        split_nodes = pd.concat([split_nodes, removed_split_nodes], axis=0)
    return boundaries, split_nodes, basins


def extract_bed_level_surface_storage(volume_data, nodes):
    increment = volume_data.increment.data[0]

    # bedlevel
    bedlevel = volume_data.bedlevel.to_dataframe().T
    bedlevel.columns.name = "node_no"
    bedlevel.index = ["bedlevel"]

    if "bedlevel" not in nodes.columns:
        nodes = nodes.merge(bedlevel.T, left_on="node_no", right_index=True)
    # get lowest bedlevel within basin
    basins_bedlevels = nodes[["basin", "bedlevel"]].groupby(by="basin").min()
    bedlevel_basin = nodes[["basin"]].merge(basins_bedlevels, left_on="basin", right_index=True)
    bedlevel_basin = bedlevel_basin[["bedlevel"]].T

    # surface from volume_data
    surface_df = volume_data.surface.to_dataframe().unstack()
    surface_df = surface_df.replace(0.0, np.nan).ffill(axis=1)
    surface_df.index.name = "node_no"
    surface_df = surface_df["surface"].T
    surface_df = pd.concat([bedlevel - 0.01, surface_df]).reset_index(drop=True)
    surface_df.iloc[0] = 0

    # define zlevels
    zlevels = bedlevel - 0.01
    for i in range(1, len(surface_df)):
        zlevels = pd.concat([zlevels, bedlevel + increment * (i-1)])
    zlevels = zlevels.reset_index(drop=True)
    z_range = np.arange(np.floor(zlevels.min().min()), np.ceil(zlevels.max().max())+increment, increment)

    # find surface levels simulation using interpolation
    node_surface_df = pd.DataFrame(index=z_range, columns=surface_df.columns)
    node_surface_df.index.name = "zlevel"
    for col in node_surface_df.columns:
        df_data_col = pd.DataFrame(index=zlevels[col], data=surface_df[col].values, columns=[col])[col]
        node_surface_df[col] = np.interp(z_range, zlevels[col].values, df_data_col.values)

    node_storage_df = ((node_surface_df + node_surface_df.shift(1))/2.0 * increment).cumsum()

    node_bedlevel = bedlevel_basin
    node_bedlevel.index = ["bedlevel"]
    node_bedlevel.index.name = "condition"

    orig_bedlevel = bedlevel.T
    return node_surface_df, node_storage_df, node_bedlevel, orig_bedlevel


def get_waterlevels_table_from_simulations(map_data):
    if map_data is None:
        return None
    node_h_df1 = map_data.mesh1d_s1.to_dataframe().unstack().mesh1d_s1
    old_index = node_h_df1.index.copy()
    node_h_df = pd.concat([
        node_h_df1[col].sort_values().reset_index(drop=True)
        for col in node_h_df1.columns
    ], axis=1)
    node_h_df.index = old_index
    return node_h_df


def get_discharges_table_from_simulations(map_data):
    if map_data is None:
        return None
    return map_data.mesh1d_q1.to_dataframe().unstack().mesh1d_q1


def get_discharges_table_structures_from_simulations(his_data):
    if his_data is None:
        return None, None, None, None, None, None
    if "weirgen_discharge" in his_data.keys():
        weir_q_df = his_data.weirgen_discharge.to_dataframe().unstack().weirgen_discharge
    else:
        weir_q_df = None
    if "uniweir_discharge" in his_data.keys():
        uniweir_q_df = his_data.uniweir_discharge.to_dataframe().unstack().uniweir_discharge
    else:
        uniweir_q_df = None
    if "orifice_discharge" in his_data.keys():
        orifice_q_df = his_data.orifice_discharge.to_dataframe().unstack().orifice_discharge
    else:
        orifice_q_df = None
    if "culvert_discharge" in his_data.keys():
        culvert_q_df = his_data.culvert_discharge.to_dataframe().unstack().culvert_discharge
    else:
        culvert_q_df = None
    if "bridge_discharge" in his_data.keys():
        bridge_q_df = his_data.bridge_discharge.to_dataframe().unstack().bridge_discharge
    else:
        bridge_q_df = None
    if "pump_structure_discharge" in his_data.keys():
        pump_q_df = his_data.pump_structure_discharge.to_dataframe().unstack().pump_structure_discharge
    else:
        pump_q_df = None
    return weir_q_df, uniweir_q_df, orifice_q_df, culvert_q_df, bridge_q_df, pump_q_df


def get_basins_outflows_including_settings(split_nodes, basin_connections, boundary_connections, 
                                           weirs, uniweirs, pumps, culverts, orifices, set_names):
    gdfs_list = [weirs, uniweirs, pumps, culverts, orifices]
    gdfs_columns_list = [
        [("general", "structure_id"), ("structure", "crestwidth")] + [(set_name, "crestlevel") for set_name in set_names],
        [("general", "structure_id")] + [(set_name, "crestlevel") for set_name in set_names],
        [("general", "structure_id"), ("structure", "orientation"), ("structure", "controlside"), ("structure", "numstages"), ("structure", "capacity")] + 
            [(set_name, "startlevelsuctionside") for set_name in set_names] + 
            [(set_name, "stoplevelsuctionside") for set_name in set_names],
        [("general", "structure_id"), ("structure", "crestlevel")],
        [("general", "structure_id"), ("structure", "crestwidth"), ("structure", "crestlevel")],
    ]
    gdf_total = gpd.GeoDataFrame()
    for gdf, gdf_columns in zip(gdfs_list, gdfs_columns_list):
        if gdf is None:
            continue
        gdf_total = pd.concat([gdf_total, gdf[gdf_columns]])

    gdf_total = gdf_total[["general", "structure"] + set_names]

    basins_split_nodes = split_nodes[["split_node", "split_node_id", "split_node_node_id", "ribasim_type"]]
    basins_outflows1 = basin_connections[
        basin_connections["connection"]=="basin_to_split_node"
    ]
    basins_outflows1 = (
        basins_outflows1[["basin", "split_node"]]
        .merge(basins_split_nodes, how="left", on="split_node")
    )
    if "structure_id" in basins_outflows1.columns:
        basins_outflows1 = basins_outflows1.drop(columns=["structure_id"])
    
    basins_outflows = pd.concat([basins_outflows1], keys=['general'], axis=1)

    gdf_total.columns = ['__'.join(col).strip('__') for col in gdf_total.columns.values]
    basins_outflows.columns = ['__'.join(col).strip('__') for col in basins_outflows.columns.values]
    basins_outflows = basins_outflows.merge(
        gdf_total, 
        how="left", 
        left_on="general__split_node_id", 
        right_on="general__structure_id"
    )
    basins_outflows = (
        basins_outflows
        .sort_values(by="general__basin")
        .reset_index(drop=True)
        .drop(columns=["general__structure_id"])
    )
    
    # basins_outflows2 = boundary_connections[boundary_connections.connection=="basin_to_split_node"]
    # basins_outflows2 = basins_outflows2[["basin", "split_node", "split_node_id"]]
    # basins_outflows2["ribasim_type"] = "ManningResistance"
    # basins_outflows2.columns = ["general__" + col for col in basins_outflows2.columns]

    # basins_outflows = pd.concat([basins_outflows1, basins_outflows2])

    for set_name in set_names:
        basins_outflows[set_name + "__targetlevel"] = np.nan
        if set_name + "__crestlevel" in basins_outflows.columns:
            basins_outflows[set_name + "__targetlevel"] = basins_outflows[set_name + "__targetlevel"].fillna(basins_outflows[set_name + "__crestlevel"])
        if set_name + "__stoplevelsuctionside" in basins_outflows.columns: 
            basins_outflows[set_name + "__targetlevel"] = basins_outflows[set_name + "__targetlevel"].fillna(basins_outflows[set_name + "__stoplevelsuctionside"])
        basins_outflows["general__split_node_node_id"] = basins_outflows["general__split_node_node_id"].fillna(-1).astype(int)

    gdf_total.columns = gdf_total.columns.str.split("__", expand=True)
    basins_outflows.columns = basins_outflows.columns.str.split("__", expand=True)
    basins_outflows = basins_outflows[["general", "structure"] + set_names]

    return basins_outflows.reset_index(drop=True)


def get_targetlevels_nodes_using_weirs_pumps(nodes, basins_outflows, set_names, name_column="targetlevel"):
    basins_outflows = basins_outflows[["general"] + set_names]
    basins_outflows_columns = [("general", "basin")] + [(set_name, name_column) for set_name in set_names]
    basins_outflows_sel = basins_outflows[basins_outflows_columns]
    basins_outflows_sel.columns = ["basin"] + set_names
    def convert_list_str_to_float(x):
        if isinstance(x, float):
            return x
        if isinstance(x, list):
            x = x[0]
            if isinstance(x, float):
                return x
            return x[0]
        if isinstance(x, str):
            while x[0] == '[':
                x = x[1:-1]
            return float(x)
    for set_name in set_names:
        basins_outflows_sel[set_name] = basins_outflows_sel[set_name].apply(lambda x: convert_list_str_to_float(x)).astype(float)

    basins_outflows_crest_levels = basins_outflows_sel.groupby(by="basin").min()
    node_targetlevel = nodes[["node_no", "basin"]].merge(
        basins_outflows_crest_levels, 
        how="left",
        on="basin",
    ).set_index("node_no")
    node_targetlevel.index.name = "node_no"
    node_targetlevel.columns.name = "condition"
    return node_targetlevel[set_names].T


def generate_node_waterlevels_table(node_h_df, node_bedlevel, node_targetlevel, interpolation_lines, set_names):
    node_nan = node_targetlevel.loc[[set_names[0]]].copy()
    node_nan.loc[:, :] = np.nan

    # create x additional interpolation lines between targetlevel and bedlevel
    node_nan_bedlevel = pd.concat([node_nan]*interpolation_lines)
    node_nan_bedlevel.index = range(-interpolation_lines*2 - 1, -interpolation_lines - 1)

    # create x additional interpolation lines between lowest backwatercurve and targetlevel
    node_nan_targetlevel = pd.concat([node_nan]*interpolation_lines)
    node_nan_targetlevel.index = range(-interpolation_lines, 0)

    node_h = pd.DataFrame()

    for set_name in set_names:
        node_basis = pd.concat([
            node_bedlevel, 
            node_nan_bedlevel, 
            node_targetlevel.loc[[set_name]].rename(index={set_name: "targetlevel"}), 
            node_nan_targetlevel
        ])
        node_basis.index.name = "condition"

        if node_h_df is None:
            node_h_set = pd.concat([
                node_basis.copy()
            ], keys=[set_name], names=["set"])
            node_h = pd.concat([node_h, node_h_set])
        else:
            node_h_set = pd.concat([
                pd.concat([
                    node_basis, node_h_df.loc[set_name]
                ])
            ], keys=[set_name], names=["set"]).interpolate(axis=0)
            node_h = pd.concat([node_h, node_h_set])
    
    # check for increasing water level, if not then equal to previous
    # for i in range(len(node_h)-1):
    #     node_h[node_h.diff(1) <= 0.0001] = node_h.shift(1) + 0.0001
    node_h.columns.name = "node_no"
    return node_h


def translate_waterlevels_to_surface_storage(nodes_h, curve, orig_bedlevel, decimals=3):
    nodes_x = nodes_h.copy()
    nodes_h_new = nodes_h.copy()
    for col in nodes_x.columns:
        level_correction_bedlevel = orig_bedlevel.bedlevel.loc[col] - 0.01
        curve.loc[curve.index <= level_correction_bedlevel, col] = 0.0
        nodes_h_new.loc[nodes_x[col] < level_correction_bedlevel, col] = math.floor(level_correction_bedlevel*10)/10.0
        interp_func = scipy.interpolate.interp1d(
            curve.index, 
            curve[col].to_numpy(), 
            kind="linear", 
            fill_value="extrapolate"
        )
        nodes_x[col] = np.round(interp_func(nodes_h_new[col].to_numpy()), decimals=decimals)
    return nodes_x


def generate_surface_storage_for_nodes(node_h, node_surface_df, node_storage_df, orig_bedlevel, set_names, decimals=3):
    node_h_new = pd.DataFrame()
    node_a = pd.DataFrame()
    node_v = pd.DataFrame()
    node_a.columns.name = "node_no"
    node_v.columns.name = "node_no"

    for set_name in set_names:
        node_h_set = node_h.loc[set_name].interpolate(limit_area="inside")
        # fill between bed level and target_level and water levels
        for i, ind in enumerate(node_h_set.index[1:]):
            columns_nan = pd.isnull(node_h_set.loc[ind]).index[pd.isnull(node_h_set.loc[ind])]
            node_h_set.loc[ind, columns_nan] = node_h_set.loc[node_h_set.index[i], columns_nan] + 0.2
        
        a_set = translate_waterlevels_to_surface_storage(node_h_set, node_surface_df, orig_bedlevel, decimals=decimals)
        node_a_set = pd.concat([a_set], keys=[set_name], names=["set"])
        node_a = pd.concat([node_a, node_a_set])
        v_set = translate_waterlevels_to_surface_storage(node_h_set, node_storage_df, orig_bedlevel, decimals=decimals)
        node_v_set = pd.concat([v_set], keys=[set_name], names=["set"])
        node_v = pd.concat([node_v, node_v_set])
        
        node_h_set = pd.concat([node_h_set], keys=[set_name], names=["set"])
        node_h_new = pd.concat([node_h_new, node_h_set])
    
    return node_h_new, node_a, node_v


def generate_waterlevels_for_basins(basins, node_h):
    basin_h = basins[["basin_node_id", "node_no"]].set_index("basin_node_id")
    basins_ids = basin_h.index
    basin_h = node_h.T.loc[basin_h["node_no"]]
    basin_h.index = basins_ids
    return basin_h.T


def generate_surface_storage_for_basins(node_a, node_v, nodes):
    nodes = nodes[["node_no", "basin_node_id"]]
    basin_a = pd.DataFrame()
    basin_v = pd.DataFrame()

    for set_name in node_a.index.get_level_values(0).unique():
        basin_a_set = node_a.loc[set_name].T.merge(nodes, how="inner", left_index=True, right_on="node_no").drop(columns=["node_no"])
        basin_a_set = basin_a_set.groupby(by="basin_node_id").sum().T
        basin_a_set = pd.concat([basin_a_set], keys=[set_name], names=["set"])
        basin_a = pd.concat([basin_a, basin_a_set])

        basin_v_set = node_v.loc[set_name].T.merge(nodes, how="inner", left_index=True, right_on="node_no").drop(columns=["node_no"])
        basin_v_set = basin_v_set.groupby(by="basin_node_id").sum().T
        basin_v_set = pd.concat([basin_v_set], keys=[set_name], names=["set"])
        basin_v = pd.concat([basin_v, basin_v_set])
    
    basin_a.index.names = ["set", "condition"]
    basin_v.index.names = ["set", "condition"]
    return basin_a, basin_v


def generate_h_relation_basins_nodes(nodes, node_h_basin, basin_h):
    nodes_basin = nodes[["node_no", "basin_node_id", "geometry"]]
    node_h_basin_stacked = node_h_basin.stack()
    node_h_basin_stacked.name = "node_no_h"
    node_h_results = node_h_basin_stacked.reset_index().merge(
        nodes_basin, 
        how='left', 
        on="node_no"
    )
    basin_h_stacked = basin_h.stack()
    basin_h_stacked.name = "basin_h"
    basin_h_results = basin_h_stacked.reset_index()
    basin_node_h_relation = gpd.GeoDataFrame(
        node_h_results.merge(
            basin_h_results, 
            how='left', 
            on=("set", "condition", "basin_node_id")
        ),
        geometry="geometry",
        crs=nodes.crs
    )
    return basin_node_h_relation


def preprocessing_ribasim_model_tables(
    dummy_model, map_data, his_data, volume_data, nodes, weirs, uniweirs, pumps, culverts, orifices, boundaries, basins, split_nodes, 
    basin_connections, boundary_connections, interpolation_lines, set_names, split_node_type_conversion, split_node_id_conversion
):
    boundaries, split_nodes, basins = generate_ribasim_types_for_all_split_nodes(
        boundaries=boundaries, 
        split_nodes=split_nodes, 
        basins=basins, 
        split_node_type_conversion=split_node_type_conversion, 
        split_node_id_conversion=split_node_id_conversion
    )

    if dummy_model:
        basins_outflows = get_basins_outflows_including_settings(
            split_nodes=split_nodes, 
            basin_connections=basin_connections,
            boundary_connections=boundary_connections,
            weirs=weirs,
            uniweirs=uniweirs,
            pumps=pumps, 
            culverts=culverts,
            orifices=orifices,
            set_names=set_names
        )
        return basins_outflows, None, None, None, None, None, None, None, \
            None, None, None, None, None, None, None, None, None, None, None
    
    # prepare all data
    basins_outflows = get_basins_outflows_including_settings(
        split_nodes=split_nodes, 
        basin_connections=basin_connections,
        boundary_connections=boundary_connections,
        weirs=weirs,
        uniweirs=uniweirs,
        pumps=pumps,
        culverts=culverts,
        orifices=orifices,
        set_names=set_names
    )
    node_targetlevel = get_targetlevels_nodes_using_weirs_pumps(nodes, basins_outflows, set_names)

    node_surface_df, node_storage_df, node_bedlevel, orig_bedlevel = extract_bed_level_surface_storage(volume_data, nodes)
    node_h_df = get_waterlevels_table_from_simulations(map_data)
    node_h_basin = generate_node_waterlevels_table(node_h_df, node_bedlevel, node_targetlevel, interpolation_lines, set_names)
    node_h_node, node_a, node_v = generate_surface_storage_for_nodes(node_h_basin, node_surface_df, node_storage_df, orig_bedlevel, set_names)
    basin_h = generate_waterlevels_for_basins(basins, node_h_node)
    basin_a, basin_v = generate_surface_storage_for_basins(node_a, node_v, nodes)
    edge_q_df = get_discharges_table_from_simulations(map_data)
    weir_q_df, uniweir_q_df, orifice_q_df, culvert_q_df, bridge_q_df, pump_q_df = get_discharges_table_structures_from_simulations(his_data)

    basins_nodes_h_relation = generate_h_relation_basins_nodes(
        nodes=nodes,
        node_h_basin=node_h_basin,
        basin_h=basin_h
    )

    return basins_outflows, node_h_basin, node_h_node, node_a, node_v, basin_h, basin_a, basin_v, \
        node_bedlevel, node_targetlevel, orig_bedlevel, basins_nodes_h_relation, edge_q_df, \
            weir_q_df, uniweir_q_df, orifice_q_df, culvert_q_df, bridge_q_df, pump_q_df
