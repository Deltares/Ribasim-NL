"""
Read network locations from D-Hydro simulation
Harm Nomden (Sweco)
"""
import configparser
import datetime
import subprocess
from pathlib import Path

import geopandas as gpd
import hydrolib.core.dflowfm as hcdfm
import numpy as np
import pandas as pd
import xarray as xr
import xugrid as xu
from shapely.geometry import LineString, Point

from ..utils.general_functions import (
    find_directory_in_directory, find_file_in_directory, find_nearest_edges_no,
    find_nearest_nodes, get_points_on_linestrings_based_on_distances,
    read_ini_file_with_similar_sections, replace_string_in_file, extract_segment_from_linestring)


def get_dhydro_files(simulation_path: Path):
    """Get DHydro input files"""
    input_files = dict()
    mdu_file = ""
    mdu_file = find_file_in_directory(simulation_path, ".mdu")
    print(f"  - MDU-file: {mdu_file}")

    replace_string_in_file(mdu_file, "*\n", "# *\n")
    mdu = configparser.ConfigParser()
    mdu_dir = Path(mdu_file).parent
    mdu.read(mdu_file)
    replace_string_in_file(mdu_file, "# *\n", "*\n")

    input_files["mdu_file"] = mdu_file
    input_files["net_file"] = Path(mdu_dir, mdu["geometry"]["netfile"])
    input_files["structure_file"] = Path(mdu_dir, mdu["geometry"]["structurefile"])
    input_files["cross_loc_file"] = Path(mdu_dir, mdu["geometry"]["crosslocfile"])
    input_files["cross_def_file"] = Path(mdu_dir, mdu["geometry"]["crossdeffile"])
    input_files["friction_file"] = Path(mdu_dir, mdu["geometry"]["frictfile"])
    input_files["external_forcing_file"] = Path(
        mdu_dir, mdu["external forcing"]["extforcefilenew"]
    )
    input_files["obs_file"] = Path(mdu_dir, mdu["output"]["obsfile"])

    volume_nc_file = Path(mdu_dir, "PerGridpoint_volume.nc")
    if volume_nc_file.exists():
        input_files['volume_file'] = volume_nc_file
    else:
        input_files['volume_file'] = ""

    output_dir = mdu["output"]["outputdir"]
    input_files["output_dir"] = Path(find_directory_in_directory(simulation_path, output_dir))
    input_files["output_his_file"] = Path(find_file_in_directory(simulation_path, "his.nc"))
    input_files["output_map_file"] = Path(find_file_in_directory(simulation_path, "map.nc"))
    return input_files


def get_dhydro_network_data(network_file: Path):
    """Get DHydro network locations"""
    print("  - network:", end="", flush=True)
    return hcdfm.net.models.Network.from_file(network_file), xr.open_dataset(network_file)


def get_dhydro_branches_from_network_data(network_data, crs):
    """Get DHydro branches"""
    branch_keys = [b for b in network_data._mesh1d.branches.keys()]
    branch_geom = [b.geometry for b in network_data._mesh1d.branches.values()]
    branches_df = pd.DataFrame({
        "branch_id": branch_keys, 
        "branch_geom": branch_geom
    })
    branches_df["geometry"] = branches_df.apply(
        lambda row: LineString(row["branch_geom"]), axis=1
    )
    branches_gdf = gpd.GeoDataFrame(
        branches_df, 
        geometry="geometry", 
        crs=crs
    ).drop("branch_geom", axis=1)
    print(f" branches ({len(branches_gdf)}x)", end="", flush=True)
    return branches_gdf


def get_dhydro_network_nodes_from_network_nc(network_nc, crs):
    try:
        nodes_df = pd.DataFrame({
            "network_node_id": network_nc.network_node_id,
            "X": network_nc.network_node_x,
            "Y": network_nc.network_node_y,
        })
    except:
        nodes_df = pd.DataFrame({
            "network_node_id": network_nc.Network_node_id,
            "X": network_nc.Network_node_x,
            "Y": network_nc.Network_node_y,
        })

    nodes_df["geometry"] = list(zip(nodes_df["X"], nodes_df["Y"]))
    nodes_df["geometry"] = nodes_df["geometry"].apply(Point)
    nodes_gdf = gpd.GeoDataFrame(nodes_df, geometry="geometry", crs=crs)
    nodes_gdf["network_node_id"] = nodes_gdf["network_node_id"].astype(str).str.strip(" ")
    print(f" network-nodes ({len(nodes_gdf)}x)", end="", flush=True)
    return nodes_gdf.drop(['X', 'Y'], axis=1)


def get_dhydro_nodes_from_network_data(network_data, crs):
    """Get DHydro nodes"""
    nodes_df = pd.DataFrame({
        "branch_id": network_data._mesh1d.mesh1d_node_branch_id,
        "chainage": network_data._mesh1d.mesh1d_node_branch_offset,
        "node_id": network_data._mesh1d.mesh1d_node_id,
        "geometry": list(zip(network_data._mesh1d.mesh1d_node_x, network_data._mesh1d.mesh1d_node_y))
    })
    nodes_df["node_no"] = nodes_df.index
    nodes_df["geometry"] = nodes_df["geometry"].apply(Point)
    nodes_gdf = gpd.GeoDataFrame(nodes_df, geometry="geometry", crs=crs)
    print(f" nodes ({len(nodes_gdf)}x)", end="", flush=True)
    return nodes_gdf


def get_dhydro_edges_from_network_data(network_data, nodes_gdf, branches_gdf, crs):
    """Get DHydro edges"""
    edges_df = pd.DataFrame({
        "branch_id": network_data._mesh1d.mesh1d_edge_branch_id,
        "chainage": network_data._mesh1d.mesh1d_edge_branch_offset,
        "X": network_data._mesh1d.mesh1d_edge_x,
        "Y": network_data._mesh1d.mesh1d_edge_y,
        "from_node": network_data._mesh1d.mesh1d_edge_nodes[:, 0],
        "to_node": network_data._mesh1d.mesh1d_edge_nodes[:, 1],
    })
    edges_df["branch_id"] = edges_df["branch_id"].map(branches_gdf["branch_id"].to_dict())

    edges_df["geometry"] = ""
    edges_gdf = edges_df.merge(
        nodes_gdf[["node_no", "geometry"]],
        how="inner",
        left_on="from_node",
        right_on="node_no",
        suffixes=["", "_from"],
    )
    edges_gdf = edges_gdf.merge(
        nodes_gdf[["node_no", "geometry"]],
        how="inner",
        left_on="to_node",
        right_on="node_no",
        suffixes=["", "_to"],
    )

    edges_gdf["geometry"] = edges_gdf.apply(
        lambda row: LineString([row["geometry_from"], row["geometry_to"]]), axis=1
    )
    edges_gdf = gpd.GeoDataFrame(edges_gdf, geometry="geometry", crs=crs)
    
    edges_gdf = edges_gdf.merge(branches_gdf.rename(columns={'geometry': 'geometry_branch'}), how='left', on='branch_id')
    edges_gdf["geometry"] = edges_gdf.apply(
        lambda x: extract_segment_from_linestring(
            x["geometry_branch"], 
            x["geometry_from"], 
            x["geometry_to"]
        ), axis=1
    )
    edges_gdf["edge_no"] = edges_gdf.index
    edges_gdf = edges_gdf[
        ["edge_no", "branch_id", "geometry", "from_node", "to_node"]
    ]
    print(f" edges ({len(edges_gdf)})")#, end="", flush=True)
    return edges_gdf


def get_dhydro_structures_locations(
    structures_file: Path, 
    branches_gdf: gpd.GeoDataFrame,
    edges_gdf: gpd.GeoDataFrame
):
    """Get DHydro structures locations"""
    # get structure file (e.g. "structures.ini")
    print("  - structures:", end="", flush=True)
    m = hcdfm.structure.models.StructureModel(structures_file)
    structures_df = pd.DataFrame([f.__dict__ for f in m.structure])
    structures_df = structures_df.drop('name', axis=1)
    structures_df = structures_df.rename({"branchid": "branch_id", "id": "structure_id"}, axis=1)
    structures_gdf = get_points_on_linestrings_based_on_distances(
        linestrings=branches_gdf,
        linestring_id_column='branch_id',
        points=structures_df,
        points_linestring_id_column='branch_id',
        points_distance_column='chainage'
    )
    structures_gdf = structures_gdf.rename(columns={"type": "object_type"})
    structures_gdf = find_nearest_edges_no(
        gdf1=structures_gdf,
        gdf2=branches_gdf.set_index("branch_id").sort_index(),
        new_column = "branch_id"
    )
    structures_gdf = find_nearest_edges_no(
        gdf1=structures_gdf, 
        gdf2=edges_gdf.set_index("edge_no").sort_index(),
        new_column="edge_no",
        subset="branch_id",
    )
    object_types = list(structures_gdf.object_type.unique())
    for object_type in object_types:
        len_objects = len(structures_gdf[structures_gdf.object_type==object_type])
        print(f" {object_type} ({len_objects}x),", end="", flush=True)
    structures_gdf["node_no"] = -1
    return structures_gdf


def check_number_of_pumps_at_pumping_station(pumps_gdf: gpd.GeoDataFrame, set_name: str):
    """Check number of pumps at pumping station and combine them into one representative pump
    Input:  Geodataframe with pumps with multiple per location
    Output: Geodataframe with one pump per location. 
            Total capacity (sum), Max start level, Min stop level"""
    crs = pumps_gdf.crs
    pumps_gdf = pumps_gdf.groupby(
        pumps_gdf.geometry.to_wkt(), 
        as_index=False
    ).agg({
        ('general', 'structure_id'): 'first',
        ('general', 'branch_id'): 'first', 
        ('general', 'object_type'): 'first',
        ('general', 'chainage'): 'first',
        ('general', 'edge_no'): 'first',
        ('general', 'node_no'): 'first',
        ('structure', 'comments'): 'first', 
        ('structure', 'orientation'): 'first',
        ('structure', 'controlside'): 'first',
        ('structure', 'numstages'): 'first',
        ('structure', 'capacity'): 'sum',
        ('structure', 'numreductionlevels'): 'first',
        ('structure', 'head'): 'first',
        ('structure', 'reductionfactor'): 'first',
        (set_name, 'startlevelsuctionside'): 'max',
        (set_name, 'stoplevelsuctionside'): 'min',
        (set_name, 'startleveldeliveryside'): 'min',
        (set_name, 'stopleveldeliveryside'): 'max',
        ('geometry', ''): 'first',
    }).reset_index(drop=True)
    pumps_gdf = gpd.GeoDataFrame(
        pumps_gdf.drop(columns=("geometry", '')),
        geometry=pumps_gdf["geometry"],
        crs=crs
    )
    return pumps_gdf


def split_dhydro_structures(structures_gdf: gpd.GeoDataFrame, set_name: str):
    """Get all DHydro structures dataframes"""

    list_structure_types = list(structures_gdf['object_type'].unique())
    structures_gdf_dict = {}
    for structure_type in list_structure_types:
        # skip all compounds
        if structure_type == "compound":
            continue
        # get structure type data
        structure_gdf = structures_gdf.loc[structures_gdf["object_type"] == structure_type].dropna(how='all', axis=1)

        if structure_type == "culvert":
            structure_gdf["crestlevel"] = structure_gdf[["leftlevel", "rightlevel"]].max(axis=1)

        # comments are sometimes a separate object instead of string
        if 'comments' in structure_gdf.columns:
            structure_gdf.loc[:, 'comments'] = structure_gdf.loc[:, 'comments'].astype(str)
        
        # create multi-index to separate general values from set-specific values.
        header_0 = ["structure_id", "branch_id", "object_type", "chainage", "edge_no", "node_no"]
        headers_2 = {
            'weir': ["crestlevel"],
            'uniweir': ["crestlevel"],
            'universalWeir': ["crestlevel"],
            'orifice': ["gateloweredgelevel"],
            'pump': ["startlevelsuctionside", "stoplevelsuctionside", "startleveldeliveryside", "stopleveldeliveryside"],
        }
        if structure_type in headers_2.keys():
            header_2 = headers_2[structure_type]
        else:
            header_2 = []
        if structure_type not in ['pump']:
            header_2 += ["upstream_upperlimit", "upstream_setpoint", "upstream_lowerlimit", 
                         "downstream_upperlimit", "downstream_setpoint", "downstream_lowerlimit"]
            for h in header_2:
                if h not in structure_gdf.columns:
                    structure_gdf[h] = np.nan
        header_1 = [c for c in structure_gdf.columns if c not in header_0 + header_2 + ['geometry']]

        structure_gdf = gpd.GeoDataFrame(
            pd.concat([
                structure_gdf[header_0], 
                structure_gdf[header_1],
                structure_gdf[header_2]
            ], keys=['general', 'structure', set_name], axis=1),
            geometry=structure_gdf['geometry'],
            crs=structure_gdf.crs
        )

        # in case of pumps: 
        # - check if multiple pumps in one pumping station
        if structure_type == "pump":
            # check for multiple pumps if gdf is not empty
            if ~structure_gdf.empty:
                old_no_pumps = len(structure_gdf)
                structure_gdf = check_number_of_pumps_at_pumping_station(structure_gdf, set_name)
                if old_no_pumps > len(structure_gdf):
                    print(f" pumps ({old_no_pumps}x->{len(structure_gdf)}x)", end="", flush=True)
                else:
                    print(f" pumps ({len(structure_gdf)}x)", end="", flush=True)
            else:
                print(f" {structure_type}s ({len(structure_gdf)}x)", end="", flush=True)
        
        structures_gdf_dict[structure_type] = structure_gdf.sort_values(by=("general", "structure_id")).reset_index(drop=True)
    print(f" ")
    return structures_gdf_dict


def get_dhydro_external_forcing_locations(
    external_forcing_file: str, 
    branches_gdf: gpd.GeoDataFrame, 
    network_nodes_gdf: gpd.GeoDataFrame, 
    nodes_gdf: gpd.GeoDataFrame
):
    """Get all DHydro boundaries and laterals"""
    print("  - external forcing (locations):", end="", flush=True)
    
    boundaries_gdf = read_ini_file_with_similar_sections(external_forcing_file, "Boundary")
    boundaries_gdf = boundaries_gdf.rename({"nodeid": "network_node_id"}, axis=1)
    boundaries_gdf["network_node_id"] = boundaries_gdf["network_node_id"].str.strip("b \'\"")
    network_nodes_gdf["network_node_id"] = network_nodes_gdf["network_node_id"].str.strip("b \'\"")
    boundaries_gdf = network_nodes_gdf[["network_node_id", "geometry"]].merge(
        boundaries_gdf, 
        how="right", 
        left_on="network_node_id", 
        right_on="network_node_id"
    )
    boundaries_gdf = boundaries_gdf.sjoin(find_nearest_nodes(boundaries_gdf, nodes_gdf, "node_no"))
    
    boundaries_gdf = boundaries_gdf.reset_index(drop=True)
    boundaries_gdf.insert(0, "boundary_id", boundaries_gdf.index + 1)
    boundaries_gdf = boundaries_gdf.rename(columns={"network_node_id": "name"})
    boundaries_gdf = boundaries_gdf.rename(
        columns={"name": "boundary_name", "quantity": "boundary_type"}
    ).drop(columns="index_right")
    boundaries_gdf["boundary_type"] = boundaries_gdf["boundary_type"].map(
        {"dischargebnd": "FlowBoundary", "waterlevelbnd": "LevelBoundary"}
    )
    print(f" boundaries ({len(boundaries_gdf)}x)", end="", flush=True)

    laterals_gdf = read_ini_file_with_similar_sections(external_forcing_file, "Lateral")
    laterals_gdf["chainage"] = laterals_gdf["chainage"].astype(float)
    laterals_gdf = laterals_gdf.rename({"branchid": "branch_id"}, axis=1)
    laterals_gdf = get_points_on_linestrings_based_on_distances(
        linestrings=branches_gdf,
        linestring_id_column="branch_id",
        points=laterals_gdf,
        points_linestring_id_column="branch_id",
        points_distance_column="chainage"
    )
    print(f" laterals ({len(laterals_gdf)}x)")
    return boundaries_gdf, laterals_gdf


def get_dhydro_forcing_data(
    mdu_input_dir: Path,
    boundaries_gdf: gpd.GeoDataFrame, 
    laterals_gdf: gpd.GeoDataFrame
):
    """Get DHydro forcing data"""
    print("  - external forcing (data):", end="", flush=True)
    print(" boundaries", end="", flush=True)
    boundaries_data = None
    boundaries_forcing_files = boundaries_gdf['forcingfile'].unique()
    for boundaries_forcing_file in boundaries_forcing_files:
        boundaries_forcing_file_path = Path(mdu_input_dir, boundaries_forcing_file)
        forcingmodel_object = hcdfm.ForcingModel(boundaries_forcing_file_path)
        if boundaries_data is None:
            boundaries_data = pd.DataFrame([forcing.dict() for forcing in forcingmodel_object.forcing])
        else:
            boundaries_data = pd.concat([boundaries_data, pd.DataFrame([forcing.dict() for forcing in forcingmodel_object.forcing])])
    
    print(" laterals")
    laterals_data = None
    laterals_forcing_files = laterals_gdf['discharge'].unique()
    for laterals_forcing_file in laterals_forcing_files:
        if laterals_forcing_file == "realtime":
            continue
        laterals_forcing_file_path = Path(mdu_input_dir, laterals_forcing_file)
        forcingmodel_object = hcdfm.ForcingModel(laterals_forcing_file_path)
        if laterals_data is None:
            laterals_data = pd.DataFrame([forcing.dict() for forcing in forcingmodel_object.forcing])
        else:
            laterals_data = pd.concat([laterals_data, pd.DataFrame([forcing.dict() for forcing in forcingmodel_object.forcing])])

    # convert datablock to timeseries dataframe
    def assess_lateral_data(lateral_data):
        if lateral_data.function=="timeseries":
            start_datetime = lateral_data.quantityunitpair[0]["unit"][-19:]
            start_datetime = datetime.datetime.strptime(start_datetime, "%Y-%m-%d %H:%M:%S")
            ts = [start_datetime + datetime.timedelta(minutes=t[0]) for t in lateral_data.datablock]
            lateral_data_data = [l[1] for l in lateral_data.datablock]
            laterals_df = pd.Series(index=ts, data=lateral_data_data, name=lateral_data["name"])
            laterals_df.index = pd.to_datetime(laterals_df.index)
            return laterals_df
        if lateral_data.function=="constant":
            return {lateral_data["name"]: lateral_data["datablock"][0][0]}


    def get_laterals_data_df(laterals_data, laterals_gdf):
        if laterals_data is None:
            return None
        laterals_data = laterals_data.merge(laterals_gdf["name"], how="right", left_on="name", right_on="name")
        laterals_df = pd.DataFrame()
        laterals_floats = dict()
        for i, lateral_data in laterals_data.iterrows():
            lateral_data = assess_lateral_data(lateral_data)
            if isinstance(lateral_data, pd.Series):
                laterals_df = pd.concat([laterals_df, lateral_data], axis=1)
            if isinstance(lateral_data, dict):
                laterals_floats.update(lateral_data)
        for lateral_name, lateral_float in laterals_floats.items():
            laterals_df[lateral_name] = lateral_float
        laterals_df.index = pd.to_datetime(laterals_df.index)
        return laterals_df

    laterals_data = get_laterals_data_df(laterals_data, laterals_gdf)

    return boundaries_data, laterals_data


def get_dhydro_volume_based_on_basis_simulations(
    mdu_input_dir: Path,
    volume_tool_bat_file: Path, 
    volume_tool_force: bool = False,
    volume_tool_increment: float = 0.1
):
    mdu_file = find_file_in_directory(mdu_input_dir, ".mdu")
    volume_nc_file = find_file_in_directory(mdu_input_dir, "PerGridpoint_volume.nc")
    if volume_nc_file is None or volume_tool_force:
        print(f"  - volume_tool: new level-volume dataframe not found")
        subproces_cli = f'"{volume_tool_bat_file}" --mdufile "{mdu_file.name}" --increment {str(volume_tool_increment)} --outputfile volume.nc --output "All"'
        subprocess.Popen(subproces_cli, cwd=str(mdu_file.parent))
        volume_nc_file = find_file_in_directory(mdu_input_dir, "PerGridpoint_volume.nc")
        print(f"  - volume_tool: new level-volume dataframe created: {volume_nc_file.name}")
    else:
        print("  - volume_tool: file already exists, use force=True to force recalculation volume")
    volume = xu.open_dataset(volume_nc_file)
    return volume


def get_dhydro_data_from_simulation(
    simulation_path: Path, 
    set_name: str,
    volume_tool_bat_file: Path, 
    volume_tool_force: bool = False,
    volume_tool_increment: float = 0.1,
    crs: int = 28992,
):
    """Get DHydro data from simulation"""
    network_data = None
    network_nodes_gdf = None
    files = None
    branches_gdf = None
    nodes_gdf = None
    edges_gdf = None
    structures_gdf = None
    structures_dict = None
    boundaries_gdf = None
    laterals_gdf = None
    laterals_data = None
    boundaries_data = None

    files = get_dhydro_files(simulation_path)

    network_data, network_nc = get_dhydro_network_data(files['net_file'])
    network_nodes_gdf = get_dhydro_network_nodes_from_network_nc(network_nc, crs)
    branches_gdf = get_dhydro_branches_from_network_data(network_data, crs)
    nodes_gdf = get_dhydro_nodes_from_network_data(network_data, crs)
    edges_gdf = get_dhydro_edges_from_network_data(network_data, nodes_gdf, branches_gdf, crs)

    structures_gdf = get_dhydro_structures_locations(
        structures_file=files['structure_file'], 
        branches_gdf=branches_gdf, 
        edges_gdf=edges_gdf
    )
    structures_dict = split_dhydro_structures(structures_gdf, set_name)

    boundaries_gdf, laterals_gdf = get_dhydro_external_forcing_locations(
        external_forcing_file=files['external_forcing_file'], 
        branches_gdf=branches_gdf, 
        network_nodes_gdf=network_nodes_gdf, 
        nodes_gdf=nodes_gdf
    )
    mdu_input_dir = Path(files['mdu_file']).parent
    boundaries_data, laterals_data = get_dhydro_forcing_data(
        mdu_input_dir=mdu_input_dir, 
        boundaries_gdf=boundaries_gdf, 
        laterals_gdf=laterals_gdf
    )
    
    volume_data = get_dhydro_volume_based_on_basis_simulations(
        mdu_input_dir=mdu_input_dir, 
        volume_tool_bat_file=volume_tool_bat_file, 
        volume_tool_force=volume_tool_force,
        volume_tool_increment=volume_tool_increment
    )

    results = dict(
        network_data=network_data,
        network_nodes_gdf=network_nodes_gdf,
        files=files,
        branches_gdf=branches_gdf,
        nodes_gdf=nodes_gdf,
        edges_gdf=edges_gdf,
        structures_gdf=structures_gdf,
        structures_dict=structures_dict,
        boundaries_gdf=boundaries_gdf,
        laterals_gdf=laterals_gdf,
        laterals_data=laterals_data,
        boundaries_data=boundaries_data,
        volume_data=volume_data
    )

    return results
