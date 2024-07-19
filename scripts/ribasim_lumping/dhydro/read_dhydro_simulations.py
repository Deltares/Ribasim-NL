from typing import List, Union
from pathlib import Path
import xarray as xr
import pandas as pd
import xugrid as xu
from .read_dhydro_network import get_dhydro_data_from_simulation
from .read_dhydro_simulations_utils import (
    get_data_from_simulations_set,
    combine_data_from_simulations_sets,
)


def add_dhydro_basis_network(
    model_dir: Path,
    set_name: str,
    simulation_name: str, 
    volume_tool_bat_file: Path = None, 
    volume_tool_force: bool = False,
    volume_tool_increment: float = 0.1,
    crs: int = 28992,
):
    """Extracts nodes, edges, weirs, pumps from his/map"""
    basis_simulation_path = Path(model_dir, simulation_name)
    results = get_dhydro_data_from_simulation(
        simulation_path=basis_simulation_path, 
        set_name=set_name,
        crs=crs,
        volume_tool_bat_file=volume_tool_bat_file, 
        volume_tool_force=volume_tool_force,
        volume_tool_increment=volume_tool_increment
    )

    results_gdfs = []
    for gdf_name in [
        'network_data', 'branches_gdf', 'network_nodes_gdf',
        'edges_gdf', 'nodes_gdf', 'boundaries_gdf', 'laterals_gdf', 'volume_data'
    ]:
        results_gdfs.append(results.get(gdf_name, None))

    network_data, branches_gdf, network_nodes_gdf, edges_gdf, \
        nodes_gdf, boundaries_gdf, laterals_gdf, volume_data = results_gdfs

    structures_dict = results.get('structures_dict', None)
    if structures_dict is None:
        weirs_gdf, uniweirs_gdf, pumps_gdf, orifices_gdf, \
            bridges_gdf, culverts_gdf = None, None, None, None, None, None
    else:
        weirs_gdf = structures_dict.get('weir', None)
        uniweirs_gdf = structures_dict.get('universalWeir', None)
        pumps_gdf = structures_dict.get('pump', None)
        orifices_gdf = structures_dict.get('orifice', None)
        bridges_gdf = structures_dict.get('bridge', None)
        culverts_gdf = structures_dict.get('culvert', None)

    boundaries_data = results.get('boundaries_data', None)
    laterals_data = results.get('laterals_data', None)

    return network_data, branches_gdf, network_nodes_gdf, edges_gdf, \
        nodes_gdf, boundaries_gdf, laterals_gdf, weirs_gdf, \
        uniweirs_gdf, pumps_gdf, orifices_gdf, culverts_gdf, bridges_gdf, \
        boundaries_data, laterals_data, volume_data


def add_dhydro_simulation_data(
    set_name: str,
    model_dir: Path,
    simulation_names: List[str],
    simulation_ts: Union[List, pd.DatetimeIndex] = [-1],
    his_data: xr.Dataset = None,
    map_data: xu.UgridDataset = None
):
    """receives his- and map-data. calculations should be placed in dhydro_results_dir
    - set_name
    - within directory: simulations_dir
    - at timestamps: simulations_ts"""
    simulations_dir = Path(model_dir)
    if not simulations_dir.exists():
        raise ValueError(
            f"Directory D-Hydro calculations does not exist: {simulations_dir}"
        )
    if his_data is not None:
        if set_name in his_data.set:
            print(
                f'    x set_name "{set_name}" already taken. data not overwritten. change set_name'
            )
            return his_data, map_data#, self.boundaries_data

    new_his_data, new_map_data = get_data_from_simulations_set(
        set_name=set_name,
        simulations_dir=simulations_dir,
        simulations_names=simulation_names,
        simulations_ts=simulation_ts,
    )
    his_data = combine_data_from_simulations_sets(his_data, new_his_data)
    map_data = combine_data_from_simulations_sets(map_data, new_map_data, xugrid=True)

    return his_data, map_data

