# pylint: disable=missing-function-docstring
import os
from pathlib import Path
from typing import List, Union, Tuple
import datetime
import pandas as pd
import numpy as np
import dfm_tools as dfmt
import xarray as xr
import xugrid as xu
import hydrolib.core.dflowfm as hcdfm
from .read_dhydro_network import get_dhydro_files


def get_simulation_names_from_dir(path_dir) -> List[str]:
    """search directory and find all dhydro-projects (.dsproj)"""
    if not Path(path_dir).exists():
        raise ValueError("Path of simulations does not exist")
    simulations_names = [f for f in os.listdir(path_dir) if f.endswith(".dsproj")]
    return simulations_names


def get_data_from_simulation(
    simulation_path: Path,
    simulations_ts: Union[List, pd.DatetimeIndex],
    n_start: int = 0,
) -> Tuple[xr.Dataset, xu.UgridDataset]:
    """Gets simulation data
    - from a simulation
    - at certain timestamps.
    - Replaces time coordinate with counter 'condition' (int). Starts counting at n_start
    Returns: map_data (edges/nodes) and his_data (structures) from one simulation"""
    files = get_dhydro_files(simulation_path)
    his_file = files['output_his_file']
    map_file = files['output_map_file']

    # file names
    if not his_file.exists():
        raise ValueError(f'no his_file present: {simulation_path}*_his.nc')
    if not map_file.exists():
        raise ValueError(f'no his_file present: {simulation_path}*_map.nc')
    
    try:
        his_data = xr.open_mfdataset([his_file], preprocess=dfmt.preprocess_hisnc)
        map_data_xr = xr.open_dataset(map_file)
        map_data = xu.open_dataset(map_file)
        map_data["mesh1d_edge_nodes"] = map_data_xr["mesh1d_edge_nodes"]
    except:
        his_data = xr.open_mfdataset([his_file], preprocess=dfmt.preprocess_hisnc, decode_times=False)
        map_data_xr = xr.open_dataset(map_file, decode_times=False)
        map_data = xu.open_dataset(map_file, decode_times=False)
        map_data["mesh1d_edge_nodes"] = map_data_xr["mesh1d_edge_nodes"]
    
    if isinstance(simulations_ts[0], (datetime.datetime, pd.Timestamp)):
        his_data = his_data.sel(time=simulations_ts)
    else:
        his_data = his_data.isel(time=simulations_ts)
    his_data = his_data.rename({"time": "condition"})
    his_data.coords["condition"] = np.arange(n_start, n_start + len(his_data.condition))

    if isinstance(simulations_ts[0], (datetime.datetime, pd.Timestamp)):
        map_data = map_data.sel(time=simulations_ts)
    else:
        map_data = map_data.isel(time=simulations_ts)
    map_data = map_data.rename({"time": "condition"})
    map_data.coords["condition"] = np.arange(n_start, n_start + len(map_data.condition))
    return files, map_data, his_data


def get_data_from_simulations_set(
    set_name: str,
    simulations_dir: Path,
    simulations_names: List[str],
    simulations_ts: Union[List, pd.DatetimeIndex],
) -> Tuple[xr.Dataset, xu.UgridDataset]:
    """ "Combines simulation data:
    - from several simulations (names)
    - from simulation folder (dir)
    - at predefined timestamps (ts)
    - replaces simulation timestamp with condition (int)
    Returns: map_data (edges/nodes), his_data (structures) and boundary data, all simulations combined 
    """
    print(f"Read D-HYDRO simulations sets")
    his_data = None
    map_data = None
    # boundary_data = None
    n_start = 0
    
    for simulation_name in simulations_names:
        simulation_path = Path(simulations_dir, simulation_name)
        print(
            f" - Simulation set ({set_name}): {simulation_name} | Timestamps: {len(simulations_ts)} | his.nc and map.nc"
        )
        files, map_data_x, his_data_x = get_data_from_simulation(
            simulation_path=simulation_path,
            simulations_ts=simulations_ts,
            n_start=n_start,
        )

        if his_data is None or map_data is None:
            his_data = his_data_x
            map_data = map_data_x
        else:
            his_data = combine_data_from_simulations_sets(
                nc_data=his_data, 
                nc_data_new=his_data_x, 
                xugrid=False, 
                dim='condition'
            )
            map_data = combine_data_from_simulations_sets(
                nc_data=map_data, 
                nc_data_new=map_data_x, 
                xugrid=True, 
                dim='condition'
            )

        for var_name, var in map_data.data_vars.items():
            if "condition" in var.dims:
                if "set" not in var.dims:
                    map_data[var_name] = var.expand_dims(set=[set_name])
                elif set_name not in var.set:
                    map_data[var_name] = var.expand_dims(set=[set_name])
        for var_name, var in his_data.data_vars.items():
            if "condition" in var.dims:
                if "set" not in var.dims:
                    his_data[var_name] = var.expand_dims(set=[set_name])
                elif set_name not in var.set:
                    his_data[var_name] = var.expand_dims(set=[set_name])
        n_start = len(his_data.condition)
    
    return his_data, map_data #, boundary_data


def combine_data_from_simulations_sets(
    nc_data: Union[xr.Dataset, xu.UgridDataset],
    nc_data_new: Union[xr.Dataset, xu.UgridDataset],
    xugrid: bool = False,
    dim: str = "set",
) -> Union[xr.Dataset, xu.UgridDataset]:
    """ "Combine his.nc and map.nc data from two cases over dimension DIM assuming
    that all nc-variables not including DIM as dimension are equal"""
    nc_set_vars = [v_n for v_n, v in nc_data_new.data_vars.items() if dim in v.dims]
    nc_nonset_vars = [v_n for v_n, v in nc_data_new.data_vars.items() if dim not in v.dims]
    if nc_data is None:
        nc_data = nc_data_new[nc_set_vars]
    else:
        if xugrid:
            nc_data = xu.concat(
                [nc_data[nc_set_vars], nc_data_new[nc_set_vars]], dim=dim
            )
        else:
            nc_data = xr.concat(
                [nc_data[nc_set_vars], nc_data_new[nc_set_vars]], dim=dim
            )
    if xugrid:
        nc_data = xu.merge([nc_data_new[nc_nonset_vars], nc_data])
    else:
        nc_data = xr.merge([nc_data_new[nc_nonset_vars], nc_data])
    return nc_data

