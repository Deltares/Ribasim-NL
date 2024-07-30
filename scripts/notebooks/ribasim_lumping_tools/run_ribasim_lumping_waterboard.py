"""
Generate Ribasim network and model with ribasim_lumping package for waterboard Rijn en IJssel based on HyDAMO data

Contactperson:      Harm Nomden (Sweco)

Last update:        11-07-2024
"""

import os
import warnings
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from ribasim_lumping import create_ribasim_lumping_network

warnings.simplefilter("ignore")
pd.options.mode.chained_assignment = None
# change workdir
os.chdir(os.path.dirname(os.path.abspath(__file__)))
warnings.simplefilter(action="ignore", category=UserWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)


# Settings #
def run_ribasim_lumping_for_waterboard(
    base_dir: Path,
    waterschap: str,
    dx: float = 100.0,
    buffer_distance: float = 1.0,
    assign_unassigned_areas_to_basins: bool = True,
    remove_isolated_basins: bool = True,
    include_flow_boundary_basins: bool = True,
    include_level_boundary_basins: bool = False,
):
    ts_start = pd.Timestamp.now()
    print(f"\n\nRun RIBASIM-lumping for waterboard {waterschap}")
    base_dir = Path(base_dir, waterschap, "verwerkt")

    # define network name, base dir
    source_type = "hydamo"

    # directory results
    results_dir = Path(base_dir, "4_ribasim")
    simulation_code = "."

    # Create networkanalysis
    network = create_ribasim_lumping_network(base_dir=base_dir, name=waterschap, results_dir=results_dir, crs=28992)

    ## -------- AREAS --------
    # areas (discharge units: afwaterende eenheden)
    areas_file = Path(base_dir, "3_input", "areas.gpkg")
    areas_gpkg_layer = "areas"
    areas_code_column = "CODE"

    # drainage areas (afvoergebieden)
    drainage_areas_file = Path(base_dir, "3_input", "areas.gpkg")
    drainage_areas_gpkg_layer = "drainage_areas"
    drainage_areas_code_column = "CODE"

    # Load areas
    network.read_areas(
        areas_file_path=areas_file,
        layer_name=areas_gpkg_layer,
        areas_code_column=areas_code_column,
    )
    network.read_drainage_areas(
        drainage_areas_file_path=drainage_areas_file,
        layer_name=drainage_areas_gpkg_layer,
        drainage_areas_code_column=drainage_areas_code_column,
    )

    ## -------- HYDAMO DATA --------
    # HyDAMO data
    hydamo_network_file = Path(base_dir, "4_ribasim", "hydamo.gpkg")
    hydamo_split_network_dx = (
        dx  # split up hydamo hydroobjects in sections of approximate 25 m. Use None to don't split
    )

    # Read HyDAMO network data
    network.add_basis_network(
        source_type=source_type,
        hydamo_network_file=hydamo_network_file,
        hydamo_split_network_dx=hydamo_split_network_dx,
    )

    ## -------- RIBASIM INPUT --------
    # input files
    ribasim_input_boundary_file = Path(base_dir, "3_input", "ribasim_input.gpkg")
    ribasim_input_boundary_gpkg_layer = "boundaries"
    ribasim_input_split_nodes_file = Path(base_dir, "3_input", "ribasim_input.gpkg")
    ribasim_input_split_nodes_gpkg_layer = "split_nodes"

    network.read_split_nodes(
        split_nodes_file_path=ribasim_input_split_nodes_file,
        layer_name=ribasim_input_split_nodes_gpkg_layer,
        buffer_distance=buffer_distance,
        crs=28992,
    )

    network.read_boundaries(
        boundary_file_path=ribasim_input_boundary_file,
        layer_name=ribasim_input_boundary_gpkg_layer,
        buffer_distance=buffer_distance,
        crs=28992,
    )

    # generate Ribasim network
    network.generate_ribasim_lumping_network(
        simulation_code=simulation_code,
        remove_isolated_basins=remove_isolated_basins,
        include_flow_boundary_basins=include_flow_boundary_basins,
        include_level_boundary_basins=include_level_boundary_basins,
        remove_holes_min_area=5000.0,
        assign_unassigned_areas_to_basins=assign_unassigned_areas_to_basins,
    )
    ts_end = pd.Timestamp.now()
    print(f"RIBASIM-lumping for waterboard {waterschap} ready: {ts_end-ts_start}")


if __name__ == "__main__":
    base_dir = Path("..\\..\\Ribasim modeldata")
    waterschap = "AaenMaas"
    dx = 250.0

    run_ribasim_lumping_for_waterboard(
        base_dir=base_dir,
        waterschap=waterschap,
        dx=dx,
        buffer_distance=1.0,
        assign_unassigned_areas_to_basins=False if waterschap == "ValleienVeluwe" else True,
        remove_isolated_basins=False,
    )
