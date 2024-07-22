"""
RIBASIM LUMPING NETWORK module to create a RIBASIM Lumping network

Project: National Hydrological Model (Netherlands)
Created by: Harm Nomden (Sweco, 2024)
"""
import datetime
import os
import shutil
from pathlib import Path

import contextily as cx
import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import ribasim
import xarray as xr
import xugrid as xu
from pydantic import BaseModel, ConfigDict
from shapely.geometry import Point

from .dhydro.read_dhydro_simulations import (
    add_dhydro_basis_network,
    add_dhydro_simulation_data,
)
from .hydamo.read_hydamo_network import add_hydamo_basis_network
from .ribasim_model_generator.generate_ribasim_model import generate_ribasim_model
from .ribasim_model_generator.generate_ribasim_model_preprocessing import (
    preprocessing_ribasim_model_tables,
)
from .ribasim_model_generator.generate_ribasim_model_tables import (
    generate_ribasim_model_tables,
)
from .ribasim_network_generator.generate_ribasim_network import (
    generate_ribasim_network_using_split_nodes,
)
from .ribasim_network_generator.generate_split_nodes import (
    add_split_nodes_based_on_selection,
)
from .utils.general_functions import (
    assign_unassigned_areas_to_basin_areas,
    log_and_remove_duplicate_geoms,
    read_geom_file,
    snap_to_network,
    split_edges_by_split_nodes,
)


class RibasimLumpingNetwork(BaseModel):
    """RIBASIM LUMPING NETWORK class to keep all data and network generation"""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    name: str
    base_dir: Path
    results_dir: Path
    path_ribasim_executable: Path = None
    dhydro_basis_dir: Path = None,
    dhydro_results_dir: Path = None,
    areas_gdf:gpd.GeoDataFrame = None
    drainage_areas_gdf: gpd.GeoDataFrame = None
    supply_areas_gdf: gpd.GeoDataFrame = None
    his_data: xu.UgridDataset = None
    map_data: xu.UgridDataset = None
    network_data: xr.Dataset = None
    volume_data: xr.Dataset = None
    network_graph: nx.DiGraph = None
    branches_gdf: gpd.GeoDataFrame = None
    network_nodes_gdf: gpd.GeoDataFrame = None
    edges_gdf: gpd.GeoDataFrame = None
    nodes_gdf: gpd.GeoDataFrame = None
    structures_gdf: gpd.GeoDataFrame = None
    stations_gdf: gpd.GeoDataFrame = None
    pumps_gdf: gpd.GeoDataFrame = None
    pumps_df: pd.DataFrame = None
    weirs_gdf: gpd.GeoDataFrame = None
    orifices_gdf: gpd.GeoDataFrame = None
    bridges_gdf: gpd.GeoDataFrame = None
    culverts_gdf: gpd.GeoDataFrame = None
    uniweirs_gdf: gpd.GeoDataFrame = None
    sluices_gdf: gpd.GeoDataFrame = None
    closers_gdf: gpd.GeoDataFrame = None
    boundaries_gdf: gpd.GeoDataFrame = None
    boundaries_data: pd.DataFrame = None
    boundaries_timeseries_data: pd.DataFrame = None
    laterals_gdf: gpd.GeoDataFrame = None
    laterals_data: pd.DataFrame = None
    simulation_code: str = None
    simulation_path: Path = None
    basin_areas_gdf: gpd.GeoDataFrame = None
    basins_gdf: gpd.GeoDataFrame = None
    split_nodes: gpd.GeoDataFrame = None
    basin_connections_gdf: gpd.GeoDataFrame = None
    boundary_connections_gdf: gpd.GeoDataFrame = None
    nodes_h_df: pd.DataFrame = None
    nodes_h_basin_df: pd.DataFrame = None
    nodes_a_df: pd.DataFrame = None
    nodes_v_df: pd.DataFrame = None
    basins_h_df: pd.DataFrame = None
    basins_a_df: pd.DataFrame = None
    basins_v_df: pd.DataFrame = None
    basins_nodes_h_relation: gpd.GeoDataFrame = None
    edge_q_df: pd.DataFrame = None
    weir_q_df: pd.DataFrame = None
    uniweir_q_df: pd.DataFrame = None
    orifice_q_df: pd.DataFrame = None
    culvert_q_df: pd.DataFrame = None
    bridge_q_df: pd.DataFrame = None
    pump_q_df: pd.DataFrame = None
    basins_outflows: pd.DataFrame = None
    node_bedlevel: pd.DataFrame = None
    node_targetlevel: pd.DataFrame = None
    method_boundaries: int = 1
    method_laterals: int = 1
    laterals_areas_data: pd.DataFrame = None
    laterals_drainage_per_ha: pd.Series = None
    method_initial_waterlevels: int = 1
    initial_waterlevels_set_name: str = ""
    initial_waterlevels_timestep: int = 0
    initial_waterlevels_areas_id_column: str = ""
    initial_waterlevels_outside_areas: float = 0.0
    ribasim_model: ribasim.Model = None
    basis_source_types: list[str] = []
    basis_set_names: list[str] = []
    basis_set_start_months: list[int] = []
    basis_set_start_days: list[int] = []
    basis_model_dirs: list[Path] = []
    basis_simulations_names: list[str] = []
    source_types: list[str] = []
    set_names: list[str] = []
    model_dirs: list[Path] = []
    simulations_names: list[list] = []
    simulations_output_dirs: list[str] = []
    simulations_ts: list[list | pd.DatetimeIndex] = []
    crs: int = 28992

    """RIBASIM LUMPING NETWORK class to keep all data and network generation"""

    def add_basis_network(
            self, 
            source_type: str,
            set_name: str = "",
            set_start_month: int = 1,
            set_start_day: int = 1,
            dhydro_model_dir: Path = '.', 
            dhydro_simulation_name: str = 'default',
            dhydro_volume_tool_bat_file: Path = '.', 
            dhydro_volume_tool_force: bool = False,
            dhydro_volume_tool_increment: float = 0.1,
            hydamo_network_file: Path = 'hydamo.gpkg',
            hydamo_split_network_dx: float = None,
        ) -> tuple[gpd.GeoDataFrame]:
        """
        Add (detailed) base network which will used to derive Ribasim network. Source type can either be "dhydro" or
        "hydamo". 
        If "dhydro", provide arguments for D-HYDRO volume tool (arguments dhydro_volume_*) and model simulation
        (model_dir, simulation_name). All necessary information (like boundaries and split nodes) will be derived from
        the D-HYDRO model data.
        If "hydamo", provide arguments for paths to files containing network geometries (hydamo_network_file) and
        in case of geopackage also the layer name (hydamo_network_gpkg_layer). HyDAMO does not contain information for
        boundaries and split nodes, so these need to be provided additionally as Ribasim input.
        """
        results = None
        if source_type == "dhydro":
            print(f'Network {self.name} - Analysis D-HYDRO model')
            if set_name is None or not isinstance(set_name, str):
                raise ValueError("set_name for dhydro should be a string")
            results = add_dhydro_basis_network(
                set_name=set_name,
                model_dir=dhydro_model_dir,
                simulation_name=dhydro_simulation_name,
                volume_tool_bat_file=dhydro_volume_tool_bat_file, 
                volume_tool_force=dhydro_volume_tool_force,
                volume_tool_increment=dhydro_volume_tool_increment
            )
            self.basis_model_dirs.append(dhydro_model_dir)
            self.basis_simulations_names.append(dhydro_simulation_name)
            if results is not None:
                self.network_data, self.branches_gdf, self.network_nodes_gdf, self.edges_gdf, \
                    self.nodes_gdf, self.boundaries_gdf, self.laterals_gdf, weirs_gdf, \
                    uniweirs_gdf, pumps_gdf, orifices_gdf, culverts_gdf, self.bridges_gdf, \
                    self.boundaries_data, self.laterals_data, self.volume_data = results

                def combine_old_gdf_with_new_gdf_using_setname(old_gdf, new_gdf, set_name):
                    if new_gdf is not None:
                        if old_gdf is None:
                            return new_gdf.copy()
                        else:
                            return gpd.GeoDataFrame(
                                old_gdf.drop(
                                    columns=['geometry', set_name], errors='ignore'
                                ).merge(
                                    new_gdf[[set_name, 'geometry']], 
                                    how='outer', 
                                    left_index=True,
                                    right_index=True
                                ),
                                geometry="geometry",
                                crs=old_gdf.crs
                            )

                self.pumps_gdf = combine_old_gdf_with_new_gdf_using_setname(self.pumps_gdf, pumps_gdf, set_name)
                self.weirs_gdf = combine_old_gdf_with_new_gdf_using_setname(self.weirs_gdf, weirs_gdf, set_name)
                self.uniweirs_gdf = combine_old_gdf_with_new_gdf_using_setname(self.uniweirs_gdf, uniweirs_gdf, set_name)
                self.orifices_gdf = combine_old_gdf_with_new_gdf_using_setname(self.orifices_gdf, orifices_gdf, set_name)
                self.culverts_gdf = combine_old_gdf_with_new_gdf_using_setname(self.culverts_gdf, culverts_gdf, set_name)

            self.basis_set_start_months.append(set_start_month)
            self.basis_set_start_days.append(set_start_day)
            self.basis_model_dirs.append(dhydro_model_dir)
            self.basis_simulations_names.append(dhydro_simulation_name)
        
        elif source_type == 'hydamo':
            print(f'Network {self.name} - Analysis HyDAMO data')
            # add network from HyDAMO files
            results = add_hydamo_basis_network(
                hydamo_network_file=hydamo_network_file,
                hydamo_split_network_dx=hydamo_split_network_dx
            )
            if results is not None:
                self.branches_gdf, self.network_nodes_gdf, self.edges_gdf, self.nodes_gdf, \
                    self.weirs_gdf, self.culverts_gdf, self.pumps_gdf, \
                    self.pumps_df, self.sluices_gdf, self.closers_gdf = results
            
        self.basis_source_types.append(source_type)
        self.basis_set_names.append(set_name)

        return results


    def add_simulation_set(
        self,
        set_name: str,
        model_dir: Path,
        simulation_names: list[str],
        source_type: str = 'dhydro',
        simulation_ts: list | pd.DatetimeIndex = [-1],
    ):
        """Add simulation set to network."""
        if source_type == 'dhydro':
            self.his_data, self.map_data = add_dhydro_simulation_data(
                set_name=set_name,
                model_dir=model_dir,
                simulation_names=simulation_names,
                simulation_ts=simulation_ts,
                his_data=self.his_data,
                map_data=self.map_data
            )
            self.source_types.append(source_type)
            self.set_names.append(set_name)
            self.model_dirs.append(model_dir)
            self.simulations_names.append(simulation_names)
            self.simulations_ts.append(simulation_ts)
        else:
            print(f"  x for this source type ({source_type}) no model type is added")
        return self.his_data, self.map_data


    def read_areas(
            self, 
            areas_file_path: Path, 
            areas_code_column: str = None, 
            layer_name: str = None, 
            crs: int = 28992
        ):
        """
        Add discharge unit areas (e.g. "afwateringseenheden" or "peilgebiedenpraktijk") from file. 
        In case of geopackage, provide layer_name. Overwrites previously defined areas
        """
        print(f'Network {self.name} - Analysis Areas')
        self.areas_gdf = read_geom_file(
            filepath=areas_file_path, 
            layer_name=layer_name, 
            crs=crs,
            explode_geoms=False,
            code_column=areas_code_column
        )
        self.areas_gdf["area"] = self.areas_gdf.index+1
        print(f" - areas ({len(self.areas_gdf)}x)")
    

    def read_drainage_areas(
            self, 
            drainage_areas_file_path: Path,
            drainage_areas_code_column: str = None, 
            layer_name: str = None, 
            crs: int = 28992
        ):
        """
        Add drainage areas (e.g. "afvoergebieden") from file. 
        In case of geopackage, provide layer_name.
        Overwrites previously defined drainage areas
        """
        print(f'Network {self.name} - Analysis Drainage Areas')
        self.drainage_areas_gdf = read_geom_file(
            filepath=drainage_areas_file_path, 
            layer_name=layer_name, 
            crs=crs,
            explode_geoms=False,
            code_column=drainage_areas_code_column
        )
        no_drainage_areas = len(self.drainage_areas_gdf) \
            if self.drainage_areas_gdf is not None else 0
        print(f" - drainage areas ({no_drainage_areas}x)")


    def read_supply_areas(
            self, 
            supply_areas_file_path: Path,
            supply_areas_code_column: str = None, 
            layer_name: str = None, 
            crs: int = 28992
        ):
        """
        Add supply areas (e.g. "afvoergebieden") from file. In case of geopackage, provide layer_name.
        Overwrites previously defined supply areas

        Args:
            supply_areas_file_path (Path):      Path to file containing supply areas geometries
            layer_name (str):                   Layer name in geopackage. Needed when file is a geopackage
            crs (int):                          (optional) CRS EPSG code. Default 28992 (RD New) 
        """
        print(f'Network {self.name} - Analysis Supply Areas')
        self.supply_areas_gdf = read_geom_file(
            filepath=supply_areas_file_path, 
            layer_name=layer_name, 
            crs=crs,
            explode_geoms=False,
            code_column=supply_areas_code_column
        )
        print(f" - supply areas ({len(self.supply_areas_gdf)}x)")
    

    def read_boundaries(
            self, 
            boundary_file_path: Path,
            layer_name: str = None,
            crs: int = 28992,
            buffer_distance: float = 10.0,
            min_length_edge: float = 2.0
        ):
        """
        Add Ribasim boundaries from file. In case of geopackage, provide layer_name. 
        Overwrites previously defined boundaries

        Args:
            boundary_file_path (Path):  Path to file containing boundary geometries
            layer_name (str):           Layer name in geopackage. Needed when file is a geopackage
            crs (int):                  (optional) CRS EPSG code. Default 28992 (RD New)     
        """
        print(f'Network {self.name} - Analysis Boundaries')
        self.boundaries_gdf = read_geom_file(
            filepath=boundary_file_path, 
            layer_name=layer_name, 
            crs=crs,
            remove_z_dim=True
        )
        self.boundaries_gdf = log_and_remove_duplicate_geoms(self.boundaries_gdf, colname='boundary_id')

        # Because the split node and boundary locations won't always nicely match up with HyDAMO network, we need to adjust this
        # by snapping split nodes and boundaries to network nodes/edges
        self.boundaries_gdf = snap_to_network(
            snap_type='boundary',
            points=self.boundaries_gdf, 
            edges=self.edges_gdf, 
            nodes=self.nodes_gdf, 
            buffer_distance=buffer_distance,
            min_length_edge=min_length_edge
        )
        self.boundaries_gdf["boundary_id"] = self.boundaries_gdf.index + 1
    
        if 'quantity' in self.boundaries_gdf.columns:
            self.boundaries_gdf['quantity'] = self.boundaries_gdf['quantity'].replace({
                'dischargebnd': 'FlowBoundary', 
                'waterlevelbnd': 'LevelBoundary'
            })
        self.boundaries_gdf = self.boundaries_gdf.rename({'quantity': 'boundary_type'})
        self.boundaries_gdf["boundary_type"] = self.boundaries_gdf["boundary_type"].fillna("LevelBoundary")

        boundaries_not_on_network = self.boundaries_gdf.loc[self.boundaries_gdf['node_no'] == -1]
        print(f" - Remove non-snapped boundaries from dataset ({len(boundaries_not_on_network)})")
        self.boundaries_gdf = self.boundaries_gdf.loc[[i not in boundaries_not_on_network.index for i in self.boundaries_gdf.index]]
    
    
    def export_or_update_all_ribasim_structures_specs(self, structure_specs_dir_path: Path):
        """Export or update ribasim structure specifications"""
        for structure_type in ['pump', 'weir', 'orifice', 'culvert']:
            structure_specs_path = Path(structure_specs_dir_path, f"ribasim_{structure_type}s_specs.xlsx")
            self.export_or_update_ribasim_structure_specs(
                structure_type=structure_type,
                structure_specs_path=structure_specs_path
            )


    def export_or_update_ribasim_structure_specs(self, structure_type: str, structure_specs_path: Path):
        """Export or update ribasim structure specifications"""
        if structure_type not in ['pump', 'weir', 'orifice', 'culvert']:
            raise ValueError(f" x not able export/update structure type {structure_type}")
        gdfs = {
            "pump": self.pumps_gdf,
            "weir": self.weirs_gdf,
            "orifice": self.orifices_gdf,
            "culvert": self.culverts_gdf
        }
        structures_columns = {
            "pump": ['capacity'],
            "weir": ['crestwidth'],
            "orifice": ['crestwidth'],
            "culvert": ['crestwidth']
        }
        control_headers = [
            "upstream_upperlimit", "upstream_setpoint", "upstream_lowerlimit", 
            "downstream_upperlimit", "downstream_setpoint", "downstream_lowerlimit"
        ]
        sets_columns = {
            "pump": ['startlevelsuctionside', 'stoplevelsuctionside', 'startleveldeliveryside', 'stopleveldeliveryside'],
            "weir": ['crestlevel'] + control_headers,
            "orifice": ['crestlevel'] + control_headers,
            "culvert": ['crestlevel'] + control_headers
        }
        gdf = gdfs[structure_type]
        general_columns = ['structure_id']
        structure_columns = structures_columns[structure_type]
        set_columns = sets_columns[structure_type]
        all_columns_second_level = general_columns + structure_columns + set_columns

        if not structure_specs_path.exists():
            print(f" x ribasim_input_{structure_type} ('{structure_specs_path}') DOES NOT exist. network.{structure_type}s_gdf will be EXPORTED.")
            gdf_export = gdf.drop(columns=[col for col in gdf.columns if col[1] not in all_columns_second_level])
            gdf_export.to_excel(structure_specs_path)
            return gdf

        print(f" x ribasim_input_{structure_type} ('{structure_specs_path}') DOES exist. network.{structure_type}s_gdf will be UPDATED.")
        gdf_input = pd.read_excel(structure_specs_path, header=[0,1], index_col=0)
        for set_name in self.basis_set_names:
            if set_name in gdf_input.columns.get_level_values(level=0):
                gdf_input[set_name] = gdf_input[set_name].replace('', np.nan)
        for col in gdf.columns:
            if col[1] in all_columns_second_level:
                gdf[col] = gdf_input[col]
        return gdf


    def add_split_nodes(
            self,
            stations: bool = False,
            pumps: bool = False,
            weirs: bool = False,
            orifices: bool = False,
            bridges: bool = False,
            culverts: bool = False,
            uniweirs: bool = False,
            edges: bool = False,
            structures_ids_to_include: list[str] = [],
            structures_ids_to_exclude: list[str] = [],
            edge_ids_to_include: list[int] = [],
            edge_ids_to_exclude: list[int] = [],
        ) -> gpd.GeoDataFrame:
        """Set split nodes geodataframe in network object. Overwrites previously defined split nodes"""

        self.split_nodes  = add_split_nodes_based_on_selection(
            stations=stations,
            pumps=pumps,
            weirs=weirs,
            uniweirs=uniweirs,
            orifices=orifices,
            culverts=culverts,
            bridges=bridges,
            edges=edges,
            structures_ids_to_include=structures_ids_to_include,
            structures_ids_to_exclude=structures_ids_to_exclude,
            edge_ids_to_include=edge_ids_to_include,
            edge_ids_to_exclude=edge_ids_to_exclude,
            list_gdfs=[
                self.stations_gdf, 
                self.pumps_gdf, 
                self.weirs_gdf, 
                self.orifices_gdf, 
                self.bridges_gdf, 
                self.culverts_gdf,
                self.uniweirs_gdf,
                self.edges_gdf
            ]
        )
        return self.split_nodes
    

    def read_split_nodes(
            self, 
            split_nodes_file_path: Path, 
            layer_name: str = None, 
            crs: int = 28992,
            buffer_distance: float = 2.0,
            min_length_edge: float = 2.0
        ):
        """Add split nodes in network object from file. Overwrites previously defined split nodes

        Args:
            split_nodes_file_path (Path):   Path to file containing split node geometries
            layer_name (str):               Layer name in geopackage. Needed when file is a geopackage
            crs (int):                      (optional) CRS EPSG code. Default 28992 (RD New) 
        """
        print(f'Network {self.name} - Analysis Split Nodes')
        split_nodes = read_geom_file(
            filepath=split_nodes_file_path, 
            layer_name=layer_name, 
            crs=crs,
            remove_z_dim=True    
        )
        split_nodes_columns = ["split_node", "split_node_id", "object_type", "object_function", "geometry"]
        split_nodes = split_nodes.drop(columns=[c for c in split_nodes.columns if c not in split_nodes_columns])
        split_nodes = log_and_remove_duplicate_geoms(split_nodes, colname='split_node')
        
        # check whether new split_nodes without split_type and split_node_id are included.
        old_split_nodes = split_nodes[(~split_nodes["split_node_id"].isna()) & (split_nodes["object_type"] != "")]
        new_split_nodes = split_nodes[~((~split_nodes["split_node_id"].isna()) & (split_nodes["object_type"] != ""))]

        if new_split_nodes.empty:
            self.split_nodes = old_split_nodes
            if 'split_node' not in self.split_nodes.columns or self.split_nodes.split_node.isna().any():
                self.split_nodes['split_node'] = self.split_nodes.index
            return self.split_nodes

        if "dhydro" in self.basis_source_types:
            gdf_names = ["pump", "weir", "uniweir", "orifice", "culvert", "openwater"]
            gdfs = [self.pumps_gdf, self.weirs_gdf, self.uniweirs_gdf, self.culverts_gdf, None]
        elif "hydamo" in self.basis_source_types:
            gdf_names = ["gemaal", "sluis", "stuw", "afsluitmiddel", "duikersifonhevel", "openwater"]
            gdfs = [self.pumps_gdf, self.sluices_gdf, self.weirs_gdf, self.closers_gdf, self.culverts_gdf, None]
        else:
            raise ValueError("")
        
        hydamo_objects = gpd.GeoDataFrame()
        for gdf_name, gdf in zip(gdf_names, gdfs):
            if gdf is not None and not gdf.empty:
                gdf["object_type"] = gdf_name
                hydamo_objects = pd.concat([hydamo_objects, gdf[["code", "object_type", "geometry"]]])

        hydamo_objects.object_type = pd.Categorical(
            hydamo_objects.object_type, 
            categories=gdf_names, 
            ordered=True
        )
        hydamo_objects = hydamo_objects.sort_values('object_type')
        hydamo_objects_buffer = hydamo_objects[["object_type", "code", "geometry"]].rename(columns={"code": "split_node_id"})
        hydamo_objects_buffer.geometry = hydamo_objects_buffer.geometry.buffer(0.001)

        new_split_nodes = gpd.sjoin(
            new_split_nodes[
                ["object_type", "object_function", "geometry"]
            ].rename(columns={'object_type': 'split_node_object_type'}), 
            hydamo_objects_buffer,
            how='left'
        ).drop(columns=["index_right"])

        # check if object_type is provided for split_node to use that in case of duplicates after spatial join
        dup_new_split_nodes = new_split_nodes.loc[new_split_nodes.index.duplicated(keep=False)]
        non_dup_new_split_nodes = new_split_nodes.loc[~new_split_nodes.index.duplicated(keep=False)]
        filtered_dup_new_split_nodes = pd.DataFrame()
        for i in np.unique(dup_new_split_nodes.index):
            tmp = dup_new_split_nodes.loc[i]
            tmp = tmp.loc[[s == o for s, o in zip(tmp['split_node_object_type'], tmp['object_type'])]]
            if tmp.empty:
                # this means either that object_type in split_nodes is not equal to joined hydamo object type(s)
                # just take the first of the joined hydamo objects
                tmp = dup_new_split_nodes.loc[i].iloc[[0]]
            else:
                # in case multiple of the same object type are joined, take the first one
                tmp = tmp.sort_values('object_type').iloc[[0]]
            filtered_dup_new_split_nodes = pd.concat([filtered_dup_new_split_nodes, tmp])
        new_split_nodes = pd.concat([non_dup_new_split_nodes, filtered_dup_new_split_nodes])
        new_split_nodes.drop(columns=['split_node_object_type'], inplace=True)

        split_nodes = pd.concat([old_split_nodes, new_split_nodes]).sort_values(by=["split_node", "object_type"])
        split_nodes["object_type"] = split_nodes["object_type"].fillna("openwater")

        non_open_water_no_id = split_nodes[~split_nodes["split_node_id"].isnull()]
        open_water_no_id = split_nodes[split_nodes["split_node_id"].isnull()]
        open_water_no_id = gpd.sjoin(
            open_water_no_id,
            self.edges_gdf[["edge_id", "edge_no", "from_node", "to_node", "geometry"]],
            how="left"
        )
        open_water_no_id = open_water_no_id.loc[~open_water_no_id.index.duplicated(keep='first')]
        open_water_no_id["split_node_id"] = open_water_no_id["edge_id"]
        split_nodes = pd.concat([non_open_water_no_id, open_water_no_id])

        split_nodes = split_nodes.reset_index(drop=True)
        split_nodes["split_node"] = split_nodes.index + 1

        split_nodes = log_and_remove_duplicate_geoms(split_nodes, colname='split_node')

        self.split_nodes = split_nodes.copy()

        # Because the split node and boundary locations won't always nicely match up with HyDAMO network, we need to adjust this
        # by snapping split nodes and boundaries to network nodes/edges
        self.split_nodes = snap_to_network(
            snap_type='split_node',
            points=self.split_nodes, 
            edges=self.edges_gdf, 
            nodes=self.nodes_gdf, 
            buffer_distance=buffer_distance,
            min_length_edge=min_length_edge
        )
        
        # split edges by split node locations so we end up with an network where split nodes are only located on nodes (and not edges)
        self.split_nodes, self.edges_gdf, self.nodes_gdf = split_edges_by_split_nodes(
            self.split_nodes, 
            self.edges_gdf, 
            buffer_distance=0.1,  # some small buffer to be sure but should actually not be necessary because of previous snap actions
        )
        if 'split_node' not in self.split_nodes.columns or self.split_nodes.split_node.isna().any():
            self.split_nodes['split_node'] = self.split_nodes.index
        
        # remove non-snapped split nodes and boundaries
        split_nodes_not_on_network = self.split_nodes.loc[
            (self.split_nodes['edge_no'] == -1) & 
            (self.split_nodes['node_no'] == -1)
        ]
        print(f" - Remove non-snapped split nodes from dataset ({len(split_nodes_not_on_network)})")
        self.split_nodes = self.split_nodes.loc[
            [i not in split_nodes_not_on_network.index for i in self.split_nodes.index]
        ]
        return self.split_nodes
    

    def set_split_nodes(self, split_nodes_gdf: gpd.GeoDataFrame) -> None:
        """Set split nodes geodataframe in network object. Overwrites previously defined split nodes"""
        self.split_nodes = split_nodes_gdf


    def read_areas_laterals_timeseries(
        self, 
        areas_laterals_path: Path, 
        sep: str = ',', 
        index_col: int = 0, 
        dayfirst=False
    ):
        """Read timeseries data laterals"""
        self.laterals_areas_data = pd.read_csv(areas_laterals_path, index_col=index_col, sep=sep, parse_dates=True, dayfirst=dayfirst)


    def read_boundaries_timeseries_data(
        self, 
        boundaries_timeseries_path: Path, 
        skiprows=0, 
        sep=",", 
        index_col=0
    ):
        """Read timeseries data boundaries"""
        boundary_csv_data = pd.read_csv(
            boundaries_timeseries_path, 
            sep=sep,
            skiprows=skiprows, 
            index_col=index_col, 
            parse_dates=True
        )
        boundary_csv_data = boundary_csv_data.interpolate()
        self.boundaries_timeseries_data = boundary_csv_data


    def generate_ribasim_lumping_model(
            self,
            simulation_code: str,
            set_name: str,
            split_node_type_conversion: dict,
            split_node_id_conversion: dict,
            starttime: str = None,
            endtime: str = None,
        ) -> ribasim.Model:
        """Generate RIBASIM model using RIBASIM-LUMPING"""
        self.generate_ribasim_lumping_network(
            simulation_code=simulation_code,
        )
        ribasim_model = self.generate_ribasim_model_complete(
            set_name=set_name,
            starttime=starttime,
            endtime=endtime,
            split_node_type_conversion=split_node_type_conversion,
            split_node_id_conversion=split_node_id_conversion,
        )
        return ribasim_model


    def generate_ribasim_lumping_network(
            self,
            simulation_code: str,
            use_laterals_for_basin_area: bool = False,
            assign_unassigned_areas_to_basins: bool = True,
            remove_isolated_basins: bool = False,
            include_flow_boundary_basins: bool = True,
            include_level_boundary_basins: bool = False,
            remove_holes_min_area: float = 10.0,
            option_edges_hydroobjects: bool = False,
        ) -> dict:
        """
        Generate ribasim_lumping network. This function generates all 

        Args:
            simulation_code (str):              give name for ribasim-simulation
            split_node_type_conversion (Dict):  dictionary general conversion of object-type to ribasim-type (e.g. weir: TabulatedRatingCurve)
            split_node_id_conversion (Dict):    dictionary specific conversion of split-node-id to ribasim-type (e.g. KST01234: Outlet)
            use_laterals_for_basin_area (bool): use standard lateral inflow per second per area applied to basin_areas.
            remove_isolated_basins (bool):      
        """
        print(f'Network {self.name} - Generate RIBASIM lumping network')
        # first some checks
        self.simulation_code = simulation_code
        self.simulation_path = Path(self.results_dir, simulation_code)
        if self.split_nodes is None:
            raise ValueError("no split_nodes defined: use .add_split_nodes(), .add_split_nodes_from_file() or .set_split_nodes()")
        if self.nodes_gdf is None or self.edges_gdf is None:
            raise ValueError(
                "no nodes and/or edges defined: add d-hydro simulation results or hydamo network"
            )
        if self.areas_gdf is None:
            print("no areas defined, will not generate basin_areas")
        if self.boundaries_gdf is None:
            print(
                "no boundaries defined, will not generate boundaries and boundaries_basin_connections"
            )
        # self.split_node_type_conversion = split_node_type_conversion
        # self.split_node_id_conversion = split_node_id_conversion

        results = generate_ribasim_network_using_split_nodes(
            nodes=self.nodes_gdf,
            edges=self.edges_gdf,
            split_nodes=self.split_nodes,
            areas=self.areas_gdf,
            boundaries=self.boundaries_gdf,
            laterals=self.laterals_gdf,
            use_laterals_for_basin_area=use_laterals_for_basin_area,
            remove_isolated_basins=remove_isolated_basins,
            include_flow_boundary_basins=include_flow_boundary_basins,
            include_level_boundary_basins=include_level_boundary_basins,
            remove_holes_min_area=remove_holes_min_area,
            crs=self.crs,
            option_edges_hydroobjects=option_edges_hydroobjects,
        )
        self.boundaries_gdf = results['boundaries']
        self.basin_areas_gdf = results['basin_areas']
        self.basins_gdf = results['basins']
        self.areas_gdf = results['areas']
        self.nodes_gdf = results['nodes']
        self.edges_gdf = results['edges']
        self.split_nodes = results['split_nodes']
        self.network_graph = results['network_graph']
        self.basin_connections_gdf = results['basin_connections']
        self.boundary_connections_gdf = results['boundary_connections']

        # assign areas to basin areas which have no edge within it so it is not assigned to any basin area
        # also update the basin and basin area code in edges and nodes
        if assign_unassigned_areas_to_basins:
            results = assign_unassigned_areas_to_basin_areas(
                self.areas_gdf,
                self.basin_areas_gdf, 
                self.drainage_areas_gdf,
                # self.edges_gdf,
            )
            self.areas_gdf = results['areas']
            self.basin_areas_gdf = results['basin_areas']
            if 'edges' in results.keys():
                self.edges_gdf = results['edges']

        # Export to geopackage
        self.export_to_geopackage(simulation_code=simulation_code)
        return results


    def generate_ribasim_model_complete(
        self, 
        set_name: str,
        dummy_model: bool = False,
        interpolation_lines: int = 5,
        saveat: int = 24*3600,
        maxiters: int = None,
        starttime: datetime.datetime = None,
        endtime: datetime.datetime = None,
        results_subgrid: bool = False,
        results_dir: str = 'results',
        database_gpkg: str = 'database.gpkg',
        split_node_type_conversion: dict = None, 
        split_node_id_conversion: dict = None,
    ) -> ribasim.Model:
        """Generate RIBASIM model. From RIBASIM lumping to ribasim.Model"""
        if not dummy_model and set_name not in self.basis_set_names:
            # print(f"set_name {set_name} not in available set_names")
            raise ValueError(f'set_name {set_name} not in available set_names')
        
        # preprocessing data to input for tables
        basins_outflows, node_h_basin, node_h_node, node_a, node_v, basin_h, basin_a, basin_v, \
            node_bedlevel, node_targetlevel, orig_bedlevel, basins_nodes_h_relation, \
                edge_q_df, weir_q_df, uniweir_q_df, \
                    orifice_q_df, culvert_q_df, bridge_q_df, pump_q_df = \
                        preprocessing_ribasim_model_tables(
                            dummy_model=dummy_model,
                            map_data=self.map_data, 
                            his_data=self.his_data,
                            volume_data=self.volume_data, 
                            nodes=self.nodes_gdf, 
                            weirs=self.weirs_gdf,
                            uniweirs=self.uniweirs_gdf,
                            pumps=self.pumps_gdf, 
                            culverts=self.culverts_gdf,
                            orifices=self.orifices_gdf,
                            boundaries=self.boundaries_gdf,
                            basins=self.basins_gdf, 
                            split_nodes=self.split_nodes, 
                            basin_connections=self.basin_connections_gdf, 
                            boundary_connections=self.boundary_connections_gdf,
                            interpolation_lines=interpolation_lines,
                            set_names=self.basis_set_names,
                            split_node_type_conversion=split_node_type_conversion, 
                            split_node_id_conversion=split_node_id_conversion
                        )

        self.nodes_gdf["bedlevel"] = orig_bedlevel
        self.nodes_h_df = node_h_node
        self.nodes_h_basin_df = node_h_basin
        self.nodes_a_df = node_a
        self.nodes_v_df = node_v
        self.basins_h_df = basin_h
        self.basins_a_df = basin_a
        self.basins_v_df = basin_v
        self.basins_nodes_h_relation = basins_nodes_h_relation
        self.edge_q_df = edge_q_df
        self.weir_q_df = weir_q_df
        self.uniweir_q_df = uniweir_q_df
        self.orifice_q_df = orifice_q_df
        self.culvert_q_df = culvert_q_df
        self.bridge_q_df = bridge_q_df
        self.pump_q_df = pump_q_df
        self.basins_outflows = basins_outflows
        self.node_bedlevel = node_bedlevel
        self.node_targetlevel = node_targetlevel
        
        basin_h_initial = None
        if not dummy_model and basin_h is not None:
            if self.method_initial_waterlevels == 1:
                basin_h_initial = basin_h.loc[set_name].loc["targetlevel"]
                # raise ValueError('method initial waterlevels = 1 not yet implemented')
            elif self.method_initial_waterlevels == 2:
                basin_h_initial = basin_h.loc[self.initial_waterlevels_set_name]
                if self.initial_waterlevels_timestep in basin_h_initial.index:
                    basin_h_initial = basin_h_initial.loc[self.initial_waterlevels_timestep]
                else:
                    basin_h_initial = basin_h_initial.iloc[-1]
            elif self.method_initial_waterlevels == 3:
                raise ValueError('method initial waterlevels = 3 not yet implemented')
            else:
                raise ValueError('method initial waterlevels not 1, 2 or 3')

        # generate ribasim model tables
        tables = generate_ribasim_model_tables(
            dummy_model=dummy_model,
            basin_h=basin_h, 
            basin_a=basin_a, 
            basins=self.basins_gdf, 
            basin_areas=self.basin_areas_gdf,
            areas=self.areas_gdf,
            basins_nodes_h_relation=self.basins_nodes_h_relation,
            laterals=self.laterals_gdf,
            laterals_data=self.laterals_data,
            boundaries=self.boundaries_gdf, 
            boundaries_data=self.boundaries_data,
            split_nodes=self.split_nodes,
            basins_outflows=basins_outflows,
            set_name=set_name,
            method_boundaries=self.method_boundaries,
            boundaries_timeseries_data=self.boundaries_timeseries_data,
            method_laterals=self.method_laterals,
            laterals_areas_data=self.laterals_areas_data,
            laterals_drainage_per_ha=self.laterals_drainage_per_ha,
            basin_h_initial=basin_h_initial,
            saveat=saveat,
            edge_q_df=edge_q_df, 
            weir_q_df=weir_q_df, 
            uniweir_q_df=uniweir_q_df, 
            orifice_q_df=orifice_q_df, 
            culvert_q_df=culvert_q_df, 
            bridge_q_df=bridge_q_df, 
            pump_q_df=pump_q_df,
        )

        # generate ribasim model
        ribasim_model = generate_ribasim_model(
            simulation_filepath=Path(self.results_dir, self.simulation_code),
            basins=self.basins_gdf.copy(),
            split_nodes=self.split_nodes.copy(),
            boundaries=self.boundaries_gdf.copy(),
            basin_connections=self.basin_connections_gdf.copy(),
            boundary_connections=self.boundary_connections_gdf.copy(),
            tables=tables,
            database_gpkg=database_gpkg,
            results_dir=results_dir,
        )
        # Export ribasim model
        if self.simulation_path is None:
            self.simulation_path = Path(self.results_dir, self.simulation_code)
        # check for timestep (saveat)
        if maxiters is None:
            ribasim_model.solver = ribasim.Solver(saveat=saveat, sparse=False)
        else:
            ribasim_model.solver = ribasim.Solver(saveat=saveat, maxiters=maxiters, sparse=False)
        ribasim_model.results = ribasim.Results(subgrid=results_subgrid)

        self.ribasim_model = ribasim_model
        self.write_ribasim_model(
            starttime=starttime, 
            endtime=endtime
        )
        return ribasim_model


    def write_ribasim_model(self, starttime=None, endtime=None):
        if starttime is not None and endtime is not None:
            self.ribasim_model.starttime = starttime
            self.ribasim_model.endtime = endtime

        print(f"Export location: {Path(self.results_dir, self.simulation_code)}")
        # export ribasim_network
        self.export_to_geopackage(simulation_code=self.simulation_code)

        # export ribasim_model
        self.ribasim_model.write(Path(self.simulation_path, "ribasim.toml"))
        
        # write bat-file
        with open(Path(self.simulation_path, "run_ribasim_model.bat"), 'w') as f:
            f.write(f"{str(self.path_ribasim_executable)} ribasim.toml\n")
            f.write(f"pause")
        

    def export_to_geopackage(
            self, 
            simulation_code: str, 
            results_dir: 
            Path | str = None
        ):
        """Export RIBASIM lumping results to ribasim_network.gpkg"""
        if results_dir is None:
            results_dir = self.results_dir
        results_network_dir = Path(results_dir, simulation_code)
        if not Path(results_network_dir).exists():
            Path(results_network_dir).mkdir()
        gpkg_path = Path(results_network_dir, "ribasim_network.gpkg")
        qgz_path = Path(results_network_dir, "ribasim_network.qgz")

        gdfs_orig = dict(
            areas=self.areas_gdf,
            branches=self.branches_gdf,
            nodes=self.nodes_gdf,
            edges=self.edges_gdf,
            stations=self.stations_gdf,
            pumps=self.pumps_gdf,
            weirs=self.weirs_gdf,
            orifices=self.orifices_gdf,
            bridges=self.bridges_gdf,
            culverts=self.culverts_gdf,
            uniweirs=self.uniweirs_gdf,
            basin_areas=self.basin_areas_gdf,
            split_nodes=self.split_nodes,
            basins=self.basins_gdf,
            basin_connections=self.basin_connections_gdf,
            laterals=self.laterals_gdf,
            boundaries=self.boundaries_gdf,
            boundary_connections=self.boundary_connections_gdf,
            node_h=self.nodes_h_df,
            node_a=self.nodes_a_df,
            node_v=self.nodes_v_df,
            basin_h=self.basins_h_df,
            basin_a=self.basins_a_df,
            basin_v=self.basins_v_df,
            basins_nodes_h_relation=self.basins_nodes_h_relation
        )
        gdfs_none = dict()
        gdfs = dict()
        for gdf_name, gdf in gdfs_orig.items():
            if gdf is None:
                gdfs_none[gdf_name] = gdf
            elif "geometry" not in gdf.columns:
                if gdf.columns.name is not None:
                    column = gdf.columns.name
                    gdf = gdf.stack()
                    gdf.name = "data"
                    gdf = gdf.reset_index().reset_index().sort_values(by=[column, "index"]).reset_index(drop=True).drop(columns="index")
                gdf["geometry"] = Point(0,0)
                gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=28992)
                gdfs[gdf_name] = gdf
            else:
                gdfs[gdf_name] = gdf

        print(f"Exporting to geopackage:")
        print(" - available: ", end="", flush=True)
        for gdf_name, gdf in gdfs.items():
            print(f"{gdf_name}, ", end="", flush=True)
            gdf_copy = gdf.copy()
            if isinstance(gdf_copy.columns, pd.MultiIndex):
                gdf_copy.columns = ['__'.join(col).strip('__') for col in gdf_copy.columns.values]
            gdf_copy.to_file(gpkg_path, layer=gdf_name, driver="GPKG")

        print("")
        print(" - not available: ", end="", flush=True)
        empty_gdf = gpd.GeoDataFrame(columns=["geometry"], geometry="geometry", crs=self.crs)
        for gdf_name, gdf in gdfs_none.items():
            print(f"{gdf_name}, ", end="", flush=True)
            empty_gdf.to_file(gpkg_path, layer=gdf_name, driver="GPKG")
        
        if not qgz_path.exists():
            qgz_path_stored_dir = os.path.abspath(os.path.dirname(__file__))
            qgz_path_stored = Path(qgz_path_stored_dir, "assets\\ribasim_network.qgz")
            shutil.copy(qgz_path_stored, qgz_path)
        print("")
        print(f"Export location: {qgz_path}")


    def plot(self):
        """"Plot RIBASIM lumping network"""
        fig, ax = plt.subplots(figsize=(10, 10))
        if self.basin_areas_gdf is not None:
            cmap = matplotlib.colors.ListedColormap(np.random.rand(len(self.basin_areas_gdf)*2, 3))
            self.basin_areas_gdf.plot(ax=ax, column='basin_node_id', cmap=cmap, alpha=0.35, zorder=1)
            self.basin_areas_gdf.plot(ax=ax, facecolor='none', edgecolor='black', linewidth=0.25, label='basin_areas', zorder=1)
        elif self.areas_gdf is not None:
            cmap = matplotlib.colors.ListedColormap(np.random.rand(len(self.areas_gdf)*2, 3))
            self.areas_gdf.plot(ax=ax, column='code', cmap=cmap, alpha=0.35, zorder=1)
            self.areas_gdf.plot(ax=ax, facecolor='none', edgecolor='black', linewidth=0.2, label='areas', zorder=1)
        # if self.ribasim_model is not None:
        #     self.ribasim_model.plot(ax=ax)
        if self.edges_gdf is not None:
            self.edges_gdf.plot(ax=ax, linewidth=1, color='blue', label='hydro-objecten', zorder=2)
        if self.split_nodes is not None:
            self.split_nodes.plot(ax=ax, color='black', label='split_nodes', zorder=3)
        if self.boundaries_gdf is not None:
            self.boundaries_gdf.plot(ax=ax, color='red', marker='s', label='boundary', zorder=3)
        ax.axis('off')
        ax.legend(prop={"size": 10}, loc="lower right", bbox_to_anchor=(1.4, 0.0))
        cx.add_basemap(ax, crs=self.areas_gdf.crs)
        return fig, ax


def create_ribasim_lumping_network(**kwargs):
    return RibasimLumpingNetwork(**kwargs)
