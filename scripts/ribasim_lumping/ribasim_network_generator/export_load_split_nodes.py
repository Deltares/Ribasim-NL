import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
from typing import List, Dict, Any, Union, Optional, Tuple
from pathlib import Path


def write_structures_to_excel(
    pumps: gpd.GeoDataFrame = None,
    weirs: gpd.GeoDataFrame = None,
    orifices: gpd.GeoDataFrame = None,
    bridges: gpd.GeoDataFrame = None,
    culverts: gpd.GeoDataFrame = None,
    uniweirs: gpd.GeoDataFrame = None,
    split_nodes: gpd.GeoDataFrame = None,
    split_node_type_conversion: Dict = None,
    split_node_id_conversion: Dict = None,
    results_dir: Union[Path, str] = None,
 ):
    """ export all structures and splitnode info to excel file with seperate sheet per structure type
    input: network with structure gdfs, splitnodes, split node type conversion tables"""

    list_gdfs = [pumps, weirs, orifices, bridges, culverts, uniweirs]
    structures = pd.DataFrame(
        columns=['mesh1d_node_id', 'mesh1d_nEdges', 'geometry', 'object_type']
    )

    if split_nodes is not None:
        splitnodes = split_nodes.copy()
        # splitnodes['splitnode']='yes'

    with pd.ExcelWriter(f"{results_dir}/structures.xlsx", engine='xlsxwriter') as writer:  
        for gdf in list_gdfs:
            if gdf is None:
                continue
            # merge structure gdf with splitnode
            if split_nodes is None:
                structure=gdf.copy()
            else:
                if split_node_type_conversion is not None:
                    # voeg conversie tabel toe
                    split_node_type_conversion = split_node_type_conversion
                    if isinstance(split_node_type_conversion, Dict):
                        for key, value in split_node_type_conversion.items():
                            splitnodes['type'] = splitnodes['split_type'].replace(split_node_type_conversion)
                    split_node_id_conversion = split_node_id_conversion
                    if isinstance(split_node_id_conversion, Dict):
                        for key, value in split_node_id_conversion.items():
                            if len(splitnodes[splitnodes['mesh1d_node_id'] == key]) == 0:
                                print(f" * split_node type conversion id={key} (type={value}) does not exist")
                            splitnodes.loc[splitnodes['mesh1d_node_id'] == key, 'type'] = value

                # merge structures with splitnodes
                structure = gdf.merge(
                    splitnodes, 
                    left_on='mesh1d_node_id', 
                    right_on='mesh1d_node_id', 
                    how='left', 
                    suffixes=('', '_spl'),
                    indicator=True
                )
                structure['use_splitnode'] = structure['_merge'].map({'both': 'yes', 'left_only': 'no'})
                
            # save structures in excelfile, with seperate sheet per structuretype 
            if not structure.empty:
                struct_name=structure['object_type'][0]
                print(f'write {struct_name} to excel')
                structure = structure[structures.columns + ['mesh1d_node_id', 'projection_x', 'projection_y', 'object_type','use_splitnode','status','splitnode','type']]
                structure.to_excel(writer, sheet_name=struct_name)


def read_structures_from_excel(excel_path):
    """ import all structure ids from excelfile 
    use columns mesh1d_node_id, use_splitnode and type 
    and output:
    - dictionary with excel data
    - list of structures to include as splitnode 
    - dictionary with structure ids and splitnode conversion type """
    structures_excel = pd.read_excel(excel_path, sheet_name=None) #sheet_name None to read all sheets as dictionary

    structures_ids_to_include_as_splitnode = []
    split_node_id_conversion = dict()

    for key in structures_excel:
        structure_name = key
        structure_df = structures_excel.get(structure_name)
        print(f'read sheet {key}')
        # filter the structures to use as splitnode and add to list structures_ids_to_include_as_splitnode
        if 'use_splitnode' not in structure_df.columns:
            print(f'- no column named use_splitnode found in sheet {key}')
        else:
            structure_splitnodes = structure_df.loc[structure_df['use_splitnode'] == 'yes' ] 
            structure_ids = list(structure_splitnodes['mesh1d_node_id'])
            structures_ids_to_include_as_splitnode = structures_ids_to_include_as_splitnode + structure_ids
            if 'type' not in structure_df.columns:
                print(f'- no column named type found in sheet {key}')
            else:
                # convert to dictionary for split_node_id_conversion
                structure_dict = dict(zip(structure_splitnodes['mesh1d_node_id'], structure_splitnodes['type']))
                split_node_id_conversion.update(structure_dict)
    
    return structures_excel, structures_ids_to_include_as_splitnode, split_node_id_conversion


