from pathlib import Path
from shapely.geometry import LineString, Point
from typing import Tuple
import geopandas as gpd
import pandas as pd
import fiona

from ..utils.general_functions import read_geom_file, generate_nodes_from_edges, split_edges_by_dx


def add_hydamo_basis_network(
    hydamo_network_file: Path = 'hydamo.gpkg',
    hydamo_split_network_dx: float = None,
    crs: int = 28992,
):
    # ) -> Tuple[
    #     gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, 
    #     gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame, 
    #     gpd.GeoDataFrame, gpd.GeoDataFrame
    # ]:
    """
    Load network data from HyDAMO files

    Args:
        hydamo_network_file (Path):         Path to file containing network geometries (hydroobjects)
        hydamo_network_gpkg_layer (str):    Layer name in geopackage. Needed when file is a geopackage
        crs (int):                          (optional) CRS EPSG code. Default 28992 (RD New)
    
    Returns:
        Tuple containing GeoDataFrames with branches, edges nodes
    """
    branches_gdf = read_geom_file(
        filepath=hydamo_network_file, 
        layer_name="hydroobject", 
        crs=crs, 
        remove_z_dim=True
    )
    branches_gdf = branches_gdf.rename(columns={'code': 'branch_id'})[['branch_id', 'geometry']]
    branches_gdf, network_nodes_gdf = generate_nodes_from_edges(branches_gdf)
    print(f" - branches ({len(branches_gdf)}x)", end=', ')

    # Split up hydamo edges with given distance as approximate length of new edges
    if hydamo_split_network_dx is None:
        edges_gdf = branches_gdf.copy().rename(columns={"branch_id": "edge_id"})
    else:
        edges_gdf = split_edges_by_dx(
            edges=branches_gdf, 
            dx=hydamo_split_network_dx,
        )
    edges_gdf, nodes_gdf = generate_nodes_from_edges(edges_gdf)
    edges_gdf.index.name = "index"
    print(f" edges ({len(edges_gdf) if edges_gdf is not None else 0}x)", end=', ')

    # Read structures and data according to hydamo-format
    weirs_gdf, culverts_gdf, pumps_gdf, sluices_gdf, closers_gdf = None, None, None, None, None
    
    pumps_gdf = read_geom_file(
        filepath=hydamo_network_file,
        layer_name="gemaal",
        crs=crs
    )
    print(f" pumps ({len(pumps_gdf) if pumps_gdf is not None else 0}x)", end=', ')

    sluices_gdf = read_geom_file(
        filepath=hydamo_network_file,
        layer_name="sluis",
        crs=crs
    )
    print(f"sluices ({len(sluices_gdf) if sluices_gdf is not None else 0}x)", end=', ')

    weirs_gdf  = read_geom_file(
        filepath=hydamo_network_file,
        layer_name="stuw",
        crs=crs
    )
    print(f"weirs ({len(weirs_gdf) if weirs_gdf is not None else 0}x)", end=', ')

    culverts_gdf  = read_geom_file(
        filepath=hydamo_network_file,
        layer_name="duikersifonhevel",
        crs=crs
    )
    print(f"culverts ({len(culverts_gdf) if culverts_gdf is not None else 0}x)", end=', ')

    closers_gdf = read_geom_file(
        filepath=hydamo_network_file,
        layer_name="afsluitmiddel",
        crs=crs
    )
    print(f"closers ({len(closers_gdf) if closers_gdf is not None else 0}x)")

    if "pomp" in fiona.listlayers(hydamo_network_file):
        pumps_df = gpd.read_file(hydamo_network_file, layer="pomp")
    else:
        pumps_df = None
    
    # set column names to lowercase and return
    results = [branches_gdf, network_nodes_gdf, edges_gdf, nodes_gdf, weirs_gdf, culverts_gdf, pumps_gdf, pumps_df, sluices_gdf, closers_gdf]
    results = [None if x is None else x.rename(columns={c: c.lower() for c in x.columns}) for x in results]
    return results

