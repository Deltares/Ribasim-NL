from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import geopandas as gpd
import pandas as pd
import ribasim
from shapely.geometry import LineString


def generate_ribasim_nodes_static(
    boundaries: gpd.GeoDataFrame, 
    split_nodes: gpd.GeoDataFrame, 
    basins: gpd.GeoDataFrame, 
):
    """Generate Ribasim Nodes"""
    # Ribasim Nodes Static
    nodes = pd.concat([
        boundaries.rename(columns={"boundary_node_id": "node_id"}), 
        split_nodes.rename(columns={"split_node_node_id": "node_id", "split_node_id": "name"}),
        basins.rename(columns={"basin_node_id": "node_id"}),
    ])

    print(f"nodes ({len(nodes)}x), ", end="", flush=True)
    ribasim_nodes_static = gpd.GeoDataFrame(
        data=nodes,
        geometry='geometry',
        crs=split_nodes.crs
    )
    ribasim_nodes_static = ribasim_nodes_static.set_index("node_id")
    ribasim_nodes_static = ribasim_nodes_static[["ribasim_type", "name", "geometry"]]
    ribasim_nodes_static = ribasim_nodes_static.rename(columns={"ribasim_type": "node_type"})

    if ~ribasim_nodes_static.empty:
        ribasim_nodes = ribasim.Node(df=ribasim_nodes_static)
    else:
        ribasim_nodes = None
    
    return ribasim_nodes


def generate_ribasim_edges(
    basin_connections: gpd.GeoDataFrame, 
    boundary_connections: gpd.GeoDataFrame
):
    """generate ribasim edges between nodes, using basin connections and boundary-basin connections"""
    edges = pd.concat([
        basin_connections[["from_node_id", "to_node_id", "geometry"]], 
        boundary_connections[["from_node_id", "to_node_id", "geometry"]], 
    ], ignore_index=True)

    print(f"edges ({len(edges)}x), ", end="", flush=True)

    edges["edge_type"] = "flow"
    ribasim_edges_static = gpd.GeoDataFrame(
        data=edges,
        geometry='geometry',
        crs=basin_connections.crs
    )
    if ribasim_edges_static.empty:
        ribasim_edges = None
    else:
        ribasim_edges = ribasim.Edge(df=ribasim_edges_static)
    return ribasim_edges


def generate_ribasim_basins(
    basin_profile: pd.DataFrame,
    basin_time: pd.DataFrame,
    basin_state: pd.DataFrame,
    basin_subgrid: pd.DataFrame,
    basin_areas: gpd.GeoDataFrame
):
    """Generate settings for Ribasim Basins:
    static: node_id, drainage, potential_evaporation, infiltration, precipitation, urban_runoff
    profile: node_id, level, area, storage
    """
    if basin_profile.empty or basin_time.empty:
        print(f"basins (--)", end="", flush=True)
        return ribasim.Basin()
    print(f"basins ({len(basin_state)}x)", end="", flush=True)
    ribasim_basins = ribasim.Basin(
        profile=basin_profile, 
        time=basin_time, 
        state=basin_state, 
        subgrid=basin_subgrid,
        area=basin_areas
    )
    return ribasim_basins


def generate_ribasim_level_boundaries(
        level_boundary_static: gpd.GeoDataFrame,
        level_boundary_time: pd.DataFrame,
):
    """generate ribasim level boundaries for all level boundary nodes
    static: node_id, level"""
    if level_boundary_time is not None:
        print('level')
        return ribasim.LevelBoundary(time=level_boundary_time)
    elif level_boundary_static is None or level_boundary_static.empty:
        print(f"boundaries (--", end="", flush=True)
        return ribasim.LevelBoundary()
    print(f"boundaries ({len(level_boundary_static)}x)", end="", flush=True)
    return ribasim.LevelBoundary(static=level_boundary_static)


def generate_ribasim_flow_boundaries(
        flow_boundary_static: gpd.GeoDataFrame, 
        flow_boundary_time: pd.DataFrame):
    """generate ribasim flow boundaries for all flow boundary nodes
    static: node_id, flow_rate"""
    print("flow_boundaries ", end="", flush=True)
    if flow_boundary_time is not None:
        return ribasim.FlowBoundary(time=flow_boundary_time)
    elif flow_boundary_static is None or flow_boundary_static.empty:
        print("   x no flow boundaries")
        return ribasim.FlowBoundary()
    return ribasim.FlowBoundary(static=flow_boundary_static)


def generate_ribasim_pumps(pump_static: gpd.GeoDataFrame):
    """generate ribasim pumps for all pump nodes
    static: node_id, flow_rate""" 
    print("pumps ", end="", flush=True)
    if pump_static is None or pump_static.empty:
        print("   x no pumps")
        return ribasim.Pump()
    return ribasim.Pump(static=pump_static)


def generate_ribasim_outlets(outlet_static: gpd.GeoDataFrame):
    """generate ribasim outlets for all outlet nodes
    static: node_id, flow_rate"""
    print("outlets ", end="", flush=True)
    if outlet_static is None or outlet_static.empty:
        print("   x no outlets", end="", flush=True)
        return ribasim.Outlet()
    return ribasim.Outlet(static=outlet_static)


def generate_ribasim_tabulatedratingcurves(
    tabulated_rating_curve_static: pd.DataFrame
):
    """generate ribasim tabulated rating using dummyvalues for level and flow_rate
    static: node_id, level, flow_rate"""
    print("tabulatedratingcurve ", end="", flush=True)
    if tabulated_rating_curve_static is None or tabulated_rating_curve_static.empty:
        print("   x no tabulated rating curve")
        return ribasim.TabulatedRatingCurve()
    return ribasim.TabulatedRatingCurve(static=tabulated_rating_curve_static)


def generate_ribasim_manningresistances(manningresistance_static: gpd.GeoDataFrame):
    """generate ribasim manning resistances
    static: node_id, length, manning_n, profile_width, profile_slope"""
    print("manningresistances ", end="", flush=True)
    if manningresistance_static is None or manningresistance_static.empty:
        print("   x no manningresistance")
        return ribasim.ManningResistance()
    return ribasim.ManningResistance(static=manningresistance_static)
    

def generate_ribasim_linear_resistances(linearresistance_static: gpd.GeoDataFrame):
    """generate ribasim linear resistances
    static: node_id, resistance, max_flow_rate"""
    print("linearresistances ", end="", flush=True)
    if linearresistance_static is None or linearresistance_static.empty:
        print("   x no linearresistance")
        return ribasim.LinearResistance()
    return ribasim.LinearResistance(static=linearresistance_static)


def generate_ribasim_fractional_flows():
    return ribasim.FractionalFlow()


def generate_ribasim_terminals():
    return ribasim.Terminal()


def generate_ribasim_discrete_controls():
    return ribasim.DiscreteControl()


def generate_ribasim_pid_controls():
    return ribasim.PidControl()


def generate_ribasim_user_demands():
    return ribasim.UserDemand()


def generate_ribasim_allocations():
    return ribasim.Allocation()


def generate_ribasim_solvers():
    return ribasim.Solver()


def generate_ribasim_loggings():
    return ribasim.Logging()


def generate_ribasim_model(
    simulation_filepath: Path,
    basins: gpd.GeoDataFrame = None, 
    split_nodes: gpd.GeoDataFrame = None, 
    boundaries: gpd.GeoDataFrame = None, 
    basin_connections: gpd.GeoDataFrame = None, 
    boundary_connections: gpd.GeoDataFrame = None, 
    tables: Dict = None,
    database_gpkg: str = 'database.gpkg',
    results_dir: str = 'results'
):
    """generate ribasim model from ribasim nodes and edges and
    optional input; ribasim basins, level boundary, flow_boundary, pump, tabulated rating curve and manning resistance """
    
    print("Generate ribasim model: ", end="", flush=True)
    
    ribasim_nodes = generate_ribasim_nodes_static(
        boundaries=boundaries, 
        split_nodes=split_nodes, 
        basins=basins,
    )

    ribasim_edges = generate_ribasim_edges(
        basin_connections=basin_connections,
        boundary_connections=boundary_connections
    )
    
    ribasim_basins = generate_ribasim_basins(
        basin_profile=tables['basin_profile'],
        basin_time=tables['basin_time'], 
        basin_state=tables['basin_state'],
        basin_subgrid=tables['basin_subgrid'],
        basin_areas=tables['basin_areas']
    )

    ribasim_level_boundaries = generate_ribasim_level_boundaries(
        level_boundary_static=tables['level_boundary_static'],
        level_boundary_time=tables['level_boundary_time']
    )

    ribasim_flow_boundaries = generate_ribasim_flow_boundaries(
        flow_boundary_static=tables['flow_boundary_static'],
        flow_boundary_time=tables['flow_boundary_time']
    )

    ribasim_pumps = generate_ribasim_pumps(
        pump_static=tables['pump_static']
    )

    ribasim_outlets = generate_ribasim_outlets(
        outlet_static=tables['outlet_static']
    )

    ribasim_tabulated_rating_curve = generate_ribasim_tabulatedratingcurves(
        tabulated_rating_curve_static=tables['tabulated_rating_curve_static'], 
    )

    ribasim_manning_resistance = generate_ribasim_manningresistances(
        manningresistance_static=tables['manningresistance_static'], 
    )

    linear_resistances = generate_ribasim_linear_resistances(
        linearresistance_static=tables['linearresistance_static'], 
    )

    fractions_flows = generate_ribasim_fractional_flows()

    terminals = generate_ribasim_terminals()

    discrete_controls = generate_ribasim_discrete_controls()

    pid_controls = generate_ribasim_pid_controls()

    users = generate_ribasim_user_demands()

    allocations = generate_ribasim_allocations()

    solvers = generate_ribasim_solvers()

    loggings = generate_ribasim_loggings()

    starttime = tables['basin_time']["time"].iloc[0].strftime("%Y-%m-%d %H:%M")
    endtime = tables['basin_time']["time"].iloc[-1].strftime("%Y-%m-%d %H:%M")

    print("")
    network = ribasim.Network(
        node=ribasim_nodes,
        edge=ribasim_edges,
        filepath=simulation_filepath
    )
    ribasim_model = ribasim.Model(
        # modelname=simulation_code,
        network=network,
        basin=ribasim_basins,
        level_boundary=ribasim_level_boundaries,
        flow_boundary=ribasim_flow_boundaries,
        pump=ribasim_pumps,
        outlet=ribasim_outlets,
        tabulated_rating_curve=ribasim_tabulated_rating_curve,
        manning_resistance=ribasim_manning_resistance,
        fractional_flow=fractions_flows,
        linear_resistance=linear_resistances,
        terminal=terminals,
        discrete_control=discrete_controls,
        pid_control=pid_controls,
        user=users,
        allocation=allocations,
        solver=solvers,
        logging=loggings,
        starttime=starttime,
        endtime=endtime,
    )

    # add database name and results folder
    ribasim_model.database = database_gpkg
    ribasim_model.results_dir = results_dir
    return ribasim_model

