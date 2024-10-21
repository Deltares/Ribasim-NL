import sys
import warnings
from pathlib import Path

import pandas as pd
import ribasim
from crossings_to_ribasim import *
from ribasim import Model

pd.set_option("display.max_columns", None)
warnings.filterwarnings("ignore")  # Let op!!!!!!!!!!!!!! Waarschuwingen worden genegeerd


ribasim.__version__


# load ribasim_nl
module_path = Path.cwd() / "../ribasim_nl/"
sys.path.append(str(module_path))

# load crossings_to_ribasim.py
sys.path.append("peilbeheerst_model")


# # Amstel, Gooi en Vecht


model_characteristics = {
    # model description
    "waterschap": "AmstelGooienVecht",
    "modelname": "20240417_samenwerkdag",
    "modeltype": "boezemmodel",
    # define paths
    "path_postprocessed_data": r"../../../../Data_postprocessed/Waterschappen/AmstelGooienVecht/AGV.gpkg",
    "path_crossings": "../../../../Data_crossings/AmstelGooienVecht/agv_crossings_v05.gpkg",
    "path_boezem": "../../../../Data_shortest_path/AGV/AGV_shortest_path.gpkg",
    "path_Pdrive": None,
    "path_goodcloud_password": "../../../../Data_overig/password_goodcloud.txt",
    # apply filters
    "crossings_layer": "crossings_hydroobject_filtered",
    "in_use": True,
    "agg_links_in_use": True,
    "agg_areas_in_use": True,
    "aggregation": True,
    # data storage settings
    "write_Pdrive": False,
    "write_Zdrive": True,
    "write_goodcloud": True,
    "write_checks": True,
    "write_symbology": True,
    # numerical settings
    "solver": None,
    "logging": None,
    "starttime": "2024-01-01 00:00:00",
    "endtime": "2024-01-02 00:00:00",
}

waterboard = CrossingsToRibasim(model_characteristics=model_characteristics)

post_processed_data, crossings = waterboard.read_files()
post_processed_data, crossings = waterboard.routing_processor(post_processed_data, crossings)
crossings = waterboard.assign_node_ids(crossings)
edges = waterboard.create_edges(crossings)
nodes, edges = waterboard.create_nodes(crossings, edges)
edges = waterboard.embed_boezems(edges, post_processed_data, crossings)

# create individual model parts of the network
network = RibasimNetwork(nodes=nodes, edges=edges, model_characteristics=model_characteristics)

edge = network.edge()
basin_node, basin_profile, basin_static, basin_state, basin_area = network.basin()
pump_node, pump_static = network.pump()
tabulated_rating_curve_node, tabulated_rating_curve_static = network.tabulated_rating_curve()
level_boundary_node, level_boundary_static = network.level_boundary()
flow_boundary_node, flow_boundary_static = network.flow_boundary()
manning_resistance_node, manning_resistance_static = network.manning_resistance()
terminal_node = network.terminal()

# linear_resistance = network.linear_resistance()
# fractional_flow = network.fractional_flow()
# outlet = network.outlet()
# discrete_control = network.discrete_control()
# pid_control = network.pid_control()

# insert the individual model modules in an actual model
model = Model(starttime=model_characteristics["starttime"], endtime=model_characteristics["endtime"], crs="EPSG:28992")

model.edge.df = edge

model.basin.node.df = basin_node
model.basin.profile = basin_profile
model.basin.static = basin_static
model.basin.state = basin_state
model.basin.area = basin_area

model.pump.node.df = pump_node
model.pump.static = pump_static

model.tabulated_rating_curve.node.df = tabulated_rating_curve_node
model.tabulated_rating_curve.static = tabulated_rating_curve_static

model.manning_resistance.node.df = manning_resistance_node
model.manning_resistance.static = manning_resistance_static

model.level_boundary.node.df = level_boundary_node
model.level_boundary.static = level_boundary_static

model.flow_boundary.node.df = flow_boundary_node
model.flow_boundary.static = flow_boundary_static

model.terminal.node.df = terminal_node

# add checks and metadata
checks = network.check(model, post_processed_data=post_processed_data, crossings=crossings)
model = network.add_meta_data(model, checks, post_processed_data, crossings)

# write the result
network.WriteResults(model=model, checks=checks)


# # Delfland


model_characteristics = {
    # model description
    "waterschap": "Delfland",
    "modelname": "20240423_omgedraaid",
    "modeltype": "boezemmodel",
    # define paths
    "path_postprocessed_data": r"../../../../Data_postprocessed/Waterschappen/Delfland/Delfland.gpkg",
    "path_crossings": "../../../../Data_crossings/Delfland/delfland_crossings_v08.gpkg",
    "path_boezem": "../../../../Data_shortest_path/Delfland/Delfland_shortest_path.gpkg",
    "path_Pdrive": None,
    "path_goodcloud_password": "../../../../Data_overig/password_goodcloud.txt",
    # apply filters
    "crossings_layer": "crossings_hydroobject_filtered",
    "in_use": True,
    "agg_links_in_use": True,
    "agg_areas_in_use": True,
    "aggregation": True,
    # data storage settings
    "write_Pdrive": False,
    "write_Zdrive": True,
    "write_goodcloud": True,
    "write_checks": True,
    "write_symbology": True,
    # numerical settings
    "solver": None,
    "logging": None,
    "starttime": "2024-01-01 00:00:00",
    "endtime": "2024-01-02 00:00:00",
}

waterboard = CrossingsToRibasim(model_characteristics=model_characteristics)

post_processed_data, crossings = waterboard.read_files()
post_processed_data, crossings = waterboard.routing_processor(post_processed_data, crossings)
crossings = waterboard.assign_node_ids(crossings)
edges = waterboard.create_edges(crossings)
nodes, edges = waterboard.create_nodes(crossings, edges)
edges = waterboard.embed_boezems(edges, post_processed_data, crossings)


# create individual model parts of the network
network = RibasimNetwork(nodes=nodes, edges=edges, model_characteristics=model_characteristics)

edge = network.edge()
basin_node, basin_profile, basin_static, basin_state, basin_area = network.basin()
pump_node, pump_static = network.pump()
tabulated_rating_curve_node, tabulated_rating_curve_static = network.tabulated_rating_curve()
level_boundary_node, level_boundary_static = network.level_boundary()
flow_boundary_node, flow_boundary_static = network.flow_boundary()
manning_resistance_node, manning_resistance_static = network.manning_resistance()
terminal_node = network.terminal()

# linear_resistance = network.linear_resistance()
# fractional_flow = network.fractional_flow()
# outlet = network.outlet()
# discrete_control = network.discrete_control()
# pid_control = network.pid_control()
# insert the individual model modules in an actual model
model = Model(starttime=model_characteristics["starttime"], endtime=model_characteristics["endtime"], crs="EPSG:28992")

model.edge = edge

model.basin.node = basin_node
model.basin.profile = basin_profile
model.basin.static = basin_static
model.basin.state = basin_state
model.basin.area = basin_area

model.pump.node = pump_node
model.pump.static = pump_static

model.tabulated_rating_curve.node = tabulated_rating_curve_node
model.tabulated_rating_curve.static = tabulated_rating_curve_static

model.level_boundary.node = level_boundary_node
model.level_boundary.static = level_boundary_static

model.flow_boundary.node = flow_boundary_node
model.flow_boundary.static = flow_boundary_static

model.manning_resistance.node = manning_resistance_node
model.manning_resistance.static = manning_resistance_static

model.terminal.node = terminal_node


# add checks and metadata
checks = network.check(post_processed_data=post_processed_data, crossings=crossings)
model = network.add_meta_data(model, checks, post_processed_data, crossings)

# write the result
network.WriteResults(model=model, checks=checks)


# # Hollandse Delta


model_characteristics = {
    # model description
    "waterschap": "HollandseDelta",
    "modelname": "20240417_samenwerkdag",
    "modeltype": "boezemmodel",
    # define paths
    "path_postprocessed_data": r"../../../../Data_postprocessed/Waterschappen/Hollandse_Delta/HD.gpkg",
    "path_crossings": "../../../../Data_crossings/Hollandse_Delta/hd_crossings_v06.gpkg",
    "path_Pdrive": None,
    "path_goodcloud_password": "../../../../Data_overig/password_goodcloud.txt",
    "path_boezem": "../../../../Data_shortest_path/Hollandse_Delta/HD_shortest_path.gpkg",
    # apply filters
    "crossings_layer": "crossings_hydroobject_filtered",
    "in_use": True,
    "agg_links_in_use": True,
    "agg_areas_in_use": True,
    "aggregation": True,
    # data storage settings
    "write_Pdrive": False,
    "write_Zdrive": True,
    "write_goodcloud": True,
    "write_checks": True,
    "write_symbology": True,
    # numerical settings
    "solver": None,
    "logging": None,
    "starttime": "2024-01-01 00:00:00",
    "endtime": "2024-01-02 00:00:00",
}

waterboard = CrossingsToRibasim(model_characteristics=model_characteristics)

post_processed_data, crossings = waterboard.read_files()
post_processed_data, crossings = waterboard.routing_processor(post_processed_data, crossings)
crossings = waterboard.assign_node_ids(crossings)
edges = waterboard.create_edges(crossings)
nodes, edges = waterboard.create_nodes(crossings, edges)
edges = waterboard.embed_boezems(edges, post_processed_data, crossings)


# create individual model parts of the network
network = RibasimNetwork(nodes=nodes, edges=edges, model_characteristics=model_characteristics)

edge = network.edge()
basin_node, basin_profile, basin_static, basin_state, basin_area = network.basin()
pump_node, pump_static = network.pump()
tabulated_rating_curve_node, tabulated_rating_curve_static = network.tabulated_rating_curve()
level_boundary_node, level_boundary_static = network.level_boundary()
flow_boundary_node, flow_boundary_static = network.flow_boundary()
manning_resistance_node, manning_resistance_static = network.manning_resistance()
terminal_node = network.terminal()

# linear_resistance = network.linear_resistance()
# fractional_flow = network.fractional_flow()
# outlet = network.outlet()
# discrete_control = network.discrete_control()
# pid_control = network.pid_control()

# insert the individual model modules in an actual model
model = Model(starttime=model_characteristics["starttime"], endtime=model_characteristics["endtime"], crs="EPSG:28992")

model.edge = edge

model.basin.node = basin_node
model.basin.profile = basin_profile
model.basin.static = basin_static
model.basin.state = basin_state
model.basin.area = basin_area

model.pump.node = pump_node
model.pump.static = pump_static

model.tabulated_rating_curve.node = tabulated_rating_curve_node
model.tabulated_rating_curve.static = tabulated_rating_curve_static

model.level_boundary.node = level_boundary_node
model.level_boundary.static = level_boundary_static

model.flow_boundary.node = flow_boundary_node
model.flow_boundary.static = flow_boundary_static

model.manning_resistance.node = manning_resistance_node
model.manning_resistance.static = manning_resistance_static

model.terminal.node = terminal_node

# add checks and metadata
checks = network.check(post_processed_data=post_processed_data, crossings=crossings)
model = network.add_meta_data(model, checks, post_processed_data, crossings)

# write the result
network.WriteResults(model=model, checks=checks)


# # Hollands Noorderkwartier


model_characteristics = {
    # model description
    "waterschap": "HollandsNoorderkwartier",
    "modelname": "20240502",
    "modeltype": "boezemmodel",
    # define paths
    "path_postprocessed_data": r"../../../../Data_postprocessed/Waterschappen/HHNK/Noorderkwartier.gpkg",
    "path_crossings": "../../../../Data_crossings/HHNK/hhnk_crossings_v26.gpkg",
    "path_Pdrive": None,
    "path_goodcloud_password": "../../../../Data_overig/password_goodcloud.txt",
    "path_boezem": "../../../../Data_shortest_path/HHNK/HHNK_shortest_path.gpkg",
    # apply filters
    "crossings_layer": "crossings_hydroobject_filtered",
    "in_use": True,
    "agg_links_in_use": True,
    "agg_areas_in_use": True,
    "aggregation": True,
    # data storage settings
    "write_Pdrive": False,
    "write_Zdrive": True,
    "write_goodcloud": True,
    "write_checks": True,
    "write_symbology": True,
    # numerical settings
    "solver": None,
    "logging": None,
    "starttime": "2024-01-01 00:00:00",
    "endtime": "2024-01-02 00:00:00",
}

waterboard = CrossingsToRibasim(model_characteristics=model_characteristics)

post_processed_data, crossings = waterboard.read_files()
post_processed_data, crossings = waterboard.routing_processor(post_processed_data, crossings)
crossings = waterboard.assign_node_ids(crossings)
edges = waterboard.create_edges(crossings)
nodes, edges = waterboard.create_nodes(crossings, edges)
edges = waterboard.embed_boezems(edges, post_processed_data, crossings)
edges = waterboard.change_boezems_manually(edges)


# create individual model parts of the network
network = RibasimNetwork(nodes=nodes, edges=edges, model_characteristics=model_characteristics)

edge = network.edge()
basin_node, basin_profile, basin_static, basin_state, basin_area = network.basin()
pump_node, pump_static = network.pump()
tabulated_rating_curve_node, tabulated_rating_curve_static = network.tabulated_rating_curve()
level_boundary_node, level_boundary_static = network.level_boundary()
flow_boundary_node, flow_boundary_static = network.flow_boundary()
manning_resistance_node, manning_resistance_static = network.manning_resistance()
terminal_node = network.terminal()

# linear_resistance = network.linear_resistance()
# fractional_flow = network.fractional_flow()
# outlet = network.outlet()
# discrete_control = network.discrete_control()
# pid_control = network.pid_control()

# insert the individual model modules in an actual model
model = Model(starttime=model_characteristics["starttime"], endtime=model_characteristics["endtime"], crs="EPSG:28992")

model.edge = edge

model.basin.node = basin_node
model.basin.profile = basin_profile
model.basin.static = basin_static
model.basin.state = basin_state
model.basin.area = basin_area

model.pump.node = pump_node
model.pump.static = pump_static

model.tabulated_rating_curve.node = tabulated_rating_curve_node
model.tabulated_rating_curve.static = tabulated_rating_curve_static

model.level_boundary.node = level_boundary_node
model.level_boundary.static = level_boundary_static

model.flow_boundary.node = flow_boundary_node
model.flow_boundary.static = flow_boundary_static

model.terminal.node = terminal_node

# add checks and metadata
checks = network.check(model=model, post_processed_data=post_processed_data, crossings=crossings)
model = network.add_meta_data(model, checks, post_processed_data, crossings)

# write the result
network.WriteResults(model=model, checks=checks)


# # Rijnland


model_characteristics = {
    # model description
    "waterschap": "Rijnland",
    "modelname": "20240414_aggregated",
    "modeltype": "boezemmodel",
    # define paths
    "path_postprocessed_data": r"../../../../Data_postprocessed/Waterschappen/Rijnland/Rijnland.gpkg",
    "path_crossings": "../../../../Data_crossings/Rijnland/rijnland_crossings_v04.gpkg",
    "path_Pdrive": None,
    "path_boezem": "../../../../Data_shortest_path/Rijnland/Rijnland_shortest_path.gpkg",
    "path_goodcloud_password": "../../../../Data_overig/password_goodcloud.txt",
    # apply filters
    "crossings_layer": "crossings_hydroobject_filtered",
    "in_use": True,
    "agg_links_in_use": True,
    "agg_areas_in_use": True,
    "aggregation": True,
    # data storage settings
    "write_Pdrive": False,
    "write_Zdrive": True,
    "write_goodcloud": True,
    "write_checks": True,
    "write_symbology": True,
    # numerical settings
    "solver": None,
    "logging": None,
    "starttime": "2024-01-01 00:00:00",
    "endtime": "2024-01-02 00:00:00",
}


waterboard = CrossingsToRibasim(model_characteristics=model_characteristics)

post_processed_data, crossings = waterboard.read_files()
post_processed_data, crossings = waterboard.routing_processor(post_processed_data, crossings)
crossings = waterboard.assign_node_ids(crossings)
edges = waterboard.create_edges(crossings)
nodes, edges = waterboard.create_nodes(crossings, edges)
edges = waterboard.embed_boezems(edges, post_processed_data, crossings)


# create individual model parts of the network
network = RibasimNetwork(nodes=nodes, edges=edges, model_characteristics=model_characteristics)

edge = network.edge()
basin_node, basin_profile, basin_static, basin_state, basin_area = network.basin()
pump_node, pump_static = network.pump()
tabulated_rating_curve_node, tabulated_rating_curve_static = network.tabulated_rating_curve()
level_boundary_node, level_boundary_static = network.level_boundary()
flow_boundary_node, flow_boundary_static = network.flow_boundary()
manning_resistance_node, manning_resistance_static = network.manning_resistance()
terminal_node = network.terminal()

# linear_resistance = network.linear_resistance()
# fractional_flow = network.fractional_flow()
# outlet = network.outlet()
# discrete_control = network.discrete_control()
# pid_control = network.pid_control()

# insert the individual model modules in an actual model
model = Model(starttime=model_characteristics["starttime"], endtime=model_characteristics["endtime"], crs="EPSG:28992")

model.edge = edge

model.basin.node = basin_node
model.basin.profile = basin_profile
model.basin.static = basin_static
model.basin.state = basin_state
model.basin.area = basin_area


model.pump.node = pump_node
model.pump.static = pump_static

model.tabulated_rating_curve.node = tabulated_rating_curve_node
model.tabulated_rating_curve.static = tabulated_rating_curve_static

model.level_boundary.node = level_boundary_node
model.level_boundary.static = level_boundary_static

model.flow_boundary.node = flow_boundary_node
model.flow_boundary.static = flow_boundary_static

model.manning_resistance.node = manning_resistance_node
model.manning_resistance.static = manning_resistance_static

model.terminal.node = terminal_node

# add checks and metadata
checks = network.check(post_processed_data=post_processed_data, crossings=crossings, model=model)
model = network.add_meta_data(model, checks, post_processed_data, crossings)

# write the result
network.WriteResults(model=model, checks=checks)


# # Rivierenland


model_characteristics = {
    # model description
    "waterschap": "Rivierenland",
    "modelname": "20240402_bug_fix",
    "modeltype": "boezemmodel",
    # define paths
    "path_postprocessed_data": r"../../../../Data_postprocessed/Waterschappen/WSRL/WSRL.gpkg",
    "path_crossings": "../../../../Data_crossings/WSRL/wsrl_crossings_v06.gpkg",
    "path_Pdrive": None,
    "path_goodcloud_password": "../../../../Data_overig/password_goodcloud.txt",
    "path_boezem": "../../../../Data_shortest_path/WSRL/WSRL_shortest_path.gpkg",
    # apply filters
    "crossings_layer": "crossings_hydroobject_filtered",
    "in_use": True,
    "agg_links_in_use": True,
    "agg_areas_in_use": True,
    "aggregation": True,
    # data storage settings
    "write_Pdrive": False,
    "write_Zdrive": True,
    "write_goodcloud": True,
    "write_checks": True,
    "write_symbology": True,
    # numerical settings
    "solver": None,
    "logging": None,
    "starttime": "2024-01-01 00:00:00",
    "endtime": "2024-01-02 00:00:00",
}

waterboard = CrossingsToRibasim(model_characteristics=model_characteristics)

post_processed_data, crossings = waterboard.read_files()
post_processed_data, crossings = waterboard.routing_processor(post_processed_data, crossings)
crossings = waterboard.assign_node_ids(crossings)
edges = waterboard.create_edges(crossings)
nodes, edges = waterboard.create_nodes(crossings, edges)
edges = waterboard.embed_boezems(edges, post_processed_data, crossings)


# create individual model parts of the network
network = RibasimNetwork(nodes=nodes, edges=edges, model_characteristics=model_characteristics)

edge = network.edge()
basin_node, basin_profile, basin_static, basin_state, basin_area = network.basin()
pump_node, pump_static = network.pump()
tabulated_rating_curve_node, tabulated_rating_curve_static = network.tabulated_rating_curve()
level_boundary_node, level_boundary_static = network.level_boundary()
flow_boundary_node, flow_boundary_static = network.flow_boundary()
manning_resistance_node, manning_resistance_static = network.manning_resistance()
terminal_node = network.terminal()

# linear_resistance = network.linear_resistance()
# fractional_flow = network.fractional_flow()
# outlet = network.outlet()
# discrete_control = network.discrete_control()
# pid_control = network.pid_control()

# insert the individual model modules in an actual model
model = Model(starttime=model_characteristics["starttime"], endtime=model_characteristics["endtime"], crs="EPSG:28992")

model.edge = edge

model.basin.node = basin_node
model.basin.profile = basin_profile
model.basin.static = basin_static
model.basin.state = basin_state
model.basin.area = basin_area

model.pump.node = pump_node
model.pump.static = pump_static

model.tabulated_rating_curve.node = tabulated_rating_curve_node
model.tabulated_rating_curve.static = tabulated_rating_curve_static

model.level_boundary.node = level_boundary_node
model.level_boundary.static = level_boundary_static

model.flow_boundary.node = flow_boundary_node
model.flow_boundary.static = flow_boundary_static

model.terminal.node = terminal_node

# add checks and metadata
checks = network.check(post_processed_data=post_processed_data, crossings=crossings)
model = network.add_meta_data(model, checks, post_processed_data, crossings)

# write the result
network.WriteResults(model=model, checks=checks)

model.tabulated_rating_curve.node = tabulated_rating_curve_node
model.tabulated_rating_curve.static = tabulated_rating_curve_static

model.level_boundary.node = level_boundary_node
model.level_boundary.static = level_boundary_static

model.flow_boundary.node = flow_boundary_node
model.flow_boundary.static = flow_boundary_static

model.manning_resistance.node = manning_resistance_node
model.manning_resistance.static = manning_resistance_static

model.terminal.node = terminal_node

# add checks and metadata
checks = network.check(post_processed_data=post_processed_data, crossings=crossings)
model = network.add_meta_data(model, checks, post_processed_data, crossings)

# write the result
network.WriteResults(model=model, checks=checks)


# # Scheldestromen


model_characteristics = {
    # model description
    "waterschap": "Scheldestromen",
    "modelname": "20240417_samenwerkdag",
    "modeltype": "boezemmodel",
    # define paths
    "path_postprocessed_data": r"../../../../Data_postprocessed/Waterschappen/Scheldestromen/Scheldestromen.gpkg",
    "path_crossings": "../../../../Data_crossings/Scheldestromen/scheldestromen_crossings_v02.gpkg",
    "path_Pdrive": None,
    "path_boezem": "../../../../Data_shortest_path/Scheldestromen/Scheldestromen_shortest_path.gpkg",
    "path_goodcloud_password": "../../../../Data_overig/password_goodcloud.txt",
    # apply filters
    "crossings_layer": "crossings_hydroobject_filtered",
    "in_use": True,
    "agg_links_in_use": True,
    "agg_areas_in_use": True,
    "aggregation": True,
    # data storage settings
    "write_Pdrive": False,
    "write_Zdrive": True,
    "write_goodcloud": True,
    "write_checks": True,
    "write_symbology": True,
    # numerical settings
    "solver": None,
    "logging": None,
    "starttime": "2024-01-01 00:00:00",
    "endtime": "2024-01-02 00:00:00",
}


waterboard = CrossingsToRibasim(model_characteristics=model_characteristics)

post_processed_data, crossings = waterboard.read_files()
post_processed_data, crossings = waterboard.routing_processor(post_processed_data, crossings)
crossings = waterboard.assign_node_ids(crossings)
edges = waterboard.create_edges(crossings)
nodes, edges = waterboard.create_nodes(crossings, edges)
edges = waterboard.embed_boezems(edges, post_processed_data, crossings)


# create individual model parts of the network
network = RibasimNetwork(nodes=nodes, edges=edges, model_characteristics=model_characteristics)

edge = network.edge()
basin_node, basin_profile, basin_static, basin_state, basin_area = network.basin()
pump_node, pump_static = network.pump()
tabulated_rating_curve_node, tabulated_rating_curve_static = network.tabulated_rating_curve()
level_boundary_node, level_boundary_static = network.level_boundary()
flow_boundary_node, flow_boundary_static = network.flow_boundary()
manning_resistance_node, manning_resistance_static = network.manning_resistance()
terminal_node = network.terminal()

# linear_resistance = network.linear_resistance()
# fractional_flow = network.fractional_flow()
# outlet = network.outlet()
# discrete_control = network.discrete_control()
# pid_control = network.pid_control()

# insert the individual model modules in an actual model
model = Model(starttime=model_characteristics["starttime"], endtime=model_characteristics["endtime"], crs="EPSG:28992")

model.edge.df = edge

model.basin.node.df = basin_node
model.basin.profile = basin_profile
model.basin.static = basin_static
model.basin.state = basin_state
model.basin.area = basin_area

model.pump.node.df = pump_node
model.pump.static = pump_static

model.tabulated_rating_curve.node.df = tabulated_rating_curve_node
model.tabulated_rating_curve.static = tabulated_rating_curve_static

model.manning_resistance.node.df = manning_resistance_node
model.manning_resistance.static = manning_resistance_static

model.level_boundary.node.df = level_boundary_node
model.level_boundary.static = level_boundary_static

model.flow_boundary.node.df = flow_boundary_node
model.flow_boundary.static = flow_boundary_static

model.terminal.node.df = terminal_node

# add checks and metadata
checks = network.check(model, post_processed_data=post_processed_data, crossings=crossings)
model = network.add_meta_data(model, checks, post_processed_data, crossings)

# write the result
network.WriteResults(model=model, checks=checks)


# # Schieland en de Krimpenerwaard


model_characteristics = {
    # model description
    "waterschap": "SchielandendeKrimpenerwaard",
    "modelname": "20240429_check",
    "modeltype": "boezemmodel",
    # define paths
    "path_postprocessed_data": r"../../../../Data_postprocessed/Waterschappen/HHSK/HHSK.gpkg",
    "path_crossings": "../../../../Data_crossings/HHSK/hhsk_crossings_v04.gpkg",
    "path_Pdrive": None,
    "path_boezem": "../../../../Data_shortest_path/HHSK/HHSK_shortest_path.gpkg",
    "path_goodcloud_password": "../../../../Data_overig/password_goodcloud.txt",
    # apply filters
    "crossings_layer": "crossings_hydroobject_filtered",
    "in_use": True,
    "agg_links_in_use": True,
    "agg_areas_in_use": True,
    "aggregation": True,
    # data storage settings
    "write_Pdrive": False,
    "write_Zdrive": True,
    "write_goodcloud": True,
    "write_checks": True,
    "write_symbology": True,
    # numerical settings
    "solver": None,
    "logging": None,
    "starttime": "2024-01-01 00:00:00",
    "endtime": "2024-01-02 00:00:00",
}


waterboard = CrossingsToRibasim(model_characteristics=model_characteristics)

post_processed_data, crossings = waterboard.read_files()
post_processed_data, crossings = waterboard.routing_processor(post_processed_data, crossings)
crossings = waterboard.assign_node_ids(crossings)
edges = waterboard.create_edges(crossings)
nodes, edges = waterboard.create_nodes(crossings, edges)
edges = waterboard.embed_boezems(edges, post_processed_data, crossings)

# create individual model parts of the network
network = RibasimNetwork(nodes=nodes, edges=edges, model_characteristics=model_characteristics)

edge = network.edge()
basin_node, basin_profile, basin_static, basin_state, basin_area = network.basin()

pump_node, pump_static = network.pump()
tabulated_rating_curve_node, tabulated_rating_curve_static = network.tabulated_rating_curve()
level_boundary_node, level_boundary_static = network.level_boundary()
flow_boundary_node, flow_boundary_static = network.flow_boundary()
manning_resistance_node, manning_resistance_static = network.manning_resistance()
terminal_node = network.terminal()

# linear_resistance = network.linear_resistance()
# fractional_flow = network.fractional_flow()
# outlet = network.outlet()
# discrete_control = network.discrete_control()
# pid_control = network.pid_control()

# insert the individual model modules in an actual model
model = Model(starttime=model_characteristics["starttime"], endtime=model_characteristics["endtime"], crs="EPSG:28992")

model.edge = edge

model.basin.node = basin_node
model.basin.profile = basin_profile
model.basin.static = basin_static
model.basin.state = basin_state
model.basin.area = basin_area

model.pump.node = pump_node
model.pump.static = pump_static

model.tabulated_rating_curve.node = tabulated_rating_curve_node
model.tabulated_rating_curve.static = tabulated_rating_curve_static

model.level_boundary.node = level_boundary_node
model.level_boundary.static = level_boundary_static

model.flow_boundary.node = flow_boundary_node
model.flow_boundary.static = flow_boundary_static

model.manning_resistance.node = manning_resistance_node
model.manning_resistance.static = manning_resistance_static

model.terminal.node = terminal_node

# add checks and metadata
checks = network.check(model=model, post_processed_data=post_processed_data, crossings=crossings)
model = network.add_meta_data(model, checks, post_processed_data, crossings)

# write the result
network.WriteResults(model=model, checks=checks)


model.tabulated_rating_curve.static


# # Wetterskip


model_characteristics = {
    # model description
    "waterschap": "WetterskipFryslan",
    "modelname": "20240417_samenwerkdag",
    "modeltype": "boezemmodel",
    # define paths
    "path_postprocessed_data": r"../../../../Data_postprocessed/Waterschappen/Wetterskip/Wetterskip.gpkg",
    "path_crossings": "../../../../Data_crossings/Wetterskip/wetterskip_crossings_v06.gpkg",
    "path_Pdrive": None,
    "path_boezem": "../../../../Data_shortest_path/Wetterskip/Wetterskip_shortest_path.gpkg",
    "path_goodcloud_password": "../../../../Data_overig/password_goodcloud.txt",
    # apply filters
    "crossings_layer": "crossings_hydroobject_filtered",
    "in_use": True,
    "agg_links_in_use": True,
    "agg_areas_in_use": True,
    "aggregation": True,  ############################ LET OP
    # data storage settings
    "write_Pdrive": False,
    "write_Zdrive": True,
    "write_goodcloud": True,
    "write_checks": True,
    "write_symbology": True,
    # numerical settings
    "solver": None,
    "logging": None,
    "starttime": "2024-01-01 00:00:00",
    "endtime": "2024-01-02 00:00:00",
}


waterboard = CrossingsToRibasim(model_characteristics=model_characteristics)

post_processed_data, crossings = waterboard.read_files()
post_processed_data, crossings = waterboard.routing_processor(post_processed_data, crossings)
crossings = waterboard.assign_node_ids(crossings)
edges = waterboard.create_edges(crossings)
nodes, edges = waterboard.create_nodes(crossings, edges)
edges = waterboard.embed_boezems(edges, post_processed_data, crossings)


# create individual model parts of the network
network = RibasimNetwork(nodes=nodes, edges=edges, model_characteristics=model_characteristics)

edge = network.edge()
basin_node, basin_profile, basin_static, basin_state, basin_area = network.basin()
pump_node, pump_static = network.pump()
tabulated_rating_curve_node, tabulated_rating_curve_static = network.tabulated_rating_curve()
level_boundary_node, level_boundary_static = network.level_boundary()
flow_boundary_node, flow_boundary_static = network.flow_boundary()
manning_resistance_node, manning_resistance_static = network.manning_resistance()
terminal_node = network.terminal()

# linear_resistance = network.linear_resistance()
# fractional_flow = network.fractional_flow()
# outlet = network.outlet()
# discrete_control = network.discrete_control()
# pid_control = network.pid_control()

# insert the individual model modules in an actual model
model = Model(starttime=model_characteristics["starttime"], endtime=model_characteristics["endtime"], crs="EPSG:28992")

model.edge = edge

model.basin.node = basin_node
model.basin.profile = basin_profile
model.basin.static = basin_static
model.basin.state = basin_state
model.basin.area = basin_area

model.pump.node = pump_node
model.pump.static = pump_static

model.tabulated_rating_curve.node = tabulated_rating_curve_node
model.tabulated_rating_curve.static = tabulated_rating_curve_static

model.level_boundary.node = level_boundary_node
model.level_boundary.static = level_boundary_static

model.flow_boundary.node = flow_boundary_node
model.flow_boundary.static = flow_boundary_static

model.manning_resistance.node = manning_resistance_node
model.manning_resistance.static = manning_resistance_static

model.terminal.node = terminal_node

# add checks and metadata
checks = network.check(model=model, post_processed_data=post_processed_data, crossings=crossings)
model = network.add_meta_data(model, checks, post_processed_data, crossings)

# write the result
network.WriteResults(model=model, checks=checks)


# # Zuiderzeeland


model_characteristics = {
    # model description
    "waterschap": "Zuiderzeeland",
    "modelname": "20240417_samenwerkdag",
    "modeltype": "boezemmodel",
    # define paths
    "path_postprocessed_data": r"../../../../Data_postprocessed/Waterschappen/Zuiderzeeland/Zuiderzeeland.gpkg",
    "path_crossings": "../../../../Data_crossings/Zuiderzeeland/zzl_crossings_v05.gpkg",
    "path_Pdrive": None,
    "path_goodcloud_password": "../../../../Data_overig/password_goodcloud.txt",
    "path_boezem": "../../../../Data_shortest_path/Zuiderzeeland/Zuiderzeeland_shortest_path.gpkg",
    # apply filters
    "crossings_layer": "crossings_hydroobject_filtered",
    "in_use": True,
    "agg_links_in_use": True,
    "agg_areas_in_use": True,
    "aggregation": True,
    # data storage settings
    "write_Pdrive": False,
    "write_Zdrive": True,
    "write_goodcloud": True,
    "write_checks": True,
    "write_symbology": True,
    # numerical settings
    "solver": None,
    "logging": None,
    "starttime": "2024-01-01 00:00:00",
    "endtime": "2024-01-02 00:00:00",
}

waterboard = CrossingsToRibasim(model_characteristics=model_characteristics)

post_processed_data, crossings = waterboard.read_files()
post_processed_data, crossings = waterboard.routing_processor(post_processed_data, crossings)
crossings = waterboard.assign_node_ids(crossings)
edges = waterboard.create_edges(crossings)
nodes, edges = waterboard.create_nodes(crossings, edges)
edges = waterboard.embed_boezems(edges, post_processed_data, crossings)


# create individual model parts of the network
network = RibasimNetwork(nodes=nodes, edges=edges, model_characteristics=model_characteristics)

edge = network.edge()
basin_node, basin_profile, basin_static, basin_state, basin_area = network.basin()
pump_node, pump_static = network.pump()
tabulated_rating_curve_node, tabulated_rating_curve_static = network.tabulated_rating_curve()
level_boundary_node, level_boundary_static = network.level_boundary()
flow_boundary_node, flow_boundary_static = network.flow_boundary()
manning_resistance_node, manning_resistance_static = network.manning_resistance()
terminal_node = network.terminal()

# linear_resistance = network.linear_resistance()
# fractional_flow = network.fractional_flow()
# outlet = network.outlet()
# discrete_control = network.discrete_control()
# pid_control = network.pid_control()

# insert the individual model modules in an actual model
model = Model(starttime=model_characteristics["starttime"], endtime=model_characteristics["endtime"], crs="EPSG:28992")

model.edge = edge

model.basin.node = basin_node
model.basin.profile = basin_profile
model.basin.static = basin_static
model.basin.state = basin_state
model.basin.area = basin_area

model.pump.node = pump_node
model.pump.static = pump_static

model.tabulated_rating_curve.node = tabulated_rating_curve_node
model.tabulated_rating_curve.static = tabulated_rating_curve_static

model.level_boundary.node = level_boundary_node
model.level_boundary.static = level_boundary_static

model.flow_boundary.node = flow_boundary_node
model.flow_boundary.static = flow_boundary_static

model.manning_resistance.node = manning_resistance_node
model.manning_resistance.static = manning_resistance_static

model.terminal.node = terminal_node

# add checks and metadata
checks = network.check(model=model, post_processed_data=post_processed_data, crossings=crossings)
model = network.add_meta_data(model, checks, post_processed_data, crossings)

# write the result
network.WriteResults(model=model, checks=checks)