# Modified version of the AGV part of 02_crossings_to_ribasim.py.

# %%

import pandas as pd
from ribasim import Model

from peilbeheerst_model import CrossingsToRibasim, RibasimNetwork, waterschap_data
from ribasim_nl import CloudStorage

# %%
waterschap = "AmstelGooienVecht"
waterschap_struct = waterschap_data[waterschap]

cloud = CloudStorage()
verwerkt_dir = cloud.joinpath(waterschap, "verwerkt")
# cloud.download_verwerkt(waterschap)
# cloud.download_basisgegevens()

# %%

pd.set_option("display.max_columns", None)
# warnings.filterwarnings("ignore")


model_characteristics = {
    # model description
    "waterschap": "AmstelGooienVecht",
    "modelname": "repro",
    "modeltype": "boezemmodel",
    # define paths
    "path_postprocessed_data": verwerkt_dir / "postprocessed.gpkg",
    "path_crossings": verwerkt_dir / "crossings.gpkg",
    "path_boezem": verwerkt_dir / "shortest_path.gpkg",
    "path_Pdrive": None,
    # apply filters
    "crossings_layer": "crossings_hydroobject_filtered",
    "in_use": True,
    "agg_links_in_use": True,
    "agg_areas_in_use": True,
    "aggregation": True,
    # data storage settings
    "write_goodcloud": False,  # TODO
    "write_checks": True,
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

# %%

cloud.upload_verwerkt(waterschap)

# %%
