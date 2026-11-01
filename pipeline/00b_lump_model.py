# %%
"""Lump the HyDAMO network of a water authority into its input model, `modellen/<authority>_2024_6_3`.

The hydroobjects of `verwerkt/4_ribasim/hydamo.gpkg` are split in edges of about 250 m, and lumped into basins at the
split nodes of `verwerkt/3_input/ribasim_input.gpkg`. Every area of `verwerkt/3_input/areas.gpkg` goes to a basin,
preferably one in its drainage area, and the gaps between them up to 5000 m² are filled. Nodes on the KRW water bodies
of `verwerkt/4_ribasim/krw.gpkg` are hoofdwater. See `ribasim_nl.lumping`.

Usage: pixi run python pipeline/00b_lump_model.py <authority>
"""

import sys

from ribasim_nl.lumping.hydamo import read_areas, read_boundaries, read_hydamo_network, read_split_nodes
from ribasim_nl.lumping.model import lumped_model, read_krw
from ribasim_nl.lumping.network import lump_network
from ribasim_nl.lumping.utils import read_layer

from ribasim_nl import CloudStorage

cloud = CloudStorage()

# the meta_code of the nodes, as in 2024
CODES = {
    "AaenMaas": "NL38",
    "BrabantseDelta": "NL25",
    "DeDommel": "NL27",
    "DrentsOverijsselseDelta": "NL59",
    "HunzeenAas": "NL33",
    "Limburg": "NL60",
    "RijnenIJssel": "NL7",
    "StichtseRijnlanden": "NL14",
    "ValleienVeluwe": "NL8",
    "Vechtstromen": "NL44",
}

# the node IDs that fix scripts gave basins that the 2024 lumping left out, by their 2024 node ID, where these are not
# the next node IDs of the input model, see `ribasim_nl.lumping.model.number_lost_basins`
LOST_BASIN_NODE_IDS = {
    "DeDommel": {1278: 1278, 1446: 1446},
    "Limburg": {1743: 2472, 2286: 2474, 1957: 2476, 2151: 2478, 2014: 2480},
    "Vechtstromen": {node_id: node_id + 947 for node_id in range(1356, 1371)},
}

authorities = sys.argv[1:]
assert authorities, "pass the authorities as arguments"

for authority in authorities:
    print(authority)
    verwerkt = cloud.joinpath(authority, "verwerkt")
    network = read_hydamo_network(verwerkt / "4_ribasim/hydamo.gpkg", dx=250.0)
    areas_gpkg = verwerkt / "3_input/areas.gpkg"
    areas = read_areas(areas_gpkg, "areas")
    drainage_areas = read_layer(areas_gpkg, "drainage_areas", explode=False, code_column="CODE")
    ribasim_input = verwerkt / "3_input/ribasim_input.gpkg"
    split_nodes, network = read_split_nodes(ribasim_input, "split_nodes", network, buffer_distance=1.0)
    boundaries = read_boundaries(ribasim_input, "boundaries", network.nodes, buffer_distance=1.0)
    lumped = lump_network(network, split_nodes, boundaries, areas, remove_isolated=True)
    krw = read_krw(verwerkt / "4_ribasim/krw.gpkg")
    model = lumped_model(
        lumped,
        CODES[authority],
        krw,
        drainage_areas=drainage_areas,
        max_gap_area=5000.0,
        lost_basin_node_ids=LOST_BASIN_NODE_IDS.get(authority),
    )
    model.write(cloud.model_dir(authority, "2024_6_3") / "model.toml")
