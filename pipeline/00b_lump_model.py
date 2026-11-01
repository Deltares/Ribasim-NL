# %%
"""Lump the HyDAMO network of a water authority into its input model, `modellen/<authority>_2024_6_3`.

The hydroobjects of `verwerkt/4_ribasim/hydamo.gpkg` are split in edges of about 250 m, and lumped into basins at the
split nodes of `verwerkt/3_input/ribasim_input.gpkg`, with the areas of `verwerkt/3_input/areas.gpkg`. Nodes on the KRW
water bodies of `verwerkt/4_ribasim/krw.gpkg` are hoofdwater. See `ribasim_nl.lumping`.

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
    lumped = lump_network(
        network,
        split_nodes,
        boundaries,
        areas,
        drainage_areas=drainage_areas,
        remove_isolated=True,
        remove_holes_min_area=5000.0,
        # in 2024, the areas of Vallei en Veluwe were not assigned
        assign_unassigned=authority != "ValleienVeluwe",
    )
    model = lumped_model(lumped, CODES[authority], read_krw(verwerkt / "4_ribasim/krw.gpkg"))
    model.write(cloud.model_dir(authority, "2024_6_3") / "model.toml")
