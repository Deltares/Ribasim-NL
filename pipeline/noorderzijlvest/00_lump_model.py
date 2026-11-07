# %%
"""Lump the D-HYDRO model of Noorderzijlvest into its input model, `modellen/Noorderzijlvest_2024_6_3`.

The computational grid of the D-HYDRO boezemmodel is lumped into basins at its pumps, weirs and orifices, and
halfway the edges nearest to the open water split nodes. The hydrological units are assigned to basins by the
laterals of the model. Nodes on the KRW water bodies of `verwerkt/4_ribasim/krw.gpkg` are hoofdwater. See
`ribasim_nl.lumping`.
"""

import geopandas as gpd
from ribasim_nl.lumping.dhydro import read_dhydro_network, select_split_nodes
from ribasim_nl.lumping.hydamo import read_areas
from ribasim_nl.lumping.model import lumped_model, read_krw
from ribasim_nl.lumping.network import lump_network

from ribasim_nl import CloudStorage

cloud = CloudStorage()
authority = "Noorderzijlvest"
verwerkt = cloud.joinpath(authority, "verwerkt")

# the supply pumps and weirs, which do not split the network in the model
HARDE_KNIP = [
    "iKGM004",
    "iKGM036",
    "iKGM069",
    "iKGM073",
    "iKGM086",
    "iKGM101",
    "iKGM102",
    "iKGM129",
    "iKGM157",
    "iKGM163",
    "iKGM165",
    "iKGM189",
    "iKGM190",
    "iKGM192",
    "iKGM194",
    "iKGM198",
    "iKGM206",
    "iKGM214",
    "iKGM226",
    "iKGM241",
    "iKGM248",
    "iKGM260",
    "iKGM265",
    "iKGM295",
    "iKGM302",
    "iKST0163",
    "iKST0470",
    "iKST0569",
    "iKST0572",
    "iKST0624",
    "iKST0707",
    "iKST6330",
    "iKST6352",
    "iKST6386",
    "iKST6388",
    "iKST6415",
    "iKST6622",
    "iKST9950",
]

# %%
dflowfm = verwerkt / "2_voorbewerking/2_dhydro/basis/winter/nzv_boezemmodel/dflowfm"
network, boundaries, laterals = read_dhydro_network(dflowfm / "FlowFM.mdu")
areas = read_areas(
    verwerkt / "2_voorbewerking/1_areas/HydrologischeEenheden_v38_modelselectie2.shp", code_column="HEIDENT"
)
open_water = gpd.read_file(verwerkt / "3_input/openwater.gpkg", layer="openwater")
open_water_edges = open_water.sjoin_nearest(network.edges, max_distance=100.0)["edge_no"]
split_nodes = select_split_nodes(
    network,
    structure_types=["pump", "weir", "orifice", "universalWeir"],
    exclude_structure_ids=["KST1026", "KST1033", "KST1081"],
    edge_nos=list(open_water_edges),
)
lumped = lump_network(network, split_nodes, boundaries, areas, laterals=laterals)
model = lumped_model(lumped, "NL34", read_krw(verwerkt / "4_ribasim/krw.gpkg"), harde_knip=HARDE_KNIP)
model.write(cloud.model_dir(authority, "2024_6_3") / "model.toml")
