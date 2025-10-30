import warnings
from pathlib import Path

import geopandas as gpd
import pandas as pd

warnings.simplefilter(action="ignore", category=UserWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)


def generate_ribasim_network(waterschap_path, split_nodes_type_conversion, split_nodes_id_conversion=None):
    ribasim_network_dir = Path(waterschap_path, "4_ribasim")
    ribasim_network_path = Path(ribasim_network_dir, "ribasim_network.gpkg")
    Path(ribasim_network_dir, "foreign_input.gpkg")
    ribasim_model_path = Path(ribasim_network_dir, "ribasim_model.gpkg")

    # Ribasim basins
    basins = gpd.read_file(ribasim_network_path, layer="basins")
    basins = basins[["basin", "basin_node_id", "geometry"]].rename(
        columns={"basin": "meta_id", "basin_node_id": "node_id"}
    )
    basins["node_type"] = "Basin"
    basin_areas = gpd.read_file(ribasim_network_path, layer="basin_areas")
    basin_areas.basin_node_id = basin_areas.basin_node_id.astype(int)
    basin_areas["meta_area"] = basin_areas.geometry.area
    basins = basins.merge(
        basin_areas[["basin_node_id", "meta_area"]].rename(columns={"basin_node_id": "node_id"}),
        how="left",
        on="node_id",
    )
    basin_areas = basin_areas[["basin_node_id", "meta_area", "geometry"]]
    basins["name"] = "Basin"

    # Ribasim boundaries
    boundaries = gpd.read_file(ribasim_network_path, layer="boundaries")
    boundaries["node_id"] = boundaries["boundary_id"]
    boundaries = boundaries[["node_id", "boundary_id", "boundary_name", "boundary_type", "geometry"]].rename(
        columns={"boundary_id": "meta_id", "boundary_name": "meta_name", "boundary_type": "node_type"}
    )
    level_boundaries = boundaries.loc[boundaries["node_type"] == "LevelBoundary", :].copy()
    level_boundaries["meta_water_level"] = -10.0

    boundaries.loc[boundaries["node_type"] == "FlowBoundary", :].copy()

    # Ribasim lumping split nodes
    split_nodes = gpd.read_file(ribasim_network_path, layer="split_nodes")
    split_nodes = gpd.read_file(ribasim_network_path, layer="split_nodes")
    split_nodes = split_nodes.rename(
        columns={"object_type": "meta_object_type", "object_function": "meta_object_function"}
    )

    # Conversion split_node from object-type + object-function to node_type
    if "meta_object_function" in split_nodes.columns:
        split_nodes = split_nodes.merge(
            split_nodes_type_conversion, how="left", on=["meta_object_type", "meta_object_function"]
        )
    else:
        split_nodes = split_nodes.merge(split_nodes_type_conversion_dhydro, how="left", on="meta_object_type")

    if isinstance(split_nodes_id_conversion, dict):
        for key, value in split_nodes_id_conversion.items():
            if len(split_nodes[split_nodes["split_node_id"] == key]) == 0:
                print(f"   * split_node type conversion id={key} (type={value}) does not exist")
            split_nodes.loc[split_nodes["split_node_id"] == key, "meta_object_function"] = value

    # define final split_nodes
    split_nodes = split_nodes.loc[split_nodes.status & (split_nodes.meta_object_function != "harde_knip"), :].copy()
    if "meta_object_function" not in split_nodes:
        split_nodes["meta_object_function"] = ""
    split_nodes = split_nodes[
        [
            "split_node",
            "split_node_id",
            "split_node_node_id",
            "node_type",
            "meta_object_type",
            "meta_object_function",
            "geometry",
        ]
    ]
    split_nodes = split_nodes.rename(
        columns={"split_node": "meta_split_node_id", "split_node_id": "name", "split_node_node_id": "node_id"}
    )

    # Combine all nodes
    nodes = gpd.GeoDataFrame(pd.concat([boundaries, split_nodes, basins]), crs=28992).reset_index(drop=True)

    # Combine all links
    basin_connections = gpd.read_file(ribasim_network_path, layer="basin_connections")
    boundary_connections = gpd.read_file(ribasim_network_path, layer="boundary_connections")

    links_columns = ["from_node_id", "to_node_id", "connection", "geometry"]
    links = gpd.GeoDataFrame(
        pd.concat([basin_connections[links_columns], boundary_connections[links_columns]]),
        geometry="geometry",
        crs=28992,
    )

    links = links[["from_node_id", "to_node_id", "geometry"]]

    links = links.merge(
        nodes[["node_id", "node_type"]].rename(columns={"node_id": "from_node_id", "node_type": "from_node_type"}),
        how="left",
        on="from_node_id",
    )
    links = links.merge(
        nodes[["node_id", "node_type"]].rename(columns={"node_id": "to_node_id", "node_type": "to_node_type"}),
        how="left",
        on="to_node_id",
    )
    links["link_type"] = "flow"

    # Export nodes and links
    nodes.drop_duplicates(keep="first").to_file(ribasim_model_path, layer="Node")
    links.drop_duplicates(keep="first").to_file(ribasim_model_path, layer="Edge")

    print(f" - no of nodes: {len(nodes)}")
    print(f" - no of links: {len(links)}")
    return nodes, links, split_nodes


base_dir = Path("..\\Ribasim modeldata")

waterschappen = [
    "Noorderzijlvest",
    "HunzeenAas",
    "DrentsOverijsselseDelta",
    "Vechtstromen",
    "RijnenIJssel",
    "ValleienVeluwe",
    "StichtseRijnlanden",
    "BrabantseDelta",
    "DeDommel",
    "AaenMaas",
    "Limburg",
]


# HYDAMO (10 waterschappen)
split_nodes_type_conversion_hydamo = pd.DataFrame(
    columns=["meta_object_type", "meta_object_function", "node_type"],
    data=[
        ["stuw", "", "TabulatedRatingCurve"],
        ["stuw", "afwaterend", "TabulatedRatingCurve"],
        ["stuw", "inlaat", "Outlet"],
        ["afsluitmiddel", "", "Outlet"],
        ["afsluitmiddel", "inlaat", "Outlet"],
        ["afsluitmiddel", "uitlaat", "Outlet"],
        ["duikersifonhevel", "", "ManningResistance"],
        ["duikersifonhevel", "inlaat", "Outlet"],
        ["duikersifonhevel", "afwaterend", "TabulatedRatingCurve"],
        ["duikersifonhevel", "open verbinding", "ManningResistance"],
        ["openwater", "", "ManningResistance"],
        ["openwater", "open verbinding", "ManningResistance"],
        ["openwater", "afwaterend", "TabulatedRatingCurve"],
        ["gemaal", "", "Pump"],
        ["gemaal", "afvoer", "Pump"],
        ["gemaal", "aanvoer", "Pump"],
        ["gemaal", "aanvoer/afvoer", "Pump"],
        ["sluis", "", "Outlet"],
        ["sluis", "schut- en lekverlies", "Outlet"],
        ["sluis", "spui", "Outlet"],
        ["sluis", "keersluis", "Outlet"],
    ],
)

# DHYDRO (NOORDERZIJLVEST)
split_nodes_type_conversion_dhydro = pd.DataFrame(
    columns=["meta_object_type", "node_type"],
    data=[
        ["weir", "TabulatedRatingCurve"],
        ["uniweir", "TabulatedRatingCurve"],
        ["universalWeir", "TabulatedRatingCurve"],
        ["pump", "Pump"],
        ["culvert", "ManningResistance"],
        ["openwater", "ManningResistance"],
        ["orifice", "Outlet"],
    ],
)

te_verwijderen_aanvoergemalen = [
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
split_nodes_id_conversion_dhydro = dict.fromkeys(te_verwijderen_aanvoergemalen, "harde_knip")


for waterschap in waterschappen:
    print(f"Waterschap: {waterschap}")
    waterschap_path = Path(base_dir, waterschap, "verwerkt")
    if waterschap == "Noorderzijlvest":
        nodes, links, split_nodes = generate_ribasim_network(
            waterschap_path, split_nodes_type_conversion_dhydro, split_nodes_id_conversion_dhydro
        )
    else:
        nodes, links, split_nodes = generate_ribasim_network(waterschap_path, split_nodes_type_conversion_hydamo)
