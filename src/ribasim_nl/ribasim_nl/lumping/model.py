"""Convert a lumped network into a Ribasim model with default tables, the input model of a water authority."""

import logging
from collections.abc import Sequence
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
from pandera.typing.geopandas import GeoDataFrame
from ribasim.geometry.area import BasinAreaSchema
from ribasim.geometry.link import LinkSchema
from ribasim.geometry.node import NodeSchema
from ribasim.input_base import TableModel

from ribasim_nl.lumping.network import LumpedNetwork
from ribasim_nl.lumping.utils import quicksort_order
from ribasim_nl.model import Model

logger = logging.getLogger(__name__)

# the node type of the split nodes on each type of HyDAMO object, with each function
HYDAMO_NODE_TYPES = pd.DataFrame(
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

# the node type of the split nodes on each type of D-HYDRO structure
DHYDRO_NODE_TYPES = pd.DataFrame(
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

NODE_TYPES = [
    "Basin",
    "Pump",
    "Outlet",
    "TabulatedRatingCurve",
    "ManningResistance",
    "LinearResistance",
    "LevelBoundary",
    "FlowBoundary",
]


def read_krw(path: Path) -> gpd.GeoDataFrame:
    """The KRW water bodies: the layer `krw_line` buffered by 10 m, and the layer `krw_vlak`.

    As in 2024, other layers, such as `krw_polygon`, are not used.
    """
    layers = list(pyogrio.list_layers(path)[:, 0])
    parts = []
    if "krw_line" in layers:
        lines = gpd.read_file(path, layer="krw_line").explode(index_parts=True)
        lines["geometry"] = lines.geometry.buffer(10, join_style="bevel")
        parts.append(lines)
    if "krw_vlak" in layers:
        parts.append(gpd.read_file(path, layer="krw_vlak").explode(index_parts=True))
    krw = pd.concat(parts)[["owmident", "owmnaam", "owmtype", "geometry"]].reset_index(drop=True)
    krw.columns = ["meta_krw_id", "meta_krw_name", "meta_krw_type", "geometry"]
    return gpd.GeoDataFrame(krw, geometry="geometry")


def nodes_and_links(lumped: LumpedNetwork, harde_knip: Sequence[str] = ()) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """The nodes, the boundaries, used split nodes and basins, and the links of a lumped network.

    The node type of a split node follows from its object type and, for HyDAMO, its object function. The split nodes
    with an ID in `harde_knip` are left out. As in 2024, the links are merged with each node of their node ID, so a
    link to an ID of both a split node and a basin is duplicated.
    """
    crs = lumped.basins.crs
    basins = lumped.basins[["basin", "basin_node_id", "geometry"]].rename(
        columns={"basin": "meta_id", "basin_node_id": "node_id"}
    )
    basins["node_type"] = "Basin"
    basin_areas = lumped.basin_areas[["basin_node_id", "geometry"]].copy()
    basin_areas["basin_node_id"] = basin_areas["basin_node_id"].astype(int)
    basin_areas["meta_area"] = basin_areas.geometry.area
    basins = basins.merge(
        basin_areas[["basin_node_id", "meta_area"]].rename(columns={"basin_node_id": "node_id"}),
        how="left",
        on="node_id",
    )
    basins["name"] = "Basin"

    boundaries = lumped.boundaries.copy()
    boundaries["node_id"] = boundaries["boundary_id"]
    boundaries = boundaries[["node_id", "boundary_id", "boundary_name", "boundary_type", "geometry"]].rename(
        columns={"boundary_id": "meta_id", "boundary_name": "meta_name", "boundary_type": "node_type"}
    )

    split_nodes = lumped.split_nodes.rename(
        columns={"object_type": "meta_object_type", "object_function": "meta_object_function"}
    )
    if "meta_object_function" in split_nodes.columns:
        split_nodes = split_nodes.merge(HYDAMO_NODE_TYPES, how="left", on=["meta_object_type", "meta_object_function"])
    else:
        split_nodes = split_nodes.merge(DHYDRO_NODE_TYPES, how="left", on="meta_object_type")
        # as in 2024, where a missing function was written as text
        split_nodes["meta_object_function"] = "nan"
    missing = set(harde_knip) - set(split_nodes["split_node_id"])
    if missing:
        logger.warning(f"No split nodes {sorted(missing)} to leave out")
    split_nodes.loc[split_nodes["split_node_id"].isin(harde_knip), "meta_object_function"] = "harde_knip"
    split_nodes = split_nodes.loc[split_nodes["status"] & (split_nodes["meta_object_function"] != "harde_knip")]
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
    ].rename(columns={"split_node": "meta_split_node_id", "split_node_id": "name", "split_node_node_id": "node_id"})

    nodes = gpd.GeoDataFrame(pd.concat([boundaries, split_nodes, basins]), geometry="geometry", crs=crs)
    nodes = nodes.reset_index(drop=True)

    columns = ["from_node_id", "to_node_id", "geometry"]
    links = pd.concat([lumped.basin_connections[columns], lumped.boundary_connections[columns]])
    node_types = nodes[["node_id", "node_type"]]
    links = links.merge(
        node_types.rename(columns={"node_id": "from_node_id", "node_type": "from_node_type"}),
        how="left",
        on="from_node_id",
    )
    links = links.merge(
        node_types.rename(columns={"node_id": "to_node_id", "node_type": "to_node_type"}), how="left", on="to_node_id"
    )
    links = gpd.GeoDataFrame(links, geometry="geometry", crs=crs)
    return nodes.drop_duplicates(keep="first"), links.drop_duplicates(keep="first")


def lumped_model(
    lumped: LumpedNetwork,
    waterbeheercode: str,
    krw: gpd.GeoDataFrame,
    harde_knip: Sequence[str] = (),
) -> Model:
    """A Ribasim model of a lumped network, with default tables.

    Nodes on a KRW water body have `meta_categorie` hoofdwater, the others doorgaand. Each node ID is used once, for
    the first node with it, and each link once for the node types at its ends. The links are numbered in their order.
    """
    crs = "EPSG:28992"
    nodes, links = nodes_and_links(lumped, harde_knip=harde_knip)
    assert nodes.crs == crs, f"the lumped network is not in {crs}"
    nodes["meta_code"] = waterbeheercode
    nodes = nodes.sjoin(krw.to_crs(crs), how="left").drop(columns=["index_right"])
    nodes = nodes.drop_duplicates(subset="node_id", keep="first")
    nodes["meta_categorie"] = "doorgaand"
    nodes.loc[nodes["meta_krw_id"].notna(), "meta_categorie"] = "hoofdwater"
    nodes = nodes[nodes["node_type"].isin(NODE_TYPES)]

    links.index = pd.RangeIndex(1, len(links) + 1, name="link_id")
    links = links.drop_duplicates(subset=["from_node_id", "from_node_type", "to_node_id", "to_node_type"])
    links["link_type"] = "flow"

    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs=crs)
    # the input models are not valid yet, their `01_fix_model.py` makes them valid
    model.use_validation = False
    model.node.df = GeoDataFrame[NodeSchema](nodes.set_index("node_id"))
    model.link.df = GeoDataFrame[LinkSchema](links[["from_node_id", "to_node_id", "link_type", "geometry"]])
    # sorted by node ID, with the unstable sort with which ribasim wrote the table in 2024
    areas = lumped.basin_areas[["basin_node_id", "geometry"]].rename(columns={"basin_node_id": "node_id"})
    model.basin.area.df = GeoDataFrame[BasinAreaSchema](
        areas.iloc[quicksort_order(areas["node_id"])].reset_index(drop=True)
    )

    # the default tables of each node type, two rows for the profiles and rating curves
    defaults: list[tuple[str, TableModel, dict[str, list[float]]]] = [
        ("Basin", model.basin.profile, {"area": [0.01, 1000.0], "level": [0.0, 1.0]}),
        (
            "Basin",
            model.basin.static,
            {
                "precipitation": [0.005 / 86400],
                "potential_evaporation": [0.001 / 86400],
                "drainage": [0.0],
                "infiltration": [0.0],
            },
        ),
        ("Basin", model.basin.state, {"level": [0.0]}),
        ("Pump", model.pump.static, {"flow_rate": [0.1]}),
        ("Outlet", model.outlet.static, {"flow_rate": [0.1]}),
        (
            "ManningResistance",
            model.manning_resistance.static,
            {"length": [100.0], "manning_n": [0.04], "profile_width": [10.0], "profile_slope": [1.0]},
        ),
        ("TabulatedRatingCurve", model.tabulated_rating_curve.static, {"level": [0.0, 5.0], "flow_rate": [0.0, 0.1]}),
        ("LinearResistance", model.linear_resistance.static, {"resistance": [0.005]}),
        ("FlowBoundary", model.flow_boundary.static, {"flow_rate": [0.1]}),
        ("LevelBoundary", model.level_boundary.static, {"level": [0.0]}),
    ]
    for node_type, table, values in defaults:
        node_ids = nodes.loc[nodes["node_type"] == node_type, "node_id"].to_numpy()
        if len(node_ids) == 0:
            continue
        n_rows = len(next(iter(values.values())))
        table.df = pd.DataFrame(  # ty: ignore[invalid-assignment]
            {"node_id": node_ids.repeat(n_rows), **{k: np.tile(v, len(node_ids)) for k, v in values.items()}}
        )
    return model
