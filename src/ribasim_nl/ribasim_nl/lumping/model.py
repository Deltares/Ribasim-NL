"""Convert a lumped network into a Ribasim model with default tables, the input model of a water authority."""

import itertools
import logging
from collections.abc import Mapping, Sequence
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio
import shapely
from pandera.typing.geopandas import GeoDataFrame
from ribasim.geometry.area import BasinAreaSchema
from ribasim.geometry.link import LinkSchema
from ribasim.geometry.node import NodeSchema
from ribasim.input_base import TableModel

from ribasim_nl.lumping.network import LumpedNetwork, assign_areas
from ribasim_nl.model import Model

logger = logging.getLogger(__name__)

# the distance within which a link ends at its node, in m, as in `ribasim_nl.NetworkValidator`
LINK_END_TOLERANCE = 1.0

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
    assert parts, f"no layer krw_line or krw_vlak in {path}"
    krw = pd.concat(parts)[["owmident", "owmnaam", "owmtype", "geometry"]].reset_index(drop=True)
    krw.columns = ["meta_krw_id", "meta_krw_name", "meta_krw_type", "geometry"]
    return gpd.GeoDataFrame(krw, geometry="geometry")


def nodes_and_links(lumped: LumpedNetwork, harde_knip: Sequence[str] = ()) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """The nodes, the boundaries, used split nodes and basins, and the links of a lumped network.

    The node type of a split node follows from its object type and, for HyDAMO, its object function. The split nodes
    with an ID in `harde_knip` are left out. As in 2024, the links are merged with each node of their node ID, so a
    link to an ID of both a split node and a basin is duplicated, but not with the basins that were left out in 2024,
    with `boundary_without_split_node`. That gives the links the IDs they got in 2024.
    """
    crs = lumped.basins.crs
    basins = lumped.basins[["basin", "basin_node_id", "boundary_without_split_node", "geometry"]].rename(
        columns={"basin": "meta_id", "basin_node_id": "node_id"}
    )
    basins["node_type"] = "Basin"
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
    nodes["boundary_without_split_node"] = nodes["boundary_without_split_node"].fillna(False).astype(bool)

    columns = ["from_node_id", "to_node_id", "geometry"]
    links = pd.concat([lumped.basin_connections[columns], lumped.boundary_connections[columns]])
    node_types = nodes.loc[~nodes["boundary_without_split_node"], ["node_id", "node_type"]]
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


def _at_nodes(node_ids: np.ndarray, points: np.ndarray, node_geometry: gpd.GeoSeries) -> np.ndarray:
    """Whether each point is within `LINK_END_TOLERANCE` of the node with its node ID in `node_geometry`."""
    exists = np.isin(node_ids, node_geometry.index)
    at_node = np.zeros(len(node_ids), dtype=bool)
    at_node[exists] = (
        shapely.distance(points[exists], node_geometry.loc[node_ids[exists]].to_numpy()) <= LINK_END_TOLERANCE
    )
    return at_node


def number_lost_basins(
    nodes: gpd.GeoDataFrame,
    links: gpd.GeoDataFrame,
    lost: gpd.GeoDataFrame,
    node_ids: Mapping[int, int] | None = None,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Give the basins that were left out in 2024 the node IDs that the fix scripts gave them, and connect them.

    In 2024, a basin was left out of the input model if a boundary or split node had its node ID, or if it was the
    basin of a LevelBoundary without split node. Its links then ended at another node, or at none, and links to a node
    ID of two nodes were duplicated. The fix scripts dropped the duplicated links, and added a basin with the next node
    ID at each end of a link that `ribasim_nl.NetworkValidator.node_invalid_connectivity` finds further than
    `LINK_END_TOLERANCE` from its node, in the order of the links, from end before to end. Like that validator, this
    checks the end of a link whose start is at its node only if the node of the end exists. The lost basins found are
    added with these node IDs, so the references to them remain valid, and connected to all links that end on them.
    The others remain left out, and their links as they were. `lost` holds the basins with their 2024 node ID.
    `node_ids` maps the 2024 node ID of lost basins that a fix script numbered otherwise to the node ID it gave them.
    """
    node_ids = dict(node_ids or {})
    assert lost["node_id"].is_unique
    links = links.drop_duplicates(subset=["from_node_id", "to_node_id", "link_type", "geometry"])
    geometry = links.geometry.to_numpy()
    columns = ("from_node_id", "to_node_id")
    end_node_ids = [links[c].to_numpy() for c in columns]
    points = [shapely.get_point(geometry, 0), shapely.get_point(geometry, -1)]
    node_geometry = nodes.set_index("node_id").geometry
    at_node = [_at_nodes(n, p, node_geometry) for n, p in zip(end_node_ids, points, strict=True)]
    on_lost = [_at_nodes(n, p, lost.set_index("node_id").geometry) for n, p in zip(end_node_ids, points, strict=True)]
    invalid = ~at_node[0] | (np.isin(end_node_ids[1], node_geometry.index) & ~at_node[1])

    # the lost basins at the ends that the validator finds, in the order of the links, from end before to end
    found = invalid[:, None] & ~np.column_stack(at_node) & np.column_stack(on_lost)
    position, end = np.nonzero(found)
    found_ids = pd.unique(np.column_stack(end_node_ids)[position, end])
    assert set(node_ids) <= set(found_ids), f"no lost basins {sorted(set(node_ids) - set(found_ids))} to number"
    assert not set(node_ids.values()) & set(nodes["node_id"]), "a lost basin gets the node ID of another node"
    next_node_ids = (i for i in itertools.count(nodes["node_id"].max() + 1) if i not in node_ids.values())
    new_node_id = {i: node_ids[i] if i in node_ids else next(next_node_ids) for i in found_ids}

    links = links.copy()
    for column, ids, on_basin in zip(columns, end_node_ids, on_lost, strict=True):
        connect = on_basin & np.isin(ids, found_ids)
        new_ids = ids.copy()
        new_ids[connect] = [new_node_id[i] for i in ids[connect]]
        links[column] = new_ids
    lost = lost[lost["node_id"].isin(found_ids)]
    lost = lost.assign(node_id=lost["node_id"].map(new_node_id))
    logger.info(f"Adding {len(lost)} basins that were left out in 2024")
    nodes = gpd.GeoDataFrame(pd.concat([nodes, lost], ignore_index=True), geometry="geometry", crs=nodes.crs)
    return nodes, links


def lumped_model(
    lumped: LumpedNetwork,
    waterbeheercode: str,
    krw: gpd.GeoDataFrame,
    drainage_areas: gpd.GeoDataFrame | None = None,
    max_gap_area: float = 0.0,
    harde_knip: Sequence[str] = (),
    lost_basin_node_ids: Mapping[int, int] | None = None,
) -> Model:
    """A Ribasim model of a lumped network, with default tables.

    Nodes on a KRW water body have `meta_categorie` hoofdwater, the others doorgaand. The links are numbered in their
    order, as in 2024, see `nodes_and_links`. Each node ID is used once, for the first node with it. The basins that
    were left out in 2024 are numbered after the other nodes, or with `lost_basin_node_ids`, see `number_lost_basins`.
    Every area goes to a basin, preferably one in its drainage area, and the gaps between the basin areas up to
    `max_gap_area` m² are filled, see `ribasim_nl.lumping.network.assign_areas`.
    """
    crs = "EPSG:28992"
    nodes, links = nodes_and_links(lumped, harde_knip=harde_knip)
    assert nodes.crs == crs, f"the lumped network is not in {crs}"
    nodes["meta_code"] = waterbeheercode
    nodes = nodes.sjoin(krw.to_crs(crs), how="left").drop(columns=["index_right"])
    # a node on several KRW water bodies is on the first
    nodes = nodes.loc[~nodes.index.duplicated(keep="first")]
    nodes["meta_categorie"] = "doorgaand"
    nodes.loc[nodes["meta_krw_id"].notna(), "meta_categorie"] = "hoofdwater"
    lost = (nodes["node_type"] == "Basin") & (
        nodes["node_id"].duplicated(keep="first") | nodes["boundary_without_split_node"]
    )
    nodes, lost_basins = nodes.loc[~lost], nodes.loc[lost]
    assert nodes["node_id"].is_unique
    nodes = nodes[nodes["node_type"].isin(NODE_TYPES)]

    links.index = pd.RangeIndex(1, len(links) + 1, name="link_id")
    links = links.drop_duplicates(subset=["from_node_id", "from_node_type", "to_node_id", "to_node_type"])
    links["link_type"] = "flow"
    nodes, links = number_lost_basins(nodes, links, lost_basins, lost_basin_node_ids)
    nodes = nodes.drop(columns=["boundary_without_split_node"])

    # the basin areas of the basins in the model
    basin_nodes = nodes.loc[nodes["node_type"] == "Basin"]
    basin_node_id = pd.Series(basin_nodes["node_id"].to_numpy(), index=basin_nodes["meta_id"].astype(int))
    assert basin_node_id.index.is_unique, "basins of the same lumped basin"
    _, basin_areas = assign_areas(
        lumped.areas.to_crs(crs),
        gpd.GeoDataFrame({"basin": basin_node_id.index}, geometry=basin_nodes.geometry.to_numpy(), crs=crs),
        None if drainage_areas is None else drainage_areas.to_crs(crs),
        max_gap_area=max_gap_area,
    )
    basin_areas["node_id"] = basin_areas["basin"].map(basin_node_id)
    nodes["meta_area"] = nodes["node_id"].map(basin_areas.set_index("node_id").area)
    basin_areas = basin_areas.sort_values("node_id", kind="stable")[["node_id", "geometry"]].reset_index(drop=True)

    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs=crs)
    # the input models are not valid yet, their `01_fix_model.py` makes them valid
    model.use_validation = False
    model.node.df = GeoDataFrame[NodeSchema](nodes.set_index("node_id"))
    model.link.df = GeoDataFrame[LinkSchema](links[["from_node_id", "to_node_id", "link_type", "geometry"]])
    model.basin.area.df = GeoDataFrame[BasinAreaSchema](basin_areas)

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
