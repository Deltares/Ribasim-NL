"""Read the 1D network, structures, boundaries and laterals of a D-HYDRO model to lump."""

import configparser
from pathlib import Path

import geopandas as gpd
import hydrolib.core.dflowfm as hcdfm
import numpy as np
import pandas as pd
import xarray as xr
from shapely.geometry import LineString
from shapely.ops import nearest_points

from ribasim_nl.lumping.network import Network
from ribasim_nl.lumping.utils import extract_segment_from_linestring, merge_grouped

# the order in which the structures of each type become split nodes
SPLIT_NODE_STRUCTURES = ["pump", "weir", "orifice", "bridge", "culvert", "universalWeir"]


def read_ini_sections(path: Path, section: str) -> pd.DataFrame:
    """The sections named `section` of an INI file that may repeat sections, as rows."""

    class NumberedSections(dict):
        """Number the sections, so repeated sections are kept."""

        count: dict[str, int] = {}  # noqa: RUF012

        def __setitem__(self, key: str, value: object) -> None:
            if isinstance(value, dict):
                self.count[key] = self.count.get(key, -1) + 1
                key = f"{key}{self.count[key]}"
            super().__setitem__(key, value)

    config = configparser.ConfigParser(dict_type=NumberedSections, strict=False)
    config.read(path)
    # the section proxies have the names before numbering
    sections = config._sections  # ty: ignore[unresolved-attribute]
    return pd.DataFrame([dict(v) for k, v in sections.items() if k.startswith(section)])


def read_mdu_files(mdu_file: Path) -> dict[str, Path]:
    """The network, structure and external forcing files of a D-HYDRO model."""
    mdu = configparser.ConfigParser()
    mdu.read_string(mdu_file.read_text().replace("*\n", "# *\n"))
    return {
        "net_file": mdu_file.parent / mdu["geometry"]["netfile"],
        "structure_file": mdu_file.parent / mdu["geometry"]["structurefile"],
        "external_forcing_file": mdu_file.parent / mdu["external forcing"]["extforcefilenew"],
    }


def points_along_branches(
    branches: gpd.GeoDataFrame, points: pd.DataFrame, distance_column: str = "chainage"
) -> gpd.GeoDataFrame:
    """Locate points on the branch of their `branch_id`, at the distance in `distance_column`."""
    located = gpd.GeoDataFrame(merge_grouped(branches, points, on="branch_id"), geometry="geometry", crs=branches.crs)
    located["geometry"] = located.geometry.interpolate(located[distance_column].astype(float))
    return located


def read_edges(
    network: hcdfm.net.models.Network, crs: str
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """The branches, and the edges and nodes of the computational grid of a D-HYDRO network."""
    mesh1d = network._mesh1d
    branches = gpd.GeoDataFrame(
        {"branch_id": list(mesh1d.branches.keys())},
        geometry=[LineString(b.geometry) for b in mesh1d.branches.values()],
        crs=crs,
    )
    nodes = gpd.GeoDataFrame(
        {
            "branch_id": mesh1d.mesh1d_node_branch_id,
            "chainage": mesh1d.mesh1d_node_branch_offset,
            "node_id": mesh1d.mesh1d_node_id,
            "node_no": np.arange(len(mesh1d.mesh1d_node_x)),
        },
        geometry=gpd.points_from_xy(mesh1d.mesh1d_node_x, mesh1d.mesh1d_node_y),
        crs=crs,
    )
    edges = pd.DataFrame(
        {
            "branch_id": branches["branch_id"].to_numpy()[mesh1d.mesh1d_edge_branch_id],
            "from_node": mesh1d.mesh1d_edge_nodes[:, 0],
            "to_node": mesh1d.mesh1d_edge_nodes[:, 1],
        }
    )
    node_geometry = pd.DataFrame({"node_no": nodes["node_no"], "geometry": nodes.geometry})
    edges = merge_grouped(edges, node_geometry, left_on="from_node", right_on="node_no", suffixes=("", "_from"))
    edges = merge_grouped(edges, node_geometry, left_on="to_node", right_on="node_no", suffixes=("_from", "_to"))
    branch_geometry = dict(zip(branches["branch_id"], branches.geometry, strict=True))
    edges = gpd.GeoDataFrame(
        {
            "edge_no": np.arange(len(edges)),
            "branch_id": edges["branch_id"],
            "from_node": edges["from_node"],
            "to_node": edges["to_node"],
        },
        geometry=[
            extract_segment_from_linestring(branch_geometry[b], p1, p2)
            for b, p1, p2 in zip(edges["branch_id"], edges["geometry_from"], edges["geometry_to"], strict=True)
        ],
        crs=crs,
    )
    return branches, edges, nodes


def nearest_index(gdf: gpd.GeoDataFrame, other: gpd.GeoDataFrame) -> np.ndarray:
    """The index of the nearest geometry in `other` for each geometry in `gdf`."""
    ix_gdf, ix_other = other.sindex.nearest(gdf.geometry, return_all=False)
    assert (ix_gdf == np.arange(len(gdf))).all()
    return other.index.to_numpy()[ix_other]


def read_structures(
    structure_file: Path, branches: gpd.GeoDataFrame, edges: gpd.GeoDataFrame
) -> dict[str, gpd.GeoDataFrame]:
    """The structures by type, except compounds, on the nearest branch and its nearest edge.

    The pumps of a pumping station are combined. Each type is sorted by `structure_id`.
    """
    model = hcdfm.structure.models.StructureModel(structure_file)
    structures = pd.DataFrame([s.__dict__ for s in model.structure]).drop(columns="name")  # ty: ignore[unresolved-attribute]
    structures = structures.rename(columns={"branchid": "branch_id", "id": "structure_id", "type": "object_type"})
    structures = points_along_branches(branches, structures[["structure_id", "object_type", "branch_id", "chainage"]])
    structures["branch_id"] = nearest_index(structures, branches.set_index("branch_id").sort_index())
    # the nearest edge on the branch of each structure
    on_branch = []
    for branch_id in structures["branch_id"].unique():
        branch_structures = structures[structures["branch_id"] == branch_id].copy()
        branch_edges = edges[edges["branch_id"] == branch_id].set_index("edge_no").sort_index()
        branch_structures["edge_no"] = nearest_index(branch_structures, branch_edges)
        on_branch.append(branch_structures)
    structures = pd.concat(on_branch)
    structures["node_no"] = -1

    by_type = {}
    for object_type in structures["object_type"].unique():
        if object_type == "compound":
            continue
        gdf = structures[structures["object_type"] == object_type]
        if object_type == "pump":
            # the pumps of a pumping station, at one location, as one
            gdf = gdf.groupby(gdf.geometry.to_wkt(), as_index=False).first().reset_index(drop=True)
        gdf = gpd.GeoDataFrame(gdf, geometry="geometry", crs=edges.crs)
        by_type[object_type] = gdf.sort_values(by="structure_id", kind="stable").reset_index(drop=True)
    return by_type


def read_boundaries_and_laterals(
    external_forcing_file: Path, branches: gpd.GeoDataFrame, network_nodes: gpd.GeoDataFrame, nodes: gpd.GeoDataFrame
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """The boundaries on their network node, with their nearest grid node, and the laterals on their branch.

    As in 2024, the nearest grid node excludes the grid node with the index of the boundary.
    """
    boundaries = read_ini_sections(external_forcing_file, "Boundary").rename(columns={"nodeid": "network_node_id"})
    boundaries["network_node_id"] = boundaries["network_node_id"].str.strip("b '\"")
    boundaries = network_nodes[["network_node_id", "geometry"]].merge(boundaries, how="right", on="network_node_id")
    nearest_node_no = []
    for index, point in zip(boundaries.index, boundaries.geometry, strict=True):
        nearest = nearest_points(point, nodes.drop(index=index).geometry.union_all())[1]
        nearest_node_no.append(nodes.loc[nodes.geometry == nearest, "node_no"].iloc[0])
    nearest_nodes = gpd.GeoDataFrame({"node_no": nearest_node_no}, geometry=boundaries.geometry, crs=nodes.crs)
    boundaries = boundaries.sjoin(nearest_nodes).drop(columns="index_right").reset_index(drop=True)
    boundaries.insert(0, "boundary_id", boundaries.index + 1)
    boundaries = boundaries.rename(columns={"network_node_id": "boundary_name", "quantity": "boundary_type"})
    boundaries["boundary_type"] = boundaries["boundary_type"].map(
        {"dischargebnd": "FlowBoundary", "waterlevelbnd": "LevelBoundary"}
    )

    laterals = read_ini_sections(external_forcing_file, "Lateral").rename(columns={"branchid": "branch_id"})
    laterals = points_along_branches(branches, laterals)
    return boundaries, laterals


def read_dhydro_network(mdu_file: Path, crs: str = "EPSG:28992") -> tuple[Network, gpd.GeoDataFrame, gpd.GeoDataFrame]:
    """Read the computational grid of the 1D network of a D-HYDRO model, and its structures, boundaries and laterals.

    The edges are numbered as in 2024, see `ribasim_nl.lumping.utils.merge_grouped`. Returns the network, the
    boundaries and the laterals.
    """
    files = read_mdu_files(mdu_file)
    network = hcdfm.net.models.Network.from_file(files["net_file"])
    with xr.open_dataset(files["net_file"]) as ds:
        prefix = "network" if "network_node_id" in ds else "Network"
        network_nodes = gpd.GeoDataFrame(
            {"network_node_id": ds[f"{prefix}_node_id"].to_numpy().astype(str)},
            geometry=gpd.points_from_xy(ds[f"{prefix}_node_x"].to_numpy(), ds[f"{prefix}_node_y"].to_numpy()),
            crs=crs,
        )
    network_nodes["network_node_id"] = network_nodes["network_node_id"].str.strip(" ").str.strip("b '\"")
    branches, edges, nodes = read_edges(network, crs)
    structures = read_structures(files["structure_file"], branches, edges)
    boundaries, laterals = read_boundaries_and_laterals(files["external_forcing_file"], branches, network_nodes, nodes)
    return Network(edges=edges, nodes=nodes, structures=structures), boundaries, laterals


def select_split_nodes(
    network: Network,
    structure_types: list[str],
    exclude_structure_ids: list[str],
    edge_nos: list[int],
) -> gpd.GeoDataFrame:
    """Split nodes at the structures of `structure_types`, except those excluded, and halfway the edges `edge_nos`.

    Only the first split node on each edge is kept. The split nodes are numbered, as `split_node`.
    """
    columns = ["split_node_id", "geometry", "object_type", "split_type", "edge_no", "node_no"]
    split_nodes = pd.concat(
        [
            network.structures[t].rename(columns={"structure_id": "split_node_id"})
            for t in SPLIT_NODE_STRUCTURES
            if t in structure_types and t in network.structures
        ]
    ).reindex(columns=columns)
    split_nodes = split_nodes[~split_nodes["split_node_id"].isin(exclude_structure_ids)]

    open_water = network.edges.loc[network.edges["edge_no"].isin(edge_nos), ["edge_no", "branch_id", "geometry"]]
    open_water["geometry"] = [g.interpolate(0.5, normalized=True) for g in open_water.geometry]
    open_water["split_node_id"] = open_water["edge_no"]
    open_water["object_type"] = "openwater"
    open_water["node_no"] = -1
    split_nodes = pd.concat([split_nodes, open_water]).drop_duplicates(subset="edge_no", keep="first")
    split_nodes["node_no"] = -1
    split_nodes = split_nodes.reset_index(drop=True)
    split_nodes.insert(0, "split_node", split_nodes.index + 1)
    return gpd.GeoDataFrame(split_nodes, geometry="geometry", crs=network.edges.crs)
