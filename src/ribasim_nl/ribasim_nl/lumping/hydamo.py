"""Read the HyDAMO data, the split nodes and boundaries, and the areas to lump a network with."""

import logging
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyogrio

from ribasim_nl.lumping.network import Network
from ribasim_nl.lumping.utils import (
    drop_duplicate_geometries,
    generate_nodes_from_edges,
    get_node_no_and_edge_no_for_points,
    quicksort_order,
    read_layer,
    snap_points_to_nodes_and_edges,
    split_edges_by_dx,
    split_edges_by_split_nodes,
)

logger = logging.getLogger(__name__)

# the HyDAMO layers of the structures that split nodes can be on, in order of preference
STRUCTURE_LAYERS = ["gemaal", "sluis", "stuw", "afsluitmiddel", "duikersifonhevel"]


def _list_layers(path: Path) -> list[str]:
    return list(pyogrio.list_layers(path)[:, 0])


def apply_hydamo_changes(source: Path, removals: Path, additions: Path, target: Path) -> None:
    """Write the HyDAMO layers of `source` to `target`, with the objects of `removals` removed and `additions` added.

    Objects are removed by `code`, else `globalid`, else geometry. As in 2024, layers in `additions` get a column
    `index`, with the row numbers before the additions. Layers without geometries are written as attribute tables.
    The columns keep their types, unlike in 2024, when some were written as text.
    """
    assert source != target
    target.unlink(missing_ok=True)
    removal_layers, addition_layers = _list_layers(removals), _list_layers(additions)
    for layer in _list_layers(source):
        if layer == "layer_styles":
            continue
        gdf = gpd.read_file(source, layer=layer)
        if layer in removal_layers:
            remove = gpd.read_file(removals, layer=layer)
            key = next((c for c in ("code", "globalid") if c in gdf.columns and c in remove.columns), None)
            if key is not None:
                gdf = gdf.loc[~gdf[key].isin(remove[key])]
            else:
                gdf = gdf.loc[~gdf.geometry.normalize().isin(remove.geometry.normalize())]
            logger.info(f"{layer}: removed {len(remove)} objects")
        if layer in addition_layers:
            add = gpd.read_file(additions, layer=layer)
            if isinstance(add, gpd.GeoDataFrame) and add.crs is not None:
                assert gdf.crs is not None, f"{layer}: the additions have a CRS, the source has not"
                add = add.to_crs(gdf.crs)
            # keep the column types of the source, where the additions allow
            for column in add.columns.intersection(gdf.columns).drop("geometry", errors="ignore"):
                if add[column].dtype != gdf[column].dtype:
                    try:
                        add[column] = add[column].astype(gdf[column].dtype)
                    except (TypeError, ValueError):
                        logger.warning(f"{layer}: column {column} of the additions has another type")
            # an empty layer would turn the types of its columns into object
            gdf = (pd.concat([gdf, add]) if len(add) > 0 else gdf).reset_index()
            logger.info(f"{layer}: added {len(add)} objects")
        if isinstance(gdf, gpd.GeoDataFrame) and not gdf.geometry.isna().all():
            pyogrio.write_dataframe(gdf, target, layer=layer, promote_to_multi=False)
        else:
            pyogrio.write_dataframe(pd.DataFrame(gdf.drop(columns="geometry", errors="ignore")), target, layer=layer)


def read_areas(path: Path, layer: str | None = None, code_column: str = "CODE") -> gpd.GeoDataFrame:
    """Read areas, with their `code`, and number them, as `area`."""
    areas = read_layer(path, layer, explode=False, code_column=code_column)
    assert areas is not None, f"no layer {layer} in {path}"
    areas["area"] = areas.index + 1
    return areas


def read_hydamo_network(path: Path, dx: float | None = None) -> Network:
    """Read the network of the HyDAMO hydroobjects, split in edges of about `dx` m, and its structures.

    The column names of the structures are lowercased.
    """
    branches = read_layer(path, "hydroobject", remove_z=True)
    assert branches is not None, f"no hydroobject layer in {path}"
    branches = branches.rename(columns={"code": "branch_id"})[["branch_id", "geometry"]]
    branches, _ = generate_nodes_from_edges(branches)
    edges = branches.rename(columns={"branch_id": "edge_id"}) if dx is None else split_edges_by_dx(branches, dx=dx)
    edges, nodes = generate_nodes_from_edges(edges)
    structures = {}
    for layer in STRUCTURE_LAYERS:
        gdf = read_layer(path, layer)
        if gdf is not None:
            structures[layer] = gdf.rename(columns={c: c.lower() for c in gdf.columns})
    return Network(edges=edges, nodes=nodes, structures=structures)


def read_split_nodes(
    path: Path, layer: str, network: Network, buffer_distance: float = 2.0, min_length_edge: float = 2.0
) -> tuple[gpd.GeoDataFrame, Network]:
    """Read split nodes, snap them to the network, and split its edges at them.

    Split nodes without `split_node_id` or `object_type` take these from the structure they are on, else from the
    edge they are on, as `openwater`. They are snapped to a node within a tenth of `buffer_distance`, unless it has
    3 edges or more, else to an edge within `buffer_distance`. Split nodes that are not on the network are removed.
    Returns the split nodes and the network with the split edges.
    """
    split_nodes = read_layer(path, layer, remove_z=True)
    assert split_nodes is not None, f"no layer {layer} in {path}"
    columns = ["split_node", "split_node_id", "object_type", "object_function", "geometry"]
    split_nodes = drop_duplicate_geometries(split_nodes[[c for c in split_nodes.columns if c in columns]])

    complete = split_nodes["split_node_id"].notna() & (split_nodes["object_type"] != "")
    old_split_nodes, new_split_nodes = split_nodes[complete], split_nodes[~complete]
    assert not new_split_nodes.empty, "split nodes without ID or type are expected, to snap all split nodes"

    # the structures each new split node is on, of the type of the split node if possible
    structures = pd.concat(
        [
            gdf.assign(object_type=name)[["code", "object_type", "geometry"]]
            for name, gdf in network.structures.items()
            if not gdf.empty
        ]
    )
    structures["object_type"] = pd.Categorical(
        structures["object_type"], categories=[*STRUCTURE_LAYERS, "openwater"], ordered=True
    )
    # sorted with the unstable sort of 2024, which decides between structures at one location
    structures = gpd.GeoDataFrame(
        structures.iloc[quicksort_order(structures["object_type"].cat.codes)].rename(columns={"code": "split_node_id"}),
        geometry="geometry",
        crs=network.edges.crs,
    )
    structures["geometry"] = structures.geometry.buffer(0.001)
    new_split_nodes = gpd.sjoin(
        new_split_nodes[["object_type", "object_function", "geometry"]].rename(
            columns={"object_type": "split_node_object_type"}
        ),
        structures[["object_type", "split_node_id", "geometry"]],
        how="left",
    ).drop(columns=["index_right"])
    duplicated = new_split_nodes.index.duplicated(keep=False)
    filtered = []
    for i in np.unique(new_split_nodes.index[duplicated]):
        candidates = new_split_nodes.loc[[i]]
        same_type = candidates.loc[
            [s == o for s, o in zip(candidates["split_node_object_type"], candidates["object_type"], strict=True)]
        ]
        if same_type.empty:
            filtered.append(candidates.iloc[[0]])
        else:
            filtered.append(same_type.iloc[quicksort_order(same_type["object_type"].cat.codes)].iloc[[0]])
    new_split_nodes = pd.concat([new_split_nodes[~duplicated], *filtered]).drop(columns=["split_node_object_type"])

    split_nodes = pd.concat([old_split_nodes, new_split_nodes]).sort_values(by=["split_node", "object_type"])
    split_nodes["object_type"] = split_nodes["object_type"].astype(object).fillna("openwater")

    # split nodes on open water get the ID of their edge
    with_id = split_nodes[split_nodes["split_node_id"].notna()]
    open_water = gpd.sjoin(
        split_nodes[split_nodes["split_node_id"].isna()],
        network.edges[["edge_id", "edge_no", "from_node", "to_node", "geometry"]],
        how="left",
    )
    open_water = open_water.loc[~open_water.index.duplicated(keep="first")]
    open_water["split_node_id"] = open_water["edge_id"]
    split_nodes = pd.concat([with_id, open_water]).reset_index(drop=True)
    split_nodes["split_node"] = split_nodes.index + 1
    split_nodes = drop_duplicate_geometries(gpd.GeoDataFrame(split_nodes, geometry="geometry", crs=network.edges.crs))

    split_nodes = snap_points_to_nodes_and_edges(
        split_nodes,
        nodes=network.nodes,
        edges=network.edges,
        edges_bufdist=buffer_distance,
        nodes_bufdist=buffer_distance * 0.1,
        n_edges_to_node_limit=3,
        min_length_edge=min_length_edge,
    )
    split_nodes, edges, nodes = split_edges_by_split_nodes(split_nodes, network.edges, buffer_distance=0.1)
    not_on_network = (split_nodes["edge_no"] == -1) & (split_nodes["node_no"] == -1)
    logger.info(f"Removing {not_on_network.sum()} split nodes that are not on the network")
    split_nodes = split_nodes.loc[~not_on_network]
    return split_nodes, Network(edges=edges, nodes=nodes, structures=network.structures)


def read_boundaries(path: Path, layer: str, nodes: gpd.GeoDataFrame, buffer_distance: float = 10.0) -> gpd.GeoDataFrame:
    """Read boundaries, snap them to a node within `buffer_distance`, and number them, as `boundary_id`.

    Boundaries without `boundary_type` are LevelBoundaries. Boundaries that are not on a node are removed.
    """
    boundaries = read_layer(path, layer, remove_z=True)
    assert boundaries is not None, f"no layer {layer} in {path}"
    boundaries = drop_duplicate_geometries(boundaries)
    boundaries = snap_points_to_nodes_and_edges(boundaries, nodes=nodes, nodes_bufdist=buffer_distance)
    boundaries = get_node_no_and_edge_no_for_points(boundaries, nodes=nodes)
    boundaries["boundary_id"] = boundaries.index + 1
    boundaries["boundary_type"] = boundaries["boundary_type"].fillna("LevelBoundary")
    not_on_network = boundaries["node_no"] == -1
    logger.info(f"Removing {not_on_network.sum()} boundaries that are not on a node")
    return boundaries.loc[~not_on_network]
