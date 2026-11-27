"""Read the HyDAMO data, the split nodes and boundaries, and the areas to lump a network with."""

import logging
import re
from collections.abc import Collection
from pathlib import Path

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import pyogrio

from ribasim_nl.lumping.connect import connect_lines
from ribasim_nl.lumping.network import SPLIT_EDGE_OFFSET, Bypass, Network, find_bypasses, separates
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
# a hydroobject that bypasses a split node is left out if it is at most this long, in m, as a fish passage is, and has no
# structure of these HyDAMO layers without a split node, see `read_network_without_bypasses`
MAX_BYPASS_LENGTH = 150.0
BYPASS_STRUCTURE_LAYERS = ["gemaal", "sluis", "stuw"]
MAX_BYPASS_ROUNDS = 10


def _list_layers(path: Path) -> list[str]:
    return list(pyogrio.list_layers(path)[:, 0])


def _split_from(code: str) -> list[str]:
    """The code, and the codes of the lines it was split from, see `ribasim_nl.lumping.connect`: `A-0.2` of `A-0`, `A`."""
    codes = [code]
    while (parent := re.sub(r"[-.]\d+$", "", codes[-1])) != codes[-1]:
        codes.append(parent)
    return codes


def _remove_objects(gdf: gpd.GeoDataFrame, remove: gpd.GeoDataFrame, layer: str, connected: bool) -> gpd.GeoDataFrame:
    """The objects of `gdf` without those of `remove`, by `code`, else `globalid`, else geometry.

    With `connected`, for lines that `ribasim_nl.lumping.connect` connected, a removal by code also removes the pieces
    that the line was split into, so it still applies when a new line ends on it, and every object of `remove` with a
    code or globalid must be in `gdf`.
    """
    key = next((c for c in ("code", "globalid") if c in gdf.columns and c in remove.columns), None)
    if key is None:
        return gdf.loc[~gdf.geometry.normalize().isin(remove.geometry.normalize())]
    removals = set(remove[key].dropna())
    if key == "code" and connected:
        matches = gdf[key].map(
            lambda code: next((c for c in _split_from(code) if c in removals), None), na_action="ignore"
        )
    else:
        matches = gdf[key].where(gdf[key].isin(removals))
    missing = sorted(removals - set(matches.dropna()))
    assert not (connected and missing), f"{layer}: no objects to remove with {key} {missing}"
    return gdf.loc[matches.isna()]


def _add_objects(gdf: gpd.GeoDataFrame, add: gpd.GeoDataFrame, layer: str) -> gpd.GeoDataFrame:
    """The objects of `gdf` and `add`, with a column `index` with the row numbers before.

    The added objects get the CRS of `gdf`, and the types of its columns where they allow.
    """
    if isinstance(add, gpd.GeoDataFrame) and add.crs is not None:
        assert gdf.crs is not None, f"{layer}: the additions have a CRS, the source has not"
        add = add.to_crs(gdf.crs)
    for column in add.columns.intersection(gdf.columns).drop("geometry", errors="ignore"):
        if add[column].dtype != gdf[column].dtype:
            try:
                add[column] = add[column].astype(gdf[column].dtype)
            except (TypeError, ValueError):
                logger.warning(f"{layer}: column {column} of the additions has another type")
    # an empty layer would turn the types of its columns into object
    return (pd.concat([gdf, add]) if len(add) > 0 else gdf).reset_index()


def _connect_hydroobjects(hydroobject: gpd.GeoDataFrame, distance: float) -> gpd.GeoDataFrame:
    """The hydroobjects connected where they end within `distance` of each other, if each code is used only once."""
    duplicated = sorted(hydroobject.loc[hydroobject["code"].duplicated() & hydroobject["code"].notna(), "code"])
    assert not duplicated, f"several hydroobjects with the codes {duplicated}"
    return connect_lines(hydroobject, distance)


def _write_layer(gdf: gpd.GeoDataFrame, target: Path, layer: str) -> None:
    """Write a layer, or an attribute table if it has no geometries."""
    if isinstance(gdf, gpd.GeoDataFrame) and not gdf.geometry.isna().all():
        pyogrio.write_dataframe(gdf, target, layer=layer, promote_to_multi=False)
    else:
        pyogrio.write_dataframe(pd.DataFrame(gdf.drop(columns="geometry", errors="ignore")), target, layer=layer)


def apply_hydamo_changes(
    source: Path, removals: Path, additions: Path, target: Path, connect_distance: float | None = None
) -> None:
    """Write the HyDAMO layers of `source` to `target`, with the objects of `removals` removed and `additions` added.

    Objects are removed by `code`, else `globalid`, else geometry. As in 2024, layers in `additions` get a column
    `index`, with the row numbers before the additions. Layers without geometries are written as attribute tables.
    The columns keep their types, unlike in 2024, when some were written as text.

    With `connect_distance`, the hydroobjects that end within that distance of each other are connected after the
    changes, see `ribasim_nl.lumping.connect.connect_lines`. An added hydroobject then need not end exactly on the
    one it connects to, and that one need not be split there by hand. Every removal must then remove an object, a
    removal by code also removes the pieces of the line, and the hydroobjects with a code must each have their own.
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
            gdf = _remove_objects(gdf, remove, layer, connected=connect_distance is not None)
            logger.info(f"{layer}: removed {len(remove)} objects")
        if layer in addition_layers:
            add = gpd.read_file(additions, layer=layer)
            gdf = _add_objects(gdf, add, layer)
            logger.info(f"{layer}: added {len(add)} objects")
        if connect_distance is not None and layer.lower() == "hydroobject":
            gdf = _connect_hydroobjects(gdf, connect_distance)
        _write_layer(gdf, target, layer)


def read_areas(path: Path, layer: str | None = None, code_column: str = "CODE") -> gpd.GeoDataFrame:
    """Read areas, with their `code`, and number them, as `area`."""
    areas = read_layer(path, layer, explode=False, code_column=code_column)
    assert areas is not None, f"no layer {layer} in {path}"
    areas["area"] = areas.index + 1
    return areas


def read_hydamo_network(path: Path, dx: float | None = None, exclude: Collection[str] = ()) -> Network:
    """Read the network of the HyDAMO hydroobjects, split in edges of about `dx` m, and its structures.

    The hydroobjects with a code in `exclude` are left out. The edges get the `code` and `created_date` of their
    hydroobject, as `hydroobject` and `created`. The column names of the structures are lowercased.
    """
    branches = read_layer(path, "hydroobject", remove_z=True)
    assert branches is not None, f"no hydroobject layer in {path}"
    if "created_date" in branches:
        created = pd.to_datetime(branches["created_date"], utc=True)
    else:
        created = pd.Series(pd.NaT, index=branches.index, dtype="datetime64[ns, UTC]")
    branches = branches.assign(branch_id=branches["code"], hydroobject=branches["code"], created=created)
    branches = branches[["branch_id", "hydroobject", "created", "geometry"]]
    missing = set(exclude) - set(branches["branch_id"])
    assert not missing, f"no hydroobjects {sorted(missing)} to leave out in {path}"
    branches = branches[~branches["branch_id"].isin(exclude)]
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
    path: Path,
    layer: str,
    network: Network,
    buffer_distance: float = 2.0,
    min_length_edge: float = 2.0,
    off_network: Collection[str] = (),
) -> tuple[gpd.GeoDataFrame, Network]:
    """Read split nodes, snap them to the network, and split its edges at them.

    Split nodes without `split_node_id` or `object_type` take these from the structure they are on, else from the
    edge they are on, as `openwater`. They are snapped to a node within a tenth of `buffer_distance`, unless it has
    3 edges or more, else to an edge within `buffer_distance`. A split node that is not on the network is an error,
    but those with a `split_node_id` in `off_network`, which are left out. Returns the split nodes and the network
    with the split edges.
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
    off = split_nodes.loc[not_on_network, ["split_node_id", "object_type", "geometry"]]
    unexpected = off[~off["split_node_id"].isin(off_network)]
    assert unexpected.empty, (
        f"split nodes that are not on the network, within {buffer_distance} m of an edge, or within "
        f"{buffer_distance * 0.1} m of a node with less than 3 edges:\n{unexpected.to_wkt()}"
    )
    on_network = set(off_network) - set(off["split_node_id"])
    assert not on_network, f"split nodes {sorted(on_network)} are on the network after all"
    return split_nodes.loc[~not_on_network], Network(edges=edges, nodes=nodes, structures=network.structures)


def _without_prefix(code: object) -> object:
    """A code without the prefix `GEM_` that the split nodes of 2024 gave pumping stations."""
    return code.removeprefix("GEM_") if isinstance(code, str) else code


def _leave_out(
    network: Network,
    split_nodes: gpd.GeoDataFrame,
    graph: nx.Graph,
    bypass: Bypass,
    removed: set[int],
    anchors: set[int],
    codes: set[str],
    max_length: float,
    buffer_distance: float,
) -> tuple[list[int] | None, str]:
    """The edges of the hydroobject to leave out for a bypass, see `read_network_without_bypasses`, or None.

    Also returns the code of the hydroobject and its length, or else what is on the way around the split node.
    `removed` are the edges that are left out already, and `codes` those of the hydroobjects that can be left out.
    """
    edges = network.edges.set_index("edge_no")
    hydroobject = edges["hydroobject"]
    # a hydroobject that was split, see `ribasim_nl.lumping.connect`, is left out with all its pieces
    watercourse = hydroobject.map(lambda code: _split_from(code)[-1], na_action="ignore")
    frames = [
        network.structures[layer][["code", "geometry"]]
        for layer in BYPASS_STRUCTURE_LAYERS
        if layer in network.structures
    ]
    structures = gpd.GeoDataFrame(pd.concat(frames), geometry="geometry", crs=edges.crs)
    with_split_node = set(split_nodes["split_node_id"].map(_without_prefix))
    structures = structures[~structures["code"].map(_without_prefix).isin(with_split_node)]
    # the watercourses of the split node itself, and when they were created, if known
    row = split_nodes.loc[split_nodes["split_node"] == bypass.split_node].iloc[0]
    at_node = (edges["from_node"] == row["node_no"]) | (edges["to_node"] == row["node_no"])
    own = set(watercourse[[row["edge_no"]]] if row["edge_no"] != -1 else watercourse[at_node])
    own_created = edges.loc[watercourse.isin(own), "created"].max()
    way = set(bypass.edge_nos)
    candidates = []
    for code in sorted(set(watercourse[bypass.edge_nos].dropna()) - own):
        pieces = set(watercourse.index[watercourse == code])
        geometry = edges.loc[sorted(pieces)].geometry.union_all()
        created = edges.loc[sorted(pieces), "created"].min()
        if (
            pd.notna(created)
            and (pd.isna(own_created) or created > own_created)
            and pieces <= way
            and set(hydroobject[sorted(pieces)]) <= codes
            and geometry.length <= max_length
            and not (structures.distance(geometry) <= buffer_distance).any()
        ):
            candidates.append((geometry.length, code, pieces))
    for length, code, pieces in sorted(candidates):
        if separates(graph, bypass, removed | pieces, anchors):
            return sorted(pieces), f"{code}, {length:.0f} m"
    on_way = structures[structures.distance(edges.loc[bypass.edge_nos].geometry.union_all()) <= buffer_distance]
    return (
        None,
        f"the way around it is {sorted(set(hydroobject[bypass.edge_nos].dropna()))}, with the structures without split node {sorted(on_way['code'])}",
    )


def read_network_without_bypasses(
    path: Path,
    split_nodes_path: Path,
    dx: float,
    buffer_distance: float,
    max_bypass_length: float = MAX_BYPASS_LENGTH,
    off_network: Collection[str] = (),
) -> tuple[gpd.GeoDataFrame, Network]:
    """Read the network and its split nodes, see `read_hydamo_network` and `read_split_nodes`, without bypasses.

    A hydroobject can connect the two sides of a split node without it, such as a new fish passage around a weir.
    Then the split node separates nothing, and its structure would be left out of the model, see
    `ribasim_nl.lumping.network.find_bypasses`. The shortest hydroobject on the shortest way around the split node is
    then left out, as part of the structure, with the other pieces it was split into, if these:

    - are all on that way, unlike the pieces of a watercourse that goes on, have a code, and are not of the watercourse
      of the split node itself;
    - were created after the watercourse of the split node, by their `created_date`: a watercourse that was there
      before, such as one that continues the watercourse of the split node, is not left out;
    - are together at most `max_bypass_length` m long, and have no weir, sluice or pumping station without a split
      node;
    - separate the sides of the split node, while each side still reaches another split node or a boundary.

    Else the bypass is an error, to decide by hand, for instance with a split node on the way around. This repeats until
    no split node is bypassed. Returns the split nodes and the network, as `read_split_nodes`, which gets `off_network`.
    """
    hydroobject = read_layer(path, "hydroobject")
    assert hydroobject is not None, f"no hydroobject layer in {path}"
    codes = set(hydroobject["code"].dropna())
    excluded: set[str] = set()
    for _ in range(MAX_BYPASS_ROUNDS):
        network = read_hydamo_network(path, dx=dx, exclude=excluded)
        split_nodes, network = read_split_nodes(
            split_nodes_path, "split_nodes", network, buffer_distance=buffer_distance, off_network=off_network
        )
        graph, bypasses = find_bypasses(network, split_nodes)
        if not bypasses:
            return split_nodes, network
        boundaries = read_boundaries(split_nodes_path, "boundaries", network.nodes, buffer_distance=buffer_distance)
        anchors = {n for n in graph if n >= SPLIT_EDGE_OFFSET} | set(boundaries["node_no"])
        removed: set[int] = set()
        unresolved = []
        for bypass in bypasses:
            if separates(graph, bypass, removed, anchors):
                continue
            name = split_nodes.loc[split_nodes["split_node"] == bypass.split_node, "split_node_id"].iloc[0]
            edge_nos, description = _leave_out(
                network, split_nodes, graph, bypass, removed, anchors, codes, max_bypass_length, buffer_distance
            )
            if edge_nos is None:
                unresolved.append(f"{name}: {description}")
            else:
                logger.info(f"Split node {name} is bypassed by {description}, which is left out")
                removed |= set(edge_nos)
        assert not unresolved, "split nodes that are bypassed, see `read_network_without_bypasses`:\n" + "\n".join(
            unresolved
        )
        excluded |= set(network.edges.loc[network.edges["edge_no"].isin(removed), "hydroobject"])
    raise AssertionError(f"split nodes are still bypassed after {MAX_BYPASS_ROUNDS} rounds")


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
    if "boundary_type" not in boundaries.columns:
        boundaries["boundary_type"] = None
    boundaries["boundary_type"] = boundaries["boundary_type"].fillna("LevelBoundary")
    not_on_network = boundaries["node_no"] == -1
    logger.info(f"Removing {not_on_network.sum()} boundaries that are not on a node")
    return boundaries.loc[~not_on_network]
