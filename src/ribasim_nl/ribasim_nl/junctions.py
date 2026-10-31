import logging
from collections import defaultdict
from typing import Any, cast

import geopandas as gpd
import pandas as pd
import shapely
from ribasim import Model, Node
from ribasim.input_base import NodeData
from ribasim.validation import can_connect

LOG = logging.getLogger(__name__)


def find_common_linestring(
    linestrings: list[shapely.LineString],
    converging: bool = True,
) -> tuple[list[shapely.LineString], list[int | None], list[shapely.LineString]]:
    """Given a list of linestrings, returns the overlapping parts starting from the end.

    This function finds groups of linestrings that share common coordinate sequences
    starting from their end points when converging is True, or starting from their start points
    when converging is False.
    It returns the common parts as new linestrings, a mapping indicating which original linestring
    belongs to which common part and a list of the linestrings after being stripped of their common parts.

    Example:
        If linestring A ends with [(2,2), (3,3)] and linestring B ends with [(2,2), (3,3)],
        both will be mapped to the same common linestring containing [(2,2), (3,3)].
        Independent linestrings will have None in their mapping position.
    """
    if len(linestrings) <= 1:
        return [], [None] * len(linestrings), linestrings

    # Convert linestrings to coordinate arrays (reversed to start from end)
    if converging:
        coords_list = [list(ls.coords)[::-1] for ls in linestrings]
    else:
        coords_list = [list(ls.coords) for ls in linestrings]

    # Build sparse adjacency matrix of overlap lengths between linestrings
    n = len(linestrings)
    overlap_lengths: dict[tuple[int, int], int] = {}

    for i in range(n):
        for j in range(i + 1, n):
            coords_i = coords_list[i]
            coords_j = coords_list[j]
            # Find how many coordinates they share from the start (end of original)
            common_length = 0
            for coord_i, coord_j in zip(coords_i, coords_j, strict=False):
                if coord_i == coord_j:
                    common_length += 1
                else:
                    break

            # If a linestring is fully part of the other one, or identical to it, keep its first segment
            # separate, such that the linestrings can still be joined.
            common_length = min(common_length, len(coords_i) - 1, len(coords_j) - 1)
            if common_length >= 2:  # Need at least 2 points to form a line
                overlap_lengths[(i, j)] = common_length

    # Process overlaps to find groups of overlapping linestrings
    groups: dict[int, int] = {}
    group_id = 0
    lengths: dict[int, int] = {}
    for (i, j), length in overlap_lengths.items():
        # New common group
        if i not in groups and j not in groups:
            groups[i] = group_id
            groups[j] = group_id
            lengths[group_id] = length
            group_id += 1
        # Existing groups
        elif i in groups and j not in groups:
            existing_group_id = groups[j] = groups[i]
            lengths[existing_group_id] = min(lengths[existing_group_id], length)
        elif j in groups and i not in groups:
            existing_group_id = groups[i] = groups[j]
            lengths[existing_group_id] = min(lengths[existing_group_id], length)
        # Both in groups, merge if different
        else:
            if groups[i] != groups[j]:
                gid_to_keep = groups[i]
                gid_to_remove = groups[j]
                for k in groups:
                    if groups[k] == gid_to_remove:
                        groups[k] = gid_to_keep
                lengths[gid_to_keep] = min(lengths[gid_to_keep], lengths[gid_to_remove])
                del lengths[gid_to_remove]

    # Create common linestrings and mapping
    common_linestrings: list[shapely.LineString] = []
    linestring_mapping: list[int | None] = [None] * len(linestrings)
    stripped_linestrings = list(linestrings).copy()
    linestrings_list = list(linestrings)

    reverse_groups = defaultdict(list)
    for idx, gid in groups.items():
        reverse_groups[gid].append(idx)

    for i, (gid, indices) in enumerate(reverse_groups.items()):
        length = lengths[gid]
        if converging:
            common_coords = list(linestrings_list[indices[0]].coords)[-length:]
        else:
            common_coords = list(linestrings_list[indices[0]].coords)[:length]
        common_linestrings.append(shapely.LineString(common_coords))
        for idx in indices:
            linestring_mapping[idx] = i
            if converging:
                stripped_linestrings[idx] = shapely.LineString(
                    list(linestrings_list[idx].coords)[: -length + 1]
                )  # Keep last point for connectivity
            else:
                stripped_linestrings[idx] = shapely.LineString(
                    list(linestrings_list[idx].coords)[length - 1 :]
                )  # Keep first point for connectivity

    return common_linestrings, linestring_mapping, stripped_linestrings


def _junctionify(model: Model, links: gpd.GeoDataFrame, converging: bool = True) -> list[int]:
    junction_ids = []
    field = "to_node_id" if converging else "from_node_id"
    node_type = model.node.df["node_type"]
    # only merge where the node can be connected to a Junction
    mergeable = [can_connect("Junction", t) if converging else can_connect(t, "Junction") for t in node_type]
    links = links[links[field].map(pd.Series(mergeable, index=node_type.index))]
    for node_id, group in links.groupby(field):
        assert isinstance(node_id, int)
        if len(group) == 1:
            continue
        LOG.debug(f"Processing links with {field} #{node_id} with {len(group)} links")
        common_linestrings, linestring_mapping, stripped_linestrings = find_common_linestring(
            group["geometry"].tolist(), converging
        )
        # Introduce Junction for each overlapping part
        # And change the to_node_id of the lines to that junction

        for i, common_linestring in enumerate(common_linestrings):
            idx = 0 if converging else -1
            junction = model.junction.add(Node(geometry=shapely.Point(common_linestring.coords[idx])))
            junction_ids.append(junction.node_id)
            node = NodeData(node_id, node_type[node_id], cast(shapely.Point, model.node.df.at[node_id, "geometry"]))
            if converging:
                model.link.add(from_node=junction, to_node=node, geometry=common_linestring)
            else:
                model.link.add(from_node=node, to_node=junction, geometry=common_linestring)
            LOG.debug(
                f"  Added Junction #{junction.node_id} for common linestring {i} with {len(common_linestring.coords)} points"
            )
            for idx, mapping in enumerate(linestring_mapping):
                if mapping == i:
                    LOG.debug(f"    Updating link #{group.index[idx]} to new {field} Junction #{junction.node_id}")
                    model.link.df.loc[group.index[idx], field] = junction.node_id
                    # the stubs of pandas do not allow geometries as values
                    cast(Any, model.link.df.loc)[group.index[idx], "geometry"] = stripped_linestrings[idx]

    return junction_ids


def junctionify[M: Model](model: M) -> M:
    """Add Junction nodes in a model inplace where flow links share common geometry.

    Useful for model visualization and debugging, as otherwise there will be many
    overlapping line segments that are hard to distinguish.

    Assumes that all flow link geometries between node a and b start with point a and end with point b.
    """
    flow = model.link.df[model.link.df.link_type == "flow"]
    geometries = flow.geometry.to_numpy()
    disconnected = ~(
        shapely.equals_exact(
            shapely.get_point(geometries, 0), model.node.df.geometry.reindex(flow.from_node_id).to_numpy(), 1e-6
        )
        & shapely.equals_exact(
            shapely.get_point(geometries, -1), model.node.df.geometry.reindex(flow.to_node_id).to_numpy(), 1e-6
        )
    )
    if disconnected.any():
        LOG.warning(f"{disconnected.sum()} flow link geometries do not connect their nodes: {flow.index[disconnected]}")

    # starting from common end points
    links = model.link.df[model.link.df.link_type == "flow"]
    links = links[[geom is not None for geom in links.geometry]]
    new_junctions = _junctionify(model, links, converging=True)
    iteration = 0
    while len(new_junctions) > 0:
        LOG.debug(f"Iteration {iteration} with {len(new_junctions)} new junctions")
        nlinks = model.link.df[model.link.df.to_node_id.isin(new_junctions)]
        new_junctions = _junctionify(model, nlinks, converging=True)
        iteration += 1

    # starting from common begin points
    links = model.link.df[model.link.df.link_type == "flow"]
    links = links[[geom is not None for geom in links.geometry]]
    new_junctions = _junctionify(model, links, converging=False)
    iteration = 0
    while len(new_junctions) > 0:
        LOG.debug(f"Iteration {iteration} with {len(new_junctions)} new junctions")
        nlinks = model.link.df[model.link.df.from_node_id.isin(new_junctions)]
        new_junctions = _junctionify(model, nlinks, converging=False)
        iteration += 1

    # return updated model
    return model
