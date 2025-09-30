import logging
from collections import defaultdict

import shapely
from ribasim import Model, Node

from ribasim_nl import CloudStorage

logger = logging.getLogger(__name__)


def find_common_linestring(
    linestrings: list[shapely.LineString],
) -> tuple[list[shapely.LineString], list[int | None], list[shapely.LineString]]:
    """Given a list of linestrings, returns the overlapping parts starting from the end.

    This function finds groups of linestrings that share common coordinate sequences
    starting from their end points. It returns the common parts as new linestrings,
    a mapping indicating which original linestring belongs to which common part and
    a list of the linestrings after being stripped of their common parts.

    Example:
        If linestring A ends with [(2,2), (3,3)] and linestring B ends with [(2,2), (3,3)],
        both will be mapped to the same common linestring containing [(2,2), (3,3)].
        Independent linestrings will have None in their mapping position.
    """
    if len(linestrings) <= 1:
        return [], [None] * len(linestrings)

    # Convert linestrings to coordinate arrays (reversed to start from end)
    coords_list = [list(ls.coords)[::-1] for ls in linestrings]

    # Build sparse adjacency matrix of overlap lengths between linestrings
    n = len(linestrings)
    overlap_lengths = {}

    for i in range(n):
        for j in range(i + 1, n):
            coords_i = coords_list[i]
            coords_j = coords_list[j]
            # Find how many coordinates they share from the start (end of original)
            common_length = 0
            for coord_i, coord_j in zip(coords_i, coords_j):
                if coord_i == coord_j:
                    common_length += 1
                else:
                    break

            if common_length >= 2:  # Need at least 2 points to form a line
                overlap_lengths[(i, j)] = common_length

    # Process overlaps to find groups of overlapping linestrings
    groups = {}
    group_id = 0
    lengths = {}
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
    common_linestrings = []
    linestring_mapping = [None] * len(linestrings)
    stripped_linestrings = list(linestrings).copy()

    reverse_groups = defaultdict(list)
    for idx, gid in groups.items():
        reverse_groups[gid].append(idx)

    for i, (gid, indices) in enumerate(reverse_groups.items()):
        length = lengths[gid]
        common_coords = list(linestrings.iloc[indices[0]].coords)[-length:]
        common_linestrings.append(shapely.LineString(common_coords))
        for idx in indices:
            linestring_mapping[idx] = i
            stripped_linestrings[idx] = shapely.LineString(
                list(linestrings.iloc[idx].coords)[: -length + 1]
            )  # Keep last point for connectivity

    return common_linestrings, linestring_mapping, stripped_linestrings


def _junctionfy(links):
    junction_ids = []
    grouped_links = links.groupby("to_node_id")
    for to_node_id, group in grouped_links:
        if len(group) == 1:
            continue
        print(f"Processing links with to_node_id #{to_node_id} with {len(group)} links")
        common_linestrings, linestring_mapping, stripped_linestrings = find_common_linestring(group.geometry)
        # Introduce Junction for each overlapping part
        # And change the to_node_id of the lines to that junction
        group.geometry = stripped_linestrings
        for i, common_linestring in enumerate(common_linestrings):
            junction = model.junction.add(Node(geometry=shapely.Point(common_linestring.coords[0])))
            junction_ids.append(junction.node_id)
            model.link.add(
                from_node=junction,
                to_node=Node(to_node_id, shapely.Point(0, 0), node_type="Junction"),
                geometry=common_linestring,
            )
            print(
                f"  Added Junction #{x.node_id} for common linestring {i} with {len(common_linestring.coords)} points"
            )
            for idx, mapping in enumerate(linestring_mapping):
                if mapping == i:
                    print(f"    Updating link #{group.index[idx]} to new to_node_id Junction #{x.node_id}")
                    model.link.df.loc[group.index[idx], "to_node_id"] = junction.node_id
                    model.link.df.loc[group.index[idx], "geometry"] = stripped_linestrings[idx]

    return junction_ids


def junctionfy(
    model,
):
    links = model.link.df[model.link.df.link_type == "flow"]
    new_junctions = _junctionfy(links)
    iteration = 0
    while len(new_junctions) > 0:
        print("Iteration", iteration, "with", len(new_junctions), "new junctions")
        nlinks = model.link.df[model.link.df.to_node_id.isin(new_junctions)]
        new_junctions = _junctionfy(nlinks)
        iteration += 1

    return model


if __name__ == "__main__":
    cloud = CloudStorage()

    toml_path_input = cloud.joinpath("Rijkswaterstaat/modellen/lhm_coupled_2025_9_0/lhm.toml")
    toml_path_output = cloud.joinpath("Rijkswaterstaat/modellen/lhm_coupled_junction/lhm.toml")

    model = Model.read(toml_path_input)
    model = junctionfy(model)
    model.write(toml_path_output)
