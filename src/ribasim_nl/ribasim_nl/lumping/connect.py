"""Connect lines: the lumping connects lines only where their ends have the same coordinates.

See `ribasim_nl.lumping.hydamo.read_hydamo_network`. `connect_lines` gives the ends that lie close together one
coordinate, and splits a line where another one ends on it. `join_parts` gives every line its own code first, by which
the pieces are named.
"""

import itertools
import logging

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from shapely.geometry import LineString

logger = logging.getLogger(__name__)

type Coordinate = tuple[float, ...]


def _next_join(chains: list[tuple[int, list[Coordinate]]]) -> tuple[int, int] | None:
    """The positions of the first chain that ends where another one starts, and of that one, if there is one.

    A closed chain is not joined.
    """
    for i, (_, first) in enumerate(chains):
        if first[0] == first[-1]:
            continue
        j = next((j for j, (_, other) in enumerate(chains) if j != i and other[0] == first[-1]), None)
        if j is not None:
            return i, j
    return None


def _join_chains(chains: list[tuple[int, list[Coordinate]]]) -> list[tuple[int, list[Coordinate]]]:
    """Join the chains of which one ends where another starts, until none do.

    A chain is a row number, which a joined chain keeps of the first, and coordinates.
    """
    chains = list(chains)
    while (pair := _next_join(chains)) is not None:
        i, j = pair
        chains[i] = (chains[i][0], [*chains[i][1], *chains[j][1][1:]])
        del chains[j]
    return chains


def join_parts(lines: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Make the lines with the same code into one line where they follow each other, and remove the Z coordinates.

    A water authority can deliver a hydroobject in parts, as a multi-part line or as several objects with the same
    code. Parts of which one ends where the next starts are joined, with the attributes of the first. The lines that
    are left with a code get `<code>.<n>`, from 2, in order of decreasing length, so every line has its own code.
    """
    lines = lines.explode(ignore_index=True)
    lines = lines.set_geometry(lines.geometry.force_2d())
    duplicated = lines["code"].duplicated(keep=False) & lines["code"].notna()
    if not duplicated.any():
        return lines
    rows, codes, geometries = [], [], []
    for code, group in lines[duplicated].groupby("code", sort=True):
        chains = _join_chains([(row, list(line.coords)) for row, line in zip(group.index, group.geometry, strict=True)])
        lengths = [LineString(coords).length for _, coords in chains]
        order = np.argsort(-np.array(lengths), kind="stable")
        for n, position in enumerate(order):
            row, coords = chains[position]
            rows.append(row)
            codes.append(code if n == 0 else f"{code}.{n + 1}")
            geometries.append(LineString(coords))
    logger.info(f"Joined the lines of {duplicated.sum()} parts with the same code into {len(rows)} lines")
    parts = lines.loc[rows].copy()
    parts["code"] = codes
    parts = parts.set_geometry(gpd.GeoSeries(geometries, index=parts.index, crs=lines.crs))
    result = pd.concat([lines[~duplicated], parts], ignore_index=True)
    assert result["code"].dropna().is_unique
    return gpd.GeoDataFrame(result, geometry="geometry", crs=lines.crs)


def _ends(lines: gpd.GeoDataFrame) -> tuple[list[list[Coordinate]], list[Coordinate], list[Coordinate]]:
    """The coordinates of each line, and those of its start and its end."""
    coords = [list(line.coords) for line in lines.geometry]
    return coords, [c[0] for c in coords], [c[-1] for c in coords]


def snap_ends(lines: gpd.GeoDataFrame, tolerance: float) -> gpd.GeoDataFrame:
    """Give the ends of lines that are within `tolerance` of each other the same coordinate, so that they connect.

    Ends that are linked by ends within the tolerance form a group. Each group gets the coordinate that most of its
    lines end at, the first in the order of the lines of equal ones. A line whose ends are in the same group, since
    it is shorter than the tolerance, keeps its ends.
    """
    coords, start, end = _ends(lines)
    ends = start + end
    unique = list(dict.fromkeys(ends))
    count = pd.Series(ends, dtype=object).value_counts()
    points = shapely.points(unique)
    first, second = shapely.STRtree(points).query(points, predicate="dwithin", distance=tolerance)

    # the groups of ends, as the trees of a union-find, with the first end of each group as its root
    parent = np.arange(len(unique))

    def root(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = int(parent[i])
        return i

    for i, j in zip(first.tolist(), second.tolist(), strict=True):
        if i < j:
            root_i, root_j = root(i), root(j)
            parent[max(root_i, root_j)] = min(root_i, root_j)
    groups: dict[int, list[int]] = {}
    for i in range(len(unique)):
        groups.setdefault(root(i), []).append(i)

    snapped: dict[Coordinate, Coordinate] = {}
    for members in groups.values():
        if len(members) > 1:
            target = max(members, key=lambda i: (count[unique[i]], -i))
            snapped.update({unique[i]: unique[target] for i in members if i != target})
    logger.info(f"Moved {len(snapped)} ends of lines onto an end within {tolerance} m")

    geometries = []
    for line_coords in coords:
        new_start = snapped.get(line_coords[0], line_coords[0])
        new_end = snapped.get(line_coords[-1], line_coords[-1])
        if new_start == new_end and line_coords[0] != line_coords[-1]:
            new_start, new_end = line_coords[0], line_coords[-1]
        geometries.append(LineString([new_start, *line_coords[1:-1], new_end]))
    return lines.set_geometry(gpd.GeoSeries(geometries, index=lines.index, crs=lines.crs))


def _insert(coords: list[Coordinate], point: Coordinate) -> tuple[list[Coordinate], int]:
    """Insert a point in a line next to its nearest vertex, on the side that makes the line shortest.

    Returns the new coordinates of the line, and the position of that vertex.
    """
    xy = np.array([c[:2] for c in coords])
    nearest = int(np.argmin(np.hypot(xy[:, 0] - point[0], xy[:, 1] - point[1])))
    after = [*coords[: nearest + 1], point, *coords[nearest + 1 :]]
    before = [*coords[:nearest], point, *coords[nearest:]]
    return (after if LineString(after).length < LineString(before).length else before), nearest


def _ends_to_insert(lines: gpd.GeoDataFrame, buffer_distance: float) -> dict[int, list[Coordinate]]:
    """Per line, the loose ends within `buffer_distance` of it that are not one of its own ends, in order of the ends.

    A loose end is one at which no line starts, or no line ends.
    """
    _, start, end = _ends(lines)
    starts, ends = set(start), set(end)
    loose = [point for point in dict.fromkeys(start + end) if point not in starts or point not in ends]
    points = shapely.points([point[:2] for point in loose])
    tree = shapely.STRtree(lines.geometry.to_numpy())
    i_point, i_line = tree.query(points, predicate="dwithin", distance=buffer_distance)
    # in the order of the ends, then of the lines
    order = np.lexsort((i_line, i_point))
    to_insert: dict[int, list[Coordinate]] = {}
    for p, row in zip(i_point[order].tolist(), i_line[order].tolist(), strict=True):
        if loose[p] not in (start[row], end[row]):
            to_insert.setdefault(row, []).append(loose[p])
    return to_insert


def _insert_ends(
    coords: list[Coordinate], points: list[Coordinate], inserted: set[Coordinate]
) -> tuple[list[Coordinate], list[Coordinate]]:
    """Insert `points` in a line, see `_insert`, but, as in 2024, not next to an end of the line in `inserted`.

    Returns the new coordinates of the line, and the points that are inserted, which are added to `inserted`.
    """
    line_inserted = []
    for point in points:
        new_coords, nearest = _insert(coords, point)
        if (nearest == 0 and coords[0] in inserted) or (nearest == len(coords) - 1 and coords[-1] in inserted):
            continue
        coords = new_coords
        inserted.add(point)
        line_inserted.append(point)
    return coords, line_inserted


def _split_lines(lines: gpd.GeoDataFrame, to_insert: dict[int, list[Coordinate]]) -> gpd.GeoDataFrame:
    """The pieces of the lines in `to_insert` between the ends that are inserted in them, see `split_at_junctions`."""
    coords = [list(line.coords) for line in lines.geometry]
    code = lines["code"].to_numpy(dtype=object)
    has_code = lines["code"].notna().to_numpy()
    inserted: set[Coordinate] = set()
    rows, codes, geometries = [], [], []
    for row in sorted(to_insert, key=lambda row: (not has_code[row], code[row] if has_code[row] else row)):
        line_coords, line_inserted = _insert_ends(coords[row], to_insert[row], inserted)
        cuts = sorted({0, len(line_coords) - 1, *(line_coords.index(point) for point in line_inserted)})
        for n, (first, last) in enumerate(itertools.pairwise(cuts)):
            rows.append(row)
            codes.append(f"{code[row]}-{n}" if has_code[row] else None)
            geometries.append(LineString(line_coords[first : last + 1]))
    pieces = lines.iloc[rows].copy()
    pieces["code"] = codes
    return pieces.set_geometry(gpd.GeoSeries(geometries, index=pieces.index, crs=lines.crs))


def _remove_short_pieces(
    lines: gpd.GeoDataFrame, is_piece: np.ndarray, length: float = 0.5
) -> tuple[gpd.GeoDataFrame, np.ndarray]:
    """Remove the pieces of at most `length` m where another line starts where they start, or ends where they end."""
    _, start, end = _ends(lines)
    shared = pd.Series(start, dtype=object).duplicated(keep=False) | pd.Series(end, dtype=object).duplicated(keep=False)
    short = is_piece & (lines.length.to_numpy() <= length) & shared.to_numpy()
    return lines[~short].reset_index(drop=True), is_piece[~short]


def split_at_junctions(lines: gpd.GeoDataFrame, buffer_distance: float = 0.5) -> gpd.GeoDataFrame:
    """Split the lines where another line ends on them within `buffer_distance`, so that they connect there.

    A port of `connect_endpoints_by_buffer` of the preprocessing of 2024, by Sweco, so that the pieces get the codes
    they got then, by which scripts refer to nodes on them, see `ribasim_nl.keys.lumped_keys`:

    - An end at which no line starts, or no line ends, is connected to each line within `buffer_distance` of which it
      is not an end. The end is inserted in that line, next to its nearest vertex. A line is extended to an end
      beyond it.
    - Such a line is replaced by its pieces between the inserted ends, with the codes `<code>-<n>`, from 0 along the
      line, also if it is only extended, or left as it is since the end was not inserted. The pieces follow the lines
      that are not split, in order of their code. The pieces of a line without code have none, and come last.
    - A piece of at most 0.5 m is removed if another line starts where it starts, or ends where it ends.
    - This repeats, for the ends that were not inserted since they were nearest to an end that was itself inserted,
      until no end is left to connect, or as many as before.
    """
    assert lines["code"].dropna().is_unique, "the pieces of a line are named after its code"
    lines = lines.reset_index(drop=True)
    is_piece = np.zeros(len(lines), dtype=bool)
    previous = 0
    while True:
        to_insert = _ends_to_insert(lines, buffer_distance)
        n_unconnected = len({point for points in to_insert.values() for point in points})
        logger.info(f"{n_unconnected} ends of lines to connect to a line within {buffer_distance} m")
        if n_unconnected in (0, previous):
            return lines
        previous = n_unconnected

        pieces = _split_lines(lines, to_insert)
        keep = ~np.isin(np.arange(len(lines)), list(to_insert))
        is_piece = np.concatenate([is_piece[keep], np.ones(len(pieces), dtype=bool)])
        lines = gpd.GeoDataFrame(pd.concat([lines[keep], pieces], ignore_index=True), geometry="geometry")
        lines, is_piece = _remove_short_pieces(lines, is_piece)


def connect_lines(lines: gpd.GeoDataFrame, distance: float = 0.5) -> gpd.GeoDataFrame:
    """Connect the lines that end within `distance` of each other, and remove their Z coordinates.

    Ends within `distance` of each other get the same coordinate, see `snap_ends`, and a line is split where another
    one ends on it within that distance, see `split_at_junctions`.
    """
    lines = lines.set_geometry(lines.geometry.force_2d())
    return split_at_junctions(snap_ends(lines, distance), distance)
