"""Trial: aggregation areas between the delivered ones, such as polders, and the peilgebieden.

A level controlled input model has one Basin per aggregation area. The delivered aggregation areas hide the target
levels within them, while one Basin per peilgebied gives many Basins that are too small to model. `refine_aggregation_areas`
keeps the peilgebieden of at least a minimum area, and joins the smaller ones with a neighbour in their aggregation
area. An area is named after its largest peilgebied and gets its target level, so the water board recognizes it.
"""

import heapq

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely

from ribasim_nl.geometry import union_polygons

# The authorities whose aggregation areas are refined: those of which the delivered aggregation areas, such as polders,
# hold several peilgebieden. The other level controlled authorities have an aggregation area per peilgebied.
REFINED = frozenset(
    {"Delfland", "HollandsNoorderkwartier", "Rijnland", "Rivierenland", "Scheldestromen", "WetterskipFryslan"}
)
# the area [m2] from which a peilgebied keeps its own Basin; 0 gives one Basin per peilgebied
MIN_AREA = 500_000.0
# The first ID of the nodes that the input model of 2024 lacks. IDs right after those of 2024 are the ones that the
# feedback stage gave to the nodes it adds, to which scripts refer.
FIRST_NEW_NODE_ID = 10_000

# the categories of the peilgebieden that are aggregated: regular peilgebieden and boezems
AGGREGATED_CATEGORIES = (0, 1)


def aggregation_area_of(
    peilgebied: gpd.GeoDataFrame, areas: gpd.GeoDataFrame, column: str, threshold: float
) -> pd.Series:
    """The aggregation area of each peilgebied: the one it overlaps most, if they together cover enough of it.

    :param peilgebied: the peilgebieden, with the column peilgebied_cat
    :param areas: the aggregation areas
    :param column: the column with the code of the aggregation areas
    :param threshold: the part of a peilgebied that aggregation areas must cover for it to be aggregated
    :return: the code of the aggregation area by the index of the peilgebieden, None for those that are not aggregated
    """
    # None, not NaN, for the peilgebieden without: the code of 2024 compares them
    result: list[str | None] = [None] * len(peilgebied)
    codes = areas[column].to_numpy()
    area_polygons = areas.geometry.to_numpy()
    categories = peilgebied["peilgebied_cat"].to_numpy()
    for i, (category, polygon) in enumerate(zip(categories, peilgebied.geometry.to_numpy(), strict=True)):
        if category not in AGGREGATED_CATEGORIES:
            continue
        positions = areas.sindex.query(polygon, predicate="intersects")
        overlap = shapely.area(shapely.intersection(area_polygons[positions], polygon)) / polygon.area
        if overlap.sum() >= threshold:
            # the first of equal overlaps
            result[i] = codes[positions[np.argmax(overlap)]]
    return pd.Series(result, index=peilgebied.index, dtype=object)


def shared_boundaries(polygons: np.ndarray, tolerance: float = 0.5, min_length: float = 5.0) -> pd.DataFrame:
    """The length over which each pair of polygons shares a boundary.

    Polygons share the part of the boundary of the first that is within `tolerance` of the second, since the
    boundaries of delivered peilgebieden do not coincide exactly. That is up to twice `tolerance` longer than the
    boundary they share, and corners that touch share that much, so `min_length` must be longer.

    :return: the positions a < b of the polygons of each pair that shares at least `min_length`, and that length
    """
    a, b = shapely.STRtree(polygons).query(polygons, predicate="dwithin", distance=tolerance)
    keep = a < b
    a, b = a[keep], b[keep]
    near = shapely.intersection(shapely.boundary(polygons[a]), shapely.buffer(polygons[b], tolerance))
    pairs = pd.DataFrame({"a": a, "b": b, "length": shapely.length(near)})
    pairs = pairs.loc[pairs["length"] >= min_length]
    return pairs.sort_values(["a", "b"], kind="stable").reset_index(drop=True)


def merge_small(
    area: np.ndarray,
    level: np.ndarray,
    within: np.ndarray,
    shared: pd.DataFrame,
    min_area: float,
    min_shared: float = 10.0,
) -> np.ndarray:
    """Join the polygons smaller than `min_area` with a neighbour, smallest first, until all groups are large enough.

    A group joins the neighbouring group with the closest level, among those with which it shares at least
    `min_shared` of boundary, or else among all its neighbours. Of equal levels, and where a level is missing, the
    longest shared boundary decides. A joined group has the level of the larger of the two, or the one that is known.
    A group without a level joins a neighbour with a level whatever its size, so that every group has a level where its
    label has one. A group without a neighbour stays as it is.

    :param area: the area of each polygon
    :param level: the target level of each polygon, NaN if unknown
    :param within: a label per polygon; only polygons with the same label are joined
    :param shared: the result of `shared_boundaries`
    :param min_area: the area from which a group is left as it is
    :return: per polygon, the position of the polygon that its group started from
    """
    group_area = area.astype(float).copy()
    group_level = level.astype(float).copy()
    parent = np.arange(len(area))
    neighbours: list[dict[int, float]] = [{} for _ in area]
    for a, b, length in zip(shared["a"], shared["b"], shared["length"], strict=True):
        if within[a] == within[b]:
            neighbours[a][b] = neighbours[a].get(b, 0.0) + length
            neighbours[b][a] = neighbours[b].get(a, 0.0) + length

    def root(i: int) -> int:
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = int(parent[i])
        return i

    def join(group: int, target: int) -> None:
        """Join a group with another one, which takes over its neighbours, and its level if it is the larger."""
        if np.isnan(group_level[target]) or (
            group_area[group] > group_area[target] and not np.isnan(group_level[group])
        ):
            group_level[target] = group_level[group]
        parent[group] = target
        group_area[target] += group_area[group]
        for n, length in neighbours[group].items():
            del neighbours[n][group]
            if n != target:
                neighbours[n][target] = neighbours[n].get(target, 0.0) + length
                neighbours[target][n] = neighbours[target].get(n, 0.0) + length
        neighbours[group] = {}

    def too_small(group: int) -> bool:
        return group_area[group] < min_area or bool(np.isnan(group_level[group]))

    heap = [(float(group_area[i]), i) for i in range(len(area)) if too_small(i)]
    heapq.heapify(heap)
    while heap:
        heap_area, group = heapq.heappop(heap)
        # skip groups that were joined or have grown since they were queued
        if parent[group] != group or heap_area != group_area[group] or not neighbours[group]:
            continue
        near = {n: length for n, length in neighbours[group].items() if length >= min_shared} or neighbours[group]

        def difference(n: int, group: int = group, near: dict[int, float] = near) -> tuple[bool, float, float, int]:
            unknown = bool(np.isnan(group_level[n]))
            both_known = not (unknown or np.isnan(group_level[group]))
            return (unknown, abs(group_level[group] - group_level[n]) if both_known else np.inf, -near[n], n)

        target = min(near, key=difference)
        join(group, target)
        if too_small(target):
            heapq.heappush(heap, (float(group_area[target]), target))
    return np.array([root(i) for i in range(len(area))])


def refine_aggregation_areas(
    peilgebied: gpd.GeoDataFrame,
    areas: gpd.GeoDataFrame,
    streefpeil: pd.DataFrame,
    *,
    column: str,
    threshold: float,
    min_area: float,
) -> tuple[gpd.GeoDataFrame, pd.Series]:
    """Aggregation areas of at least `min_area`, made of the peilgebieden of the delivered aggregation areas.

    A peilgebied of at least `min_area` is an aggregation area. A smaller one joins a neighbour in its delivered
    aggregation area, see `merge_small`. Peilgebieden that are not aggregated, see `aggregation_area_of`, are an area of
    their own. An area gets the code and the target level of its largest peilgebied.

    :param peilgebied: the peilgebieden, with the columns code, globalid and peilgebied_cat
    :param areas: the delivered aggregation areas
    :param streefpeil: the target levels, with the columns globalid and waterhoogte
    :param column: the column with the code of the delivered aggregation areas
    :param threshold: see `aggregation_area_of`
    :param min_area: the area [m2] from which a peilgebied is an aggregation area; 0 for all peilgebieden
    :return: the aggregation areas, with the columns code, globalid, delivered, streefpeil and geometry, and the code of
        the aggregation area by the index of the peilgebieden, None for those that are not aggregated
    """
    assert min_area >= 0, "the minimum area is negative"
    delivered = aggregation_area_of(peilgebied, areas, column, threshold)
    selected = peilgebied.loc[peilgebied["peilgebied_cat"].isin(AGGREGATED_CATEGORIES)]
    assert selected["code"].notna().all() and selected["code"].is_unique, "the codes of the peilgebieden are not unique"
    polygons = shapely.make_valid(selected.geometry.to_numpy())
    area = shapely.area(polygons)
    levels = streefpeil.drop_duplicates("globalid").set_index("globalid")["waterhoogte"]
    level = selected["globalid"].map(levels).to_numpy(dtype=float)
    # peilgebieden outside the delivered aggregation areas are not joined with others
    own = "peilgebied " + selected["globalid"].astype(str)
    within = delivered.loc[selected.index].where(delivered.loc[selected.index].notna(), own).to_numpy()
    group = merge_small(area, level, within, shared_boundaries(polygons), min_area)

    rows = []
    code = selected["code"].to_numpy()
    area_of = pd.Series([None] * len(peilgebied), index=peilgebied.index, dtype=object)
    for _, members in pd.Series(np.arange(len(selected))).groupby(group, sort=False):
        positions = members.to_numpy()
        # the largest peilgebied names the area; the first of equal areas
        by_area = positions[np.argsort(-area[positions], kind="stable")]
        known = by_area[~np.isnan(level[by_area])]
        rows.append(
            {
                "code": code[by_area[0]],
                "delivered": delivered.loc[selected.index[by_area[0]]],
                "streefpeil": level[known[0]] if len(known) > 0 else np.nan,
                "geometry": union_polygons(polygons[positions]),
                "position": by_area[0],
            }
        )
        area_of.loc[selected.index[positions]] = code[by_area[0]]
    # in the order of their largest peilgebied
    refined = gpd.GeoDataFrame(rows, geometry="geometry", crs=peilgebied.crs)
    refined = refined.sort_values("position", kind="stable").reset_index(drop=True)
    refined["globalid"] = "dummy_globalid_agg_area_" + refined.index.astype(str)
    return refined[["code", "globalid", "delivered", "streefpeil", "geometry"]], area_of
