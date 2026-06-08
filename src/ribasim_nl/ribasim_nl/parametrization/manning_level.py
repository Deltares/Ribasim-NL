from collections import defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import geopandas as gpd
import pandas as pd

from ribasim_nl import Model
from ribasim_nl.coupling_level_common import (
    CONTROL_NODE_TYPES,
    LEVEL_UPDATE_PROTECTION_COLUMN,
    as_float,
    as_int,
    reset_index_to_column,
)

OPEN_NODE_TYPES = {"Basin", "ManningResistance", "Junction"}


def _required_df(df: pd.DataFrame | None, table_name: str) -> pd.DataFrame:
    """Return a required Ribasim table or fail with a clear error."""
    if df is None:
        raise ValueError(f"{table_name} ontbreekt in het model.")
    return df


def _node_df(model: Model) -> pd.DataFrame:
    """Return Node table."""
    return _required_df(model.node.df, "Node")


def _link_df(model: Model) -> pd.DataFrame:
    """Return Link table."""
    return _required_df(model.link.df, "Link")


def _basin_area_df(model: Model) -> pd.DataFrame:
    """Return Basin / area table."""
    return _required_df(model.basin.area.df, "Basin / area")


def _node_type_by_id(model: Model) -> dict[int, str]:
    """Return node_type by node_id for quick lookups."""
    node_df = _node_df(model)
    return {as_int(node_id): str(node_type) for node_id, node_type in node_df["node_type"].to_dict().items()}


def _flow_link_df(model: Model) -> pd.DataFrame:
    """Return only flow links with link_id available as a column."""
    link_df = reset_index_to_column(_link_df(model).copy(), "link_id")

    if "link_type" in link_df.columns:
        link_df = link_df[link_df["link_type"].fillna("flow").eq("flow")]

    return link_df


def _undirected_open_adjacency(
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> dict[int, list[int]]:
    """Build an undirected graph over Basin/ManningResistance/Junction nodes."""
    adjacency: dict[int, list[int]] = defaultdict(list)
    for row in link_df.itertuples(index=False):
        from_node_id = as_int(row.from_node_id)
        to_node_id = as_int(row.to_node_id)
        if node_type_by_id.get(from_node_id) not in OPEN_NODE_TYPES:
            continue
        if node_type_by_id.get(to_node_id) not in OPEN_NODE_TYPES:
            continue

        adjacency[from_node_id].append(to_node_id)
        adjacency[to_node_id].append(from_node_id)

    return dict(adjacency)


def _open_component(start_node_id: int, adjacency: dict[int, list[int]], max_iter: int) -> set[int]:
    """Walk a connected open component from one Basin/Manning/Junction node."""
    component = {int(start_node_id)}
    queue: deque[int] = deque([int(start_node_id)])

    while queue and len(component) < max_iter:
        node_id = queue.popleft()
        for next_node_id in adjacency.get(node_id, []):
            if next_node_id in component:
                continue
            component.add(next_node_id)
            queue.append(next_node_id)

    if queue:
        raise ValueError(f"Open Manning-component vanaf node {start_node_id} groter dan {max_iter} nodes.")

    return component


def _component_boundary_control_node_ids(
    component_node_ids: set[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> list[int]:
    """Find Outlet/Pump nodes directly connected to an open component."""
    control_node_ids: set[int] = set()
    for row in link_df.itertuples(index=False):
        from_node_id = as_int(row.from_node_id)
        to_node_id = as_int(row.to_node_id)
        if from_node_id in component_node_ids and node_type_by_id.get(to_node_id) in CONTROL_NODE_TYPES:
            control_node_ids.add(to_node_id)
        if to_node_id in component_node_ids and node_type_by_id.get(from_node_id) in CONTROL_NODE_TYPES:
            control_node_ids.add(from_node_id)
    return sorted(control_node_ids)


def _terminal_manning_branch_basin_node_ids(
    component_node_ids: set[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
) -> set[int]:
    """Return terminal basins that should not be leveled through a Manning route."""
    protected_basin_node_ids: set[int] = set()
    component_node_ids = {int(node_id) for node_id in component_node_ids}

    for basin_node_id in component_node_ids:
        if node_type_by_id.get(basin_node_id) != "Basin":
            continue

        open_neighbor_ids: list[int] = []
        non_open_neighbor_types: set[str | None] = set()
        for row in link_df[
            link_df["from_node_id"].eq(basin_node_id) | link_df["to_node_id"].eq(basin_node_id)
        ].itertuples(index=False):
            from_node_id = as_int(row.from_node_id)
            to_node_id = as_int(row.to_node_id)
            other_node_id = to_node_id if from_node_id == basin_node_id else from_node_id

            if other_node_id in component_node_ids and node_type_by_id.get(other_node_id) in OPEN_NODE_TYPES:
                open_neighbor_ids.append(other_node_id)
            else:
                non_open_neighbor_types.add(node_type_by_id.get(other_node_id))

        if len(open_neighbor_ids) != 1:
            continue
        if node_type_by_id.get(open_neighbor_ids[0]) != "ManningResistance":
            continue
        if non_open_neighbor_types - {"TabulatedRatingCurve"}:
            continue

        protected_basin_node_ids.add(basin_node_id)

    return protected_basin_node_ids


def _target_level_by_basin_id(model: Model, target_level_column: str) -> dict[int, float]:
    """Read target level by Basin node_id from Basin / area."""
    area_df = _basin_area_df(model)
    if target_level_column not in area_df.columns:
        raise KeyError(f"Kolom {target_level_column!r} ontbreekt in Basin / area.")

    level_by_basin_id = (
        area_df[["node_id", target_level_column]]
        .dropna(subset=["node_id", target_level_column])
        .drop_duplicates(subset=["node_id"], keep="last")
        .set_index("node_id")[target_level_column]
        .astype(float)
        .to_dict()
    )
    return {as_int(node_id): as_float(level) for node_id, level in level_by_basin_id.items()}


def _truthy_series(series: pd.Series) -> pd.Series:
    """Convert bool-like Series values to a boolean mask."""
    if series.empty:
        return pd.Series(dtype=bool)
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    if pd.api.types.is_numeric_dtype(series):
        return pd.to_numeric(series, errors="coerce").fillna(0).ne(0)
    return series.astype("string").str.lower().isin({"1", "true", "yes", "ja", "y"})


def _node_ids_of_type_in_geometry(
    model: Model,
    *,
    node_type: str,
    geometry_df,
    excluded_node_ids: set[int] | None = None,
) -> list[int]:
    """Select model nodes of one type inside a geometry layer."""
    excluded_node_ids = {int(node_id) for node_id in excluded_node_ids or set()}
    node_table = _node_df(model)
    node_df = reset_index_to_column(node_table.copy(), "node_id")

    geometry_column = getattr(getattr(node_df, "geometry", None), "name", "geometry")
    if geometry_column not in node_df.columns:
        raise ValueError("Kan Manning-nodes niet ruimtelijk selecteren: model.node.df heeft geen geometry-kolom.")

    crs = getattr(node_table, "crs", None)
    node_gdf = gpd.GeoDataFrame(node_df, geometry=geometry_column, crs=crs)
    if getattr(geometry_df, "crs", None) is not None and crs is not None and geometry_df.crs != crs:
        geometry_df = geometry_df.to_crs(crs)

    geometry_union = geometry_df.geometry.union_all()
    selected_gdf = node_gdf[node_gdf["node_type"].eq(node_type) & node_gdf.geometry.within(geometry_union)].copy()
    selected_node_ids = selected_gdf["node_id"].dropna().astype(int).to_list()
    return sorted(node_id for node_id in selected_node_ids if node_id not in excluded_node_ids)


def _nearest_upstream_basin_ids_for_control(
    control_node_id: int,
    *,
    component_node_ids: set[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
    max_iter: int,
) -> list[int]:
    """Find the nearest upstream basins for a boundary control node."""
    incoming: dict[int, list[int]] = defaultdict(list)
    for row in link_df.itertuples(index=False):
        from_node_id = as_int(row.from_node_id)
        to_node_id = as_int(row.to_node_id)
        if from_node_id in component_node_ids or to_node_id == control_node_id:
            incoming[to_node_id].append(from_node_id)

    starts = [
        node_id
        for node_id in incoming.get(control_node_id, [])
        if node_id in component_node_ids and node_type_by_id.get(node_id) in OPEN_NODE_TYPES
    ]
    if not starts:
        return []

    queue: deque[tuple[int, int]] = deque((node_id, 0) for node_id in starts)
    seen_node_ids = set(starts)
    basin_ids_by_distance: dict[int, list[int]] = defaultdict(list)

    while queue and len(seen_node_ids) < max_iter:
        current_node_id, distance = queue.popleft()
        if node_type_by_id.get(current_node_id) == "Basin":
            basin_ids_by_distance[distance].append(current_node_id)
            continue

        for next_node_id in incoming.get(current_node_id, []):
            if next_node_id not in component_node_ids or next_node_id in seen_node_ids:
                continue
            if node_type_by_id.get(next_node_id) not in OPEN_NODE_TYPES:
                continue

            seen_node_ids.add(next_node_id)
            queue.append((next_node_id, distance + 1))

    if not basin_ids_by_distance:
        return []

    nearest_distance = min(basin_ids_by_distance)
    return sorted(set(basin_ids_by_distance[nearest_distance]))


def _dominant_downstream_parameterized_target_level(
    *,
    component_node_ids: set[int],
    boundary_control_node_ids: list[int],
    link_df: pd.DataFrame,
    node_type_by_id: dict[int, str],
    target_level_by_basin_id: dict[int, float],
    tolerance: float,
    max_iter: int,
) -> tuple[float | None, str, list[int], list[int]]:
    """Choose the downstream control level that dominates a Manning component."""
    outgoing: dict[int, list[int]] = defaultdict(list)
    for row in link_df.itertuples(index=False):
        outgoing[as_int(row.from_node_id)].append(as_int(row.to_node_id))

    boundary_control_node_id_set = set(boundary_control_node_ids)
    basin_route_count_by_control_id: dict[int, int] = defaultdict(int)
    open_route_count_by_control_id: dict[int, int] = defaultdict(int)
    for start_node_id in component_node_ids:
        queue: deque[int] = deque([int(start_node_id)])
        seen_node_ids = {int(start_node_id)}
        reached_control_node_ids: set[int] = set()

        while queue and len(seen_node_ids) < max_iter:
            current_node_id = queue.popleft()
            for next_node_id in outgoing.get(current_node_id, []):
                if next_node_id in boundary_control_node_id_set:
                    reached_control_node_ids.add(next_node_id)
                    continue
                if next_node_id in component_node_ids and next_node_id not in seen_node_ids:
                    seen_node_ids.add(next_node_id)
                    queue.append(next_node_id)

        for control_node_id in reached_control_node_ids:
            open_route_count_by_control_id[control_node_id] += 1
            if node_type_by_id.get(start_node_id) == "Basin":
                basin_route_count_by_control_id[control_node_id] += 1

    rows: list[dict[str, object]] = []
    for control_node_id in boundary_control_node_ids:
        if open_route_count_by_control_id[control_node_id] == 0:
            continue

        source_basin_ids = _nearest_upstream_basin_ids_for_control(
            control_node_id=control_node_id,
            component_node_ids=component_node_ids,
            link_df=link_df,
            node_type_by_id=node_type_by_id,
            max_iter=max_iter,
        )
        source_levels = [
            float(target_level_by_basin_id[basin_id])
            for basin_id in source_basin_ids
            if basin_id in target_level_by_basin_id and pd.notna(target_level_by_basin_id[basin_id])
        ]
        if not source_levels:
            continue

        rows.append(
            {
                "node_id": int(control_node_id),
                "basin_route_count": basin_route_count_by_control_id[control_node_id],
                "open_route_count": open_route_count_by_control_id[control_node_id],
                "level": min(source_levels),
                "source_basin_ids": source_basin_ids,
            }
        )

    if not rows:
        return None, "geen_dominante_downstream_control_met_basinpeil", [], []

    rows_df = pd.DataFrame(rows)
    max_open_route_count = rows_df["open_route_count"].max()
    max_basin_route_count = rows_df.loc[rows_df["open_route_count"].eq(max_open_route_count), "basin_route_count"].max()
    dominant_rows_df = rows_df[
        rows_df["open_route_count"].eq(max_open_route_count) & rows_df["basin_route_count"].eq(max_basin_route_count)
    ].copy()
    level_spread = dominant_rows_df["level"].max() - dominant_rows_df["level"].min()
    if level_spread > tolerance:
        target_level = dominant_rows_df["level"].min()
        dominant_rows_df = dominant_rows_df[(dominant_rows_df["level"] - target_level).abs().le(tolerance)].copy()
        status = "dominante_downstream_control_gelijke_score_laagste_basinpeil"
    else:
        target_level = float(dominant_rows_df["level"].iloc[0])
        status = "dominante_downstream_control_basinpeil"

    source_basin_ids = sorted(
        {
            int(basin_id)
            for basin_ids in dominant_rows_df["source_basin_ids"].to_list()
            for basin_id in basin_ids
            if int(basin_id) in target_level_by_basin_id
            and abs(float(target_level_by_basin_id[int(basin_id)]) - float(target_level)) <= tolerance
        }
    )
    return (
        float(target_level),
        status,
        sorted(dominant_rows_df["node_id"].astype(int).to_list()),
        source_basin_ids,
    )


def _node_geometry_df(model: Model) -> pd.DataFrame:
    """Return node ids and geometries for optional update GPKGs."""
    node_df = reset_index_to_column(_node_df(model).copy(), "node_id")

    geometry_column = getattr(getattr(node_df, "geometry", None), "name", "geometry")
    if geometry_column not in node_df.columns:
        raise ValueError("Kan geen GPKG schrijven: model.node.df heeft geen geometry-kolom.")

    return node_df[["node_id", geometry_column]].reset_index(drop=True)


def _write_basin_updates_gpkg(
    model: Model,
    updates_df: pd.DataFrame,
    output_gpkg: str | Path,
    *,
    layer: str = "basin_level_updates",
) -> Path | None:
    """Write changed basin levels to a GPKG when an output path is provided."""
    basin_updates_df = updates_df[updates_df["status"].eq("update") & updates_df["basin_node_id"].notna()].copy()
    if basin_updates_df.empty:
        return None

    node_geometry_df = _node_geometry_df(model=model)
    geometry_column = getattr(getattr(node_geometry_df, "geometry", None), "name", "geometry")
    crs = getattr(model.node.df, "crs", None)

    basin_updates_df["basin_node_id"] = basin_updates_df["basin_node_id"].astype(int)
    basin_updates_gdf = basin_updates_df.merge(
        node_geometry_df,
        left_on="basin_node_id",
        right_on="node_id",
        how="left",
        suffixes=("", "_geometry"),
    ).drop(columns=["node_id"], errors="ignore")
    basin_updates_gdf = gpd.GeoDataFrame(basin_updates_gdf, geometry=geometry_column, crs=crs)
    basin_updates_gdf = basin_updates_gdf[basin_updates_gdf.geometry.notna()].copy()
    if basin_updates_gdf.empty:
        return None

    output_gpkg = Path(output_gpkg)
    output_gpkg.parent.mkdir(parents=True, exist_ok=True)
    if output_gpkg.exists():
        try:
            output_gpkg.unlink()
        except PermissionError:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_gpkg = output_gpkg.with_name(f"{output_gpkg.stem}_{timestamp}{output_gpkg.suffix}")
    basin_updates_gdf.to_file(output_gpkg, layer=layer, driver="GPKG")
    return output_gpkg


def sync_parameterized_manning_basin_levels(
    model: Model,
    *,
    aanvoergebieden_df=None,
    target_level_column: str = "meta_streefpeil",
    output_gpkg: str | Path | None = None,
    excluded_manning_node_ids: list[int] | set[int] | None = None,
    protected_basin_node_ids: list[int] | set[int] | None = None,
    apply: bool = True,
    update_profile: bool = True,
    update_state: bool = True,
    tolerance: float = 1e-6,
    verbose: bool = True,
    max_iter: int = 500,
) -> pd.DataFrame:
    """Synchroniseer basin-peilen in parameterisatie via gesloten Manning-componenten.

    Deze routine raakt alleen Basin / area, Basin / state en Basin / profile. Outlet,
    Pump en DiscreteControl-tabellen worden niet aangepast. Alleen ManningResistance-nodes
    binnen `aanvoergebieden_df` worden gebruikt als startpunt; het hele open component
    rond zo'n Manning-node wordt daarna gesloten op het dominante benedenstroomse basinpeil.
    Eindbasins met precies een ManningResistance als enige open buur en hooguit
    TabulatedRatingCurve als overige directe verbinding worden beschermd.
    """
    if aanvoergebieden_df is None:
        raise ValueError("aanvoergebieden_df is verplicht voor parameterisatie-Manning-sync.")

    node_type_by_id = _node_type_by_id(model)
    link_df = _flow_link_df(model)
    open_adjacency = _undirected_open_adjacency(link_df=link_df, node_type_by_id=node_type_by_id)
    target_level_by_basin_id = _target_level_by_basin_id(model, target_level_column)
    excluded_manning_node_ids = {int(node_id) for node_id in excluded_manning_node_ids or set()}
    protected_basin_node_ids = {int(node_id) for node_id in protected_basin_node_ids or set()}

    area_df = _basin_area_df(model)
    if LEVEL_UPDATE_PROTECTION_COLUMN in area_df.columns:
        protected_basin_node_ids.update(
            area_df.loc[
                _truthy_series(area_df[LEVEL_UPDATE_PROTECTION_COLUMN]),
                "node_id",
            ]
            .dropna()
            .astype(int)
            .to_list()
        )

    start_manning_node_ids = _node_ids_of_type_in_geometry(
        model=model,
        node_type="ManningResistance",
        geometry_df=aanvoergebieden_df,
        excluded_node_ids=excluded_manning_node_ids,
    )

    records: list[dict[str, object]] = []
    component_by_key: dict[tuple[int, ...], set[int]] = {}
    selected_manning_by_component_key: dict[tuple[int, ...], set[int]] = defaultdict(set)
    for manning_node_id in start_manning_node_ids:
        if node_type_by_id.get(int(manning_node_id)) != "ManningResistance":
            continue

        component_node_ids = _open_component(
            start_node_id=int(manning_node_id),
            adjacency=open_adjacency,
            max_iter=max_iter,
        )
        component_key = tuple(sorted(component_node_ids))
        component_by_key[component_key] = component_node_ids
        selected_manning_by_component_key[component_key].add(int(manning_node_id))

    for component_id, component_key in enumerate(sorted(component_by_key), start=1):
        component_node_ids = component_by_key[component_key]
        basin_ids = sorted(node_id for node_id in component_node_ids if node_type_by_id.get(node_id) == "Basin")
        component_manning_node_ids = sorted(
            node_id for node_id in component_node_ids if node_type_by_id.get(node_id) == "ManningResistance"
        )
        selected_manning_node_ids = sorted(selected_manning_by_component_key[component_key])
        boundary_control_node_ids = _component_boundary_control_node_ids(
            component_node_ids=component_node_ids,
            link_df=link_df,
            node_type_by_id=node_type_by_id,
        )

        if not basin_ids:
            records.append(
                {
                    "component_id": component_id,
                    "selected_manning_node_ids": ",".join(map(str, selected_manning_node_ids)),
                    "manning_node_ids": ",".join(map(str, component_manning_node_ids)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "status": "geen_basin_in_manning_component",
                }
            )
            continue

        target_level, target_level_status, target_control_node_ids, target_basin_ids = (
            _dominant_downstream_parameterized_target_level(
                component_node_ids=component_node_ids,
                boundary_control_node_ids=boundary_control_node_ids,
                link_df=link_df,
                node_type_by_id=node_type_by_id,
                target_level_by_basin_id=target_level_by_basin_id,
                tolerance=tolerance,
                max_iter=max_iter,
            )
        )
        if target_level is None:
            records.append(
                {
                    "component_id": component_id,
                    "selected_manning_node_ids": ",".join(map(str, selected_manning_node_ids)),
                    "manning_node_ids": ",".join(map(str, component_manning_node_ids)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "target_level_basis": target_level_status,
                    "status": "target_level_ontbreekt",
                }
            )
            continue

        terminal_manning_branch_basin_node_ids = _terminal_manning_branch_basin_node_ids(
            component_node_ids=component_node_ids,
            link_df=link_df,
            node_type_by_id=node_type_by_id,
        )
        changed_any_basin = False
        for basin_id in basin_ids:
            old_level = target_level_by_basin_id.get(int(basin_id))
            if int(basin_id) in protected_basin_node_ids:
                status = "handmatig_peil_behouden"
                changed_any_basin = True
            elif int(basin_id) in terminal_manning_branch_basin_node_ids:
                status = "manning_eindbasin_behouden"
                changed_any_basin = True
            elif pd.notna(old_level) and abs(float(old_level) - float(target_level)) <= tolerance:
                status = "ongewijzigd"
            else:
                status = "update"
                changed_any_basin = True

            records.append(
                {
                    "component_id": component_id,
                    "selected_manning_node_ids": ",".join(map(str, selected_manning_node_ids)),
                    "manning_node_ids": ",".join(map(str, component_manning_node_ids)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "target_control_node_ids": ",".join(map(str, target_control_node_ids)),
                    "target_basin_node_ids": ",".join(map(str, target_basin_ids)),
                    "target_level_basis": target_level_status,
                    "basin_node_id": int(basin_id),
                    "old_level": old_level,
                    "new_level": target_level,
                    "status": status,
                }
            )

        if not changed_any_basin:
            records.append(
                {
                    "component_id": component_id,
                    "selected_manning_node_ids": ",".join(map(str, selected_manning_node_ids)),
                    "manning_node_ids": ",".join(map(str, component_manning_node_ids)),
                    "boundary_control_node_ids": ",".join(map(str, boundary_control_node_ids)),
                    "target_control_node_ids": ",".join(map(str, target_control_node_ids)),
                    "target_basin_node_ids": ",".join(map(str, target_basin_ids)),
                    "target_level_basis": target_level_status,
                    "status": "geen_level_afwijking",
                }
            )

    updates_df = pd.DataFrame(records)
    if updates_df.empty or "basin_node_id" not in updates_df.columns:
        return updates_df

    update_rows = updates_df[updates_df["status"].eq("update")].copy()
    conflicting = update_rows.groupby("basin_node_id")["new_level"].nunique().loc[lambda series: series.gt(1)]
    if not conflicting.empty:
        raise ValueError(f"Tegenstrijdige parameterisatie-Manning peilen voor basins: {conflicting.index.to_list()}")

    update_rows = update_rows.drop_duplicates(subset=["basin_node_id"], keep="last")
    profile_shift_by_basin_id: dict[int, float] = {}
    if apply and not update_rows.empty:
        profile_df = model.basin.profile.df
        if update_profile and profile_df is not None:
            for row in update_rows.itertuples(index=False):
                if pd.isna(row.old_level) or pd.isna(row.new_level):
                    continue
                basin_id = as_int(row.basin_node_id)
                profile_mask = profile_df["node_id"].eq(basin_id)
                if not profile_mask.any():
                    continue

                level_shift = as_float(row.new_level) - as_float(row.old_level)
                if abs(level_shift) > tolerance:
                    profile_df.loc[profile_mask, "level"] = (
                        profile_df.loc[profile_mask, "level"].astype(float) + level_shift
                    )
                    profile_shift_by_basin_id[basin_id] = level_shift

        level_by_basin_id = update_rows.set_index("basin_node_id")["new_level"].astype(float).to_dict()
        mask = area_df["node_id"].isin(level_by_basin_id)
        area_df.loc[mask, target_level_column] = area_df.loc[mask, "node_id"].map(level_by_basin_id)
        if update_state:
            state_df = area_df[["node_id", target_level_column]].rename(columns={target_level_column: "level"})
            model.basin.state.df = cast(Any, state_df)

    if profile_shift_by_basin_id:
        basin_row_mask = updates_df["basin_node_id"].notna()
        updates_df.loc[basin_row_mask, "profile_level_shift"] = updates_df.loc[basin_row_mask, "basin_node_id"].map(
            profile_shift_by_basin_id
        )

    written_output_gpkg = None
    if output_gpkg is not None:
        written_output_gpkg = _write_basin_updates_gpkg(
            model=model,
            updates_df=updates_df,
            output_gpkg=output_gpkg,
            layer="parameterized_manning_basin_level_updates",
        )

    if verbose:
        print("Parameterisatie Manning-route basin level updates:", update_rows["basin_node_id"].nunique())
        if profile_shift_by_basin_id:
            print("Parameterisatie Manning-route basin profielen verschoven:", len(profile_shift_by_basin_id))
        if output_gpkg is not None:
            if written_output_gpkg is None:
                print("Parameterisatie Manning-route GPKG niet geschreven: geen basin-updates met geometrie.")
            else:
                print(f"Parameterisatie Manning-route GPKG geschreven: {written_output_gpkg}")

    return updates_df
