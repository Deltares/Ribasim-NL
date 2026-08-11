"""Aggregate Ribasim networks and results into visualization zones.

This module groups Basin nodes, contracts internal flow paths, aggregates
time-dependent results, and writes Ribasim-compatible visualization models.
"""

from collections.abc import Hashable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Self, cast

import geopandas as gpd
import numpy as np
import pandas as pd
import ribasim
import shapely
import xarray as xr
import xugrid as xu
from ribasim import __schema_version__
from ribasim.db_utils import _set_db_schema_version
from scipy import sparse
from shapely.geometry import LineString

BOUNDARY_NODE_TYPES = frozenset({"FlowBoundary", "LevelBoundary", "Terminal"})
VERTICAL_FLUX_VARIABLES = (
    "precipitation",
    "evaporation",
    "drainage",
    "infiltration",
    "surface_runoff",
    "urban_runoff",
)
BASIN_ADDITIVE_VARIABLES = (
    "storage",
    "storage_rate",
    "balance_error",
    *VERTICAL_FLUX_VARIABLES,
)
BASIN_RATE_VARIABLES = frozenset((*BASIN_ADDITIVE_VARIABLES, "inflow_rate", "outflow_rate")) - {"storage"}
BASIN_RESULT_ATTRIBUTES = {
    "level": {
        "units": "m",
        "standard_name": "water_surface_height_above_reference_datum",
        "long_name": "water level above reference datum",
    },
    "storage": {
        "units": "m3",
        "standard_name": "surface_water_amount",
        "long_name": "water storage volume",
    },
    "inflow_rate": {
        "units": "m3 s-1",
        "standard_name": "water_volume_transport_in_river_channel",
        "long_name": "water inflow rate",
    },
    "outflow_rate": {
        "units": "m3 s-1",
        "standard_name": "water_volume_transport_in_river_channel",
        "long_name": "water outflow rate",
    },
    "storage_rate": {"units": "m3 s-1", "long_name": "water storage rate of change"},
    "precipitation": {
        "units": "m3 s-1",
        "standard_name": "lwe_precipitation_rate",
        "long_name": "liquid water equivalent precipitation rate",
    },
    "surface_runoff": {
        "units": "m3 s-1",
        "standard_name": "surface_runoff_flux",
        "long_name": "surface runoff flux",
    },
    "evaporation": {
        "units": "m3 s-1",
        "standard_name": "lwe_water_evaporation_rate",
        "long_name": "water evaporation flux",
    },
    "drainage": {"units": "m3 s-1", "long_name": "drainage flux"},
    "infiltration": {"units": "m3 s-1", "long_name": "infiltration flux"},
    "balance_error": {"units": "m3 s-1", "long_name": "water balance error"},
    "relative_error": {"units": "1", "long_name": "relative water balance error"},
    "convergence": {"units": "1", "long_name": "convergence indicator"},
}


@dataclass(frozen=True)
class NodeGroups:
    """Assignment of Basin nodes to spatial groups."""

    basin_to_group: pd.Series
    geometry: gpd.GeoDataFrame | None = None
    node_to_group: pd.Series | None = None

    @classmethod
    def from_ids(cls, model: ribasim.Model, groups: Mapping[Hashable, Sequence[int]]) -> Self:
        """Create groups from explicitly listed Basin node IDs."""
        node_df = model.node.df
        assert node_df is not None
        basin_ids = set(node_df.index[node_df["node_type"] == "Basin"])
        assignments: dict[int, Hashable] = {}
        for group, node_ids in groups.items():
            for node_id in node_ids:
                if node_id not in basin_ids:
                    raise ValueError(f"Node {node_id} is not a Basin node.")
                if node_id in assignments:
                    raise ValueError(f"Basin node {node_id} occurs in more than one group.")
                assignments[node_id] = group
        assignment = pd.Series(assignments, name="group_id", dtype="object")
        return cls(assignment, node_to_group=assignment)

    @classmethod
    def from_node_column(
        cls,
        model: ribasim.Model,
        column: str,
        *,
        exclude: Sequence[Hashable] = (),
    ) -> Self:
        """Group Basin nodes by a column, optionally leaving values ungrouped."""
        node_df = model.node.df
        assert node_df is not None
        if column not in node_df:
            raise KeyError(f"Node table has no column {column!r}.")
        node_values = node_df[column].dropna()
        node_values = node_values[~node_values.isin(exclude)].astype("object")
        node_values.name = "group_id"
        basin_values = node_values[node_values.index.isin(node_df.index[node_df["node_type"] == "Basin"])]
        return cls(basin_values, node_to_group=node_values)

    @classmethod
    def from_polygons(
        cls,
        model: ribasim.Model,
        polygons: gpd.GeoDataFrame,
        group_column: str,
    ) -> Self:
        """Assign Basin node points to polygons.

        A point on a boundary is accepted only when all matching polygons have the
        same group value. Otherwise the assignment is ambiguous and raises.
        """
        if group_column not in polygons:
            raise KeyError(f"Polygon table has no column {group_column!r}.")
        node_df = model.node.df
        assert node_df is not None
        if polygons.crs != node_df.crs:
            raise ValueError("Model nodes and grouping polygons must have the same CRS.")
        if polygons[group_column].isna().any():
            raise ValueError(f"Column {group_column!r} contains missing group values.")

        basin_nodes = node_df.loc[node_df["node_type"] == "Basin"]
        tree = shapely.STRtree(polygons.geometry.to_numpy())
        node_positions, polygon_positions = tree.query(node_df.geometry.to_numpy(), predicate="intersects")
        matches = pd.DataFrame(
            {
                "node_id": node_df.index.to_numpy()[node_positions],
                "group_id": polygons[group_column].to_numpy()[polygon_positions],
            }
        ).drop_duplicates()
        ambiguous = matches.groupby("node_id")["group_id"].nunique()
        ambiguous = ambiguous[(ambiguous > 1) & ambiguous.index.isin(basin_nodes.index)]
        if not ambiguous.empty:
            candidates = matches[matches["node_id"].isin(ambiguous.index)].groupby("node_id")["group_id"].apply(list)
            raise ValueError(f"Basin nodes intersect multiple groups: {candidates.to_dict()}")

        unique_node_ids = matches.groupby("node_id")["group_id"].nunique().loc[lambda values: values == 1].index
        node_assignment = matches[matches["node_id"].isin(unique_node_ids)].drop_duplicates("node_id")
        node_assignment = node_assignment.set_index("node_id")["group_id"].astype("object")
        node_assignment.name = "group_id"
        assignment = node_assignment[node_assignment.index.isin(basin_nodes.index)]
        geometry = polygons[[group_column, "geometry"]].dissolve(by=group_column).sort_index()
        return cls(assignment, geometry, node_assignment)


@dataclass(frozen=True)
class AggregationDiagnostics:
    """Nodes affected or skipped during aggregation."""

    ungrouped_basin_ids: tuple[int, ...]
    contracted_connector_ids: tuple[int, ...]


@dataclass
class AggregatedResults:
    """Spatially aggregated results and their mixed-resolution network."""

    dataset: xu.UgridDataset
    nodes: gpd.GeoDataFrame
    links: gpd.GeoDataFrame
    diagnostics: AggregationDiagnostics
    source_model: ribasim.Model
    basin_areas: gpd.GeoDataFrame | None = None
    budget: pd.DataFrame | None = None

    def write_visualization_model(self, directory: str | Path) -> Path:
        """Write a minimal Ribasim-shaped package for visualization tools."""
        return write_visualization_model(self, directory)


@dataclass(frozen=True)
class _ContractedPath:
    connector_id: int | None
    from_basin_id: int
    to_basin_id: int
    result_link_id: int
    link_ids: tuple[int, ...]


@dataclass(frozen=True)
class _BasinAggregation:
    """Mapping and profile-area weights for aggregated Basin results."""

    source_to_output: pd.Series
    source_area: pd.Series
    output_area: pd.Series

    @classmethod
    def from_model(
        cls,
        model: ribasim.Model,
        nodes: gpd.GeoDataFrame,
        groups: NodeGroups,
    ) -> Self:
        source_to_output = _source_basin_mapping(nodes, groups)
        profile_df = model.basin.profile.df
        if profile_df is None:
            raise ValueError("Basin profile data is required to aggregate results.")
        top_profiles = profile_df.sort_values("level").drop_duplicates("node_id", keep="last")
        source_area = top_profiles.set_index("node_id")["area"].reindex(source_to_output.index)
        invalid = source_area.isna() | (source_area <= 0)
        if invalid.any():
            raise ValueError(
                f"Missing or non-positive top profile area for Basin nodes: {source_area.index[invalid].tolist()}"
            )
        output_area = source_area.groupby(source_to_output).sum().rename("profile_area_m2")
        return cls(source_to_output, source_area, output_area)

    def source_ids_by_output(self, output_node_ids: pd.Index) -> list[tuple[int, ...]]:
        return [
            tuple(int(node_id) for node_id in self.source_to_output.index[self.source_to_output == output_node_id])
            for output_node_id in output_node_ids
        ]


class _NetworkAggregator:
    anchor_types: ClassVar[frozenset[str]] = frozenset({"Basin", *BOUNDARY_NODE_TYPES})

    def __init__(self, model: ribasim.Model, groups: NodeGroups):
        self.model = model
        self.groups = groups
        node_df = model.node.df
        link_df = model.link.df
        assert node_df is not None
        assert link_df is not None
        self.node_df = node_df.copy()
        self.link_df = link_df.loc[link_df["link_type"] == "flow"].copy()

    def absorbed_node_groups(self) -> pd.Series:
        """Assign conservative connector nodes enclosed by one group to that group."""
        assignments = self.groups.basin_to_group.copy()
        endpoints = set(self.link_df["from_node_id"]) | set(self.link_df["to_node_id"])
        candidates = set(
            self.node_df.index[
                self.node_df.index.isin(endpoints) & ~self.node_df["node_type"].isin({*self.anchor_types, "UserDemand"})
            ]
        )
        neighbors: dict[int, set[int]] = {int(node_id): set() for node_id in candidates}
        for from_id, to_id in self.link_df[["from_node_id", "to_node_id"]].to_numpy(dtype=int):
            if from_id in neighbors:
                neighbors[from_id].add(to_id)
            if to_id in neighbors:
                neighbors[to_id].add(from_id)

        remaining = set(candidates)
        while remaining:
            component = {remaining.pop()}
            pending = list(component)
            while pending:
                node_id = pending.pop()
                connected_candidates = (neighbors[node_id] & candidates) - component
                component.update(connected_candidates)
                remaining.difference_update(connected_candidates)
                pending.extend(connected_candidates)

            adjacent_anchors = {
                neighbor_id
                for node_id in component
                for neighbor_id in neighbors[node_id]
                if neighbor_id not in candidates
            }
            has_unassigned_anchor = any(
                neighbor_id not in assignments.index and self.node_df.at[neighbor_id, "node_type"] in self.anchor_types
                for neighbor_id in adjacent_anchors
            )
            adjacent_groups = assignments.reindex(list(adjacent_anchors)).dropna().unique()
            if not has_unassigned_anchor and len(adjacent_groups) == 1:
                assignments = pd.concat([assignments, pd.Series(adjacent_groups[0], index=list(component))])
        return assignments

    def contracted_paths(self) -> list[_ContractedPath]:
        paths: list[_ContractedPath] = []
        grouped_ids = set(self.groups.basin_to_group.index)

        for link_id, link in self.link_df.iterrows():
            if link.from_node_id in grouped_ids and link.to_node_id in grouped_ids:
                integer_link_id = int(cast(Any, link_id))
                paths.append(
                    _ContractedPath(
                        None,
                        int(link.from_node_id),
                        int(link.to_node_id),
                        integer_link_id,
                        (integer_link_id,),
                    )
                )

        connector_ids = self.node_df.index[~self.node_df["node_type"].isin(self.anchor_types)]
        for connector_id in connector_ids:
            incoming = self.link_df[self.link_df["to_node_id"] == connector_id]
            outgoing = self.link_df[self.link_df["from_node_id"] == connector_id]
            if len(incoming) != 1 or len(outgoing) != 1:
                continue
            incoming_link = incoming.iloc[0]
            outgoing_link = outgoing.iloc[0]
            from_id = int(incoming_link.from_node_id)
            to_id = int(outgoing_link.to_node_id)
            if from_id not in grouped_ids or to_id not in grouped_ids:
                continue
            from_group = self.groups.basin_to_group.at[from_id]
            to_group = self.groups.basin_to_group.at[to_id]
            if self.node_df.at[connector_id, "node_type"] == "UserDemand" and from_group != to_group:
                raise ValueError(
                    f"UserDemand node {connector_id} lies between grouped Basins {from_id} and {to_id}; "
                    "this non-conservative path cannot be aggregated."
                )
            paths.append(
                _ContractedPath(
                    int(connector_id),
                    from_id,
                    to_id,
                    int(outgoing.index[0]),
                    (int(incoming.index[0]), int(outgoing.index[0])),
                )
            )
        return paths


def _validate_regular_time(dataset: xu.UgridDataset) -> None:
    dynamic_dataset = cast(Any, dataset)
    if "time" not in dynamic_dataset.coords or dynamic_dataset.sizes["time"] < 3:
        return
    steps = np.diff(dynamic_dataset["time"].to_numpy())
    if not np.all(steps == steps[0]):
        raise ValueError("Temporal aggregation requires regular timesteps.")


def _has_internal_missing_values(variable: xr.DataArray) -> bool:
    values = variable.to_numpy()
    time_axis = variable.get_axis_num("time")
    values = np.moveaxis(values, time_axis, 0).reshape(values.shape[time_axis], -1)
    populated = np.any(~pd.isna(values), axis=0)
    return bool(pd.isna(values[:, populated]).any())


def aggregate_time_results(dataset: xu.UgridDataset, frequency: str) -> xu.UgridDataset:
    """Resample regular result time series to mean rates."""
    _validate_regular_time(dataset)
    dynamic_dataset = cast(Any, dataset)
    time_variables = [variable for variable in dynamic_dataset.data_vars.values() if "time" in variable.dims]
    if any(_has_internal_missing_values(variable) for variable in time_variables):
        raise ValueError("Temporal aggregation does not support missing result values.")
    aggregated = dynamic_dataset.resample(time=frequency).mean()
    if isinstance(aggregated, xr.Dataset):
        aggregated = cast(Any, xu.UgridDataset)(aggregated, dataset.grids)
    return aggregated


def aggregate_seasonal_results(dataset: xu.UgridDataset) -> xu.UgridDataset:
    """Aggregate mean rates into complete April-September and October-March periods."""
    _validate_regular_time(dataset)
    dynamic_dataset = cast(Any, dataset)
    time_variables = [variable for variable in dynamic_dataset.data_vars.values() if "time" in variable.dims]
    if any(_has_internal_missing_values(variable) for variable in time_variables):
        raise ValueError("Temporal aggregation does not support missing result values.")

    times = pd.DatetimeIndex(dynamic_dataset["time"].to_numpy())
    years = times.year - (times.month <= 3)
    start_months = np.where((times.month >= 4) & (times.month <= 9), 4, 10)
    period_starts = pd.DatetimeIndex(
        [pd.Timestamp(year=int(year), month=int(month), day=1) for year, month in zip(years, start_months, strict=True)]
    )
    step = times[1] - times[0]
    complete_starts = {
        start
        for start in period_starts.unique()
        if start >= times[0] and start + pd.DateOffset(months=6) <= times[-1] + step
    }
    if not complete_starts:
        raise ValueError("Seasonal aggregation requires at least one complete six-month period.")
    complete = period_starts.isin(complete_starts)
    grouped = (
        dynamic_dataset.isel(time=np.flatnonzero(complete))
        .assign_coords(period_start=("time", period_starts[complete]))
        .groupby("period_start")
        .mean("time")
    )
    grouped = grouped.rename(period_start="time")
    if isinstance(grouped, xr.Dataset):
        grouped = cast(Any, xu.UgridDataset)(grouped, dataset.grids)
    return grouped


def _move_line_endpoint(line: LineString, point, *, start: bool) -> LineString:
    coordinates = list(line.coords)
    coordinates[0 if start else -1] = point.coords[0]
    return LineString(coordinates)


def _group_nodes(
    node_df: gpd.GeoDataFrame,
    groups: NodeGroups,
    node_to_group: pd.Series,
    removed_node_ids: set[int],
) -> tuple[gpd.GeoDataFrame, dict[Hashable, int], dict[int, int]]:
    grouped = groups.basin_to_group
    next_node_id = int(node_df.index.max()) + 1
    unique_groups = list(pd.unique(grouped))
    group_to_node_id = {group: next_node_id + position for position, group in enumerate(unique_groups)}
    node_to_output_id = {
        int(cast(Any, node_id)): int(group_to_node_id[group]) for node_id, group in node_to_group.items()
    }

    records = []
    for group, node_id in group_to_node_id.items():
        if groups.geometry is not None and group in groups.geometry.index:
            group_geometry = cast(Any, groups.geometry.at[group, "geometry"])
            geometry = group_geometry.representative_point()
        else:
            basin_points = node_df.loc[grouped.index[grouped == group], "geometry"]
            geometry = basin_points.union_all().centroid
        records.append(
            {"node_id": node_id, "name": str(group), "node_type": "Basin", "group_id": group, "geometry": geometry}
        )

    retained = node_df.drop(index=list(removed_node_ids)).copy()
    synthetic = gpd.GeoDataFrame(records, geometry="geometry", crs=node_df.crs).set_index("node_id")
    output = gpd.GeoDataFrame(pd.concat([retained, synthetic]), geometry="geometry", crs=node_df.crs)
    return output, group_to_node_id, node_to_output_id


def _aggregate_boundary_nodes(
    node_df: gpd.GeoDataFrame,
    groups: NodeGroups,
    *,
    aggregate_flow_boundaries: bool,
    aggregate_level_boundaries: bool,
    flow_boundary_filter: Mapping[str, Any] | None,
    level_boundary_filter: Mapping[str, Any] | None,
) -> tuple[gpd.GeoDataFrame, dict[int, int], set[int]]:
    if groups.node_to_group is None:
        return node_df, {}, set()

    configurations = (
        ("FlowBoundary", aggregate_flow_boundaries, flow_boundary_filter),
        ("LevelBoundary", aggregate_level_boundaries, level_boundary_filter),
    )
    next_node_id = int(node_df.index.max()) + 1
    node_to_output_id: dict[int, int] = {}
    removed_node_ids: set[int] = set()
    records: list[dict[str, Any]] = []
    for node_type, enabled, column_filter in configurations:
        if not enabled:
            continue
        mask = node_df["node_type"] == node_type
        for column, value in (column_filter or {}).items():
            if column not in node_df:
                raise KeyError(f"Node table has no boundary filter column {column!r}.")
            mask &= node_df[column] == value
        selected_ids = node_df.index[mask & node_df.index.isin(groups.node_to_group.index)]
        assignments = groups.node_to_group.reindex(selected_ids).dropna()
        for group, boundary_ids in assignments.groupby(assignments).groups.items():
            output_id = next_node_id + len(records)
            source_ids = [int(node_id) for node_id in boundary_ids]
            geometry = node_df.loc[source_ids, "geometry"].union_all().centroid
            records.append(
                {
                    "node_id": output_id,
                    "name": f"{group} {node_type}",
                    "node_type": node_type,
                    "geometry": geometry,
                }
            )
            node_to_output_id.update(dict.fromkeys(source_ids, output_id))
            removed_node_ids.update(source_ids)

    if not records:
        return node_df, {}, set()
    retained = node_df.drop(index=list(removed_node_ids))
    synthetic = gpd.GeoDataFrame(records, geometry="geometry", crs=node_df.crs).set_index("node_id")
    output = gpd.GeoDataFrame(pd.concat([retained, synthetic]), geometry="geometry", crs=node_df.crs)
    return output, node_to_output_id, set(node_to_output_id.values())


def _separate_parallel_group_links(
    links: gpd.GeoDataFrame,
    nodes: gpd.GeoDataFrame,
    group_node_ids: set[int],
) -> None:
    endpoint_pairs: dict[tuple[int, int], list[int]] = {}
    for link_id, link in links.iterrows():
        from_id = int(link.from_node_id)
        to_id = int(link.to_node_id)
        if from_id in group_node_ids and to_id in group_node_ids:
            endpoint_pair = (min(from_id, to_id), max(from_id, to_id))
            endpoint_pairs.setdefault(endpoint_pair, []).append(int(cast(Any, link_id)))

    dynamic_links = cast(Any, links)
    for (first_id, second_id), link_ids in endpoint_pairs.items():
        if len(link_ids) < 2:
            continue
        first = cast(Any, nodes.at[first_id, "geometry"])
        second = cast(Any, nodes.at[second_id, "geometry"])
        delta_x = second.x - first.x
        delta_y = second.y - first.y
        distance = float(np.hypot(delta_x, delta_y))
        if distance == 0:
            continue
        offsets = np.linspace(-0.08 * distance, 0.08 * distance, len(link_ids))
        for link_id, offset in zip(sorted(link_ids), offsets, strict=True):
            link = cast(Any, links.loc[link_id])
            start = cast(Any, nodes.at[int(link.from_node_id), "geometry"])
            end = cast(Any, nodes.at[int(link.to_node_id), "geometry"])
            control = (
                (first.x + second.x) / 2 - delta_y / distance * offset,
                (first.y + second.y) / 2 + delta_x / distance * offset,
            )
            dynamic_links.at[link_id, "geometry"] = LineString((start, control, end))


def _build_output_network(
    aggregator: _NetworkAggregator,
    paths: list[_ContractedPath],
    *,
    aggregate_flow_boundaries: bool,
    aggregate_level_boundaries: bool,
    flow_boundary_filter: Mapping[str, Any] | None,
    level_boundary_filter: Mapping[str, Any] | None,
) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame, list[tuple[int, ...]], AggregationDiagnostics]:
    absorbed_groups = aggregator.absorbed_node_groups()
    absorbed_connector_ids = set(absorbed_groups.index) - set(aggregator.groups.basin_to_group.index)
    grouped_non_flow_ids: set[int] = set()
    if aggregator.groups.node_to_group is not None:
        is_non_flow_node = aggregator.node_df["node_type"].str.contains("Control") | (
            aggregator.node_df["node_type"] == "FlowDemand"
        )
        grouped_non_flow_ids = set(aggregator.node_df.index[is_non_flow_node]) & set(
            aggregator.groups.node_to_group.index
        )
    removed_node_ids = set(aggregator.groups.basin_to_group.index) | absorbed_connector_ids | grouped_non_flow_ids
    node_df, group_to_node_id, node_to_output_id = _group_nodes(
        aggregator.node_df,
        aggregator.groups,
        absorbed_groups,
        removed_node_ids,
    )
    node_df, boundary_node_mapping, aggregated_boundary_ids = _aggregate_boundary_nodes(
        node_df,
        aggregator.groups,
        aggregate_flow_boundaries=aggregate_flow_boundaries,
        aggregate_level_boundaries=aggregate_level_boundaries,
        flow_boundary_filter=flow_boundary_filter,
        level_boundary_filter=level_boundary_filter,
    )
    node_to_output_id.update(boundary_node_mapping)
    removed_link_ids = {link_id for path in paths for link_id in path.link_ids}
    removed_connector_ids = {
        *absorbed_connector_ids,
        *(path.connector_id for path in paths if path.connector_id is not None),
    }
    node_df = node_df.drop(index=list(removed_connector_ids), errors="ignore")

    links = aggregator.link_df.drop(index=list(removed_link_ids)).copy()
    dynamic_links = cast(Any, links)
    dynamic_nodes = cast(Any, node_df)
    source_link_ids: list[tuple[int, ...]] = [(int(link_id),) for link_id in links.index]
    for link_id, link in links.iterrows():
        original_from = int(link.from_node_id)
        original_to = int(link.to_node_id)
        from_id = node_to_output_id.get(original_from, original_from)
        to_id = node_to_output_id.get(original_to, original_to)
        dynamic_links.at[link_id, "from_node_id"] = from_id
        dynamic_links.at[link_id, "to_node_id"] = to_id
        if from_id != original_from:
            dynamic_links.at[link_id, "geometry"] = _move_line_endpoint(
                link.geometry, dynamic_nodes.at[from_id, "geometry"], start=True
            )
        if to_id != original_to:
            dynamic_links.at[link_id, "geometry"] = _move_line_endpoint(
                link.geometry, dynamic_nodes.at[to_id, "geometry"], start=False
            )

    internal_link_ids = set(links.index[links["from_node_id"] == links["to_node_id"]])
    if internal_link_ids:
        keep_positions = [position for position, link_id in enumerate(links.index) if link_id not in internal_link_ids]
        links = links.drop(index=list(internal_link_ids))
        source_link_ids = [source_link_ids[position] for position in keep_positions]

    source_ids_by_link = dict(zip(links.index, source_link_ids, strict=True))
    group_node_ids = set(group_to_node_id.values())
    junction_ids = set(node_df.index[node_df["node_type"] == "Junction"])
    contracted_junction_ids: set[int] = set()
    for junction_id in junction_ids:
        incoming_ids = list(links.index[links["to_node_id"] == junction_id])
        outgoing_ids = list(links.index[links["from_node_id"] == junction_id])
        if len(incoming_ids) != 1 or len(outgoing_ids) != 1:
            continue
        incoming_id = incoming_ids[0]
        outgoing_id = outgoing_ids[0]
        from_id = int(cast(Any, links.at[incoming_id, "from_node_id"]))
        to_id = int(cast(Any, links.at[outgoing_id, "to_node_id"]))
        endpoint_ids = {from_id, to_id}
        if not endpoint_ids.intersection(group_node_ids) or not endpoint_ids.intersection(aggregated_boundary_ids):
            continue
        current_links = cast(Any, links)
        current_links.at[outgoing_id, "from_node_id"] = from_id
        current_links.at[outgoing_id, "geometry"] = LineString(
            (dynamic_nodes.at[from_id, "geometry"], dynamic_nodes.at[to_id, "geometry"])
        )
        links = links.drop(index=incoming_id)
        source_ids_by_link.pop(incoming_id)
        contracted_junction_ids.add(int(junction_id))
    if contracted_junction_ids:
        node_df = node_df.drop(index=list(contracted_junction_ids))

    affected = links[
        links["from_node_id"].isin(aggregated_boundary_ids) | links["to_node_id"].isin(aggregated_boundary_ids)
    ]
    for duplicate_ids in affected.groupby(["from_node_id", "to_node_id"], sort=False).groups.values():
        if len(duplicate_ids) < 2:
            continue
        keep_id = duplicate_ids[0]
        source_ids_by_link[keep_id] = tuple(
            source_id for link_id in duplicate_ids for source_id in source_ids_by_link[link_id]
        )
        drop_ids = list(duplicate_ids[1:])
        links = links.drop(index=drop_ids)
        for link_id in drop_ids:
            source_ids_by_link.pop(link_id)
    source_link_ids = [source_ids_by_link[link_id] for link_id in links.index]

    cross_group: dict[tuple[Hashable, Hashable], list[int]] = {}
    for path in paths:
        from_group = aggregator.groups.basin_to_group.at[path.from_basin_id]
        to_group = aggregator.groups.basin_to_group.at[path.to_basin_id]
        if from_group != to_group:
            cross_group.setdefault((from_group, to_group), []).append(path.result_link_id)

    next_link_id = int(aggregator.link_df.index.max()) + 1
    new_records = []
    for position, ((from_group, to_group), link_ids) in enumerate(cross_group.items()):
        link_id = next_link_id + position
        from_id = group_to_node_id[from_group]
        to_id = group_to_node_id[to_group]
        new_records.append(
            {
                "link_id": link_id,
                "name": f"{from_group} -> {to_group}",
                "from_node_id": from_id,
                "to_node_id": to_id,
                "link_type": "flow",
                "geometry": LineString((dynamic_nodes.at[from_id, "geometry"], dynamic_nodes.at[to_id, "geometry"])),
            }
        )
        source_link_ids.append(tuple(link_ids))
    if new_records:
        new_links = gpd.GeoDataFrame(new_records, geometry="geometry", crs=links.crs).set_index("link_id")
        links = gpd.GeoDataFrame(pd.concat([links, new_links]), geometry="geometry", crs=links.crs)

    _separate_parallel_group_links(links, node_df, group_node_ids)

    is_ungrouped_basin = (aggregator.node_df["node_type"] == "Basin") & ~aggregator.node_df.index.isin(
        aggregator.groups.basin_to_group.index
    )
    diagnostics = AggregationDiagnostics(
        ungrouped_basin_ids=tuple(int(node_id) for node_id in aggregator.node_df.index[is_ungrouped_basin]),
        contracted_connector_ids=tuple(sorted(removed_connector_ids)),
    )
    return node_df, links, source_link_ids, diagnostics


def _mapping_matrix(source_ids: np.ndarray, rows: list[tuple[int, ...]]) -> sparse.csr_matrix:
    source_lookup = {int(source_id): position for position, source_id in enumerate(source_ids)}
    row_indices: list[int] = []
    column_indices: list[int] = []
    for row, ids in enumerate(rows):
        for source_id in ids:
            if source_id not in source_lookup:
                raise ValueError(f"Result data does not contain ID {source_id}.")
            row_indices.append(row)
            column_indices.append(source_lookup[source_id])
    return sparse.csr_matrix(
        (np.ones(len(row_indices)), (row_indices, column_indices)),
        shape=(len(rows), len(source_ids)),
    )


def _to_xugrid(
    source: xu.UgridDataset,
    nodes: gpd.GeoDataFrame,
    links: gpd.GeoDataFrame,
    source_link_ids: list[tuple[int, ...]],
    basin_aggregation: _BasinAggregation,
    vertical_variables: Sequence[str],
) -> xu.UgridDataset:
    source_data = cast(Any, source)
    node_positions = pd.Series(np.arange(len(nodes)), index=nodes.index)
    connectivity = np.column_stack(
        (
            node_positions.loc[links["from_node_id"]].to_numpy(),
            node_positions.loc[links["to_node_id"]].to_numpy(),
        )
    )
    grid = xu.Ugrid1d(
        nodes.geometry.x.to_numpy(),
        nodes.geometry.y.to_numpy(),
        -1,
        connectivity,
        name="aggregated",
        crs=nodes.crs,
    )
    node_dim = grid.node_dimension
    edge_dim = grid.edge_dimension
    dataset = xr.Dataset(coords={"time": source_data["time"]})
    dataset = dataset.assign_coords(
        node_id=(node_dim, nodes.index.to_numpy()),
        link_id=(edge_dim, links.index.to_numpy()),
        from_node_id=(edge_dim, links["from_node_id"].to_numpy()),
        to_node_id=(edge_dim, links["to_node_id"].to_numpy()),
    )

    source_edge_dim = source_data.grid.edge_dimension
    flow = source_data["flow_rate"].transpose("time", source_edge_dim)
    link_matrix = _mapping_matrix(source_data["link_id"].to_numpy(), source_link_ids)
    dataset["flow_rate"] = (("time", edge_dim), link_matrix.dot(flow.to_numpy().T).T)

    source_node_dim = source_data.grid.node_dimension
    source_node_ids = source_data["node_id"].to_numpy()
    rows = basin_aggregation.source_ids_by_output(nodes.index)
    basin_matrix = _mapping_matrix(source_node_ids, rows)
    has_basin = np.asarray(basin_matrix.getnnz(axis=1) > 0)
    sum_variables = tuple(dict.fromkeys((*BASIN_ADDITIVE_VARIABLES, *vertical_variables)))
    for variable in sum_variables:
        if variable not in source_data:
            continue
        values = source_data[variable].transpose("time", source_node_dim).to_numpy()
        aggregated = basin_matrix.dot(values.T).T
        aggregated[:, ~has_basin] = np.nan
        dataset[variable] = (("time", node_dim), aggregated)

    if "level" in source_data:
        values = source_data["level"].transpose("time", source_node_dim).to_numpy()
        source_weights = basin_aggregation.source_area.reindex(source_node_ids, fill_value=0).to_numpy()
        weighted_matrix = basin_matrix.multiply(source_weights)
        aggregated = weighted_matrix.dot(values.T).T
        output_area = basin_aggregation.output_area.reindex(nodes.index, fill_value=0).to_numpy()
        aggregated[:, has_basin] /= output_area[has_basin]
        aggregated[:, ~has_basin] = np.nan
        dataset["level"] = (("time", node_dim), aggregated)

    for variable in ("relative_error", "convergence"):
        if variable not in source_data:
            continue
        values = source_data[variable].transpose("time", source_node_dim).to_numpy()
        aggregated = basin_matrix.dot(values.T).T
        counts = np.asarray(basin_matrix.getnnz(axis=1))
        aggregated[:, has_basin] /= counts[has_basin]
        aggregated[:, ~has_basin] = np.nan
        dataset[variable] = (("time", node_dim), aggregated)

    inflow_rate, outflow_rate = _basin_flow_rates(dataset["flow_rate"].to_numpy(), links, nodes)
    dataset["inflow_rate"] = (("time", node_dim), inflow_rate)
    dataset["outflow_rate"] = (("time", node_dim), outflow_rate)

    return cast(Any, xu.UgridDataset)(dataset, grid)


def _basin_flow_rates(
    flow: np.ndarray,
    links: gpd.GeoDataFrame,
    nodes: gpd.GeoDataFrame,
) -> tuple[np.ndarray, np.ndarray]:
    basin_ids = nodes.index[nodes["node_type"] == "Basin"]
    node_positions = {int(node_id): position for position, node_id in enumerate(nodes.index)}
    inflow = np.full((flow.shape[0], len(nodes)), np.nan)
    outflow = np.full_like(inflow, np.nan)
    for node_id in basin_ids:
        position = node_positions[int(node_id)]
        inflow[:, position] = 0.0
        outflow[:, position] = 0.0

    for link_position, (from_id, to_id) in enumerate(links[["from_node_id", "to_node_id"]].to_numpy(dtype=int)):
        rates = flow[:, link_position]
        if from_id in node_positions and nodes.at[from_id, "node_type"] == "Basin":
            position = node_positions[from_id]
            outflow[:, position] += np.maximum(rates, 0)
            inflow[:, position] += np.maximum(-rates, 0)
        if to_id in node_positions and nodes.at[to_id, "node_type"] == "Basin":
            position = node_positions[to_id]
            inflow[:, position] += np.maximum(rates, 0)
            outflow[:, position] += np.maximum(-rates, 0)
    return inflow, outflow


def _source_basin_mapping(nodes: gpd.GeoDataFrame, groups: NodeGroups) -> pd.Series:
    group_to_output = nodes.loc[nodes["group_id"].notna()].reset_index().set_index("group_id")["node_id"]
    mapping = groups.basin_to_group.map(group_to_output)
    ungrouped_ids = nodes.index[(nodes["node_type"] == "Basin") & nodes["group_id"].isna()]
    ungrouped = pd.Series(ungrouped_ids, index=ungrouped_ids, dtype="int64")
    return pd.concat([mapping.astype("int64"), ungrouped]).rename("output_node_id")


def _budget_column_name(time: pd.Timestamp, term: str) -> str:
    period = str(time.year) if time.month == 1 and time.day == 1 else time.strftime("%Y_%m")
    return f"meta_{period}_{term}_mm_day"


def _build_basin_budget(
    model: ribasim.Model,
    basin_aggregation: _BasinAggregation,
    nodes: gpd.GeoDataFrame,
    dataset: xu.UgridDataset,
) -> tuple[gpd.GeoDataFrame | None, pd.DataFrame | None]:
    if not hasattr(model, "basin"):
        return None, None
    area_df = model.basin.area.df
    if area_df is None:
        return None, None

    source_areas = area_df.loc[
        area_df["node_id"].isin(basin_aggregation.source_to_output.index), ["node_id", "geometry"]
    ].copy()
    source_areas["node_id"] = source_areas["node_id"].map(basin_aggregation.source_to_output)
    basin_areas = source_areas.dissolve(by="node_id").sort_index()
    basin_areas["meta_profile_area_m2"] = basin_aggregation.output_area.reindex(basin_areas.index)

    dynamic_dataset = cast(Any, dataset)
    times = pd.DatetimeIndex(dynamic_dataset["time"].to_numpy())
    basin_ids = nodes.index[nodes["node_type"] == "Basin"].to_numpy()
    node_positions = pd.Series(np.arange(len(nodes)), index=nodes.index).loc[basin_ids].to_numpy()
    areas = basin_aggregation.output_area.reindex(basin_ids).to_numpy()
    factor = 86_400_000.0 / areas
    records: list[pd.DataFrame] = []

    node_dim = dynamic_dataset.grid.node_dimension
    for variable in dynamic_dataset.data_vars:
        data_array = dynamic_dataset[variable]
        if (
            variable not in BASIN_RATE_VARIABLES
            or variable in {"inflow_rate", "outflow_rate"}
            or "time" not in data_array.dims
            or node_dim not in data_array.dims
        ):
            continue
        values = data_array.transpose("time", node_dim).to_numpy()[:, node_positions] * factor
        records.append(
            pd.DataFrame(
                {
                    "time": np.repeat(times, len(basin_ids)),
                    "node_id": np.tile(basin_ids, len(times)),
                    "term": variable,
                    "direction": "vertical",
                    "value_mm_day": values.reshape(-1),
                }
            )
        )

    for term, variable in (("inflow", "inflow_rate"), ("outflow", "outflow_rate")):
        values = dynamic_dataset[variable].transpose("time", node_dim).to_numpy()[:, node_positions] * factor
        records.append(
            pd.DataFrame(
                {
                    "time": np.repeat(times, len(basin_ids)),
                    "node_id": np.tile(basin_ids, len(times)),
                    "term": term,
                    "direction": term,
                    "value_mm_day": values.reshape(-1),
                }
            )
        )

    budget = pd.concat(records, ignore_index=True)
    budget["profile_area_m2"] = budget["node_id"].map(basin_aggregation.output_area)
    group_ids = nodes["group_id"] if "group_id" in nodes else pd.Series(dtype="object")
    budget["group_id"] = budget["node_id"].map(group_ids)
    for (time, term), values in budget.groupby(["time", "term"], sort=False):
        column = _budget_column_name(pd.Timestamp(cast(Any, time)), str(term))
        normalized = values.set_index("node_id")["value_mm_day"]
        basin_areas[column] = normalized.reindex(basin_areas.index).to_numpy()
    return basin_areas, budget


def aggregate_spatial_results(
    model: ribasim.Model,
    groups: NodeGroups,
    *,
    dataset: xu.UgridDataset | None = None,
    vertical_variables: Sequence[str] = VERTICAL_FLUX_VARIABLES,
    aggregate_flow_boundaries: bool = False,
    aggregate_level_boundaries: bool = False,
    flow_boundary_filter: Mapping[str, Any] | None = None,
    level_boundary_filter: Mapping[str, Any] | None = None,
) -> AggregatedResults:
    """Aggregate model results while retaining ungrouped areas.

    Boundary aggregation creates one node per group and boundary type at the
    centroid of the selected source nodes. Optional filters match Node columns
    by equality.
    """
    source = model.to_xugrid(add_flow=True) if dataset is None else dataset
    aggregator = _NetworkAggregator(model, groups)
    paths = aggregator.contracted_paths()
    nodes, links, source_link_ids, diagnostics = _build_output_network(
        aggregator,
        paths,
        aggregate_flow_boundaries=aggregate_flow_boundaries,
        aggregate_level_boundaries=aggregate_level_boundaries,
        flow_boundary_filter=flow_boundary_filter,
        level_boundary_filter=level_boundary_filter,
    )
    basin_aggregation = _BasinAggregation.from_model(model, nodes, groups)
    output = _to_xugrid(source, nodes, links, source_link_ids, basin_aggregation, vertical_variables)
    basin_areas, budget = _build_basin_budget(model, basin_aggregation, nodes, output)
    return AggregatedResults(output, nodes, links, diagnostics, model, basin_areas, budget)


def aggregate_results(
    model: ribasim.Model,
    groups: NodeGroups,
    *,
    frequency: str | None = None,
    seasonal: bool = False,
    vertical_variables: Sequence[str] = VERTICAL_FLUX_VARIABLES,
    aggregate_flow_boundaries: bool = False,
    aggregate_level_boundaries: bool = False,
    flow_boundary_filter: Mapping[str, Any] | None = None,
    level_boundary_filter: Mapping[str, Any] | None = None,
) -> AggregatedResults:
    """Aggregate Ribasim results over time and space.

    Temporal aggregation is applied first because it reduces the amount of data
    processed by the spatial sparse matrices. Only regular time series are accepted.
    FlowBoundary and LevelBoundary aggregation are independently configurable;
    their optional filters match Node columns by equality.
    """
    if frequency is not None and seasonal:
        raise ValueError("Choose either frequency or seasonal aggregation, not both.")
    dataset = model.to_xugrid(add_flow=True)
    if seasonal:
        dataset = aggregate_seasonal_results(dataset)
    elif frequency is not None:
        dataset = aggregate_time_results(dataset, frequency)
    return aggregate_spatial_results(
        model,
        groups,
        dataset=dataset,
        vertical_variables=vertical_variables,
        aggregate_flow_boundaries=aggregate_flow_boundaries,
        aggregate_level_boundaries=aggregate_level_boundaries,
        flow_boundary_filter=flow_boundary_filter,
        level_boundary_filter=level_boundary_filter,
    )


def _export_node_table(results: AggregatedResults) -> gpd.GeoDataFrame:
    nodes = results.nodes.copy()
    if "group_id" in nodes:
        nodes = nodes.rename(columns={"group_id": "meta_group_id"})
    defaults = {
        "name": "",
        "subnetwork_id": None,
        "route_priority": None,
        "cyclic_time": False,
    }
    for column, default in defaults.items():
        if column not in nodes:
            nodes[column] = default
        else:
            nodes[column] = nodes[column].fillna(default)
    nodes.index.name = "node_id"
    return nodes


def _export_link_table(results: AggregatedResults) -> gpd.GeoDataFrame:
    links = results.links.copy()
    if "name" not in links:
        links["name"] = ""
    else:
        links["name"] = links["name"].fillna("")
    links.index.name = "link_id"
    return links


def _write_result_netcdf(results: AggregatedResults, results_dir: Path) -> None:
    dataset = cast(Any, results.dataset)
    edge_dim = dataset.grid.edge_dimension
    node_dim = dataset.grid.node_dimension

    flow = xr.Dataset(
        data_vars={
            "flow_rate": xr.DataArray(
                dataset["flow_rate"].transpose("time", edge_dim).to_numpy(),
                dims=("time", "link_id"),
                attrs={"units": "m3 s-1"},
            )
        },
        coords={
            "time": dataset["time"].to_numpy(),
            "link_id": dataset["link_id"].to_numpy(),
            "from_node_id": ("link_id", dataset["from_node_id"].to_numpy()),
            "to_node_id": ("link_id", dataset["to_node_id"].to_numpy()),
        },
    )
    flow.to_netcdf(results_dir / "flow.nc")

    basin_node_ids = results.nodes.index[results.nodes["node_type"] == "Basin"]
    node_positions = pd.Series(np.arange(len(results.nodes)), index=results.nodes.index).loc[basin_node_ids].to_numpy()
    basin_variables = {}
    for variable, attributes in BASIN_RESULT_ATTRIBUTES.items():
        if variable not in dataset:
            continue
        values = dataset[variable].transpose("time", node_dim).to_numpy()[:, node_positions]
        basin_variables[variable] = xr.DataArray(values, dims=("time", "node_id"), attrs=attributes)
    basin = xr.Dataset(
        data_vars=basin_variables,
        coords={
            "time": xr.DataArray(
                dataset["time"].to_numpy(),
                dims="time",
                attrs={"axis": "T", "standard_name": "time", "long_name": "time"},
            ),
            "node_id": xr.DataArray(
                basin_node_ids.to_numpy(),
                dims="node_id",
                attrs={"cf_role": "timeseries_id", "long_name": "node identifier"},
            ),
        },
        attrs={
            "Conventions": "CF-1.12",
            "references": "https://ribasim.org",
            "ribasim_version": results.source_model.ribasim_version,
            "title": "Ribasim results: basin",
        },
    )
    basin["time"].encoding["calendar"] = "standard"
    basin.to_netcdf(results_dir / "basin.nc")
    if "level" in basin:
        basin_state = basin[["level"]].isel(time=-1, drop=True)
        basin_state.to_netcdf(results_dir / "basin_state.nc")


def write_visualization_model(results: AggregatedResults, directory: str | Path) -> Path:
    """Write aggregated geometry and results as a non-runnable visualization package."""
    directory = Path(directory)
    input_dir = directory / "input"
    results_dir = directory / "results"
    input_dir.mkdir(parents=True, exist_ok=True)
    results_dir.mkdir(parents=True, exist_ok=True)

    database = input_dir / "database.gpkg"
    if database.exists():
        database.unlink()
    nodes = _export_node_table(results)
    links = _export_link_table(results)
    nodes.to_file(database, layer="Node", driver="GPKG", index=True, fid=nodes.index.name)
    links.to_file(database, layer="Link", driver="GPKG", index=True, fid=links.index.name)
    if results.basin_areas is not None:
        results.basin_areas.reset_index().to_file(database, layer="Basin / area", driver="GPKG", index=False)
    _set_db_schema_version(database, __schema_version__)
    _write_result_netcdf(results, results_dir)

    starttime = pd.Timestamp(results.source_model.starttime).strftime("%Y-%m-%d %H:%M:%S")
    endtime = pd.Timestamp(results.source_model.endtime).strftime("%Y-%m-%d %H:%M:%S")
    assert results.nodes.crs is not None
    crs = results.nodes.crs.to_string()
    version = results.source_model.ribasim_version
    toml = directory / "ribasim.toml"
    toml.write_text(
        f"starttime = {starttime}\n"
        f"endtime = {endtime}\n"
        f'crs = "{crs}"\n'
        'input_dir = "input"\n'
        'results_dir = "results"\n'
        f'ribasim_version = "{version}"\n',
        encoding="utf-8",
    )
    return toml
