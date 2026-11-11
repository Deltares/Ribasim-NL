"""Ribasim-NL-specific validations that supplement Ribasim validation."""

from collections.abc import Iterator
from typing import TYPE_CHECKING

import numpy as np
import pandas as pd
import shapely

from ribasim_nl.geometry import SLIVER_WIDTH, is_sliver
from ribasim_nl.profiles import MIN_PROFILE_AREA

if TYPE_CHECKING:
    from pandas.core.groupby import DataFrameGroupBy

    from ribasim_nl.model import Model


def _basin_profile_issues(model: "Model") -> list[str]:
    """Return issues with basin profile levels and areas."""
    basin_profile = model.basin.profile.df
    if basin_profile is None:
        return []

    issues = []
    invalid_levels = basin_profile.loc[~np.isfinite(basin_profile["level"].astype(float))]
    if not invalid_levels.empty:
        node_ids = sorted(map(int, invalid_levels["node_id"].unique()))
        issues.append(f"Basin profile levels must be finite; invalid node IDs: {node_ids}")

    invalid_areas = basin_profile.loc[basin_profile["area"] < MIN_PROFILE_AREA]
    if not invalid_areas.empty:
        node_ids = sorted(map(int, invalid_areas["node_id"].unique()))
        issues.append(f"Basin profile areas must be at least {MIN_PROFILE_AREA} m2; invalid node IDs: {node_ids}")
    return issues


# The largest range between the bottom and top of a Basin profile that is real, per `meta_categorie` of the Basin.
# Bergend Basins follow the terrain within their area, up to 136 m in Limburg. Other Basins follow their water: the main
# water system up to 67 m in the Westerschelde, and elsewhere up to 18 m. Basins without a category have the largest
# limit of the water, since the category is not known yet.
MAX_PROFILE_RANGE = {"bergend": 150.0, "hoofdwater": 80.0, "doorgaand": 30.0}  # m
DEFAULT_MAX_PROFILE_RANGE = MAX_PROFILE_RANGE["hoofdwater"]


def _deep_basin_profile_issues(model: "Model") -> list[str]:
    """Return Basins with an unreasonable range in their profile, which typically comes from a no-data level."""
    basin_profile = model.basin.profile.df
    if basin_profile is None or basin_profile.empty:
        return []
    levels = basin_profile.groupby("node_id")["level"]
    profile_range = levels.max() - levels.min()
    node_df = model.node.df
    category = node_df["meta_categorie"] if "meta_categorie" in node_df.columns else pd.Series(index=node_df.index)
    limit = category.reindex(profile_range.index).map(MAX_PROFILE_RANGE).fillna(DEFAULT_MAX_PROFILE_RANGE)
    invalid = profile_range[profile_range > limit]
    if invalid.empty:
        return []
    ranges = invalid.round(1).to_dict()
    return [f"Basin profiles must not be deeper than {MAX_PROFILE_RANGE} m; invalid node IDs and ranges: {ranges}"]


def _basin_area_issues(model: "Model") -> list[str]:
    """Return invalid basin areas, and basin areas with slivers, which `Model.clean_basin_area` removes."""
    area = model.basin.area.df
    if area is None or area.empty:
        return []

    issues = []
    geometry = area.geometry.to_numpy()
    node_id = area["node_id"].to_numpy()
    invalid = ~shapely.is_valid(geometry)
    if invalid.any():
        issues.append(f"Basin areas must be valid; invalid node IDs: {sorted(set(map(int, node_id[invalid])))}")

    parts, part_area = shapely.get_parts(geometry, return_index=True)
    # the rings of each polygon start with its exterior, the others are holes
    rings, ring_part = shapely.get_rings(parts, return_index=True)
    is_hole = np.r_[False, ring_part[1:] == ring_part[:-1]]
    sliver_parts = part_area[is_sliver(parts)]
    sliver_holes = part_area[ring_part[is_hole][is_sliver(shapely.polygons(rings[is_hole]))]]
    slivers = np.concatenate([sliver_parts, sliver_holes])
    if slivers.size:
        issues.append(
            f"Basin areas must not have parts or holes that are on average at most {SLIVER_WIDTH} m wide, "
            f"see Model.clean_basin_area; invalid node IDs: {sorted(set(map(int, node_id[slivers])))}"
        )
    return issues


def _discrete_control_condition_issues(model: "Model") -> list[str]:
    """Return issues with DiscreteControl condition thresholds."""
    condition = model.discrete_control.condition.df
    if condition is None or condition.empty:
        return []

    invalid_thresholds = condition["threshold_low"].isna() | condition["threshold_high"].isna()
    invalid_thresholds |= condition["threshold_low"] >= condition["threshold_high"]
    if not invalid_thresholds.any():
        return []

    invalid_conditions = condition.loc[invalid_thresholds]
    table_ids = sorted(map(int, invalid_conditions.index))
    node_ids = sorted(map(int, invalid_conditions["node_id"].unique()))
    return [
        "DiscreteControl conditions must have threshold_low < threshold_high; "
        f"invalid node IDs: {node_ids}; condition table IDs: {table_ids}"
    ]


CONTROLLED_COLUMNS = ["flow_rate", "min_flow_rate", "max_flow_rate", "min_upstream_level", "max_downstream_level"]


def _controlled_static_groups(model: "Model") -> Iterator[tuple[str, "DataFrameGroupBy"]]:
    """Yield the static rows with a control state of Pump and Outlet nodes, grouped by node ID."""
    for node_type in ("Pump", "Outlet"):
        static = model.get_component(node_type).static.df
        if static is None or static.empty:
            continue
        yield node_type, static.dropna(subset=["control_state"]).groupby("node_id")


def _partially_missing_control_state_issues(model: "Model") -> list[str]:
    """Return issues with Pump and Outlet parameters that are missing in some control states only.

    Switching to a control state does not update parameters that are missing in it, so such a
    parameter silently keeps the value of the previous control state. Use an explicit value
    instead, such as `math.inf` for no max_downstream_level.
    """
    issues = []
    for node_type, grouped in _controlled_static_groups(model):
        missing = grouped[CONTROLLED_COLUMNS].agg(lambda values: values.isna().mean())
        is_partial = (missing.gt(0) & missing.lt(1)).any(axis=1)
        node_ids = sorted(map(int, is_partial.index[is_partial]))
        if node_ids:
            issues.append(
                f"{node_type} parameters must be given in all control states or in none; invalid node IDs: {node_ids}"
            )
    return issues


def _inert_control_state_issues(model: "Model") -> list[str]:
    """Return issues with Pump and Outlet nodes that behave the same in every control state.

    DiscreteControl has no effect on such nodes, which typically means that the state
    that should switch a node off (`flow_rate` 0) was overwritten.
    """
    issues = []
    for node_type, grouped in _controlled_static_groups(model):
        is_inert = grouped["control_state"].nunique().gt(1)
        is_inert &= grouped[CONTROLLED_COLUMNS].nunique().le(1).all(axis=1)
        node_ids = sorted(map(int, is_inert.index[is_inert]))
        if node_ids:
            issues.append(
                f"{node_type} nodes must differ between control states, "
                f"otherwise DiscreteControl has no effect; invalid node IDs: {node_ids}"
            )
    return issues


# Node types whose static tables have control states
CONTROL_STATE_NODE_TYPES = (
    "LinearResistance",
    "ManningResistance",
    "TabulatedRatingCurve",
    "Pump",
    "Outlet",
    "PidControl",
)


def _uncontrolled_control_state_issues(model: "Model") -> list[str]:
    """Return nodes with multiple control states that are not controlled by DiscreteControl.

    Ribasim requires such nodes to have a static row without control_state, which sets their
    parameters. This typically means that their DiscreteControl was removed by mistake.
    """
    link = model.link.df
    node = model.node.df
    control = link.loc[link["link_type"] == "control"]
    from_discrete_control = control["from_node_id"].map(node["node_type"]).eq("DiscreteControl")
    discrete_controlled = set(control.loc[from_discrete_control, "to_node_id"])

    issues = []
    for node_type in CONTROL_STATE_NODE_TYPES:
        static = model.get_component(node_type).static.df
        if static is None or static.empty:
            continue
        grouped = static.groupby("node_id")["control_state"]
        invalid = grouped.nunique().gt(1) & grouped.apply(lambda states: states.notna().all())
        node_ids = sorted(int(i) for i in invalid.index[invalid] if i not in discrete_controlled)
        if node_ids:
            issues.append(
                f"{node_type} nodes with multiple control states must be controlled by DiscreteControl "
                f"or have a static row without control_state; invalid node IDs: {node_ids}"
            )
    return issues


# Below these bounds a ManningResistance has effectively no resistance, which makes the simulation needlessly stiff
MIN_MANNING_LENGTH = 10.0  # m
MIN_MANNING_N = 0.01  # s/m^(1/3)


def _manning_resistance_issues(model: "Model") -> list[str]:
    """Return ManningResistance nodes with a length or Manning's n below the minimum."""
    static_df = model.manning_resistance.static.df
    if static_df is None:
        return []
    invalid = static_df.loc[(static_df["length"] < MIN_MANNING_LENGTH) | (static_df["manning_n"] < MIN_MANNING_N)]
    if invalid.empty:
        return []
    node_ids = sorted(map(int, invalid["node_id"].unique()))
    return [
        f"ManningResistance must have length >= {MIN_MANNING_LENGTH} m and manning_n >= {MIN_MANNING_N}; "
        f"invalid node IDs: {node_ids}"
    ]


# Node types that Ribasim does not allow to be connected to each other, also not through Junctions
CONNECTOR_NODE_TYPES = {"Pump", "Outlet", "TabulatedRatingCurve", "LinearResistance", "ManningResistance"}


def _connector_through_junction_issues(model: "Model") -> list[str]:
    """Return connector nodes that are connected to other connector nodes through Junctions.

    Ribasim removes the plain Junctions, see `ribasim_nl.flow_paths.Routes`, so it rejects the links that this gives
    between e.g. a Pump and a Pump.
    """
    from ribasim_nl.flow_paths import simplified_links

    node_type = model.node.df["node_type"]
    pairs = [
        (int(from_node_id), int(to_node_id))
        for from_node_id, to_node_id in simplified_links(model)
        if node_type[from_node_id] in CONNECTOR_NODE_TYPES and node_type[to_node_id] in CONNECTOR_NODE_TYPES
    ]
    if not pairs:
        return []
    return [
        "Connector nodes must not be connected to connector nodes through Junctions, add a Basin in between; "
        f"invalid (from, to) node IDs: {sorted(set(pairs))}"
    ]


# Nodes closer than this are on top of each other, and links overlapping longer than this overlap, see ribasim_nl.layout
MIN_NODE_DISTANCE = 1.0  # m
MIN_LINK_OVERLAP = 1.0  # m


def _layout_issues(model: "Model") -> list[str]:
    """Return nodes on top of each other and overlapping links, which make the model hard to read on a map."""
    from ribasim_nl.layout import coincident_nodes, overlapping_links

    issues = []
    nodes = coincident_nodes(model, MIN_NODE_DISTANCE)
    if nodes.shape[1]:
        pairs = sorted(map(tuple, nodes.T.tolist()))
        issues.append(f"Nodes must be at least {MIN_NODE_DISTANCE} m apart, see ribasim_nl.layout; node IDs: {pairs}")
    links = overlapping_links(model, MIN_LINK_OVERLAP)
    if links.shape[1]:
        pairs = sorted(map(tuple, links.T.tolist()))
        issues.append(f"Links must not overlap over {MIN_LINK_OVERLAP} m, see ribasim_nl.layout; link IDs: {pairs}")
    return issues


def _listen_to_junction_issues(model: "Model") -> list[str]:
    """Return control nodes that listen to a Junction, which has no level or flow rate of its own."""
    node_type = model.node.df["node_type"]
    pairs = []
    for table in (model.discrete_control.variable.df, model.continuous_control.variable.df):
        if table is not None:
            listening = table[table["listen_node_id"].map(node_type) == "Junction"]
            pairs += list(zip(listening["node_id"].tolist(), listening["listen_node_id"].tolist(), strict=True))
    if not pairs:
        return []
    return [f"Control nodes must not listen to a Junction; invalid (control, Junction) node IDs: {sorted(set(pairs))}"]


def _flow_demand_capacity_issues(model: "Model") -> list[str]:
    """Return FlowDemand nodes whose demand exceeds the max_flow_rate of their Pump or Outlet.

    Without allocation, Ribasim clamps the demand to the max_flow_rate of the current control state, so the part
    above it is silently ignored.
    """
    link_df = model.link.df
    demands = [table.df for table in (model.flow_demand.static, model.flow_demand.time) if table.df is not None]
    if not demands:
        return []
    demand = pd.concat([df[["node_id", "demand"]] for df in demands]).groupby("node_id")["demand"].max()
    static = [model.get_component(i).static.df for i in ("Pump", "Outlet")]
    static = [df[["node_id", "max_flow_rate"]] for df in static if df is not None]
    max_flow_rate = pd.concat(static).groupby("node_id")["max_flow_rate"].min() if static else pd.Series()

    control = link_df[(link_df["link_type"] == "control") & link_df["from_node_id"].isin(demand.index)]
    pairs = [
        (int(from_node_id), int(to_node_id))
        for from_node_id, to_node_id in zip(control["from_node_id"], control["to_node_id"], strict=True)
        if demand[from_node_id] > max_flow_rate.get(to_node_id, np.inf)
    ]
    if not pairs:
        return []
    return [
        "FlowDemand must not exceed the max_flow_rate of its Pump or Outlet in any control state; "
        f"invalid (FlowDemand, connector) node IDs: {sorted(pairs)}"
    ]


def _user_demand_return_factor_issues(model: "Model") -> list[str]:
    """Return UserDemand nodes that return more than they take, or a negative fraction, see #1027."""
    tables = [table.df for table in (model.user_demand.static, model.user_demand.time) if table.df is not None]
    if not tables:
        return []
    return_factor = pd.concat([df[["node_id", "return_factor"]] for df in tables])
    invalid = return_factor[(return_factor["return_factor"] < 0) | (return_factor["return_factor"] > 1)]
    if invalid.empty:
        return []
    node_ids = sorted(map(int, invalid["node_id"].unique()))
    return [f"UserDemand return_factor must be between 0 and 1; invalid node IDs: {node_ids}"]


# Observation nodes are at the measurement location, near the node they observe; Lobith is 4.8 km from the Rhine boundary
MAX_OBSERVATION_DISTANCE = 5000.0  # m


def _observation_distance_issues(model: "Model") -> list[str]:
    """Return Observation nodes far from the node they observe, such as a node far along the river, or a typo."""
    links = model.link.df
    observation = links[links["link_type"] == "observation"]
    points = model.node.df.geometry
    distance = shapely.distance(
        points.loc[observation["from_node_id"]].to_numpy(), points.loc[observation["to_node_id"]].to_numpy()
    )
    far = observation[distance > MAX_OBSERVATION_DISTANCE]
    if far.empty:
        return []
    pairs = sorted(zip(far["from_node_id"].tolist(), far["to_node_id"].tolist(), strict=True))
    return [
        f"Observation nodes must be within {MAX_OBSERVATION_DISTANCE:.0f} m of the node they observe; "
        f"invalid (Observation, observed) node IDs: {pairs}"
    ]


def validate_model(model: "Model", layout: bool = True) -> None:
    """Validate Ribasim-NL conventions and report all issues together.

    With `layout`, also check that nodes are not on top of each other and links do not overlap.
    """
    issues = [
        *_basin_area_issues(model),
        *_basin_profile_issues(model),
        *_deep_basin_profile_issues(model),
        *_discrete_control_condition_issues(model),
        *_partially_missing_control_state_issues(model),
        *_inert_control_state_issues(model),
        *_uncontrolled_control_state_issues(model),
        *_connector_through_junction_issues(model),
        *_manning_resistance_issues(model),
        *_listen_to_junction_issues(model),
        *_flow_demand_capacity_issues(model),
        *_user_demand_return_factor_issues(model),
        *_observation_distance_issues(model),
        *(_layout_issues(model) if layout else []),
    ]
    if issues:
        raise ValueError("Ribasim-NL model validation failed:\n- " + "\n- ".join(issues))
