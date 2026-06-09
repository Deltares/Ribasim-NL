"""Shared helpers for coupling-level checks."""

import math
from typing import Any, cast

import pandas as pd

from ribasim_nl import Model

CONTROL_NODE_TYPES = {"Outlet", "Pump"}
STATIC_TABLE_BY_NODE_TYPE = {
    "Outlet": "Outlet / static",
    "Pump": "Pump / static",
}
RWS_AUTHORITY = "Rijkswaterstaat"
AAENMAAS_AUTHORITY = "AaenMaas"
DEDOMMEL_AUTHORITY = "DeDommel"
LIMBURG_AUTHORITY = "Limburg"
SKIP_LEVEL_UPDATE_AUTHORITIES = {"WetterskipFryslan"}
SKIP_LEVEL_UPDATE_NODE_IDS: set[int] = set()
LEVEL_UPDATE_PROTECTION_COLUMN = "meta_level_update_protected"


def normalize_numeric(series: pd.Series) -> pd.Series:
    """Convert a Series to numeric values, invalid values become NaN."""
    return pd.to_numeric(series, errors="coerce")


def as_int(value: object) -> int:
    """Cast a table value to int."""
    return int(cast(Any, value))


def as_float(value: object) -> float:
    """Cast a table value to float."""
    return float(cast(Any, value))


def is_missing(value: object) -> bool:
    """Return True for pandas-style missing values."""
    return bool(pd.isna(cast(Any, value)))


def is_present(value: object) -> bool:
    """Return True when a value is not missing."""
    return not is_missing(value)


def reset_index_to_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    """Ensure an index value is available as a normal column."""
    if column_name in df.columns:
        return df.copy()

    index_name = df.index.name or "index"
    result = df.reset_index(drop=False)
    if index_name != column_name and index_name in result.columns:
        result = result.rename(columns={index_name: column_name})
    elif "index" in result.columns and column_name not in result.columns:
        result = result.rename(columns={"index": column_name})
    return result


def positive(value: object) -> bool:
    """Return True for finite numeric values larger than zero."""
    try:
        if value is None:
            return False
        number = as_float(value)
        return math.isfinite(number) and number > 0.0
    except (TypeError, ValueError):
        return False


def truthy(value: object) -> bool:
    """Interpret bool-like values from TOML/GPKG tables."""
    if is_missing(value):
        return False
    if isinstance(value, bool):
        return value
    try:
        return float(cast(Any, value)) != 0.0
    except (TypeError, ValueError):
        return str(value).strip().lower() in {"true", "yes", "ja", "y"}


def static_row_has_capacity(row: pd.Series | dict[str, object]) -> bool:
    """Check whether a static row can convey water."""
    return positive(row.get("flow_rate")) or positive(row.get("max_flow_rate"))


def classify_functions(static_df: pd.DataFrame, flow_demand_inlet_nodes: set[int] | None = None) -> dict[int, str]:
    """Classify each control node as inlaat, uitlaat, doorlaat, or dicht."""
    flow_demand_inlet_nodes = flow_demand_inlet_nodes or set()
    control_state = static_df["control_state"].astype("string").str.lower()
    capacity = static_df.apply(static_row_has_capacity, axis=1)
    aanvoer_nodes = set(static_df.loc[control_state.eq("aanvoer") & capacity, "node_id"].astype(int))
    aanvoer_nodes |= flow_demand_inlet_nodes
    afvoer_nodes = set(static_df.loc[control_state.eq("afvoer") & capacity, "node_id"].astype(int))
    node_ids = set(static_df["node_id"].astype(int))

    functions = dict.fromkeys(node_ids, "dicht")
    for node_id in aanvoer_nodes - afvoer_nodes:
        functions[node_id] = "inlaat"
    for node_id in afvoer_nodes - aanvoer_nodes:
        functions[node_id] = "uitlaat"
    for node_id in aanvoer_nodes & afvoer_nodes:
        functions[node_id] = "doorlaat"
    return functions


def model_level_difference_threshold(model: Model) -> float:
    """Read the model hysteresis threshold used for DiscreteControl levels."""
    solver = getattr(model, "solver", None)
    value = getattr(solver, "level_difference_threshold", None)
    return 0.02 if is_missing(value) else as_float(value)


def control_node_name(model: Model, control_node_id: int) -> str | None:
    """Return the name of a DiscreteControl node, if present."""
    node_df = model.node.df
    if node_df is None:
        return None

    control_node_id = as_int(control_node_id)
    if control_node_id not in node_df.index:
        return None
    name = node_df.at[control_node_id, "name"]
    return None if is_missing(name) else str(name)


def control_node_ids_by_target_node_id(model: Model) -> dict[int, list[int]]:
    """Map controlled Outlet/Pump node_id to its DiscreteControl node_ids."""
    assert model.node.df is not None
    assert model.link.df is not None

    node_type_by_id = {
        as_int(node_id): str(node_type) for node_id, node_type in model.node.df["node_type"].to_dict().items()
    }
    link_df = reset_index_to_column(model.link.df.copy(), "link_id")
    control_links = link_df[
        link_df["link_type"].fillna("").eq("control")
        & link_df["from_node_id"].map(node_type_by_id).eq("DiscreteControl")
        & link_df["to_node_id"].map(node_type_by_id).isin(CONTROL_NODE_TYPES)
    ]
    if control_links.empty:
        return {}

    result: dict[int, list[int]] = {}
    for target_node_id, rows in control_links.groupby("to_node_id"):
        result[as_int(target_node_id)] = [as_int(value) for value in rows["from_node_id"]]
    return result


def flow_demand_controlled_node_ids(model: Model) -> set[int]:
    """Find Outlet/Pump nodes controlled by a FlowDemand node."""
    assert model.node.df is not None
    assert model.link.df is not None

    node_df = reset_index_to_column(model.node.df.copy(), "node_id")
    link_df = reset_index_to_column(model.link.df.copy(), "link_id")
    node_type_by_id = {
        as_int(node_id): str(node_type)
        for node_id, node_type in node_df.set_index("node_id")["node_type"].to_dict().items()
    }
    flow_demand_links = link_df[
        link_df["link_type"].fillna("").eq("control")
        & link_df["from_node_id"].map(node_type_by_id).eq("FlowDemand")
        & link_df["to_node_id"].map(node_type_by_id).isin(CONTROL_NODE_TYPES)
    ]
    return set(flow_demand_links["to_node_id"].dropna().astype(int))
