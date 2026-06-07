"""Shared helpers for coupling-level checks."""

from __future__ import annotations

import math
from pathlib import Path

import pandas as pd

from ribasim_nl import Model

CONTROL_NODE_TYPES = {"Outlet", "Pump"}
STATIC_TABLE_BY_NODE_TYPE = {
    "Outlet": "Outlet / static",
    "Pump": "Pump / static",
}
RWS_AUTHORITY = "Rijkswaterstaat"
LIMBURG_AUTHORITY = "Limburg"
SKIP_LEVEL_UPDATE_AUTHORITIES = {"WetterskipFryslan"}
SKIP_LEVEL_UPDATE_NODE_IDS: set[int] = set()
SKIP_MIN_UPSTREAM_UPDATE_NODE_IDS = {3800291}
LEVEL_UPDATE_PROTECTION_COLUMN = "meta_level_update_protected"


def database_gpkg_path(model: Model, toml_file: Path) -> Path:
    model_dir = toml_file.parent
    input_database_gpkg = model_dir / Path(model.input_dir) / "database.gpkg"
    legacy_database_gpkg = model_dir / "database.gpkg"
    if input_database_gpkg.exists():
        return input_database_gpkg
    if legacy_database_gpkg.exists():
        return legacy_database_gpkg
    raise FileNotFoundError(
        f"Kan geen database.gpkg vinden voor model {toml_file}. "
        f"Gezocht in {input_database_gpkg} en {legacy_database_gpkg}."
    )


def resolve_output_gpkg(toml_file: Path, output_gpkg: Path | None) -> Path:
    if output_gpkg is None:
        return toml_file.with_name("coupling_level_report.gpkg")
    if output_gpkg.is_absolute():
        return output_gpkg
    return toml_file.parent / output_gpkg


def normalize_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def reset_index_to_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
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
    try:
        if value is None:
            return False
        number = float(value)
        return math.isfinite(number) and number > 0.0
    except (TypeError, ValueError):
        return False


def truthy(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "ja", "y"}


def static_row_has_capacity(row: pd.Series | dict[str, object]) -> bool:
    return positive(row.get("flow_rate")) or positive(row.get("max_flow_rate"))


def classify_functions(static_df: pd.DataFrame, flow_demand_inlet_nodes: set[int] | None = None) -> dict[int, str]:
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
    solver = getattr(model, "solver", None)
    value = getattr(solver, "level_difference_threshold", None)
    return 0.02 if value is None or pd.isna(value) else float(value)


def control_node_name(model: Model, control_node_id: int) -> str | None:
    if int(control_node_id) not in model.node.df.index:
        return None
    name = model.node.df.at[int(control_node_id), "name"]
    return None if pd.isna(name) else str(name)


def control_node_ids_by_target_node_id(model: Model) -> dict[int, list[int]]:
    node_type_by_id = model.node.df["node_type"].to_dict()
    link_df = reset_index_to_column(model.link.df.copy(), "link_id")
    control_links = link_df[
        link_df["link_type"].fillna("").eq("control")
        & link_df["from_node_id"].map(node_type_by_id).eq("DiscreteControl")
        & link_df["to_node_id"].map(node_type_by_id).isin(CONTROL_NODE_TYPES)
    ]
    if control_links.empty:
        return {}

    return (
        control_links.groupby("to_node_id")["from_node_id"]
        .apply(lambda values: [int(value) for value in values])
        .to_dict()
    )


def flow_demand_controlled_node_ids(model: Model) -> set[int]:
    node_df = reset_index_to_column(model.node.df.copy(), "node_id")
    link_df = reset_index_to_column(model.link.df.copy(), "link_id")
    node_type_by_id = node_df.set_index("node_id")["node_type"].to_dict()
    flow_demand_links = link_df[
        link_df["link_type"].fillna("").eq("control")
        & link_df["from_node_id"].map(node_type_by_id).eq("FlowDemand")
        & link_df["to_node_id"].map(node_type_by_id).isin(CONTROL_NODE_TYPES)
    ]
    return set(flow_demand_links["to_node_id"].dropna().astype(int))
