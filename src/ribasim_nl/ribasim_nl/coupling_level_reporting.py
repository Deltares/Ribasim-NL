from __future__ import annotations

from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd

IDENTITY_COLUMNS = [
    "node_id",
    "node_type",
    "functie",
    "name",
    "meta_waterbeheerder",
    "meta_node_id_waterbeheerder",
    "static_table",
    "table_fid",
    "control_state",
]
CAPACITY_COLUMNS = [
    "flow_rate",
    "min_flow_rate",
    "max_flow_rate",
    "min_upstream_level",
    "max_downstream_level",
]
NETWORK_COLUMNS = [
    "upstream_node_id",
    "upstream_node_type",
    "upstream_basin_authority",
    "upstream_basin_streefpeil",
    "upstream_basin_min_profile_level",
    "downstream_node_id",
    "downstream_node_type",
    "downstream_basin_authority",
    "downstream_basin_streefpeil",
]
MIN_UPSTREAM_REPORT_COLUMNS = [
    *IDENTITY_COLUMNS,
    *CAPACITY_COLUMNS,
    *NETWORK_COLUMNS,
    "gecheckte_min_upstream_level",
    "verschil_min_upstream_level",
    "min_upstream_level_check_basis",
    "min_upstream_level_afwijking",
    "rws_inlet_profile_min_upstream",
    "rws_inlet_profile_min_upstream_afwijking",
    "direct_min_upstream_level_update_allowed",
    "direct_min_upstream_level_update_basis",
    "upstream_check_error",
]
LEVEL_DEVIATION_REPORT_COLUMNS = [
    *IDENTITY_COLUMNS,
    *CAPACITY_COLUMNS,
    *NETWORK_COLUMNS,
    "gecheckte_min_upstream_level",
    "verschil_min_upstream_level",
    "min_upstream_level_check_basis",
    "min_upstream_level_afwijking",
    "gecheckte_max_downstream_level",
    "verschil_max_downstream_level",
    "max_downstream_level_check_basis",
    "max_downstream_level_afwijking",
    "max_downstream_level_update_allowed",
    "level_update_protected",
    "flow_demand_controlled",
]
NOT_APPLIED_REPORT_COLUMNS = [
    *LEVEL_DEVIATION_REPORT_COLUMNS,
    "niet_aangepaste_level_kolom",
    "reden_niet_aangepast",
]
CONTROLLER_THRESHOLD_REPORT_COLUMNS = [
    "node_id",
    "node_type",
    "functie",
    "meta_waterbeheerder",
    "meta_node_id_waterbeheerder",
    "control_node_id",
    "control_name",
    "listen_node_id",
    "listen_node_authority",
    "is_coupled_condition",
    "compound_variable_id",
    "condition_id",
    "variable",
    "weight",
    "threshold_update_basis",
    "protected_static_level",
    "coupling_checked_level",
    "huidig_threshold_high",
    "huidig_threshold_low",
    "gecheckte_threshold",
]
RWS_LEAK_REPORT_COLUMNS = [
    "node_id",
    "node_type",
    "functie",
    "name",
    "meta_waterbeheerder",
    "meta_node_id_waterbeheerder",
    "upstream_basin_id",
    "upstream_basin_name",
    "downstream_basin_id",
    "downstream_basin_authority",
    "downstream_basin_name",
    "afvoer_rows",
]
OUTLET_INLET_REPORT_COLUMNS = [
    "node_id",
    "node_type",
    "functie",
    "name",
    "meta_waterbeheerder",
    "meta_node_id_waterbeheerder",
    "upstream_basin_id",
    "upstream_basin_authority",
    "upstream_basin_name",
    "upstream_basin_streefpeil",
    "reden",
]

FULL_REPORT_LAYERS = (
    ("level_afwijkingen", "deviations", LEVEL_DEVIATION_REPORT_COLUMNS),
    ("toegestane_rws_inlaat_updates", "allowed_updates", MIN_UPSTREAM_REPORT_COLUMNS),
    ("toegestane_directe_min_upstream_updates", "direct_min_upstream_updates", MIN_UPSTREAM_REPORT_COLUMNS),
    ("verdachte_niet_aangepakte_level_afwijkingen", "not_applied", NOT_APPLIED_REPORT_COLUMNS),
    ("controller_threshold_updates", "protected_controller_updates", CONTROLLER_THRESHOLD_REPORT_COLUMNS),
    ("verdachte_rws_lekken", "leaks", RWS_LEAK_REPORT_COLUMNS),
    ("verdachte_outlet_als_inlaat", "outlet_inlets", OUTLET_INLET_REPORT_COLUMNS),
)

SUSPICIOUS_REPORT_LAYERS = (
    ("verdachte_niet_aangepakte_level_afwijkingen", "not_applied", NOT_APPLIED_REPORT_COLUMNS),
    ("controller_threshold_updates", "protected_controller_updates", CONTROLLER_THRESHOLD_REPORT_COLUMNS),
    ("verdachte_rws_lekken", "leaks", RWS_LEAK_REPORT_COLUMNS),
    ("verdachte_outlet_als_inlaat", "outlet_inlets", OUTLET_INLET_REPORT_COLUMNS),
)


def resolve_writable_gpkg(output_gpkg: Path, label: str) -> Path:
    if output_gpkg.exists():
        try:
            output_gpkg.unlink()
        except PermissionError:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_gpkg = output_gpkg.with_name(f"{output_gpkg.stem}_{timestamp}{output_gpkg.suffix}")
            print(f"{label} is in gebruik; schrijf naar alternatief bestand: {output_gpkg}", flush=True)
    output_gpkg.parent.mkdir(parents=True, exist_ok=True)
    return output_gpkg


def add_geometry(node_df: gpd.GeoDataFrame, df: pd.DataFrame) -> gpd.GeoDataFrame:
    node_gdf = node_df.copy()
    if "node_id" not in node_gdf.columns:
        index_name = node_gdf.index.name or "index"
        node_gdf = node_gdf.reset_index(drop=False)
        if index_name != "node_id" and index_name in node_gdf.columns:
            node_gdf = node_gdf.rename(columns={index_name: "node_id"})
        elif "index" in node_gdf.columns and "node_id" not in node_gdf.columns:
            node_gdf = node_gdf.rename(columns={"index": "node_id"})
    node_gdf = node_gdf[["node_id", "geometry"]]
    if df.empty:
        return gpd.GeoDataFrame(df, geometry=[], crs=node_gdf.crs)
    gdf = node_gdf.merge(df, on="node_id", how="inner")
    return gpd.GeoDataFrame(gdf, geometry="geometry", crs=node_gdf.crs)


def select_report_columns(gdf: gpd.GeoDataFrame, columns: list[str]) -> gpd.GeoDataFrame:
    geometry_column = gdf.geometry.name if gdf.geometry.name in gdf.columns else "geometry"
    selected_columns = [column for column in columns if column in gdf.columns]
    if geometry_column in gdf.columns and geometry_column not in selected_columns:
        selected_columns.append(geometry_column)
    return gdf.loc[:, selected_columns].copy()


def write_gpkg_layer(gdf: gpd.GeoDataFrame, output_gpkg: Path, layer: str, columns: list[str]) -> None:
    output_gdf = select_report_columns(gdf, columns)
    if "fid" in output_gdf.columns:
        output_gdf = output_gdf.rename(columns={"fid": "source_fid"})
    output_gdf.reset_index(drop=True).to_file(output_gpkg, layer=layer, driver="GPKG")


def write_report_layers(
    *,
    node_df: gpd.GeoDataFrame,
    output_gpkg: Path,
    layers: tuple[tuple[str, str, list[str]], ...],
    tables: dict[str, pd.DataFrame],
    label: str,
) -> Path:
    output_gpkg = resolve_writable_gpkg(output_gpkg, label)
    for layer, table_key, columns in layers:
        write_gpkg_layer(add_geometry(node_df, tables[table_key]), output_gpkg, layer, columns)
    return output_gpkg


def skip_reason(row: pd.Series) -> str:
    reasons = []
    if bool(row.get("flow_demand_controlled", False)):
        reasons.append("flow_demand_beschermd")
    if bool(row.get("level_update_protected", False)):
        reasons.append("handmatig_level_beschermd")
    if bool(row.get("level_update_skipped_authority", False)):
        reasons.append("waterbeheerder_uitgesloten")
    if bool(row.get("min_upstream_update_skipped_node", False)):
        reasons.append("node_uitgesloten_van_min_upstream_update")
    if pd.notna(row.get("max_downstream_level_basin_error")):
        reasons.append("max_downstream_basin_niet_bepaald")
    if pd.notna(row.get("upstream_check_error")):
        reasons.append("upstream_niet_bepaald")
    return "; ".join(reasons) if reasons else "niet_toegestaan_door_apply_filters"


def not_applied_level_deviations(
    deviations_df: pd.DataFrame,
    allowed_updates_df: pd.DataFrame,
    direct_min_upstream_updates_df: pd.DataFrame,
) -> pd.DataFrame:
    if deviations_df.empty:
        return deviations_df.copy()

    parts = []

    max_mask = deviations_df["max_downstream_level_afwijking"].fillna(False) & ~deviations_df[
        "max_downstream_level_update_allowed"
    ].fillna(False)
    if max_mask.any():
        max_df = deviations_df.loc[max_mask].copy()
        max_df["niet_aangepaste_level_kolom"] = "max_downstream_level"
        parts.append(max_df)

    min_allowed_index = set(allowed_updates_df.index) | set(direct_min_upstream_updates_df.index)
    min_mask = deviations_df["min_upstream_level_afwijking"].fillna(False) & ~deviations_df.index.isin(
        min_allowed_index
    )
    if min_mask.any():
        min_df = deviations_df.loc[min_mask].copy()
        min_df["niet_aangepaste_level_kolom"] = "min_upstream_level"
        parts.append(min_df)

    if not parts:
        return deviations_df.iloc[0:0].copy()

    not_applied_df = pd.concat(parts, ignore_index=True)
    not_applied_df["reden_niet_aangepast"] = not_applied_df.apply(skip_reason, axis=1)
    return not_applied_df


def write_report_gpkg(
    node_df: gpd.GeoDataFrame,
    output_gpkg: Path,
    deviations_df: pd.DataFrame,
    allowed_updates_df: pd.DataFrame,
    direct_min_upstream_updates_df: pd.DataFrame,
    not_applied_df: pd.DataFrame,
    protected_controller_updates_df: pd.DataFrame,
    leaks_df: pd.DataFrame,
    outlet_inlets_df: pd.DataFrame,
) -> Path:
    return write_report_layers(
        node_df=node_df,
        output_gpkg=output_gpkg,
        layers=FULL_REPORT_LAYERS,
        tables={
            "deviations": deviations_df,
            "allowed_updates": allowed_updates_df,
            "direct_min_upstream_updates": direct_min_upstream_updates_df,
            "not_applied": not_applied_df,
            "protected_controller_updates": protected_controller_updates_df,
            "leaks": leaks_df,
            "outlet_inlets": outlet_inlets_df,
        },
        label="Rapport-GPKG",
    )


def write_suspicious_gpkg(
    node_df: gpd.GeoDataFrame,
    output_gpkg: Path,
    not_applied_df: pd.DataFrame,
    protected_controller_updates_df: pd.DataFrame,
    leaks_df: pd.DataFrame,
    outlet_inlets_df: pd.DataFrame,
) -> Path:
    return write_report_layers(
        node_df=node_df,
        output_gpkg=output_gpkg,
        layers=SUSPICIOUS_REPORT_LAYERS,
        tables={
            "not_applied": not_applied_df,
            "protected_controller_updates": protected_controller_updates_df,
            "leaks": leaks_df,
            "outlet_inlets": outlet_inlets_df,
        },
        label="Verdachte-punten-GPKG",
    )
