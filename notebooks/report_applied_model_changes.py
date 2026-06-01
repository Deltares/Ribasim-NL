from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd

STATIC_TABLES = {
    "Outlet / static": ["min_upstream_level", "max_downstream_level", "flow_rate", "min_flow_rate", "max_flow_rate"],
    "Pump / static": ["min_upstream_level", "max_downstream_level", "flow_rate", "min_flow_rate", "max_flow_rate"],
}


def database_gpkg_path(toml_file: Path) -> Path:
    input_database_gpkg = toml_file.parent / "input" / "database.gpkg"
    legacy_database_gpkg = toml_file.parent / "database.gpkg"
    if input_database_gpkg.exists():
        return input_database_gpkg
    if legacy_database_gpkg.exists():
        return legacy_database_gpkg
    raise FileNotFoundError(f"Geen database.gpkg gevonden naast {toml_file}.")


def latest_backup(database_path: Path, prefix: str) -> Path | None:
    backups = sorted(
        database_path.parent.glob(f"{database_path.stem}_{prefix}_*.gpkg"), key=lambda path: path.stat().st_mtime
    )
    return backups[-1] if backups else None


def resolve_output_gpkg(toml_file: Path, output_gpkg: Path | None) -> Path:
    if output_gpkg is None:
        return toml_file.with_name("toegepaste_model_wijzigingen.gpkg")
    if output_gpkg.is_absolute():
        return output_gpkg
    return toml_file.parent / output_gpkg


def read_table(database_path: Path, table: str) -> pd.DataFrame:
    with sqlite3.connect(database_path) as con:
        return pd.read_sql_query(f'SELECT * FROM "{table}"', con)  # noqa: S608


def different(left: pd.Series, right: pd.Series) -> pd.Series:
    left_missing = left.isna()
    right_missing = right.isna()
    return (left_missing != right_missing) | (left.notna() & right.notna() & left.ne(right))


def compare_table(before_path: Path, after_path: Path, table: str, columns: list[str], label: str) -> pd.DataFrame:
    before_df = read_table(before_path, table)
    after_df = read_table(after_path, table)
    if "fid" not in before_df.columns or "fid" not in after_df.columns:
        return pd.DataFrame()

    before_df = before_df.set_index("fid", drop=False)
    after_df = after_df.set_index("fid", drop=False)
    common_fids = before_df.index.intersection(after_df.index)
    records = []

    for fid in common_fids:
        before_row = before_df.loc[fid]
        after_row = after_df.loc[fid]
        changed_columns = [
            column
            for column in columns
            if column in before_df.columns
            and column in after_df.columns
            and bool(different(pd.Series([before_row[column]]), pd.Series([after_row[column]])).iloc[0])
        ]
        if not changed_columns:
            continue

        node_id = after_row.get("node_id", before_row.get("node_id"))
        record = {
            "stap": label,
            "static_table": table,
            "static_fid": int(fid),
            "node_id": int(node_id) if pd.notna(node_id) else None,
            "control_state": after_row.get("control_state"),
            "gewijzigde_kolommen": ",".join(changed_columns),
        }
        for column in changed_columns:
            record[f"oud_{column}"] = before_row[column]
            record[f"nieuw_{column}"] = after_row[column]
        records.append(record)

    return pd.DataFrame(records)


def compare_databases(before_path: Path, after_path: Path, label: str) -> pd.DataFrame:
    parts = []
    for table, columns in STATIC_TABLES.items():
        parts.append(compare_table(before_path, after_path, table, columns, label))
    parts = [part for part in parts if not part.empty]
    if not parts:
        return pd.DataFrame()
    return pd.concat(parts, ignore_index=True)


def add_geometry(database_path: Path, changes_df: pd.DataFrame) -> gpd.GeoDataFrame:
    node_gdf = gpd.read_file(database_path, layer="Node", engine="pyogrio", fid_as_index=True).reset_index(
        names="node_id"
    )
    node_gdf = node_gdf[["node_id", "node_type", "name", "meta_waterbeheerder", "meta_code_waterbeheerder", "geometry"]]
    if changes_df.empty:
        return gpd.GeoDataFrame(changes_df, geometry=[], crs=node_gdf.crs)
    gdf = changes_df.merge(node_gdf, on="node_id", how="left")
    return gpd.GeoDataFrame(gdf, geometry="geometry", crs=node_gdf.crs)


def write_changes(
    output_gpkg: Path,
    database_path: Path,
    all_changes_df: pd.DataFrame,
    step_changes: dict[str, pd.DataFrame],
) -> Path:
    if output_gpkg.exists():
        try:
            output_gpkg.unlink()
        except PermissionError:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_gpkg = output_gpkg.with_name(f"{output_gpkg.stem}_{timestamp}{output_gpkg.suffix}")
            print(f"Wijzigingen-GPKG is in gebruik; schrijf naar alternatief bestand: {output_gpkg}", flush=True)
    output_gpkg.parent.mkdir(parents=True, exist_ok=True)

    add_geometry(database_path, all_changes_df).to_file(output_gpkg, layer="alle_wijzigingen", driver="GPKG")
    for layer, changes_df in step_changes.items():
        add_geometry(database_path, changes_df).to_file(output_gpkg, layer=layer, driver="GPKG")
    return output_gpkg


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Schrijf een GPKG met werkelijk toegepaste modelwijzigingen.")
    parser.add_argument("--toml-file", type=Path, required=True)
    parser.add_argument("--output-gpkg", type=Path, default=None)
    parser.add_argument("--before-report-gpkg", type=Path, default=None)
    parser.add_argument("--before-check-gpkg", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    database_path = database_gpkg_path(args.toml_file)
    output_gpkg = resolve_output_gpkg(args.toml_file, args.output_gpkg)
    before_report = args.before_report_gpkg or latest_backup(database_path, "before_report_coupling_levels")
    before_check = args.before_check_gpkg or latest_backup(database_path, "before_check_coupling_levels")

    if before_report is None:
        raise FileNotFoundError(f"Geen before_report_coupling_levels backup gevonden in {database_path.parent}.")

    step_changes: dict[str, pd.DataFrame] = {}
    if before_check is not None and before_check.stat().st_mtime >= before_report.stat().st_mtime:
        step_changes["wijzigingen_report_coupling_levels"] = compare_databases(
            before_report, before_check, "report_coupling_levels"
        )
        step_changes["wijzigingen_check_coupling_levels"] = compare_databases(
            before_check, database_path, "check_coupling_levels"
        )
    else:
        step_changes["wijzigingen_sinds_report_backup"] = compare_databases(
            before_report, database_path, "sinds_report_backup"
        )

    all_changes_df = compare_databases(before_report, database_path, "totaal")
    output_gpkg = write_changes(output_gpkg, database_path, all_changes_df, step_changes)

    print(f"Model-GPKG: {database_path}")
    print(f"Backup voor report: {before_report}")
    if before_check is not None:
        print(f"Backup voor check: {before_check}")
    print(f"Wijzigingen-GPKG: {output_gpkg}")
    print(f"Alle wijzigingen: {len(all_changes_df)}")
    for layer, changes_df in step_changes.items():
        print(f"{layer}: {len(changes_df)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
