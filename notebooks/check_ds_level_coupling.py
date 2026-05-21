# %%
from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd

DEFAULT_DATABASE = Path(r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\lhm_coupled_2017\model\input\database.gpkg")
DEFAULT_OUTPUT_GPKG = Path("notebooks/level_coupling_correcties_selectie.gpkg")
CONTROL_NODE_TYPES = ("Outlet", "Pump")
STATIC_TABLE_BY_NODE_TYPE = {
    "Outlet": "Outlet / static",
    "Pump": "Pump / static",
}
SELECTED_AUTHORITIES = (
    "RijnenIJssel",
    "StichtseRijnlanden",
    "BrabantseDelta",
    "DeDommel",
    "HunzeenAas",
    "Noorderzijlvest",
    "AaenMaas",
    "ValleienVeluwe",
    "Vechtstromen",
    "DrentsOverijsselseDelta",
    "Limburg",
)

MAX_DOWNSTREAM_LEVEL_OFFSET_BY_NODE_ID = {
    # StichtseRijnlanden: gefaseerde inlaten, 1 cm onder downstream streefpeil.
    1400536: -0.01,  # Noordergemaal
    1401344: -0.01,
    1401345: -0.01,
    1400468: -0.01,  # sifon onder ARK
    1400469: -0.01,  # sifon onder ARK
    1400470: -0.01,  # sifon onder ARK
    1400601: -0.01,  # Caspargauw
    # DrentsOverijsselseDelta: bewuste aanvoer-volgorde en handmatige instelling.
    5900346: -0.01,  # Noordscheschutsluis
    5900648: -0.10,  # Holthe
    5903233: -0.01,
}
EXCLUDED_DEVIATION_NODE_IDS: set[int] = set()


def quote_name(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def read_table(con: sqlite3.Connection, table_name: str, columns: list[str] | None = None) -> pd.DataFrame:
    column_sql = "*" if columns is None else ", ".join(quote_name(column) for column in columns)
    return pd.read_sql_query(f"select {column_sql} from {quote_name(table_name)}", con)  # noqa: S608


def table_columns(con: sqlite3.Connection, table_name: str) -> set[str]:
    return {row[1] for row in con.execute(f"pragma table_info({quote_name(table_name)})")}


def read_static_tables(con: sqlite3.Connection) -> pd.DataFrame:
    static_dfs = []
    wanted_columns = [
        "fid",
        "node_id",
        "min_upstream_level",
        "flow_rate",
        "max_flow_rate",
        "max_downstream_level",
        "control_state",
        "meta_categorie",
        "meta_name",
        "meta_code",
    ]

    for node_type, table_name in STATIC_TABLE_BY_NODE_TYPE.items():
        available_columns = table_columns(con, table_name)
        columns = [column for column in wanted_columns if column in available_columns]
        static_df = read_table(con, table_name, columns=columns)
        static_df["node_type"] = node_type
        static_df["static_table"] = table_name
        static_dfs.append(static_df)

    return pd.concat(static_dfs, ignore_index=True)


def normalize_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def downstream_node_info(
    node_id: int,
    outgoing_flow_links: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
    max_iter: int = 20,
) -> tuple[int | None, int | None, str | None]:
    current_node_id = int(node_id)
    seen_node_ids = {current_node_id}
    first_link_id = None

    for _ in range(max_iter):
        downstream_links = outgoing_flow_links.get(current_node_id, [])
        if len(downstream_links) == 0:
            return None, first_link_id, "geen downstream flow-link"
        if len(downstream_links) > 1:
            return None, first_link_id, f"meerdere downstream flow-links: {downstream_links}"

        link_id, next_node_id = downstream_links[0]
        if first_link_id is None:
            first_link_id = int(link_id)
        next_node_id = int(next_node_id)
        if next_node_id in seen_node_ids:
            return None, first_link_id, f"cyclus gevonden via node {next_node_id}"

        next_node_type = node_type_by_id.get(next_node_id)
        if next_node_type != "Junction":
            return next_node_id, first_link_id, None

        current_node_id = next_node_id
        seen_node_ids.add(current_node_id)

    return None, first_link_id, f"geen downstream niet-Junction node binnen {max_iter} stappen"


def upstream_node_info(
    node_id: int,
    incoming_flow_links: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
    max_iter: int = 20,
) -> tuple[int | None, int | None, str | None]:
    current_node_id = int(node_id)
    seen_node_ids = {current_node_id}
    first_link_id = None

    for _ in range(max_iter):
        upstream_links = incoming_flow_links.get(current_node_id, [])
        if len(upstream_links) == 0:
            return None, first_link_id, "geen upstream flow-link"
        if len(upstream_links) > 1:
            return None, first_link_id, f"meerdere upstream flow-links: {upstream_links}"

        link_id, next_node_id = upstream_links[0]
        if first_link_id is None:
            first_link_id = int(link_id)
        next_node_id = int(next_node_id)
        if next_node_id in seen_node_ids:
            return None, first_link_id, f"cyclus gevonden via node {next_node_id}"

        next_node_type = node_type_by_id.get(next_node_id)
        if next_node_type != "Junction":
            return next_node_id, first_link_id, None

        current_node_id = next_node_id
        seen_node_ids.add(current_node_id)

    return None, first_link_id, f"geen upstream niet-Junction node binnen {max_iter} stappen"


def select_inlet_and_flow_control_rows(static_df: pd.DataFrame) -> pd.DataFrame:
    """Select aanvoer-rijen that can pass water in supply mode.

    Drain nodes also have an `aanvoer` control state, but with zero flow. They are
    excluded here because their max_downstream_level is not the relevant check.
    """
    control_state = static_df["control_state"].astype("string").str.lower()
    flow_rate = normalize_numeric(static_df["flow_rate"]).fillna(0.0)
    max_flow_rate = normalize_numeric(static_df["max_flow_rate"]).fillna(0.0)
    max_downstream_level = normalize_numeric(static_df["max_downstream_level"])

    mask = control_state.eq("aanvoer") & (flow_rate.ne(0.0) | max_flow_rate.gt(0.0) | max_downstream_level.notna())
    return static_df.loc[mask].copy()


def add_function_label(candidate_df: pd.DataFrame, all_static_df: pd.DataFrame) -> pd.DataFrame:
    control_state = all_static_df["control_state"].astype("string").str.lower()
    flow_rate = normalize_numeric(all_static_df["flow_rate"]).fillna(0.0)
    afvoer_nodes = set(all_static_df.loc[control_state.eq("afvoer") & flow_rate.gt(0.0), "node_id"].astype(int))

    candidate_df["functie"] = np.where(candidate_df["node_id"].astype(int).isin(afvoer_nodes), "doorlaat", "inlaat")
    return candidate_df


def build_check_dataframe(
    con: sqlite3.Connection,
    authorities: tuple[str, ...] | None,
    tolerance: float,
    upstream_supply_offset: float,
    rws_upstream_state_offset: float,
    max_rws_upstream_state_level: float,
    include_excluded: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    node_df = read_table(
        con,
        "Node",
        columns=[
            "node_id",
            "name",
            "node_type",
            "meta_waterbeheerder",
            "meta_code_waterbeheerder",
        ],
    )
    node_df = node_df[node_df["node_type"].isin(CONTROL_NODE_TYPES)].copy()

    basin_area_df = read_table(con, "Basin / area", columns=["node_id", "meta_streefpeil"])
    basin_area_df = basin_area_df.drop_duplicates(subset=["node_id"]).set_index("node_id")
    checked_level_by_basin_id = normalize_numeric(basin_area_df["meta_streefpeil"]).to_dict()
    basin_state_df = read_table(con, "Basin / state", columns=["node_id", "level"])
    basin_state_df = basin_state_df.drop_duplicates(subset=["node_id"]).set_index("node_id")
    state_level_by_basin_id = normalize_numeric(basin_state_df["level"]).to_dict()

    all_node_types_df = read_table(con, "Node", columns=["node_id", "node_type", "meta_waterbeheerder"])
    node_type_by_id = all_node_types_df.set_index("node_id")["node_type"].to_dict()
    basin_authority_by_id = all_node_types_df.set_index("node_id")["meta_waterbeheerder"].to_dict()

    link_df = read_table(con, "Link", columns=["link_id", "from_node_id", "to_node_id", "link_type"])
    flow_link_df = link_df[link_df["link_type"].eq("flow")].copy()
    outgoing_flow_links = (
        flow_link_df.groupby("from_node_id")[["link_id", "to_node_id"]]
        .apply(lambda rows: [(int(row.link_id), int(row.to_node_id)) for row in rows.itertuples()])
        .to_dict()
    )
    incoming_flow_links = (
        flow_link_df.groupby("to_node_id")[["link_id", "from_node_id"]]
        .apply(lambda rows: [(int(row.link_id), int(row.from_node_id)) for row in rows.itertuples()])
        .to_dict()
    )

    static_df = read_static_tables(con)
    static_df = static_df.merge(
        node_df,
        on=["node_id", "node_type"],
        how="inner",
    )
    candidate_df = select_inlet_and_flow_control_rows(static_df)
    candidate_df = add_function_label(candidate_df, static_df)

    downstream_results = [
        downstream_node_info(
            node_id=int(node_id),
            outgoing_flow_links=outgoing_flow_links,
            node_type_by_id=node_type_by_id,
        )
        for node_id in candidate_df["node_id"]
    ]
    upstream_results = [
        upstream_node_info(
            node_id=int(node_id),
            incoming_flow_links=incoming_flow_links,
            node_type_by_id=node_type_by_id,
        )
        for node_id in candidate_df["node_id"]
    ]
    candidate_df["downstream_basin_id"] = [node_id for node_id, _, _ in downstream_results]
    candidate_df["downstream_link_id"] = [link_id for _, link_id, _ in downstream_results]
    candidate_df["downstream_check_error"] = [error for _, _, error in downstream_results]
    candidate_df["downstream_node_type"] = candidate_df["downstream_basin_id"].map(node_type_by_id)
    candidate_df["downstream_basin_meta_waterbeheerder"] = candidate_df["downstream_basin_id"].map(
        basin_authority_by_id
    )
    candidate_df["gecheckte_max_downstream_level"] = normalize_numeric(
        candidate_df["downstream_basin_id"].map(checked_level_by_basin_id)
    )
    candidate_df["max_downstream_level_offset"] = (
        candidate_df["node_id"].map(MAX_DOWNSTREAM_LEVEL_OFFSET_BY_NODE_ID).fillna(0.0)
    )
    candidate_df["max_downstream_level_check_basis"] = pd.Series("downstream_streefpeil", index=candidate_df.index)
    max_downstream_offset_mask = (
        candidate_df["max_downstream_level_offset"].ne(0.0) & candidate_df["gecheckte_max_downstream_level"].notna()
    )
    candidate_df.loc[max_downstream_offset_mask, "gecheckte_max_downstream_level"] = (
        candidate_df.loc[max_downstream_offset_mask, "gecheckte_max_downstream_level"]
        + candidate_df.loc[max_downstream_offset_mask, "max_downstream_level_offset"]
    )
    candidate_df.loc[max_downstream_offset_mask, "max_downstream_level_check_basis"] = (
        "downstream_streefpeil_plus_handmatige_offset"
    )
    candidate_df["huidig_max_downstream_level"] = normalize_numeric(candidate_df["max_downstream_level"])
    candidate_df["upstream_basin_id"] = [node_id for node_id, _, _ in upstream_results]
    candidate_df["upstream_link_id"] = [link_id for _, link_id, _ in upstream_results]
    candidate_df["upstream_check_error"] = [error for _, _, error in upstream_results]
    candidate_df["upstream_node_type"] = candidate_df["upstream_basin_id"].map(node_type_by_id)
    candidate_df["upstream_basin_meta_waterbeheerder"] = candidate_df["upstream_basin_id"].map(basin_authority_by_id)
    candidate_df["upstream_basin_streefpeil"] = normalize_numeric(
        candidate_df["upstream_basin_id"].map(checked_level_by_basin_id)
    )
    candidate_df["upstream_basin_state_level"] = normalize_numeric(
        candidate_df["upstream_basin_id"].map(state_level_by_basin_id)
    )
    candidate_df["gecheckte_min_upstream_level"] = candidate_df["upstream_basin_streefpeil"] + upstream_supply_offset
    candidate_df["min_upstream_level_check_basis"] = pd.Series("upstream_streefpeil", index=candidate_df.index)
    valid_rws_state_level = candidate_df["upstream_basin_state_level"].le(max_rws_upstream_state_level)
    candidate_df["rws_upstream_state_level_valid"] = (
        candidate_df["upstream_basin_meta_waterbeheerder"].eq("Rijkswaterstaat")
        & candidate_df["upstream_basin_state_level"].notna()
        & valid_rws_state_level
    )
    rws_state_mask = (
        candidate_df["upstream_basin_meta_waterbeheerder"].eq("Rijkswaterstaat")
        & candidate_df["upstream_basin_state_level"].notna()
        & valid_rws_state_level
    )
    candidate_df.loc[rws_state_mask, "gecheckte_min_upstream_level"] = (
        candidate_df.loc[rws_state_mask, "upstream_basin_state_level"] + rws_upstream_state_offset
    )
    candidate_df.loc[rws_state_mask, "min_upstream_level_check_basis"] = "rijkswaterstaat_state_level"
    candidate_df["huidig_min_upstream_level"] = normalize_numeric(candidate_df["min_upstream_level"])

    checked_mask = candidate_df["downstream_node_type"].eq("Basin")
    if authorities is not None:
        checked_mask &= candidate_df["downstream_basin_meta_waterbeheerder"].isin(authorities)
    checked_df = candidate_df[checked_mask].copy()
    current_ds = normalize_numeric(checked_df["huidig_max_downstream_level"])
    expected_ds = normalize_numeric(checked_df["gecheckte_max_downstream_level"])
    current_us = normalize_numeric(checked_df["huidig_min_upstream_level"])
    expected_us = normalize_numeric(checked_df["gecheckte_min_upstream_level"])
    checked_df["verschil_max_downstream_level"] = current_ds - expected_ds
    checked_df["verschil_min_upstream_level"] = current_us - expected_us

    checked_df["max_downstream_level_afwijking"] = expected_ds.notna() & ~np.isclose(
        current_ds.to_numpy(dtype=float),
        expected_ds.to_numpy(dtype=float),
        atol=tolerance,
        rtol=0.0,
        equal_nan=False,
    )
    checked_df["min_upstream_level_afwijking"] = (
        checked_df["upstream_node_type"].eq("Basin")
        & expected_us.notna()
        & ~np.isclose(
            current_us.to_numpy(dtype=float),
            expected_us.to_numpy(dtype=float),
            atol=tolerance,
            rtol=0.0,
            equal_nan=False,
        )
    )
    deviations_df = checked_df.loc[
        checked_df["max_downstream_level_afwijking"] | checked_df["min_upstream_level_afwijking"]
    ].copy()
    if not include_excluded:
        deviations_df = deviations_df[~deviations_df["node_id"].isin(EXCLUDED_DEVIATION_NODE_IDS)].copy()

    skipped_mask = ~candidate_df["downstream_node_type"].eq("Basin")
    if authorities is not None:
        skipped_mask &= candidate_df["meta_waterbeheerder"].isin(authorities)
    skipped_df = candidate_df[skipped_mask].copy()
    return checked_df, deviations_df, skipped_df


def apply_level_updates(con: sqlite3.Connection, deviations_df: pd.DataFrame) -> tuple[int, int]:
    max_update_count = 0
    min_update_count = 0
    for row in deviations_df.itertuples():
        updates = []
        values = []
        if bool(row.max_downstream_level_afwijking) and pd.notna(row.gecheckte_max_downstream_level):
            updates.append(f"{quote_name('max_downstream_level')} = ?")
            values.append(float(row.gecheckte_max_downstream_level))
            max_update_count += 1
        if bool(row.min_upstream_level_afwijking) and pd.notna(row.gecheckte_min_upstream_level):
            updates.append(f"{quote_name('min_upstream_level')} = ?")
            values.append(float(row.gecheckte_min_upstream_level))
            min_update_count += 1
        if not updates:
            continue

        values.append(int(row.fid))
        con.execute(
            f"update {quote_name(row.static_table)} set {', '.join(updates)} where {quote_name('fid')} = ?",  # noqa: S608
            values,
        )
    con.commit()
    return max_update_count, min_update_count


def write_deviation_locations(database_path: Path, deviations_df: pd.DataFrame, output_gpkg: Path) -> None:
    output_columns = [
        "node_id",
        "node_type",
        "functie",
        "name",
        "meta_waterbeheerder",
        "downstream_link_id",
        "downstream_basin_id",
        "downstream_basin_meta_waterbeheerder",
        "huidig_max_downstream_level",
        "gecheckte_max_downstream_level",
        "verschil_max_downstream_level",
        "max_downstream_level_offset",
        "max_downstream_level_check_basis",
        "upstream_link_id",
        "upstream_basin_id",
        "upstream_basin_meta_waterbeheerder",
        "upstream_basin_streefpeil",
        "upstream_basin_state_level",
        "rws_upstream_state_level_valid",
        "huidig_min_upstream_level",
        "gecheckte_min_upstream_level",
        "verschil_min_upstream_level",
        "min_upstream_level_check_basis",
        "max_downstream_level_afwijking",
        "min_upstream_level_afwijking",
    ]
    node_gdf = gpd.read_file(database_path, layer="Node", fid_as_index=True).reset_index(names="node_id")
    node_gdf = node_gdf[["node_id", "geometry"]]
    output_gdf = deviations_df[output_columns].merge(node_gdf, on="node_id", how="left")
    output_gdf = gpd.GeoDataFrame(output_gdf, geometry="geometry", crs=node_gdf.crs)
    if output_gpkg.exists():
        try:
            output_gpkg.unlink()
        except PermissionError:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_gpkg = output_gpkg.with_name(f"{output_gpkg.stem}_{timestamp}{output_gpkg.suffix}")
            print(f"Bestaande punten-GPKG is in gebruik; schrijf naar {output_gpkg}")
    output_gdf.to_file(output_gpkg, layer="level_correcties", driver="GPKG")


def print_deviations(deviations_df: pd.DataFrame) -> None:
    columns = [
        "node_id",
        "node_type",
        "functie",
        "name",
        "meta_waterbeheerder",
        "downstream_link_id",
        "downstream_basin_id",
        "downstream_basin_meta_waterbeheerder",
        "huidig_max_downstream_level",
        "gecheckte_max_downstream_level",
        "verschil_max_downstream_level",
        "max_downstream_level_offset",
        "max_downstream_level_check_basis",
        "upstream_basin_id",
        "upstream_basin_meta_waterbeheerder",
        "upstream_basin_streefpeil",
        "upstream_basin_state_level",
        "rws_upstream_state_level_valid",
        "huidig_min_upstream_level",
        "gecheckte_min_upstream_level",
        "verschil_min_upstream_level",
        "min_upstream_level_check_basis",
    ]
    print(
        deviations_df.sort_values(["meta_waterbeheerder", "downstream_basin_id", "node_id"])[columns].to_string(
            index=False
        )
    )


def parse_args(parser: argparse.ArgumentParser) -> argparse.Namespace:
    args, unknown_args = parser.parse_known_args()
    if unknown_args:
        print(f"Genegeerde argumenten: {' '.join(unknown_args)}")
    return args


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Controleer of max_downstream_level van inlaten en doorlaten overeenkomt met "
            "meta_streefpeil van het downstream basin en min_upstream_level met het upstream "
            "streefpeil minus 4 cm in een Ribasim database.gpkg. Voor upstream Rijkswaterstaat-basins "
            "wordt Basin / state.level gebruikt met een aparte offset."
        )
    )
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE, help="Pad naar database.gpkg.")
    parser.add_argument(
        "--authority",
        action="append",
        default=None,
        help="Optioneel, herhaalbaar: filter op downstream Basin Node.meta_waterbeheerder.",
    )
    parser.add_argument(
        "--selected-authorities",
        action="store_true",
        help="Gebruik de eerder gekozen selectie waterschappen.",
    )
    parser.add_argument("--tolerance", type=float, default=1e-6, help="Numerieke tolerantie voor de vergelijking.")
    parser.add_argument(
        "--upstream-supply-offset",
        type=float,
        default=-0.04,
        help="Offset voor min_upstream_level bij aanvoer/inlaat ten opzichte van upstream meta_streefpeil.",
    )
    parser.add_argument(
        "--rws-upstream-state-offset",
        type=float,
        default=-2.0,
        help="Offset voor min_upstream_level als upstream basin Rijkswaterstaat is, ten opzichte van Basin / state.level.",
    )
    parser.add_argument(
        "--max-rws-upstream-state-level",
        type=float,
        default=100.0,
        help="Gebruik Rijkswaterstaat Basin / state.level alleen tot en met deze waarde.",
    )
    parser.add_argument(
        "--include-excluded",
        action="store_true",
        help="Neem ook bewust uitgezonderde nodes mee.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Pas de gevonden afwijkingen toe in de database.gpkg.",
    )
    parser.add_argument(
        "--output-gpkg",
        type=Path,
        default=DEFAULT_OUTPUT_GPKG,
        help="Pad voor punten-GPKG met aangepaste/afwijkende locaties.",
    )
    parser.add_argument(
        "--show-skipped",
        action="store_true",
        help="Print ook kandidaat-nodes waarvan downstream geen Basin is of niet gevonden wordt.",
    )
    args = parse_args(parser)

    if not args.database.exists():
        raise FileNotFoundError(args.database)

    if args.selected_authorities:
        authorities = SELECTED_AUTHORITIES
    elif args.authority is not None:
        authorities = tuple(args.authority)
    else:
        authorities = None

    with sqlite3.connect(args.database) as con:
        checked_df, deviations_df, skipped_df = build_check_dataframe(
            con=con,
            authorities=authorities,
            tolerance=args.tolerance,
            upstream_supply_offset=args.upstream_supply_offset,
            rws_upstream_state_offset=args.rws_upstream_state_offset,
            max_rws_upstream_state_level=args.max_rws_upstream_state_level,
            include_excluded=args.include_excluded,
        )
        max_update_count = 0
        min_update_count = 0
        if args.apply and not deviations_df.empty:
            max_update_count, min_update_count = apply_level_updates(con, deviations_df)

    print(f"Database: {args.database}")
    authority_label = ", ".join(authorities) if authorities is not None else "alle meta_waterbeheerder"
    print(f"Waterbeheerder-filter: {authority_label}")
    print(f"Gecontroleerde inlaat/doorlaat aanvoer-rijen met downstream Basin: {len(checked_df)}")
    print(f"Afwijkingen: {len(deviations_df)}")

    if deviations_df.empty:
        print("Geen afwijkingen gevonden.")
    else:
        print_deviations(deviations_df)
        write_deviation_locations(args.database, deviations_df, args.output_gpkg)
        print(f"Punten-GPKG: {args.output_gpkg}")

    if args.apply:
        print(f"Aangepaste max_downstream_level-waarden: {max_update_count}")
        print(f"Aangepaste min_upstream_level-waarden: {min_update_count}")

    if args.show_skipped and not skipped_df.empty:
        columns = [
            "node_id",
            "node_type",
            "functie",
            "name",
            "meta_waterbeheerder",
            "downstream_link_id",
            "downstream_basin_id",
            "downstream_basin_meta_waterbeheerder",
            "downstream_node_type",
            "downstream_check_error",
            "huidig_max_downstream_level",
        ]
        print("\nOvergeslagen kandidaat-nodes:")
        print(skipped_df.sort_values(["node_id"])[columns].to_string(index=False))


if __name__ == "__main__":
    main()
