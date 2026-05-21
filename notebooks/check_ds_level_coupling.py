from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

DEFAULT_DATABASE = Path(r"d:\projecten\D2306.LHM_RIBASIM\02.brongegevens\lhm_coupled_2017\model\input\database.gpkg")
CONTROL_NODE_TYPES = ("Outlet", "Pump")
STATIC_TABLE_BY_NODE_TYPE = {
    "Outlet": "Outlet / static",
    "Pump": "Pump / static",
}

# Bewuste DOD-instellingen:
# - 5900346: Noordscheschutsluis, 1 cm onder downstream streefpeil
# - 5900648: Holthe, 10 cm onder downstream streefpeil
EXCLUDED_DEVIATION_NODE_IDS = {5900346, 5900648}


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
    authority: str | None,
    tolerance: float,
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
    candidate_df["basin_id"] = [node_id for node_id, _, _ in downstream_results]
    candidate_df["downstream_link_id"] = [link_id for _, link_id, _ in downstream_results]
    candidate_df["check_error"] = [error for _, _, error in downstream_results]
    candidate_df["downstream_node_type"] = candidate_df["basin_id"].map(node_type_by_id)
    candidate_df["basin_meta_waterbeheerder"] = candidate_df["basin_id"].map(basin_authority_by_id)
    candidate_df["gecheckte_max_downstream_level"] = candidate_df["basin_id"].map(checked_level_by_basin_id)
    candidate_df["huidig_max_downstream_level"] = normalize_numeric(candidate_df["max_downstream_level"])

    checked_mask = candidate_df["downstream_node_type"].eq("Basin")
    if authority is not None:
        checked_mask &= candidate_df["basin_meta_waterbeheerder"].eq(authority)
    checked_df = candidate_df[checked_mask].copy()
    current = normalize_numeric(checked_df["huidig_max_downstream_level"])
    expected = normalize_numeric(checked_df["gecheckte_max_downstream_level"])
    checked_df["verschil"] = current - expected

    mismatch_mask = expected.notna() & ~np.isclose(
        current.to_numpy(dtype=float),
        expected.to_numpy(dtype=float),
        atol=tolerance,
        rtol=0.0,
        equal_nan=False,
    )
    deviations_df = checked_df.loc[mismatch_mask].copy()
    deviations_df = deviations_df[~deviations_df["node_id"].isin(EXCLUDED_DEVIATION_NODE_IDS)].copy()

    skipped_mask = ~candidate_df["downstream_node_type"].eq("Basin")
    if authority is not None:
        skipped_mask &= candidate_df["meta_waterbeheerder"].eq(authority)
    skipped_df = candidate_df[skipped_mask].copy()
    return checked_df, deviations_df, skipped_df


def print_deviations(deviations_df: pd.DataFrame) -> None:
    columns = [
        "node_id",
        "node_type",
        "functie",
        "name",
        "meta_waterbeheerder",
        "downstream_link_id",
        "basin_id",
        "basin_meta_waterbeheerder",
        "huidig_max_downstream_level",
        "gecheckte_max_downstream_level",
        "verschil",
    ]
    print(deviations_df.sort_values(["meta_waterbeheerder", "basin_id", "node_id"])[columns].to_string(index=False))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Controleer of max_downstream_level van inlaten en doorlaten overeenkomt met "
            "meta_streefpeil van het downstream basin in een Ribasim database.gpkg."
        )
    )
    parser.add_argument("--database", type=Path, default=DEFAULT_DATABASE, help="Pad naar database.gpkg.")
    parser.add_argument(
        "--authority",
        default=None,
        help="Optioneel: filter op downstream Basin Node.meta_waterbeheerder. Standaard wordt het hele model gecontroleerd.",
    )
    parser.add_argument("--tolerance", type=float, default=1e-6, help="Numerieke tolerantie voor de vergelijking.")
    parser.add_argument(
        "--show-skipped",
        action="store_true",
        help="Print ook kandidaat-nodes waarvan downstream geen Basin is of niet gevonden wordt.",
    )
    args = parser.parse_args()

    if not args.database.exists():
        raise FileNotFoundError(args.database)

    with sqlite3.connect(args.database) as con:
        checked_df, deviations_df, skipped_df = build_check_dataframe(
            con=con,
            authority=args.authority,
            tolerance=args.tolerance,
        )

    print(f"Database: {args.database}")
    authority_label = args.authority if args.authority is not None else "alle meta_waterbeheerder"
    print(f"Downstream basin waterbeheerder: {authority_label}")
    print(f"Gecontroleerde inlaat/doorlaat aanvoer-rijen met downstream Basin: {len(checked_df)}")
    print(f"Afwijkingen: {len(deviations_df)}")

    if deviations_df.empty:
        print("Geen afwijkingen gevonden.")
    else:
        print_deviations(deviations_df)

    if args.show_skipped and not skipped_df.empty:
        columns = [
            "node_id",
            "node_type",
            "functie",
            "name",
            "meta_waterbeheerder",
            "downstream_link_id",
            "basin_id",
            "basin_meta_waterbeheerder",
            "downstream_node_type",
            "check_error",
            "huidig_max_downstream_level",
        ]
        print("\nOvergeslagen kandidaat-nodes:")
        print(skipped_df.sort_values(["node_id"])[columns].to_string(index=False))


if __name__ == "__main__":
    main()
