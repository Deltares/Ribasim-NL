# %%
import argparse
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim_nl.settings import settings

from ribasim_nl import Model

TOML_FILE = settings.ribasim_nl_data_dir / Path(
    r"Rijkswaterstaat\modellen\lhm_sub_models\VrijAfwaterend_DOD_Vechtstromen_coupled\VrijAfwaterend_DOD_Vechtstromen_coupled.toml"
)
DEFAULT_OUTPUT_GPKG_NAME = "level_coupling_correcties_selectie.gpkg"
CONTROL_NODE_TYPES = ("Outlet", "Pump")
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
    # Vechtstromen: bewust 1 cm onder downstream streefpeil gehouden.
    4400026: -0.01,
}
EXCLUDED_DEVIATION_NODE_IDS: set[int] = set()
RWS_UPSTREAM_MIN_PROFILE_LEVEL_OFFSET = 0.1
DEFAULT_AFVOER_FLOW_RATES_BY_NODE_ID = {
    5902582: 300.0,
    5900199: 300.0,
    5900198: 300.0,
}


def reset_index_to_column(df: pd.DataFrame, column_name: str) -> pd.DataFrame:
    index_name = df.index.name or "index"
    return df.reset_index().rename(columns={index_name: column_name})


def database_gpkg_path(model: Model, toml_file: Path) -> Path:
    model_dir = toml_file.parent
    input_dir = model_dir / Path(model.input_dir)
    input_database_gpkg = input_dir / "database.gpkg"
    legacy_database_gpkg = model_dir / "database.gpkg"

    if input_database_gpkg.exists():
        return input_database_gpkg
    if legacy_database_gpkg.exists():
        return legacy_database_gpkg

    raise FileNotFoundError(
        f"Kan geen database.gpkg vinden voor model {toml_file}. "
        f"Gezocht in {input_database_gpkg} en {legacy_database_gpkg}."
    )


def default_output_gpkg(toml_file: Path) -> Path:
    return toml_file.parent / DEFAULT_OUTPUT_GPKG_NAME


def resolve_output_gpkg(toml_file: Path, output_gpkg: Path | None) -> Path:
    if output_gpkg is None:
        return default_output_gpkg(toml_file)
    if output_gpkg.is_absolute():
        return output_gpkg
    return toml_file.parent / output_gpkg


def read_static_tables(model: Model) -> pd.DataFrame:
    static_dfs = []
    wanted_columns = [
        "table_row_id",
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

    for node_type, static_table in (("Outlet", model.outlet.static.df), ("Pump", model.pump.static.df)):
        if static_table is None:
            continue

        static_df = reset_index_to_column(static_table.copy(), "table_row_id")
        available_columns = set(static_df.columns)
        columns = [column for column in wanted_columns if column in available_columns]
        static_df = static_df[columns]
        static_df["node_type"] = node_type
        static_df["static_table"] = f"{node_type} / static"
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


def downstream_non_junction_targets(
    node_id: int,
    outgoing_flow_links: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
    max_iter: int = 20,
) -> list[tuple[int | None, int, str | None]]:
    targets = []

    for first_link_id, next_node_id in outgoing_flow_links.get(int(node_id), []):
        current_node_id = int(next_node_id)
        seen_node_ids = {int(node_id), current_node_id}
        error = None

        for _ in range(max_iter):
            next_node_type = node_type_by_id.get(current_node_id)
            if next_node_type != "Junction":
                targets.append((current_node_id, int(first_link_id), None))
                break

            downstream_links = outgoing_flow_links.get(current_node_id, [])
            if len(downstream_links) == 0:
                error = "geen downstream flow-link na Junction"
                break
            if len(downstream_links) > 1:
                error = f"meerdere downstream flow-links na Junction: {downstream_links}"
                break

            _, current_node_id = downstream_links[0]
            current_node_id = int(current_node_id)
            if current_node_id in seen_node_ids:
                error = f"cyclus gevonden via node {current_node_id}"
                break
            seen_node_ids.add(current_node_id)
        else:
            error = f"geen downstream niet-Junction node binnen {max_iter} stappen"

        if error is not None:
            targets.append((None, int(first_link_id), error))

    return targets


def max_downstream_level_basin_info(
    first_downstream_basin_id: int | None,
    outgoing_flow_links: dict[int, list[tuple[int, int]]],
    node_type_by_id: dict[int, str],
    max_iter: int = 50,
) -> tuple[int | None, int | None, str | None, str]:
    """Find the basin level that should drive max_downstream_level.

    A direct downstream basin can be connected further downstream through a
    ManningResistance. In that case, keep walking downstream until a basin is
    reached that has at least one downstream Outlet/Pump. That basin's level is
    the relevant level for inlaat/doorlaat max_downstream_level.
    """
    if first_downstream_basin_id is None:
        return None, None, "geen eerste downstream Basin", "geen_downstream_basin"

    current_basin_id = int(first_downstream_basin_id)
    seen_basin_ids = {current_basin_id}
    first_manning_link_id = None

    for _ in range(max_iter):
        targets = downstream_non_junction_targets(
            node_id=current_basin_id,
            outgoing_flow_links=outgoing_flow_links,
            node_type_by_id=node_type_by_id,
        )
        target_errors = [error for _, _, error in targets if error is not None]
        if target_errors:
            return current_basin_id, first_manning_link_id, "; ".join(target_errors), "downstream_streefpeil"

        control_targets = [
            (node_id, link_id) for node_id, link_id, _ in targets if node_type_by_id.get(node_id) in CONTROL_NODE_TYPES
        ]
        if control_targets:
            basis = (
                "downstream_streefpeil"
                if first_manning_link_id is None
                else "downstream_streefpeil_na_manning_tot_basin_met_outlet_pump"
            )
            return current_basin_id, first_manning_link_id, None, basis

        manning_targets = [
            (node_id, link_id) for node_id, link_id, _ in targets if node_type_by_id.get(node_id) == "ManningResistance"
        ]
        if not manning_targets:
            basis = (
                "downstream_streefpeil"
                if first_manning_link_id is None
                else "downstream_streefpeil_na_manning_tot_basin_zonder_manning"
            )
            return current_basin_id, first_manning_link_id, None, basis
        if len(manning_targets) > 1:
            return (
                current_basin_id,
                first_manning_link_id,
                f"meerdere downstream ManningResistance-nodes: {manning_targets}",
                "downstream_streefpeil",
            )

        manning_node_id, manning_link_id = manning_targets[0]
        if first_manning_link_id is None:
            first_manning_link_id = int(manning_link_id)

        next_basin_id, _, error = downstream_node_info(
            node_id=int(manning_node_id),
            outgoing_flow_links=outgoing_flow_links,
            node_type_by_id=node_type_by_id,
        )
        if error is not None:
            return current_basin_id, first_manning_link_id, error, "downstream_streefpeil"
        if node_type_by_id.get(next_basin_id) != "Basin":
            return (
                current_basin_id,
                first_manning_link_id,
                f"downstream van ManningResistance is geen Basin maar {node_type_by_id.get(next_basin_id)}",
                "downstream_streefpeil",
            )

        current_basin_id = int(next_basin_id)
        if current_basin_id in seen_basin_ids:
            return (
                current_basin_id,
                first_manning_link_id,
                f"cyclus gevonden via Basin {current_basin_id}",
                "downstream_streefpeil",
            )
        seen_basin_ids.add(current_basin_id)

    return (
        current_basin_id,
        first_manning_link_id,
        f"geen max_downstream_level Basin gevonden binnen {max_iter} stappen",
        "downstream_streefpeil",
    )


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


def select_active_control_rows(static_df: pd.DataFrame) -> pd.DataFrame:
    """Select active control rows that can pass water.

    Zero-flow aanvoer rows for drain nodes are excluded, but active afvoer rows
    are included so their min_upstream_level can be checked against the upstream basin.
    """
    control_state = static_df["control_state"].astype("string").str.lower()
    flow_rate = normalize_numeric(static_df["flow_rate"]).fillna(0.0)
    max_flow_rate = normalize_numeric(static_df["max_flow_rate"]).fillna(0.0)
    max_downstream_level = normalize_numeric(static_df["max_downstream_level"])

    active_row = flow_rate.ne(0.0) | max_flow_rate.gt(0.0)
    active_aanvoer = control_state.eq("aanvoer") & (active_row | max_downstream_level.notna())
    active_afvoer = control_state.eq("afvoer") & active_row
    mask = active_aanvoer | active_afvoer
    return static_df.loc[mask].copy()


def add_function_label(candidate_df: pd.DataFrame, all_static_df: pd.DataFrame) -> pd.DataFrame:
    control_state = all_static_df["control_state"].astype("string").str.lower()
    flow_rate = normalize_numeric(all_static_df["flow_rate"]).fillna(0.0)
    afvoer_nodes = set(all_static_df.loc[control_state.eq("afvoer") & flow_rate.gt(0.0), "node_id"].astype(int))

    candidate_df["functie"] = np.where(candidate_df["node_id"].astype(int).isin(afvoer_nodes), "doorlaat", "inlaat")
    return candidate_df


def build_check_dataframe(
    model: Model,
    authorities: tuple[str, ...] | None,
    tolerance: float,
    upstream_supply_offset: float,
    rws_upstream_state_offset: float | None,
    max_rws_upstream_state_level: float,
    include_excluded: bool,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    node_df = reset_index_to_column(model.node.df.copy(), "node_id")
    node_df = node_df[
        [
            "node_id",
            "name",
            "node_type",
            "meta_waterbeheerder",
            "meta_code_waterbeheerder",
        ]
    ]
    node_df = node_df[node_df["node_type"].isin(CONTROL_NODE_TYPES)].copy()

    basin_area_df = model.basin.area.df.copy()[["node_id", "meta_streefpeil"]]
    basin_area_df = basin_area_df.drop_duplicates(subset=["node_id"]).set_index("node_id")
    checked_level_by_basin_id = normalize_numeric(basin_area_df["meta_streefpeil"]).to_dict()
    basin_state_df = model.basin.state.df.copy()[["node_id", "level"]]
    basin_state_df = basin_state_df.drop_duplicates(subset=["node_id"]).set_index("node_id")
    state_level_by_basin_id = normalize_numeric(basin_state_df["level"]).to_dict()
    basin_profile_df = model.basin.profile.df.copy()[["node_id", "level"]]
    basin_profile_df["level"] = normalize_numeric(basin_profile_df["level"])
    min_profile_level_by_basin_id = basin_profile_df.groupby("node_id")["level"].min().to_dict()

    all_node_types_df = reset_index_to_column(model.node.df.copy(), "node_id")[
        ["node_id", "node_type", "meta_waterbeheerder"]
    ]
    node_type_by_id = all_node_types_df.set_index("node_id")["node_type"].to_dict()
    basin_authority_by_id = all_node_types_df.set_index("node_id")["meta_waterbeheerder"].to_dict()

    link_df = reset_index_to_column(model.link.df.copy(), "link_id")[
        ["link_id", "from_node_id", "to_node_id", "link_type"]
    ]
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

    static_df = read_static_tables(model)
    static_df = static_df.merge(
        node_df,
        on=["node_id", "node_type"],
        how="inner",
    )
    candidate_df = select_active_control_rows(static_df)
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
    max_downstream_level_basin_results = [
        max_downstream_level_basin_info(
            first_downstream_basin_id=int(node_id) if node_type_by_id.get(node_id) == "Basin" else None,
            outgoing_flow_links=outgoing_flow_links,
            node_type_by_id=node_type_by_id,
        )
        for node_id in candidate_df["downstream_basin_id"]
    ]
    candidate_df["max_downstream_level_basin_id"] = [node_id for node_id, _, _, _ in max_downstream_level_basin_results]
    candidate_df["max_downstream_level_manning_link_id"] = [
        link_id for _, link_id, _, _ in max_downstream_level_basin_results
    ]
    candidate_df["max_downstream_level_basin_error"] = [error for _, _, error, _ in max_downstream_level_basin_results]
    candidate_df["max_downstream_level_check_basis"] = [basis for _, _, _, basis in max_downstream_level_basin_results]
    candidate_df["max_downstream_level_basin_node_type"] = candidate_df["max_downstream_level_basin_id"].map(
        node_type_by_id
    )
    candidate_df["max_downstream_level_basin_meta_waterbeheerder"] = candidate_df["max_downstream_level_basin_id"].map(
        basin_authority_by_id
    )
    candidate_df["gecheckte_max_downstream_level"] = normalize_numeric(
        candidate_df["max_downstream_level_basin_id"].map(checked_level_by_basin_id)
    )
    candidate_df["max_downstream_level_offset"] = (
        candidate_df["node_id"].map(MAX_DOWNSTREAM_LEVEL_OFFSET_BY_NODE_ID).fillna(0.0)
    )
    max_downstream_offset_mask = (
        candidate_df["max_downstream_level_offset"].ne(0.0) & candidate_df["gecheckte_max_downstream_level"].notna()
    )
    candidate_df.loc[max_downstream_offset_mask, "gecheckte_max_downstream_level"] = (
        candidate_df.loc[max_downstream_offset_mask, "gecheckte_max_downstream_level"]
        + candidate_df.loc[max_downstream_offset_mask, "max_downstream_level_offset"]
    )
    candidate_df.loc[max_downstream_offset_mask, "max_downstream_level_check_basis"] = (
        candidate_df.loc[max_downstream_offset_mask, "max_downstream_level_check_basis"] + "_plus_handmatige_offset"
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
    candidate_df["upstream_basin_min_profile_level"] = normalize_numeric(
        candidate_df["upstream_basin_id"].map(min_profile_level_by_basin_id)
    )
    control_state = candidate_df["control_state"].astype("string").str.lower()
    candidate_df["min_upstream_level_offset"] = np.where(control_state.eq("aanvoer"), upstream_supply_offset, 0.0)
    candidate_df["gecheckte_min_upstream_level"] = (
        candidate_df["upstream_basin_streefpeil"] + candidate_df["min_upstream_level_offset"]
    )
    candidate_df["min_upstream_level_check_basis"] = np.where(
        control_state.eq("aanvoer"), "upstream_streefpeil_plus_aanvoer_offset", "upstream_streefpeil"
    )
    candidate_df["gecheckte_min_upstream_level_is_null"] = False
    rws_basin_mask = candidate_df["upstream_basin_meta_waterbeheerder"].eq("Rijkswaterstaat")
    valid_rws_state_level = candidate_df["upstream_basin_state_level"].le(max_rws_upstream_state_level)
    candidate_df["rws_upstream_state_level_valid"] = (
        rws_basin_mask & candidate_df["upstream_basin_state_level"].notna() & valid_rws_state_level
    )
    rws_state_mask = rws_basin_mask & candidate_df["upstream_basin_state_level"].notna() & valid_rws_state_level
    rws_profile_mask = rws_basin_mask & candidate_df["upstream_basin_min_profile_level"].notna()
    if rws_upstream_state_offset is None:
        candidate_df.loc[rws_profile_mask, "gecheckte_min_upstream_level"] = (
            candidate_df.loc[rws_profile_mask, "upstream_basin_min_profile_level"]
            + RWS_UPSTREAM_MIN_PROFILE_LEVEL_OFFSET
        )
        candidate_df.loc[rws_profile_mask, "min_upstream_level_check_basis"] = (
            "rijkswaterstaat_min_profile_level_plus_offset"
        )
    else:
        candidate_df.loc[rws_state_mask, "gecheckte_min_upstream_level"] = (
            candidate_df.loc[rws_state_mask, "upstream_basin_state_level"] + rws_upstream_state_offset
        )
        candidate_df.loc[rws_state_mask, "min_upstream_level_check_basis"] = "rijkswaterstaat_state_level"
    candidate_df["huidig_min_upstream_level"] = normalize_numeric(candidate_df["min_upstream_level"])

    checked_mask = candidate_df["max_downstream_level_basin_node_type"].eq("Basin")
    if authorities is not None:
        checked_mask &= candidate_df["max_downstream_level_basin_meta_waterbeheerder"].isin(authorities)
    checked_df = candidate_df[checked_mask].copy()
    current_ds = normalize_numeric(checked_df["huidig_max_downstream_level"])
    expected_ds = normalize_numeric(checked_df["gecheckte_max_downstream_level"])
    current_us = normalize_numeric(checked_df["huidig_min_upstream_level"])
    expected_us = normalize_numeric(checked_df["gecheckte_min_upstream_level"])
    explicit_null_us = checked_df["gecheckte_min_upstream_level_is_null"].fillna(False)
    checked_df["verschil_max_downstream_level"] = current_ds - expected_ds
    checked_df["verschil_min_upstream_level"] = current_us - expected_us

    checked_df["max_downstream_level_afwijking"] = (
        checked_df["control_state"].eq("aanvoer")
        & expected_ds.notna()
        & ~np.isclose(
            current_ds.to_numpy(dtype=float),
            expected_ds.to_numpy(dtype=float),
            atol=tolerance,
            rtol=0.0,
            equal_nan=False,
        )
    )
    min_upstream_numeric_afwijking = (
        checked_df["upstream_node_type"].eq("Basin")
        & ~explicit_null_us
        & expected_us.notna()
        & ~np.isclose(
            current_us.to_numpy(dtype=float),
            expected_us.to_numpy(dtype=float),
            atol=tolerance,
            rtol=0.0,
            equal_nan=False,
        )
    )
    min_upstream_null_afwijking = checked_df["upstream_node_type"].eq("Basin") & explicit_null_us & current_us.notna()
    checked_df["min_upstream_level_afwijking"] = min_upstream_numeric_afwijking | min_upstream_null_afwijking
    deviations_df = checked_df.loc[
        checked_df["max_downstream_level_afwijking"] | checked_df["min_upstream_level_afwijking"]
    ].copy()
    if not include_excluded:
        deviations_df = deviations_df[~deviations_df["node_id"].isin(EXCLUDED_DEVIATION_NODE_IDS)].copy()

    skipped_mask = ~candidate_df["max_downstream_level_basin_node_type"].eq("Basin")
    if authorities is not None:
        skipped_mask &= candidate_df["meta_waterbeheerder"].isin(authorities)
    skipped_df = candidate_df[skipped_mask].copy()
    return checked_df, deviations_df, skipped_df


def apply_level_updates(model: Model, deviations_df: pd.DataFrame) -> tuple[int, int, int]:
    max_update_count = 0
    min_update_count = 0
    min_update_count_rws_inlet = 0

    static_df_by_node_type = {
        "Outlet": model.outlet.static.df,
        "Pump": model.pump.static.df,
    }

    for row in deviations_df.itertuples():
        static_df = static_df_by_node_type[row.node_type]
        if static_df is None or row.table_row_id not in static_df.index:
            raise KeyError(f"Kan rij {row.table_row_id} niet vinden in {row.static_table} voor node_id={row.node_id}")

        if bool(row.max_downstream_level_afwijking) and pd.notna(row.gecheckte_max_downstream_level):
            static_df.loc[row.table_row_id, "max_downstream_level"] = float(row.gecheckte_max_downstream_level)
            max_update_count += 1
        if bool(row.min_upstream_level_afwijking):
            if bool(getattr(row, "gecheckte_min_upstream_level_is_null", False)):
                static_df.loc[row.table_row_id, "min_upstream_level"] = np.nan
            elif pd.notna(row.gecheckte_min_upstream_level):
                static_df.loc[row.table_row_id, "min_upstream_level"] = float(row.gecheckte_min_upstream_level)
            else:
                continue
            min_update_count += 1
            if row.functie == "inlaat" and row.upstream_basin_meta_waterbeheerder == "Rijkswaterstaat":
                min_update_count_rws_inlet += 1

    return max_update_count, min_update_count, min_update_count_rws_inlet


def set_selected_authorities_manning_n(model: Model, manning_n: float) -> int:
    manning_node_df = model.manning_resistance.node.df
    manning_static_df = model.manning_resistance.static.df
    if manning_node_df is None or manning_static_df is None:
        return 0
    if "meta_waterbeheerder" not in manning_node_df.columns:
        raise KeyError("Kolom 'meta_waterbeheerder' ontbreekt in ManningResistance / node.")
    if "manning_n" not in manning_static_df.columns:
        raise KeyError("Kolom 'manning_n' ontbreekt in ManningResistance / static.")

    node_ids = manning_node_df.index[manning_node_df["meta_waterbeheerder"].isin(SELECTED_AUTHORITIES)]
    mask = manning_static_df["node_id"].isin(node_ids)
    model.manning_resistance.static.df.loc[mask, "manning_n"] = manning_n
    return int(mask.sum())


def set_afvoer_flow_updates(model: Model, updates: list[tuple[int, float, float | None]]) -> int:
    static_df_by_node_type = {
        "Outlet": model.outlet.static.df,
        "Pump": model.pump.static.df,
    }
    update_count = 0

    for node_id, flow_rate, max_flow_rate in updates:
        node_updated = False
        for static_df in static_df_by_node_type.values():
            if static_df is None:
                continue
            control_state = static_df["control_state"].astype("string").str.lower()
            mask = static_df["node_id"].eq(node_id) & control_state.eq("afvoer")
            if not mask.any():
                continue

            static_df.loc[mask, "flow_rate"] = float(flow_rate)
            if max_flow_rate is not None:
                static_df.loc[mask, "max_flow_rate"] = float(max_flow_rate)
            update_count += int(mask.sum())
            node_updated = True

        if not node_updated:
            raise KeyError(f"Kan geen Outlet/Pump / static afvoer-rij vinden voor node_id={node_id}")

    return update_count


def write_deviation_locations(model: Model, deviations_df: pd.DataFrame, output_gpkg: Path) -> Path:
    output_columns = [
        "node_id",
        "node_type",
        "functie",
        "control_state",
        "name",
        "meta_waterbeheerder",
        "downstream_link_id",
        "downstream_basin_id",
        "downstream_basin_meta_waterbeheerder",
        "max_downstream_level_basin_id",
        "max_downstream_level_basin_meta_waterbeheerder",
        "max_downstream_level_manning_link_id",
        "max_downstream_level_basin_error",
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
        "upstream_basin_min_profile_level",
        "rws_upstream_state_level_valid",
        "min_upstream_level_offset",
        "huidig_min_upstream_level",
        "gecheckte_min_upstream_level",
        "gecheckte_min_upstream_level_is_null",
        "verschil_min_upstream_level",
        "min_upstream_level_check_basis",
        "max_downstream_level_afwijking",
        "min_upstream_level_afwijking",
    ]

    node_gdf = reset_index_to_column(model.node.df.copy(), "node_id")
    node_gdf = node_gdf[["node_id", "geometry"]]
    output_gdf = deviations_df[output_columns].merge(node_gdf, on="node_id", how="left")
    output_gdf = gpd.GeoDataFrame(output_gdf, geometry="geometry", crs=node_gdf.crs)
    max_downstream_output_gdf = output_gdf[output_gdf["max_downstream_level_afwijking"].fillna(False)].copy()
    output_gpkg.parent.mkdir(parents=True, exist_ok=True)
    if output_gpkg.exists():
        try:
            output_gpkg.unlink()
        except PermissionError:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_gpkg = output_gpkg.with_name(f"{output_gpkg.stem}_{timestamp}{output_gpkg.suffix}")
            print(f"Bestaande punten-GPKG is in gebruik; schrijf naar {output_gpkg}")
    output_gdf.to_file(output_gpkg, layer="level_correcties", driver="GPKG")
    if not max_downstream_output_gdf.empty:
        max_downstream_output_gdf.to_file(output_gpkg, layer="max_downstream_level_correcties", driver="GPKG")
    return output_gpkg


def print_deviations(deviations_df: pd.DataFrame) -> None:
    columns = [
        "node_id",
        "node_type",
        "functie",
        "control_state",
        "name",
        "meta_waterbeheerder",
        "downstream_link_id",
        "downstream_basin_id",
        "downstream_basin_meta_waterbeheerder",
        "max_downstream_level_basin_id",
        "max_downstream_level_basin_meta_waterbeheerder",
        "max_downstream_level_manning_link_id",
        "max_downstream_level_basin_error",
        "huidig_max_downstream_level",
        "gecheckte_max_downstream_level",
        "verschil_max_downstream_level",
        "max_downstream_level_offset",
        "max_downstream_level_check_basis",
        "upstream_basin_id",
        "upstream_basin_meta_waterbeheerder",
        "upstream_basin_streefpeil",
        "upstream_basin_state_level",
        "upstream_basin_min_profile_level",
        "rws_upstream_state_level_valid",
        "min_upstream_level_offset",
        "huidig_min_upstream_level",
        "gecheckte_min_upstream_level",
        "gecheckte_min_upstream_level_is_null",
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
            "meta_streefpeil van het downstream basin en min_upstream_level van actieve aanvoer- "
            "en afvoer-rijen met het upstream "
            "streefpeil minus 4 cm in een Ribasim-model. Voor upstream Rijkswaterstaat-basins "
            "wordt standaard Basin / profile.level.min() + 0.1 gebruikt; met een expliciete "
            "Rijkswaterstaat-offset wordt Basin / state.level + offset gebruikt. Leest en schrijft via de "
            "lagen van het model achter de opgegeven TOML."
        )
    )
    parser.add_argument("--toml-file", type=Path, default=TOML_FILE, help="Pad naar een bestaand Ribasim TOML-model.")
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
        default=None,
        help=(
            "Optionele offset voor min_upstream_level als upstream basin Rijkswaterstaat is, "
            "ten opzichte van Basin / state.level. Laat weg om Basin / profile.level.min() + 0.1 te gebruiken."
        ),
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
        help="Pas de gevonden afwijkingen toe in de juiste lagen van het Ribasim-model.",
    )
    parser.add_argument(
        "--set-selected-authorities-manning-n",
        action="store_true",
        help="Zet ManningResistance / static.manning_n voor ManningResistance-nodes van SELECTED_AUTHORITIES.",
    )
    parser.add_argument(
        "--manning-n",
        type=float,
        default=None,
        help=(
            "Optionele waarde voor ManningResistance / static.manning_n voor nodes van SELECTED_AUTHORITIES. "
            "Als opgegeven, wordt de Manning-update automatisch uitgevoerd."
        ),
    )
    parser.add_argument(
        "--afvoer-flow-update",
        action="append",
        nargs=3,
        metavar=("NODE_ID", "FLOW_RATE", "MAX_FLOW_RATE"),
        default=None,
        help=(
            "Herhaalbare update voor Outlet/Pump / static afvoer-rijen: geef node_id, flow_rate en max_flow_rate op."
        ),
    )
    parser.add_argument(
        "--afvoer-flow-rate",
        action="append",
        nargs=2,
        metavar=("NODE_ID", "FLOW_RATE"),
        default=None,
        help=(
            "Herhaalbare update voor Outlet/Pump / static afvoer-rijen; zet zowel flow_rate "
            "als max_flow_rate op de opgegeven waarde."
        ),
    )
    parser.add_argument(
        "--output-gpkg",
        type=Path,
        default=None,
        help="Optioneel pad voor punten-GPKG met aangepaste/afwijkende locaties. Relatieve paden worden naast de TOML geplaatst.",
    )
    parser.add_argument(
        "--show-skipped",
        action="store_true",
        help="Print ook kandidaat-nodes waarvan downstream geen Basin is of niet gevonden wordt.",
    )
    args = parse_args(parser)

    if not args.toml_file.exists():
        raise FileNotFoundError(args.toml_file)

    output_gpkg = resolve_output_gpkg(args.toml_file, args.output_gpkg)
    model = Model.read(args.toml_file)
    database_path = database_gpkg_path(model, args.toml_file)
    model_node_ids = set(model.node.df.index.astype(int))

    if args.selected_authorities:
        authorities = SELECTED_AUTHORITIES
    elif args.authority is not None:
        authorities = tuple(args.authority)
    else:
        authorities = None

    checked_df, deviations_df, skipped_df = build_check_dataframe(
        model=model,
        authorities=authorities,
        tolerance=args.tolerance,
        upstream_supply_offset=args.upstream_supply_offset,
        rws_upstream_state_offset=args.rws_upstream_state_offset,
        max_rws_upstream_state_level=args.max_rws_upstream_state_level,
        include_excluded=args.include_excluded,
    )
    max_update_count = 0
    min_update_count = 0
    min_update_count_rws_inlet = 0
    manning_update_count = 0
    afvoer_flow_update_count = 0
    should_set_manning_n = args.set_selected_authorities_manning_n or args.manning_n is not None
    manning_n = 0.03 if args.manning_n is None else args.manning_n
    afvoer_flow_updates_by_node_id = {
        int(node_id): (float(flow_rate), float(flow_rate))
        for node_id, flow_rate in DEFAULT_AFVOER_FLOW_RATES_BY_NODE_ID.items()
        if int(node_id) in model_node_ids
    }
    if args.afvoer_flow_update is not None:
        afvoer_flow_updates_by_node_id.update(
            {
                int(node_id): (float(flow_rate), float(max_flow_rate))
                for node_id, flow_rate, max_flow_rate in args.afvoer_flow_update
            }
        )
    if args.afvoer_flow_rate is not None:
        afvoer_flow_updates_by_node_id.update(
            {int(node_id): (float(flow_rate), float(flow_rate)) for node_id, flow_rate in args.afvoer_flow_rate}
        )
    afvoer_flow_updates = [
        (node_id, flow_rate, max_flow_rate)
        for node_id, (flow_rate, max_flow_rate) in afvoer_flow_updates_by_node_id.items()
    ]

    if args.apply and not deviations_df.empty:
        max_update_count, min_update_count, min_update_count_rws_inlet = apply_level_updates(model, deviations_df)

    if should_set_manning_n:
        manning_update_count = set_selected_authorities_manning_n(model, manning_n)

    if afvoer_flow_updates:
        afvoer_flow_update_count = set_afvoer_flow_updates(model, afvoer_flow_updates)

    if (args.apply and not deviations_df.empty) or should_set_manning_n or afvoer_flow_updates:
        model.write(args.toml_file)

    print(f"Model-TOML: {args.toml_file}")
    print(f"Model-GPKG: {database_path}")
    authority_label = ", ".join(authorities) if authorities is not None else "alle meta_waterbeheerder"
    print(f"Waterbeheerder-filter: {authority_label}")
    print(f"Gecontroleerde actieve aanvoer/afvoer-rijen met downstream Basin: {len(checked_df)}")
    print(f"Afwijkingen: {len(deviations_df)}")

    if deviations_df.empty:
        print("Geen afwijkingen gevonden.")
    else:
        print_deviations(deviations_df)
        output_gpkg = write_deviation_locations(model, deviations_df, output_gpkg)
        print(f"Punten-GPKG: {output_gpkg}")

    if args.apply:
        print(f"Aangepaste max_downstream_level-waarden: {max_update_count}")
        print(f"Aangepaste min_upstream_level-waarden: {min_update_count}")
        print(f"Aangepaste min_upstream_level-waarden inlaten vanaf RWS: {min_update_count_rws_inlet}")
    if should_set_manning_n:
        print(f"Aangepaste ManningResistance manning_n-waarden: {manning_update_count}")
        print(f"ManningResistance manning_n gezet op: {manning_n}")
    if afvoer_flow_updates:
        print(f"Aangepaste Outlet/Pump afvoer flow_rate/max_flow_rate-rijen: {afvoer_flow_update_count}")

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
            "max_downstream_level_basin_id",
            "max_downstream_level_basin_meta_waterbeheerder",
            "max_downstream_level_manning_link_id",
            "max_downstream_level_basin_error",
            "huidig_max_downstream_level",
        ]
        print("\nOvergeslagen kandidaat-nodes:")
        print(skipped_df.sort_values(["node_id"])[columns].to_string(index=False))


if __name__ == "__main__":
    main()
