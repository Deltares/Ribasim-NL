# %%
import argparse
import shutil
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim_nl.control_layout import control_condition_thresholds, control_layout_key, control_layouts
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
LIMBURG_HANDMATIGE_LEVEL_NODE_IDS = {2496, 2497, 6002496, 6002497}
EXCLUDED_MIN_UPSTREAM_LEVEL_NODE_IDS = {3803097, *LIMBURG_HANDMATIGE_LEVEL_NODE_IDS}
EXCLUDED_MAX_DOWNSTREAM_LEVEL_NODE_IDS = LIMBURG_HANDMATIGE_LEVEL_NODE_IDS
EXCLUDED_MAX_DOWNSTREAM_LEVEL_AUTHORITIES = {"WetterskipFryslan"}
EXCLUDED_MIN_UPSTREAM_LEVEL_AUTHORITIES = {"WetterskipFryslan"}
RWS_UPSTREAM_MIN_PROFILE_LEVEL_OFFSET = 0.1
DEFAULT_UPSTREAM_SUPPLY_OFFSET = -0.04
UPSTREAM_SUPPLY_OFFSET = DEFAULT_UPSTREAM_SUPPLY_OFFSET
RWS_UPSTREAM_STATE_OFFSET: float | None = None
MAX_RWS_UPSTREAM_STATE_LEVEL = 100.0
INCLUDE_EXCLUDED = False
APPLY_LEVEL_UPDATES = False
UPDATE_CONTROL_NAMES = False
SET_SELECTED_AUTHORITIES_MANNING_N = False
SELECTED_AUTHORITIES_MANNING_N = 0.03


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


def backup_database_gpkg(database_path: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = database_path.with_name(f"{database_path.stem}_before_check_coupling_levels_{timestamp}.gpkg")
    shutil.copy2(database_path, backup_path)
    return backup_path


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


def positive(value: object) -> bool:
    try:
        if value is None or pd.isna(value):
            return False
        return float(value) > 0.0
    except (TypeError, ValueError):
        return False


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
) -> tuple[int | None, int | None, str | None, str]:
    if first_downstream_basin_id is None:
        return None, None, "geen eerste downstream Basin", "geen_downstream_basin"

    return int(first_downstream_basin_id), None, None, "downstream_streefpeil"


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
    max_flow_rate = normalize_numeric(all_static_df["max_flow_rate"]).fillna(0.0)
    active_capacity = flow_rate.gt(0.0) | max_flow_rate.gt(0.0)
    aanvoer_nodes = set(all_static_df.loc[control_state.eq("aanvoer") & active_capacity, "node_id"].astype(int))
    afvoer_nodes = set(all_static_df.loc[control_state.eq("afvoer") & active_capacity, "node_id"].astype(int))
    doorlaat_nodes = aanvoer_nodes & afvoer_nodes
    uitlaat_nodes = afvoer_nodes - aanvoer_nodes

    candidate_df["functie"] = "inlaat"
    candidate_df.loc[candidate_df["node_id"].astype(int).isin(doorlaat_nodes), "functie"] = "doorlaat"
    candidate_df.loc[candidate_df["node_id"].astype(int).isin(uitlaat_nodes), "functie"] = "uitlaat"
    return candidate_df


def flow_demand_controlled_node_ids(link_df: pd.DataFrame, node_type_by_id: dict[int, str]) -> set[int]:
    """Return Outlet/Pump node_ids controlled by a FlowDemand node."""
    if link_df.empty:
        return set()

    flow_demand_links = link_df[
        link_df["from_node_id"].map(node_type_by_id).eq("FlowDemand")
        & link_df["to_node_id"].map(node_type_by_id).isin(CONTROL_NODE_TYPES)
    ]
    return set(flow_demand_links["to_node_id"].astype(int))


def model_flow_demand_controlled_node_ids(model: Model) -> set[int]:
    node_type_by_id = reset_index_to_column(model.node.df.copy(), "node_id").set_index("node_id")["node_type"].to_dict()
    link_df = reset_index_to_column(model.link.df.copy(), "link_id")[["from_node_id", "to_node_id", "link_type"]]
    return flow_demand_controlled_node_ids(link_df=link_df, node_type_by_id=node_type_by_id)


def dataframe_snapshot(df: pd.DataFrame | None, node_ids: set[int] | None = None) -> pd.DataFrame:
    if df is None:
        return pd.DataFrame()
    snapshot = df.copy()
    if node_ids is not None:
        snapshot = snapshot[snapshot["node_id"].astype(int).isin(node_ids)].copy() if node_ids else snapshot.iloc[0:0]
    return snapshot.sort_index(axis=0).sort_index(axis=1).reset_index(drop=True)


def protected_apply_snapshots(model: Model) -> dict[str, pd.DataFrame]:
    flow_demand_node_ids = model_flow_demand_controlled_node_ids(model)
    return {
        "DiscreteControl / variable": dataframe_snapshot(model.discrete_control.variable.df),
        "DiscreteControl / logic": dataframe_snapshot(model.discrete_control.logic.df),
        "Outlet / static flow_demand": dataframe_snapshot(model.outlet.static.df, flow_demand_node_ids),
        "Pump / static flow_demand": dataframe_snapshot(model.pump.static.df, flow_demand_node_ids),
    }


def assert_snapshot_unchanged(label: str, before: pd.DataFrame, after: pd.DataFrame) -> None:
    try:
        pd.testing.assert_frame_equal(before, after, check_dtype=False, check_like=False)
    except AssertionError as error:
        raise RuntimeError(f"{label} is onverwacht aangepast door check_coupling_levels.") from error


def assert_protected_apply_snapshots_unchanged(
    model: Model,
    snapshots: dict[str, pd.DataFrame],
) -> None:
    current = protected_apply_snapshots(model)
    for label, before in snapshots.items():
        assert_snapshot_unchanged(label, before, current[label])


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
    flow_demand_node_ids = flow_demand_controlled_node_ids(link_df=link_df, node_type_by_id=node_type_by_id)
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
    candidate_df["flow_demand_controlled"] = candidate_df["node_id"].astype(int).isin(flow_demand_node_ids)

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
    candidate_df["max_downstream_is_coupling_link"] = (
        candidate_df["meta_waterbeheerder"].notna()
        & candidate_df["downstream_basin_meta_waterbeheerder"].notna()
        & candidate_df["meta_waterbeheerder"].ne(candidate_df["downstream_basin_meta_waterbeheerder"])
    )
    candidate_df["downstream_basin_streefpeil"] = normalize_numeric(
        candidate_df["downstream_basin_id"].map(checked_level_by_basin_id)
    )
    max_downstream_level_basin_results = [
        max_downstream_level_basin_info(
            first_downstream_basin_id=int(node_id) if node_type_by_id.get(node_id) == "Basin" else None
        )
        for row, node_id in zip(candidate_df.itertuples(index=False), candidate_df["downstream_basin_id"], strict=True)
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
    candidate_df["rws_to_model_link"] = (
        candidate_df["upstream_basin_meta_waterbeheerder"].eq("Rijkswaterstaat")
        & candidate_df["downstream_basin_meta_waterbeheerder"].notna()
        & candidate_df["downstream_basin_meta_waterbeheerder"].ne("Rijkswaterstaat")
    )
    candidate_df["min_upstream_is_coupling_link"] = candidate_df["rws_to_model_link"] | (
        candidate_df["meta_waterbeheerder"].notna()
        & candidate_df["upstream_basin_meta_waterbeheerder"].notna()
        & candidate_df["meta_waterbeheerder"].ne(candidate_df["upstream_basin_meta_waterbeheerder"])
    )
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
    supply_side_mask = control_state.eq("aanvoer") & candidate_df["functie"].isin(["inlaat", "doorlaat"])
    candidate_df["min_upstream_level_offset"] = np.where(supply_side_mask, upstream_supply_offset, 0.0)
    candidate_df["gecheckte_min_upstream_level"] = (
        candidate_df["upstream_basin_streefpeil"] + candidate_df["min_upstream_level_offset"]
    )
    candidate_df["min_upstream_level_check_basis"] = np.where(
        supply_side_mask, "upstream_streefpeil_plus_aanvoer_offset", "upstream_streefpeil"
    )
    candidate_df["gecheckte_min_upstream_level_is_null"] = False
    candidate_df["loop_min_upstream_basin_id"] = np.nan
    candidate_df["loop_min_upstream_manning_link_id"] = np.nan
    candidate_df["loop_min_upstream_error"] = "manning_doorloop_uitgeschakeld"
    candidate_df["loop_min_upstream_level"] = np.nan
    rws_basin_mask = candidate_df["upstream_basin_meta_waterbeheerder"].eq("Rijkswaterstaat")
    valid_rws_state_level = candidate_df["upstream_basin_state_level"].le(max_rws_upstream_state_level)
    candidate_df["rws_upstream_state_level_valid"] = (
        rws_basin_mask & candidate_df["upstream_basin_state_level"].notna() & valid_rws_state_level
    )
    rws_inlet_aanvoer_mask = control_state.eq("aanvoer") & candidate_df["functie"].eq("inlaat") & rws_basin_mask
    rws_min_upstream_override_mask = rws_inlet_aanvoer_mask
    rws_state_mask = (
        rws_min_upstream_override_mask
        & rws_basin_mask
        & candidate_df["upstream_basin_state_level"].notna()
        & valid_rws_state_level
    )
    rws_profile_mask = (
        rws_min_upstream_override_mask & rws_basin_mask & candidate_df["upstream_basin_min_profile_level"].notna()
    )
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
    flow_demand_mask = candidate_df["flow_demand_controlled"]
    candidate_df.loc[flow_demand_mask, "gecheckte_max_downstream_level"] = candidate_df.loc[
        flow_demand_mask, "huidig_max_downstream_level"
    ]
    candidate_df.loc[flow_demand_mask, "gecheckte_min_upstream_level"] = candidate_df.loc[
        flow_demand_mask, "huidig_min_upstream_level"
    ]
    candidate_df.loc[flow_demand_mask, "max_downstream_level_check_basis"] = "huidige_waarde_flow_demand_beschermd"
    candidate_df.loc[flow_demand_mask, "min_upstream_level_check_basis"] = "huidige_waarde_flow_demand_beschermd"
    keep_current_min_upstream_mask = (
        candidate_df["gecheckte_min_upstream_level"].isna()
        & ~candidate_df["gecheckte_min_upstream_level_is_null"].fillna(False)
        & candidate_df["huidig_min_upstream_level"].notna()
    )
    candidate_df.loc[keep_current_min_upstream_mask, "gecheckte_min_upstream_level"] = candidate_df.loc[
        keep_current_min_upstream_mask, "huidig_min_upstream_level"
    ]
    candidate_df.loc[keep_current_min_upstream_mask, "min_upstream_level_check_basis"] = (
        "huidige_waarde_gebruikt_omdat_streefpeil_ontbreekt"
    )

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
        & checked_df["functie"].isin(["inlaat", "doorlaat"])
        & checked_df["max_downstream_is_coupling_link"]
        & ~checked_df["meta_waterbeheerder"].isin(EXCLUDED_MAX_DOWNSTREAM_LEVEL_AUTHORITIES)
        & ~checked_df["node_id"].isin(EXCLUDED_MAX_DOWNSTREAM_LEVEL_NODE_IDS)
        & ~checked_df["flow_demand_controlled"]
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
        & checked_df["min_upstream_is_coupling_link"]
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
    min_upstream_null_afwijking = (
        checked_df["upstream_node_type"].eq("Basin")
        & checked_df["min_upstream_is_coupling_link"]
        & explicit_null_us
        & current_us.notna()
    )
    connected_to_rws_mask = checked_df["upstream_basin_meta_waterbeheerder"].eq("Rijkswaterstaat") | checked_df[
        "downstream_basin_meta_waterbeheerder"
    ].eq("Rijkswaterstaat")
    afvoer_hoger_eigen_basin_mask = (
        checked_df["control_state"].eq("afvoer")
        & checked_df["downstream_basin_streefpeil"].notna()
        & checked_df["upstream_basin_streefpeil"].notna()
        & checked_df["downstream_basin_streefpeil"].gt(checked_df["upstream_basin_streefpeil"])
        & ~connected_to_rws_mask
    )
    checked_df["min_upstream_level_afwijking"] = (
        (min_upstream_numeric_afwijking | min_upstream_null_afwijking)
        & ~checked_df["meta_waterbeheerder"].isin(EXCLUDED_MIN_UPSTREAM_LEVEL_AUTHORITIES)
        & ~checked_df["node_id"].isin(EXCLUDED_MIN_UPSTREAM_LEVEL_NODE_IDS)
        & ~afvoer_hoger_eigen_basin_mask
        & ~checked_df["flow_demand_controlled"]
    )
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


def format_control_level(value: float | int | None) -> str | None:
    if pd.isna(value):
        return None
    return f"{float(value):.2f}"


def control_node_ids_by_target_node_id(model: Model) -> dict[int, list[int]]:
    node_type_by_id = reset_index_to_column(model.node.df.copy(), "node_id").set_index("node_id")["node_type"].to_dict()
    link_df = reset_index_to_column(model.link.df.copy(), "link_id")[["from_node_id", "to_node_id", "link_type"]]
    control_link_df = link_df[
        link_df["from_node_id"].map(node_type_by_id).eq("DiscreteControl")
        & link_df["to_node_id"].map(node_type_by_id).isin(CONTROL_NODE_TYPES)
    ].copy()
    if control_link_df.empty:
        return {}
    return (
        control_link_df.groupby("to_node_id")["from_node_id"]
        .apply(lambda values: [int(value) for value in values])
        .to_dict()
    )


def grouped_compound_counts(df: pd.DataFrame | None, control_node_id: int) -> dict[int, int]:
    if df is None or df.empty:
        return {}
    rows = df[df["node_id"].eq(int(control_node_id))]
    if rows.empty:
        return {}
    return {
        int(compound_variable_id): int(count)
        for compound_variable_id, count in rows.groupby("compound_variable_id").size().items()
        if pd.notna(compound_variable_id)
    }


def control_logic_pairs(model: Model, control_node_id: int) -> set[tuple[str, str]]:
    logic_df = model.discrete_control.logic.df
    if logic_df is None or logic_df.empty:
        return set()
    rows = logic_df[logic_df["node_id"].eq(int(control_node_id))]
    return {(str(row.truth_state), str(row.control_state).lower()) for row in rows.itertuples(index=False)}


def control_node_name(model: Model, control_node_id: int) -> str | None:
    if control_node_id not in model.node.df.index:
        return None
    name = model.node.df.at[int(control_node_id), "name"]
    return None if pd.isna(name) else str(name)


def explicit_control_layout_from_name(control_name: str | None) -> str | None:
    if control_name is None or ":" not in control_name:
        return None
    layout_key = control_name.split(":", 1)[0].strip().lower()
    return layout_key if layout_key in control_layouts() else None


def has_intentional_named_layout_mismatch(function: str, control_name: str | None) -> bool:
    layout_key = explicit_control_layout_from_name(control_name)
    if layout_key is None:
        return False
    return layout_key != str(function).lower()


def validate_control_layout_for_sync(
    model: Model,
    *,
    target_node_id: int,
    control_node_id: int,
    function: str,
    flow_demand_controlled: bool = False,
) -> str:
    layout_key = control_layout_key(
        function=function,
        flow_demand_controlled=flow_demand_controlled,
        control_name=control_node_name(model, int(control_node_id)),
    )
    expected_layout = control_layouts().get(layout_key)
    if expected_layout is None:
        return layout_key

    expected_variable_counts, expected_condition_counts, expected_logic_pairs = expected_layout
    variable_counts = grouped_compound_counts(model.discrete_control.variable.df, int(control_node_id))
    condition_counts = grouped_compound_counts(model.discrete_control.condition.df, int(control_node_id))
    found_logic_pairs = control_logic_pairs(model, int(control_node_id))
    if (
        variable_counts != expected_variable_counts
        or condition_counts != expected_condition_counts
        or found_logic_pairs != expected_logic_pairs
    ):
        raise ValueError(
            f"{layout_key} #{target_node_id} met DiscreteControl #{control_node_id} heeft geen control.py-layout: "
            f"variables={variable_counts} verwacht={expected_variable_counts}; "
            f"conditions={condition_counts} verwacht={expected_condition_counts}; "
            f"logic={sorted(found_logic_pairs)} verwacht={sorted(expected_logic_pairs)}."
        )
    return layout_key


def control_name_prefix(current_name: object, fallback: object) -> str:
    if isinstance(current_name, str) and ":" in current_name:
        prefix = current_name.split(":", 1)[0].strip()
        if prefix:
            return prefix
    if isinstance(fallback, str) and fallback:
        return fallback
    return "control"


def expected_upstream_control_level(row: pd.Series) -> float | None:
    if pd.notna(row.get("upstream_basin_streefpeil")):
        return float(row["upstream_basin_streefpeil"])
    if pd.notna(row.get("gecheckte_min_upstream_level")):
        return float(row["gecheckte_min_upstream_level"])
    return None


def expected_downstream_control_level(row: pd.Series) -> float | None:
    if pd.notna(row.get("gecheckte_max_downstream_level")):
        return float(row["gecheckte_max_downstream_level"])
    if pd.notna(row.get("huidig_max_downstream_level")):
        return float(row["huidig_max_downstream_level"])
    return None


def expected_control_name(row: pd.Series, current_name: object) -> str | None:
    prefix = control_name_prefix(current_name=current_name, fallback=row.get("functie"))
    upstream_level = format_control_level(expected_upstream_control_level(row))
    downstream_level = format_control_level(expected_downstream_control_level(row))

    if row.get("functie") == "uitlaat":
        if upstream_level is None:
            return None
        return f"{prefix}: {upstream_level} [m+NAP]"

    if row.get("functie") in ["inlaat", "doorlaat"] and upstream_level is not None and downstream_level is not None:
        return f"{prefix}: {upstream_level}/{downstream_level} [m+NAP]"
    if row.get("functie") in ["inlaat", "doorlaat"] and downstream_level is not None:
        return f"{prefix}: {downstream_level} [m+NAP]"
    return None


def update_control_node_names(model: Model, deviations_df: pd.DataFrame) -> int:
    control_ids_by_target = control_node_ids_by_target_node_id(model)
    if not control_ids_by_target:
        return 0

    update_count = 0
    node_df = model.node.df
    for _, row in deviations_df.drop_duplicates(subset=["node_id"]).iterrows():
        if bool(row.get("flow_demand_controlled", False)):
            continue
        for control_node_id in control_ids_by_target.get(int(row["node_id"]), []):
            if control_node_id not in node_df.index:
                continue
            current_name = node_df.at[control_node_id, "name"]
            if has_intentional_named_layout_mismatch(
                str(row.get("functie")), None if pd.isna(current_name) else str(current_name)
            ):
                continue
            new_name = expected_control_name(row=row, current_name=current_name)
            if new_name is not None and current_name != new_name:
                node_df.at[control_node_id, "name"] = new_name
                update_count += 1

    return update_count


def update_discrete_control_conditions(
    model: Model,
    control_ids_by_target: dict[int, list[int]],
    target_node_id: int,
    listen_node_id: int | None,
    level_value: float,
    function: str,
) -> int:
    if listen_node_id is None:
        return 0

    variable_df = model.discrete_control.variable.df
    condition_df = model.discrete_control.condition.df
    if variable_df is None or condition_df is None:
        return 0

    control_node_ids = control_ids_by_target.get(int(target_node_id), [])
    if not control_node_ids:
        return 0

    update_count = 0
    for control_node_id in control_node_ids:
        current_name = control_node_name(model, int(control_node_id))
        if has_intentional_named_layout_mismatch(function, current_name):
            continue
        layout_key = validate_control_layout_for_sync(
            model=model,
            target_node_id=int(target_node_id),
            control_node_id=int(control_node_id),
            function=function,
        )
        variable_rows = variable_df[
            variable_df["node_id"].eq(control_node_id) & variable_df["listen_node_id"].eq(int(listen_node_id))
        ].copy()
        if variable_rows.empty:
            continue
        variable_rows["weight"] = normalize_numeric(variable_rows["weight"])
        variable_rows = variable_rows.dropna(subset=["compound_variable_id", "weight"])

        for variable_row in variable_rows.drop_duplicates(subset=["compound_variable_id"]).itertuples(index=False):
            compound_variable_id = int(variable_row.compound_variable_id)
            condition_mask = condition_df["node_id"].eq(control_node_id) & condition_df["compound_variable_id"].eq(
                compound_variable_id
            )
            if not condition_mask.any():
                continue

            condition_rows = condition_df.loc[condition_mask].copy()
            if "condition_id" in condition_rows.columns:
                condition_rows = condition_rows.sort_values("condition_id")

            threshold_values = control_condition_thresholds(
                layout_key=layout_key,
                compound_variable_id=compound_variable_id,
                variable_name=str(variable_row.variable),
                level_value=float(level_value),
                weight=float(variable_row.weight),
                level_difference_threshold=float(getattr(model.solver, "level_difference_threshold", 0.02)),
            )
            if len(threshold_values) != len(condition_rows):
                raise ValueError(
                    f"{layout_key} #{target_node_id} met DiscreteControl #{control_node_id} heeft "
                    f"{len(condition_rows)} conditions voor compound_variable_id {compound_variable_id}, "
                    f"verwacht {len(threshold_values)}."
                )

            for threshold_value, (condition_index, _condition_row) in zip(
                threshold_values, condition_rows.iterrows(), strict=True
            ):
                condition_df.loc[condition_index, "threshold_high"] = float(threshold_value)
                condition_df.loc[condition_index, "threshold_low"] = float(threshold_value)
                update_count += 1

    return update_count


def control_threshold_for_min_upstream(row: object, static_min_upstream_value: float) -> float:
    basis = str(getattr(row, "min_upstream_level_check_basis", ""))
    upstream_streefpeil = getattr(row, "upstream_basin_streefpeil", np.nan)

    if basis in {
        "upstream_streefpeil_plus_aanvoer_offset",
        "rijkswaterstaat_min_profile_level_plus_offset",
        "rijkswaterstaat_state_level",
    } and pd.notna(upstream_streefpeil):
        return float(upstream_streefpeil)

    return float(static_min_upstream_value)


def apply_level_updates(model: Model, deviations_df: pd.DataFrame) -> tuple[int, int, int, int, int]:
    max_update_count = 0
    min_update_count = 0
    min_update_count_rws_inlet = 0
    control_name_update_count = 0
    control_condition_update_count = 0
    snapshots = protected_apply_snapshots(model)

    static_df_by_node_type = {
        "Outlet": model.outlet.static.df,
        "Pump": model.pump.static.df,
    }
    control_ids_by_target = control_node_ids_by_target_node_id(model)

    for row in deviations_df.itertuples():
        if bool(getattr(row, "flow_demand_controlled", False)):
            continue
        static_df = static_df_by_node_type[row.node_type]
        if static_df is None or row.table_row_id not in static_df.index:
            raise KeyError(f"Kan rij {row.table_row_id} niet vinden in {row.static_table} voor node_id={row.node_id}")

        if bool(row.max_downstream_level_afwijking) and pd.notna(row.gecheckte_max_downstream_level):
            value = float(row.gecheckte_max_downstream_level)
            static_df.loc[row.table_row_id, "max_downstream_level"] = value
            control_condition_update_count += update_discrete_control_conditions(
                model=model,
                control_ids_by_target=control_ids_by_target,
                target_node_id=int(row.node_id),
                listen_node_id=int(row.downstream_basin_id) if pd.notna(row.downstream_basin_id) else None,
                level_value=value,
                function=str(row.functie),
            )
            max_update_count += 1
        if bool(row.min_upstream_level_afwijking):
            if bool(getattr(row, "gecheckte_min_upstream_level_is_null", False)):
                static_df.loc[row.table_row_id, "min_upstream_level"] = np.nan
            elif pd.notna(row.gecheckte_min_upstream_level):
                value = float(row.gecheckte_min_upstream_level)
                static_df.loc[row.table_row_id, "min_upstream_level"] = value
                control_condition_update_count += update_discrete_control_conditions(
                    model=model,
                    control_ids_by_target=control_ids_by_target,
                    target_node_id=int(row.node_id),
                    listen_node_id=int(row.upstream_basin_id) if pd.notna(row.upstream_basin_id) else None,
                    level_value=control_threshold_for_min_upstream(row, value),
                    function=str(row.functie),
                )
            else:
                continue
            min_update_count += 1
            if row.functie == "inlaat" and row.upstream_basin_meta_waterbeheerder == "Rijkswaterstaat":
                min_update_count_rws_inlet += 1

    control_name_update_count = update_control_node_names(model=model, deviations_df=deviations_df)
    assert_protected_apply_snapshots_unchanged(model, snapshots)

    return (
        max_update_count,
        min_update_count,
        min_update_count_rws_inlet,
        control_name_update_count,
        control_condition_update_count,
    )


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
        "max_downstream_is_coupling_link",
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
        "min_upstream_is_coupling_link",
        "rws_to_model_link",
        "upstream_basin_streefpeil",
        "upstream_basin_state_level",
        "upstream_basin_min_profile_level",
        "rws_upstream_state_level_valid",
        "loop_min_upstream_basin_id",
        "loop_min_upstream_manning_link_id",
        "loop_min_upstream_error",
        "loop_min_upstream_level",
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
        "max_downstream_is_coupling_link",
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
        "min_upstream_is_coupling_link",
        "rws_to_model_link",
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
        upstream_supply_offset=UPSTREAM_SUPPLY_OFFSET,
        rws_upstream_state_offset=RWS_UPSTREAM_STATE_OFFSET,
        max_rws_upstream_state_level=MAX_RWS_UPSTREAM_STATE_LEVEL,
        include_excluded=INCLUDE_EXCLUDED,
    )
    max_update_count = 0
    min_update_count = 0
    min_update_count_rws_inlet = 0
    control_name_update_count = 0
    control_condition_update_count = 0
    manning_update_count = 0
    should_set_manning_n = SET_SELECTED_AUTHORITIES_MANNING_N

    if APPLY_LEVEL_UPDATES and not deviations_df.empty:
        (
            max_update_count,
            min_update_count,
            min_update_count_rws_inlet,
            control_name_update_count,
            control_condition_update_count,
        ) = apply_level_updates(model, deviations_df)
    if APPLY_LEVEL_UPDATES and UPDATE_CONTROL_NAMES:
        control_name_update_count += update_control_node_names(model=model, deviations_df=checked_df)

    if should_set_manning_n:
        manning_update_count = set_selected_authorities_manning_n(model, SELECTED_AUTHORITIES_MANNING_N)

    write_model = (APPLY_LEVEL_UPDATES and not deviations_df.empty) or control_name_update_count or should_set_manning_n
    database_backup_path = None
    if write_model:
        database_backup_path = backup_database_gpkg(database_path)
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

    if APPLY_LEVEL_UPDATES:
        print(f"Aangepaste max_downstream_level-waarden: {max_update_count}")
        print(f"Aangepaste min_upstream_level-waarden: {min_update_count}")
        print(f"Aangepaste min_upstream_level-waarden inlaten vanaf RWS: {min_update_count_rws_inlet}")
        print(f"Aangepaste DiscreteControl-naamteksten: {control_name_update_count}")
        print(f"Aangepaste DiscreteControl-condition thresholds: {control_condition_update_count}")
    if database_backup_path is not None:
        print(f"Backup database voor schrijven: {database_backup_path}")
    if should_set_manning_n:
        print(f"Aangepaste ManningResistance manning_n-waarden: {manning_update_count}")
        print(f"ManningResistance manning_n gezet op: {SELECTED_AUTHORITIES_MANNING_N}")

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
