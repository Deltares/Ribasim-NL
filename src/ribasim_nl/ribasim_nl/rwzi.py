"""
Merge standalone model(s) with the RWZI model.

Replaces the Terminals with Junctions and connects them to the underlying Basin.
"""

import logging

import geopandas as gpd
import pandas as pd
from geopandas.geodataframe import GeoDataFrame

from ribasim_nl import Model, concat, prefix_index

logger = logging.getLogger(__name__)


def create_rwzi_basin_coupling(rwzi_coupled_model: Model, buffer_distance):
    """
    Match RWZI uitstroom locaties met de onderliggende basins. Wanneer geen match, probeer 20 m buffer.

    Args:
        rwzi_coupled_model: Object with attributes terminal.node.df and basin.area.df.
        buffer_distance (float): bovenaan script gedefinieerd

    Returns
    -------
        coupling_lookup (dict): Lookup table van RWZI codes en basin IDs.
        unmatched_rwzi_df (GeoDataFrame): RWZI locaties die nog niet gematcht zijn met basins.
    """
    terminals_all = rwzi_coupled_model.terminal.node.df  # pyrefly: ignore[missing-attribute]
    basins = rwzi_coupled_model.basin.area.df

    # RWZI codes
    rwzi_codes_df = terminals_all[["meta_rwzi_code"]].dropna().copy()

    # Filter terminals met RWZI code
    terminals_filtered = terminals_all[terminals_all.meta_rwzi_code.isin(rwzi_codes_df.meta_rwzi_code)].copy()
    terminals_filtered = terminals_filtered.to_crs(basins.crs)  # pyrefly: ignore[missing-attribute]

    # Basins reset
    basins_reset = basins.reset_index().rename(columns={"node_id": "couple_to_basin_id"})  # pyrefly: ignore[missing-attribute]

    # 1. Koppel terminals aan onderliggende basins
    joined_within = gpd.sjoin(
        terminals_filtered,
        basins_reset[["couple_to_basin_id", "geometry"]],
        how="left",
        predicate="within",
    )

    # TODO: assign not the first match but the best match
    joined_unique = joined_within.loc[~joined_within.index.duplicated(keep="first")]

    # matched_rwzis = terminals_filtered.loc[joined_unique["couple_to_basin_id"].notna()]
    unmatched_rwzis = terminals_filtered.loc[joined_unique["couple_to_basin_id"].isna()]

    # 2. Koppel terminals aan gebufferde basins
    basins_buffered = basins_reset.copy()
    basins_buffered["geometry"] = basins_buffered.geometry.buffer(buffer_distance)

    joined_buffer = gpd.sjoin(
        unmatched_rwzis,
        basins_buffered[["couple_to_basin_id", "geometry"]],
        how="left",
        predicate="within",
    )

    rwzis_combined = joined_unique.copy()
    matched_rwzis_buffered = joined_buffer[["couple_to_basin_id"]]

    # TODO: assign not the first match but the best match
    matched_rwzis_buffered_unique = matched_rwzis_buffered.loc[~matched_rwzis_buffered.index.duplicated(keep="first")]

    rwzis_combined.loc[unmatched_rwzis.index, "couple_to_basin_id"] = matched_rwzis_buffered_unique[
        "couple_to_basin_id"
    ]

    # Build lookup table
    coupling_df = rwzis_combined[["meta_rwzi_code", "couple_to_basin_id"]].dropna()
    coupling_lookup = dict(zip(coupling_df.meta_rwzi_code, coupling_df.couple_to_basin_id))

    # Niet gekoppelde rwzis
    unmatched_rwzi_df = rwzis_combined[rwzis_combined["couple_to_basin_id"].isna()][["meta_rwzi_code", "geometry"]]

    return coupling_lookup, unmatched_rwzi_df


def remove_unmatched_rwzi(rwzi_coupled_model: Model, unmatched_rwzi_df: GeoDataFrame, *, verbose=False):
    """
    Verwijder RWZI-terminals.

    Verwijder RWZI-terminals die buiten het model vallen én de daarbij horende
    flow-boundary-knopen uit het LHM-model en geef het model terug.
    """
    #  1. RWZI-terminals verwijderen
    terminals_removed = 0
    for node_id in unmatched_rwzi_df.index:
        rwzi_coupled_model.remove_node(node_id, remove_links=True)
        terminals_removed += 1

    #  2. bijbehorende flow boundaries verwijderen
    fb_df = rwzi_coupled_model.flow_boundary.node.df  # pyrefly: ignore[missing-attribute]
    flow_boundary_to_remove = fb_df[fb_df["meta_rwzi_code"].isin(unmatched_rwzi_df["meta_rwzi_code"])]

    flow_boundaries_removed = 0
    for node_id in flow_boundary_to_remove.index:
        rwzi_coupled_model.remove_node(node_id, remove_links=True)
        flow_boundaries_removed += 1

    #  samenvatting & return
    stats = {
        "terminals_removed": terminals_removed,
        "flow_boundaries_removed": flow_boundaries_removed,
    }

    if verbose:
        print(f"Verwijderd: {terminals_removed} RWZI-terminals en {flow_boundaries_removed} flow-boundary-knopen.")

    return rwzi_coupled_model, stats


def terminal2junction(rwzi_coupled_model, coupling_lookup, *, verbose=False):
    """
    Zet RWZI-Terminals om naar Junctions.

    Verbind ze daarna met de juiste Basin knoop.
    """
    # 1. terminal naar junction
    terminals = rwzi_coupled_model.terminal.node.df[rwzi_coupled_model.terminal.node.df["meta_rwzi_code"].notna()]

    for node_id, row in terminals.iterrows():
        if verbose:
            print(f"Converteer terminal {node_id} naar junction")
        rwzi_coupled_model.update_node(node_id, "Junction", data=[])

    # 2. verbind junctions met basin
    junctions = rwzi_coupled_model.junction.node.df[rwzi_coupled_model.junction.node.df["meta_rwzi_code"].notna()]

    for node_id, row in junctions.iterrows():
        rwzi_code = row["meta_rwzi_code"]
        basin_node_id = coupling_lookup.get(rwzi_code)

        if pd.notna(basin_node_id):
            if verbose:
                print(f"Verbind junction {node_id} met basin {basin_node_id}")
            rwzi_coupled_model.link.add(
                rwzi_coupled_model.get_node(node_id),
                rwzi_coupled_model.get_node(int(basin_node_id)),
                name=row["name"],
            )
        elif verbose:
            print(f"Geen koppeling gevonden voor RWZI-code {rwzi_code}")

    return rwzi_coupled_model


def merge_rwzi_model(
    base_model, rwzi_model_path, buffer_distance: int = 20, prefix_id: int = 999, verbose: bool = True
):
    """
    Merge an RWZI model into a base model.

    Reads the RWZI model, prefixes its indices, concatenates it with the base model,
    couples RWZI terminals to basins, removes unmatched RWZIs, and converts terminals to junctions.

    Parameters
    ----------
        base_model (Model): The base Ribasim model to merge into.
        rwzi_model_path (Path): Path to the RWZI model toml file.
        buffer_distance (float): Buffer distance in meters for spatial coupling.
        prefix_id (int): Prefix ID for the RWZI model nodes.
        verbose (bool): Whether to print progress info.

    Returns
    -------
        Model: The merged model with RWZI nodes coupled to basins.
    """
    rwzi_model = Model.read(rwzi_model_path)
    logger.info("RWZI model loaded")

    try:
        rwzi_model = prefix_index(model=rwzi_model, prefix_id=prefix_id)
    except KeyError as e:
        logger.info("Remove model results (and retry) if a node_id in Basin / state is not in node-table.")
        raise e

    rwzi_coupled_model = concat([base_model, rwzi_model], keep_original_index=True)

    coupling_lookup, unmatched_rwzi_df = create_rwzi_basin_coupling(rwzi_coupled_model, buffer_distance)
    rwzi_coupled_model, _stats = remove_unmatched_rwzi(rwzi_coupled_model, unmatched_rwzi_df, verbose=verbose)
    rwzi_coupled_model = terminal2junction(rwzi_coupled_model, coupling_lookup, verbose=verbose)

    logger.info(f"There are {len(unmatched_rwzi_df)} RWZI's not incorporated in this model")

    return rwzi_coupled_model
