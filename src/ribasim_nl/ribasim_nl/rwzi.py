"""
Merge standalone model(s) with the RWZI model.

Replaces the Terminals with Junctions and connects them to the underlying Basin.
"""

import logging

import geopandas as gpd
import pandas as pd
from geopandas.geodataframe import GeoDataFrame

from ribasim_nl import Model, concat
from ribasim_nl.reset_index import reset_index

logger = logging.getLogger(__name__)


def create_rwzi_basin_coupling(rwzi_coupled_model: Model, max_distance=100):
    """
    Match RWZI outflow locations with the underlying basins. When there is no match, try a 20 m buffer.

    Args:
        rwzi_coupled_model: Object with attributes terminal.node.df and basin.area.df.
        max_distance (float): Maximum distance to search for matching basins.

    Returns
    -------
        coupling_lookup (dict): Lookup table of RWZI codes and basin IDs.
        unmatched_rwzi_df (GeoDataFrame): RWZI locations that have not yet been matched with basins.
    """
    terminals_all = rwzi_coupled_model.terminal.node.df  # pyrefly: ignore[missing-attribute]
    basins = rwzi_coupled_model.basin.area.df
    nodes = rwzi_coupled_model.node.df

    unique_waterbeheerders = nodes["meta_waterbeheerder"].dropna().unique()  # pyrefly: ignore[unsupported-operation]

    # RWZI codes
    rwzi_codes_df = terminals_all[["meta_rwzi_code"]].dropna().copy()

    # Filter terminals met RWZI code
    terminals_filtered = terminals_all[terminals_all.meta_rwzi_code.isin(rwzi_codes_df.meta_rwzi_code)].copy()
    terminals_filtered = terminals_filtered.to_crs(basins.crs)  # pyrefly: ignore[missing-attribute]

    # Only keep terminals relevant to this model authorities
    terminals_filtered = terminals_filtered[
        terminals_filtered["meta_discharge_couple_authority"].isin(unique_waterbeheerders)
    ]

    # Basins reset
    basins_join = basins.join(nodes[["meta_categorie", "meta_waterbeheerder"]], on="node_id")  # pyrefly: ignore[missing-attribute, unsupported-operation]
    basins_reset = basins_join.reset_index().rename(columns={"node_id": "couple_to_basin_id"})
    # Filter basins on meta_categorie != 'bergend'
    basins_reset = basins_reset[~basins_reset["meta_categorie"].isin(["bergend"])]

    # 1. Koppel terminals aan onderliggende basins
    joined_within = gpd.sjoin(
        terminals_filtered,
        basins_reset[["couple_to_basin_id", "geometry"]],
        how="left",
        predicate="within",
    )

    duplicated = joined_within.index[joined_within.index.duplicated(keep="first")].unique()
    if len(duplicated) > 0:
        raise ValueError(f"RWZI terminals within multiple basins: {list(duplicated)}")
    joined_unique = joined_within

    # matched_rwzis = terminals_filtered.loc[joined_unique["couple_to_basin_id"].notna()]
    unmatched_rwzis = terminals_filtered.loc[joined_unique["couple_to_basin_id"].isna()]

    # 2. Koppel terminals aan dichtstbijzijnde basins
    joined_nearest = gpd.sjoin_nearest(
        unmatched_rwzis,
        basins_reset[["couple_to_basin_id", "geometry"]],
        how="left",
        max_distance=max_distance,
    )

    rwzis_combined = joined_unique.copy()
    matched_rwzis_nearest = joined_nearest[["couple_to_basin_id"]]

    # TODO: assign not the first match but the best match
    duplicated = joined_nearest.index[joined_nearest.index.duplicated(keep="first")].unique()
    if len(duplicated) > 0:
        raise ValueError(f"RWZI terminals has multiple nearest basins: {list(duplicated)}")
    matched_rwzis_nearest_unique = matched_rwzis_nearest.loc[~matched_rwzis_nearest.index.duplicated(keep="first")]

    rwzis_combined.loc[unmatched_rwzis.index, "couple_to_basin_id"] = matched_rwzis_nearest_unique["couple_to_basin_id"]

    # Build lookup table
    coupling_df = rwzis_combined[["meta_rwzi_code", "couple_to_basin_id"]].dropna()
    coupling_lookup = dict(zip(coupling_df.meta_rwzi_code, coupling_df.couple_to_basin_id, strict=True))

    # Niet gekoppelde rwzis
    unmatched_rwzi_df = rwzis_combined[rwzis_combined["couple_to_basin_id"].isna()][["meta_rwzi_code", "geometry"]]

    return coupling_lookup, unmatched_rwzi_df


def remove_unmatched_rwzi(rwzi_coupled_model: Model, unmatched_rwzi_df: GeoDataFrame, *, verbose=False):
    """
    Remove RWZI terminals.

    Remove RWZI terminals that fall outside the model and their associated
    flow-boundary nodes from the LHM model, and return the model.
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
    Convert RWZI Terminals into Junctions.

    Afterwards connect them to the correct Basin node.

    RWZI terminals without a coupling to a basin in this model (for example RWZIs of other
    water authorities that were carried over from the national RWZI model) are removed entirely.
    Converting them into a Junction would create an orphaned Junction (with an incoming but no
    outgoing link), which Ribasim rejects with a minimum-neighbor validation error.
    """
    # all RWZI terminals currently in the model
    terminals = rwzi_coupled_model.terminal.node.df[rwzi_coupled_model.terminal.node.df["meta_rwzi_code"].notna()]

    # split into terminals that couple to a basin in this model and terminals that do not
    coupled_mask = terminals["meta_rwzi_code"].map(lambda code: pd.notna(coupling_lookup.get(code)))
    coupled_terminals = terminals[coupled_mask]
    uncoupled_terminals = terminals[~coupled_mask]

    # 0. remove uncoupled RWZI terminals and the flow boundaries that feed them
    uncoupled_codes = set(uncoupled_terminals["meta_rwzi_code"])
    for node_id in uncoupled_terminals.index:
        if verbose:
            print(f"Verwijder niet-gekoppelde RWZI-terminal {node_id}")
        rwzi_coupled_model.remove_node(node_id, remove_links=True)
    if uncoupled_codes:
        fb_df = rwzi_coupled_model.flow_boundary.node.df
        fb_to_remove = fb_df[fb_df["meta_rwzi_code"].isin(uncoupled_codes)]
        for node_id in fb_to_remove.index:
            if verbose:
                print(f"Verwijder bijbehorende RWZI-flow-boundary {node_id}")
            rwzi_coupled_model.remove_node(node_id, remove_links=True)

    # 1. terminal naar junction (alleen voor gekoppelde terminals)
    for node_id, _row in coupled_terminals.iterrows():
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


def merge_rwzi_model(base_model, rwzi_model_path, max_distance: int = 100, verbose: bool = False):
    """
    Merge an RWZI model into a base model.

    1. Reads the RWZI model.
    2. Concatenates it with the base model.
    3. Couples RWZI terminals to basins.
    4. Removes unmatched RWZIs.
    5. Converts terminals to junctions.

    Parameters
    ----------
        base_model (Model): The base Ribasim model to merge into.
        rwzi_model_path (Path): Path to the RWZI model toml file.
        buffer_distance (float): Buffer distance in meters for spatial coupling.
        verbose (bool): Whether to print progress info.

    Returns
    -------
        Model: The merged model with RWZI nodes coupled to basins.
    """
    # load RWZI model
    rwzi_model = Model.read(rwzi_model_path)
    logger.info("RWZI model loaded")

    # add meta_categorie = RWZI so we can discriminate between other (buitenlandse aanvoer) boundaries
    assert rwzi_model.node.df is not None
    rwzi_model.node.df.loc[rwzi_model.node.df.node_type == "FlowBoundary", "meta_categorie"] = "RWZI"

    # index rwzi model with nodes >1 higher than of base model
    node_start = base_model.node.df.index.max() + 1
    link_start = base_model.link.df.index.max() + 1
    rwzi_model = reset_index(rwzi_model, node_start=node_start, link_start=link_start)
    rwzi_coupled_model = concat([base_model, rwzi_model], keep_original_index=True)

    coupling_lookup, unmatched_rwzi_df = create_rwzi_basin_coupling(rwzi_coupled_model, max_distance=max_distance)
    rwzi_coupled_model, _stats = remove_unmatched_rwzi(rwzi_coupled_model, unmatched_rwzi_df, verbose=verbose)
    rwzi_coupled_model = terminal2junction(rwzi_coupled_model, coupling_lookup, verbose=verbose)

    logger.info(f"There are {len(unmatched_rwzi_df)} RWZI's not incorporated in this model")

    return rwzi_coupled_model
