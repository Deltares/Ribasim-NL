"""
Merge standalone model(s) with the RWZI model.

Replaces the Terminals with Junctions and connects them to the underlying Basin.
"""

import logging
import math
import typing
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import geopandas as gpd
import pandas as pd
from geopandas.geodataframe import GeoDataFrame
from shapely.geometry import LineString, Point

from ribasim_nl import Model, concat
from ribasim_nl.flow_paths import Routes, place_on_link, prune_junction, split_distance, split_link
from ribasim_nl.reset_index import reset_index

# %%
logger = logging.getLogger(__name__)


# %%
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
    terminals_all = rwzi_coupled_model.terminal.node.df
    basins = rwzi_coupled_model.basin.area.df
    nodes = rwzi_coupled_model.node.df
    assert basins is not None
    assert basins.crs is not None

    unique_waterbeheerders = nodes["meta_waterbeheerder"].dropna().unique()

    # RWZI codes
    rwzi_codes_df = terminals_all[["meta_rwzi_code"]].dropna().copy()

    # Filter terminals met RWZI code
    terminals_filtered = terminals_all[terminals_all.meta_rwzi_code.isin(rwzi_codes_df.meta_rwzi_code)].copy()
    terminals_filtered = terminals_filtered.to_crs(basins.crs)

    # Only keep terminals relevant to this model authorities
    terminals_filtered = terminals_filtered[
        terminals_filtered["meta_discharge_couple_authority"].isin(unique_waterbeheerders)
    ]

    # Basins reset
    basins_join = basins.join(nodes[["meta_categorie", "meta_waterbeheerder"]], on="node_id")
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
        distance_col="distance",
    )
    for _, row in joined_nearest.dropna(subset="couple_to_basin_id").iterrows():
        logger.info(
            f"RWZI {row['meta_rwzi_code']} is outside the Basin areas, "
            f"coupled to Basin {int(row['couple_to_basin_id'])} at {row['distance']:.0f} m"
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
    fb_df = rwzi_coupled_model.flow_boundary.node.df
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


# Basins that RWZIs discharge into by RWZI code, instead of the Basin they are in, see #871
RWZI_BASINS = {
    12001: 1202759,  # Hollands Noorderkwartier, on the hoofdwater next to doorgaand Basin 1200195
    17041: 4000278,  # Hollandse Delta, on the hoofdwater next to doorgaand Basin 4000362
    28002: 3801735,  # Asten, on the hoofdwater next to doorgaand Basin 3801747
    28003: 3801473,  # Aa en Maas, on the hoofdwater 2.4 km from doorgaand Basin 3801602
    30021: 8008147,  # Stein, on the Julianakanaal instead of Limburg Basin 6001469
    30022: 6001422,  # Limburg, on the doorgaand next to hoofdwater Basin 6001434
}


def _is_bergend(nodes: pd.DataFrame, node_ids: typing.Iterable[int]) -> bool:
    return "meta_categorie" in nodes.columns and bool((nodes.loc[list(node_ids), "meta_categorie"] == "bergend").any())


def _attach_rwzi_junction(model: Model, junction_id: int, basin_id: int, max_distance: float) -> bool:
    """Attach an RWZI Junction to the route of `basin_id`, see `attach_rwzi_junctions`. Returns whether it did."""
    nodes = model.node.df  # the nodes used below are not removed
    routes = Routes(model)
    inflow = routes.links_at(junction_id, upstream=True)
    outflow = routes.links_at(junction_id, upstream=False)
    # an RWZI Junction that is attached already has more inflow than its FlowBoundary
    if len(outflow) != 1 or [routes.node_types[routes.next_node(i, True)] for i in inflow] != ["FlowBoundary"]:
        return False
    current = routes.ends(outflow[0])
    if len(current) != 1 or routes.node_types[next(iter(current))] != "Basin":
        return False

    def candidates(routes: Routes) -> dict[int, bool]:
        """The links of the routes into (True) and out of (False) the Basin, except of RWZIs and bergend Basins."""
        result = {}
        for upstream in (True, False):
            for link_id in routes.paths(basin_id, upstream):
                from_node_id = cast(int, routes.flow.at[link_id, "from_node_id"])
                to_node_id = cast(int, routes.flow.at[link_id, "to_node_id"])
                if junction_id in (from_node_id, to_node_id) or routes.node_types[from_node_id] == "FlowBoundary":
                    continue
                if _is_bergend(nodes, [from_node_id, to_node_id, *routes.ends(link_id, upstream)]):
                    continue
                result[link_id] = upstream
        return result

    point = cast(Point, nodes.at[junction_id, "geometry"])
    distances = routes.flow.loc[list(candidates(routes))].distance(point)
    if distances.empty or distances.min() > max_distance:
        return False

    name = cast(str, model.link.df.at[outflow[0], "name"])
    model.remove_links(outflow)
    prune_junction(model, routes.next_node(outflow[0], upstream=False))
    routes = Routes(model)
    options = candidates(routes)
    link_id = cast(int, routes.flow.loc[list(options)].distance(point).idxmin())
    upstream = options[link_id]
    geometry = cast(LineString, routes.flow.at[link_id, "geometry"])
    distance = split_distance(geometry, point)
    if upstream:
        # on a link into the Basin, which is split there
        split_link(model, link_id, distance, junction_id)
    else:
        # on a link out of the Basin: the RWZI cannot discharge there, since a FlowBoundary cannot be connected
        # to its connector node, so its link to the Basin runs along the route instead
        model.link.add(model.get_node(junction_id), model.get_node(basin_id), name=name)
        place_on_link(model, junction_id, basin_id, link_id)
    location = cast(Point, model.node.df.at[junction_id, "geometry"])
    model.link.df.loc[inflow, "geometry"] = gpd.GeoSeries(
        [LineString([cast(Point, nodes.at[routes.next_node(i, True), "geometry"]), location]) for i in inflow],
        index=inflow,
        crs=model.link.df.crs,
    )
    return True


def attach_rwzi_junctions(
    model: Model,
    max_distance: float = 1000.0,
    basins: Mapping[int, int] = RWZI_BASINS,
    authorities: set[str] | None = None,
) -> Model:
    """Move each RWZI Junction onto the nearest route into or out of its Basin, along the flow links and Junctions.

    An RWZI Junction, see `terminal2junction`, is located at the discharge point, with a straight link to the
    Basin it is in, or to `basins[meta_rwzi_code]` if given. Instead, it is moved onto the nearest flow link into
    or out of this Basin, directly or through Junctions only, such as the routed links of the doorgaand and
    hoofdwater Basins, except links of bergend Basins. A link into the Basin is split there; along a link out of the
    Basin runs the new link to the Basin. RWZI Junctions without such a link within `max_distance` are left as is,
    and so are those of `basins` whose Basin is not in the model, or which are attached already.

    Parameters
    ----------
        model (Model): model with RWZI Junctions, which have a meta_rwzi_code.
        max_distance (float): maximum distance in meters between the RWZI Junction and the link, except for `basins`.
        basins (Mapping[int, int]): Basin node ids by RWZI code, for RWZIs that do not discharge in the Basin they are in.
        authorities (set[str] | None): if given, only attach RWZIs of these authorities, and RWZIs of `basins`, whose
            global Basin ids are only in the model after coupling.

    Returns
    -------
        Model: the model with the RWZI Junctions on the links.
    """
    nodes = model.node.df
    if "meta_rwzi_code" not in nodes.columns:
        return model
    rwzi_junctions = nodes.loc[(nodes["node_type"] == "Junction") & nodes["meta_rwzi_code"].notna(), "meta_rwzi_code"]

    attached = 0
    for junction_id, code in rwzi_junctions.items():
        junction_id = cast(int, junction_id)
        if (
            authorities is not None
            and nodes.at[junction_id, "meta_waterbeheerder"] not in authorities
            and int(code) not in basins
        ):
            continue
        routes = Routes(model)
        outflow = routes.links_at(junction_id, upstream=False)
        ends = routes.ends(outflow[0]) if len(outflow) == 1 else frozenset()
        basin_id = basins.get(int(code), next(iter(ends)) if len(ends) == 1 else None)
        if basin_id is None or basin_id not in model.node.df.index:
            continue
        if model.get_node_type(basin_id) != "Basin":
            raise ValueError(
                f"RWZI {code} should discharge into Basin {basin_id}, not a {model.get_node_type(basin_id)}."
            )
        limit = math.inf if int(code) in basins else max_distance
        attached += _attach_rwzi_junction(model, int(junction_id), basin_id, limit)

    logger.info(f"Attached {attached} of {len(rwzi_junctions)} RWZI Junctions to a flow link of their Basin")
    return model


def merge_rwzi_model(
    base_model: Model,
    rwzi_model_path: Path,
    rwzi_coverage_path: Path,
    coverage_output_path: Path,
    max_distance: float = 100,
    verbose: bool = False,
) -> Model:
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
        rwzi_coverage_path (Path): Path to the coverage GeoJSON produced with the RWZI model.
        coverage_output_path (Path): Path for the coverage GeoJSON of the coupled model.
        max_distance (float): Maximum distance in meters for spatial coupling.
        verbose (bool): Whether to print progress info.

    Returns
    -------
        Model: The merged model with RWZI nodes coupled to basins.
    """
    # load RWZI model
    rwzi_model = Model.read(rwzi_model_path)
    logger.info("RWZI model loaded")

    # add meta_categorie = RWZI so we can discriminate between other (buitenlandse aanvoer) boundaries
    rwzi_model.node.df.loc[rwzi_model.node.df.node_type == "FlowBoundary", "meta_categorie"] = "RWZI"

    # index rwzi model with nodes >1 higher than of base model
    node_start = base_model.node.df.index.max() + 1
    link_start = base_model.link.df.index.max() + 1
    rwzi_model = reset_index(rwzi_model, node_start=node_start, link_start=link_start)
    rwzi_coupled_model = concat([base_model, rwzi_model], keep_original_index=True)

    coupling_lookup, unmatched_rwzi_df = create_rwzi_basin_coupling(rwzi_coupled_model, max_distance=max_distance)
    rwzi_coupled_model, _stats = remove_unmatched_rwzi(rwzi_coupled_model, unmatched_rwzi_df, verbose=verbose)
    rwzi_coupled_model = terminal2junction(rwzi_coupled_model, coupling_lookup, verbose=verbose)
    log_rwzi_coverage_report(
        rwzi_coupled_model,
        rwzi_coverage_path=rwzi_coverage_path,
        coverage_output_path=coverage_output_path,
        max_distance=max_distance,
    )

    logger.info(f"There are {len(unmatched_rwzi_df)} RWZI's not incorporated in this model")

    return rwzi_coupled_model


# %% Set-up geojson for rwzi coupling checks
def export_rwzi_coverage_geojson(
    rwzi_coupled_model: Model,
    rwzi_coverage_path: Path,
    coverage_output_path: Path,
    verbose: bool = False,
) -> GeoDataFrame:
    """
    Export RWZI coverage GeoJSON with LHM model inclusion flag.

    1. Reads the RWZI coordinates model coverage GeoJSON.
    2. Extracts RWZI names present in the merged model's junctions.
    3. Flags each RWZI as included in the LHM model or not (if no underlying basin, they are left out).
    4. Flags RWZI names that occur more than once in the model.
    5. Writes the updated GeoJSON to the coupled model's results directory.

    Parameters
    ----------
        rwzi_coupled_model (Model): The merged Ribasim model with RWZI nodes coupled to basins.
        rwzi_coverage_path (Path): Coverage GeoJSON produced with the RWZI model.
        coverage_output_path (Path): Output path for the coupled-model coverage GeoJSON.
        verbose (bool): Whether to print progress info.

    Returns
    -------
        GeoDataFrame: The RWZI coverage GeoDataFrame with the added coverage flags.
    """
    rwzi_gdf = gpd.read_file(rwzi_coverage_path)

    junction_df = rwzi_coupled_model.junction.node.df
    rwzi_junction_names = junction_df[junction_df["meta_rwzi_code"].notna()]["name"].str.replace("_out", "")
    duplicate_rwzi_names = set(rwzi_junction_names[rwzi_junction_names.duplicated(keep=False)])

    rwzi_lhm_coverage = rwzi_gdf.copy()
    rwzi_lhm_coverage["in_lhm_model"] = rwzi_lhm_coverage["Naam rwzi"].isin(rwzi_junction_names)
    rwzi_lhm_coverage["duplicate_rwzi_name_in_model"] = rwzi_lhm_coverage["Naam rwzi"].isin(duplicate_rwzi_names)

    coverage_output_path.parent.mkdir(parents=True, exist_ok=True)
    rwzi_lhm_coverage.to_file(coverage_output_path, driver="GeoJSON")

    if duplicate_rwzi_names:
        logger.warning("RWZI names occurring multiple times in the model:")
        for rwzi_name in sorted(duplicate_rwzi_names):
            logger.warning("  - %s", rwzi_name)

    if verbose:
        logger.info("GeoJSON with LHM model coverage written to: %s", coverage_output_path)
    return rwzi_lhm_coverage


# %% Export GeoJSON with LHM model inclusion flag
def log_rwzi_coverage_report(
    rwzi_coupled_model: Model,
    rwzi_coverage_path: Path,
    coverage_output_path: Path,
    max_distance: float = 100,
) -> GeoDataFrame:
    """
    Log a coverage report for RWZIs in the coupled model.

    Summarizes total RWZIs, coupled RWZIs, and those outside the LHM extent.
    Lists the RWZIs not incorporated into the model.

    Parameters
    ----------
        rwzi_coupled_model (Model): The merged Ribasim model with RWZI nodes coupled to basins.
        rwzi_coverage_path (Path): Coverage GeoJSON produced with the RWZI model.
        coverage_output_path (Path): Output path for the coupled-model coverage GeoJSON.
        max_distance (float): Maximum distance for coupling lookup (same as used in create_rwzi_basin_coupling).

    Returns
    -------
        GeoDataFrame with coverage flags for the coupled model.
    """
    fb_df = rwzi_coupled_model.flow_boundary.node.df
    rwzi_flow_boundaries = fb_df[fb_df["meta_rwzi_code"].notna()]

    junction_df = rwzi_coupled_model.junction.node.df
    rwzi_junctions = junction_df[junction_df["meta_rwzi_code"].notna()]

    total_rwzi = len(rwzi_flow_boundaries)
    coupled_rwzi = len(rwzi_junctions)
    _coupling_lookup, unmatched_rwzi_df = create_rwzi_basin_coupling(rwzi_coupled_model, max_distance=max_distance)
    uncoupled_rwzi = len(unmatched_rwzi_df)

    logger.info(f"\n{'=' * 60}")
    logger.info("RWZI COVERAGE REPORT")
    logger.info(f"  Total RWZIs in coupled model:       {total_rwzi}")
    logger.info(f"  RWZIs coupled to basin (junction):  {coupled_rwzi}")
    logger.info(f"  RWZIs outside LHM (removed):        {uncoupled_rwzi}")
    logger.info(f"{'=' * 60}")
    logger.info("RWZIs not incorporated (outside LHM extent):")
    for _, row in unmatched_rwzi_df.iterrows():
        rwzi_name = row["name"].replace("_out", "")
        logger.info(f"  - {rwzi_name} (RWZI code: {row['meta_rwzi_code']})")
    logger.info(f"{'=' * 60}\n")

    return export_rwzi_coverage_geojson(
        rwzi_coupled_model,
        rwzi_coverage_path,
        coverage_output_path,
        verbose=True,
    )
