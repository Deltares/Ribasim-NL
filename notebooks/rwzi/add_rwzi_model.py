"""
Merge standalone model(s) with the RWZI model.

Replaces the Terminals with Junctions and connects them to the underlying Basin.
"""

# %% Imports
import logging
from datetime import datetime

import geopandas as gpd
import pandas as pd
import ribasim

from ribasim_nl import CloudStorage, Model, concat, prefix_index

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", force=True)

# %%
# TODO: change the names (lhm_mode etc) to more suitable naming
# TODO: Check in the final model if we miss some RWZIs (Texel)

# %%
cloud = CloudStorage()
readme = f"""# Model van (deel)gebieden uit het Landelijk Hydrologisch Model inclusief RWZI afvoeren

Gegenereerd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Ribasim versie: {ribasim.__version__}
Getest (u kunt simuleren): Nee
"""

logging.info(readme)

# %%
log_file = cloud.joinpath("Rijkswaterstaat/modellen/lhm_rwzi/add_rwzi_model.log")

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.handlers.clear()

formatter = logging.Formatter("%(asctime)s - %(levelname)s: %(message)s")

file_handler = logging.FileHandler(log_file, mode="w")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

print(f"Logging to: {log_file}")


# %% Define the models that we want to merge
download_latest_model = True
remove_existing_rwzi = True
upload_model = False

logging.info("SETTINGS")
logging.info(f"  Download latest model: {download_latest_model}")
logging.info(f"  Remove existing RWZIs: {remove_existing_rwzi}")
logging.info(f"  Upload model to GoodCloud: {upload_model}")

# TODO:discuss what the buffer_distance should be
buffer_distance = 20  # meter outside the basin which is still clipped
logging.info(f"Buffer size for clipping RWZI to basin polygons is: {buffer_distance} m")

model_paths = [
    cloud.joinpath("Rijkswaterstaat/modellen/lhm_coupled_2025_9_0/lhm.toml"),
    cloud.joinpath("Basisgegevens/RWZI/modellen/rwzi/rwzi.toml"),
]

# %% Download models and update state
for idx, model_path in enumerate(model_paths):
    model = Model.read(model_path)
    logging.info(f"Load {model_path.parent.name}/{model_path.name}")

    if model_path.stem == "lhm":
        if remove_existing_rwzi:
            logging.info("Check if RWZIs are already present in LHM model")

            for node_type in ["junction", "flow_boundary", "terminal"]:
                node_df = getattr(model, node_type).node.df

                if "meta_rwzi_code" in node_df.columns:
                    rwzi_nodes = node_df[node_df["meta_rwzi_code"].notna()]
                    logging.info(f"Found {len(rwzi_nodes)} {node_type} that correspond to RWZI's")

                    for node_id in rwzi_nodes.index:
                        model.remove_node(node_id, remove_links=True)

                    logging.info(f"Removed {len(rwzi_nodes)} {node_type} nodes")

                else:
                    logging.info(f"Column 'meta_rwzi_codeist' not present in {node_type} — skipping")
        else:
            logging.info("No check if there are already RWZI's in the model")

    if model_path.stem == "rwzi":
        prefix_id = 999

        logging.warning("This model still needs a proper prefix_id")
        try:
            model = prefix_index(
                model=model,
                prefix_id=prefix_id,
            )
        except KeyError as e:
            logging.info("Remove model results (and retry) if a node_id in Basin / state is not in node-table.")
            raise e

    if idx == 0:
        rwzi_coupled_model = model

    else:
        # concat and do not mess with original_index as it has been preserved
        rwzi_coupled_model = concat([rwzi_coupled_model, model], keep_original_index=True)
        logging.info(
            f"Concatenate {model_paths[0].parent.name}/{model_paths[0].name} with {model_paths[1].parent.name}/{model_paths[1].name}"
        )


# %% functions
def create_rwzi_basin_coupling(rwzi_coupled_model, buffer_distance):
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
    terminals_all = rwzi_coupled_model.terminal.node.df
    basins = rwzi_coupled_model.basin.area.df

    # RWZI codes
    rwzi_codes_df = terminals_all[["meta_rwzi_code"]].dropna().copy()

    # Filter terminals met RWZI code
    terminals_filtered = terminals_all[terminals_all.meta_rwzi_code.isin(rwzi_codes_df.meta_rwzi_code)].copy()
    terminals_filtered = terminals_filtered.to_crs(basins.crs)

    # Basins reset
    basins_reset = basins.reset_index().rename(columns={"node_id": "couple_to_basin_id"})

    # 1. Koppel terminals aan onderliggende basins
    joined_within = gpd.sjoin(
        terminals_filtered,
        basins_reset[["couple_to_basin_id", "geometry"]],
        how="left",
        predicate="within",
    )

    # TODO: assign not the first match but the best match
    logging.warning("First match is assigned. For better results check if first match is best match")
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

    # Niet gekoppelde RWZI's
    # unmatched_rwzi_df = rwzis_combined[rwzis_combined["couple_to_basin_id"].isna()][["meta_rwzi_code", "geometry"]]
    unmatched_rwzi_df = rwzis_combined[rwzis_combined["couple_to_basin_id"].isna()][
        ["name", "meta_rwzi_code", "geometry"]
    ]
    return coupling_lookup, unmatched_rwzi_df


def remove_unmatched_rwzi(rwzi_coupled_model, unmatched_rwzi_df, *, verbose=False):
    """
    Verwijder RWZI-terminals.

    Verwijder RWZI-terminals die buiten het model vallen én de daarbij horende
    flow-boundary-knopen uit het LHM-model.
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
        logging.info(
            f"Removed {terminals_removed} RWZI-terminals en {flow_boundaries_removed} FlowBoundaries that fall outside the LHM model."
        )

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
            logging.info(f"  Linked '{row['name']}' (junction {node_id}) to basin {int(basin_node_id)}")
        elif verbose:
            logging.info(f"Geen koppeling gevonden voor RWZI-code {rwzi_code}")

    return rwzi_coupled_model


# %% Create RWZI Basin coupling
coupling_lookup, unmatched_rwzi_df = create_rwzi_basin_coupling(rwzi_coupled_model, buffer_distance)
# %% Remove unmatched RWZIs
rwzi_coupled_model, stats = remove_unmatched_rwzi(rwzi_coupled_model, unmatched_rwzi_df, verbose=True)
# %% Change terminal to junction
rwzi_coupled_model = terminal2junction(rwzi_coupled_model, coupling_lookup, verbose=True)

# %%
logging.info("Write LHM model including RWZIs")
ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/lhm_rwzi/lhm.toml")
rwzi_coupled_model.write(ribasim_toml)

logging.info(f"There are {len(unmatched_rwzi_df)} RWZI's not incorporated in this model")

# %% Export GeoJSON with LHM model inclusion flag
rwzi_coverage_path = cloud.joinpath("Basisgegevens/RWZI/modellen/rwzi/RWZI_coordinates_model_coverage.geojson")
rwzi_gdf = gpd.read_file(rwzi_coverage_path)

junction_df = rwzi_coupled_model.junction.node.df
rwzi_junction_names = set(junction_df[junction_df["meta_rwzi_code"].notna()]["name"].str.replace("_out", ""))

rwzi_gdf_copy = rwzi_gdf.copy()
rwzi_gdf_copy["in_lhm_model"] = rwzi_gdf_copy["Naam rwzi"].isin(rwzi_junction_names)

output_geojson = cloud.joinpath("Rijkswaterstaat/modellen/lhm_rwzi/RWZI_coordinates_lhm_coverage.geojson")
rwzi_gdf_copy.to_file(output_geojson, driver="GeoJSON")

logging.info(f"GeoJSON with LHM model coverage written to: {output_geojson}")


# %% Coverage report
fb_df = rwzi_coupled_model.flow_boundary.node.df
rwzi_flow_boundaries = fb_df[fb_df["meta_rwzi_code"].notna()]

junction_df = rwzi_coupled_model.junction.node.df
rwzi_junctions = junction_df[junction_df["meta_rwzi_code"].notna()]

total_rwzi = len(rwzi_flow_boundaries)
coupled_rwzi = len(rwzi_junctions)
uncoupled_rwzi = len(unmatched_rwzi_df)

logging.info(f"\n{'=' * 60}")
logging.info("RWZI COVERAGE REPORT")
logging.info(f"  Total RWZIs in coupled model:       {total_rwzi}")
logging.info(f"  RWZIs coupled to basin (junction):  {coupled_rwzi}")
logging.info(f"  RWZIs outside LHM (removed):        {uncoupled_rwzi}")
logging.info(f"{'=' * 60}")
logging.info("RWZIs not incorporated (outside LHM extent):")
for _, row in unmatched_rwzi_df.iterrows():
    rwzi_name = row["name"].replace("_out", "")
    logging.info(f"  - {rwzi_name} (RWZI code: {row['meta_rwzi_code']})")
logging.info(f"{'=' * 60}\n")


print("Done.")
