# %% Imports
import logging
from datetime import datetime

import geopandas as gpd
import pandas as pd
import ribasim

from ribasim_nl import CloudStorage, Model, concat, prefix_index, reset_index
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.case_conversions import pascal_to_snake_case

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

** Samengevoegde modellen (beheerder: modelnaam (versie)**
"""

logging.info(readme)

# %% Define the models that we want to merge
download_latest_model = True
# upload_model = False
# TODO:discuss what the buffer_distance should be
buffer_distance = 20  # meter outside the basin which is still clipped

model_specs_to_merge = [
    {
        "authority": "Rijkswaterstaat",
        "model": "lhm_vrij_coupled",
        "find_toml": True,
    },
    {
        "authority": "DeDommel",
        "model": "DeDommel",
        "find_toml": True,
    },
    # {
    #     "authority": "WetterskipFryslan",
    #     "model": "WetterskipFryslan_boezemmodel",
    #     "find_toml": True,
    # },
]

model_specs = model_specs_to_merge + [
    {
        "authority": "Basisgegevens/RWZI",
        "model": "rwzi",
        "find_toml": True,
    }
]


def get_model_path(model, model_version):
    return cloud.joinpath(model["authority"], "modellen", model_version.path_string)


authorities = [item["authority"] for item in model_specs_to_merge]
logging.info(f"Adding the RWZI's to {authorities}")


# %% Download models and update state
for idx, model_spec in enumerate(model_specs):
    logging.info(f"{model_spec['authority']} - {model_spec['model']}")

    # get version
    if "model_version" in model_spec.keys():
        model_version = model_spec["model_version"]
        logging.info("model version: %s", model_version)

    else:
        model_versions = [i for i in cloud.uploaded_models(model_spec["authority"]) if i.model == model_spec["model"]]
        if model_versions:
            model_version = sorted(model_versions, key=lambda x: x.sorter)[-1]
        else:
            raise ValueError(f"No models with name {model_spec['model']} in the cloud")
        logging.info("model version not defined, latest is: %s", model_version)
    model_path = get_model_path(model_spec, model_version)

    # download model if not yet downloaded
    if not model_path.exists():
        if download_latest_model:
            logging.info(f"Downloaden versie: {model_version.version}")
            url = cloud.joinurl(model_spec["authority"], "modellen", model_version.path_string)
            cloud.download_content(url)
        else:
            model_versions = sorted(model_versions, key=lambda x: x.version, reverse=True)
            model_paths = (get_model_path(model_spec, i) for i in model_versions)
            model_path = next((i for i in model_paths if i.exists()), None)
            if model_path is None:
                raise ValueError(f"No models with name {model_spec['model']} on local drive")

    # find toml
    if model_spec["find_toml"]:
        tomls = list(model_path.glob("*.toml"))
        if len(tomls) > 1:
            raise ValueError(f"User provided more than one toml-file: {len(tomls)}, remove one!")
        else:
            model_path = tomls[0]
            logging.info("found 1 toml-file")
    else:
        model_path = model_path.joinpath(f"{model_spec['model']}.toml")

    # read model
    model = Model.read(model_path)
    logging.info("model is loaded")

    # add meta_waterbeheerder
    for node_type in model.node_table().df.node_type.unique():
        ribasim_node = getattr(model, pascal_to_snake_case(node_type))
        ribasim_node.node.df.loc[:, "meta_waterbeheerder"] = model_spec["authority"]

    if model_spec["authority"] == "Rijkswaterstaat":
        model = reset_index(model)

    # TODO: give the RWZI models a good prefix_id
    if model_spec["authority"] == "Basisgegevens/RWZI":
        prefix_id = 999
        logging.warning("this model still needs a proper prefix_id")

    else:
        prefix_id = waterbeheercode[model_spec["authority"]]

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
        readme += f"""

**{model_spec["authority"]}**: {model_spec["model"]} ({model_version.version})"""


# %% Match the terminals and the underlying basins
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


coupling_lookup, unmatched_rwzi_df = create_rwzi_basin_coupling(rwzi_coupled_model, buffer_distance)


# %% Verwijder de RWZI's die buiten het model vallen
def remove_unmatched_rwzi(rwzi_coupled_model, unmatched_rwzi_df, *, verbose=False):
    """
    Verwijder RWZI‑terminals.

    Verwijder RWZI‑terminals die buiten het model vallen én de daarbij horende flow‑boundary‑knopen uit het LHM‑model en geef het model terug.
    """
    #  1. RWZI‑terminals verwijderen
    terminals_removed = 0
    for node_id in unmatched_rwzi_df.index:
        rwzi_coupled_model.remove_node(node_id, remove_edges=True)
        terminals_removed += 1

    #  2. bijbehorende flow boundaries verwijderen
    fb_df = rwzi_coupled_model.flow_boundary.node.df
    flow_boundary_to_remove = fb_df[fb_df["meta_rwzi_code"].isin(unmatched_rwzi_df["meta_rwzi_code"])]

    flow_boundaries_removed = 0
    for node_id in flow_boundary_to_remove.index:
        rwzi_coupled_model.remove_node(node_id, remove_edges=True)
        flow_boundaries_removed += 1

    #  samenvatting & return
    stats = {
        "terminals_removed": terminals_removed,
        "flow_boundaries_removed": flow_boundaries_removed,
    }

    if verbose:
        print(f"Verwijderd: {terminals_removed} RWZI‑terminals en {flow_boundaries_removed} flow‑boundary‑knopen.")

    return rwzi_coupled_model, stats


rwzi_coupled_model, stats = remove_unmatched_rwzi(rwzi_coupled_model, unmatched_rwzi_df, verbose=True)


# %% Verander de terminals in junctions en koppel deze aan de bijbehorende basins
def terminal2junction(rwzi_coupled_model, coupling_lookup, *, verbose=False):
    """
    Zet RWZI-terminals om naar junctions.

    Verbind ze daarna met de juiste basin-knoop.
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
            rwzi_coupled_model.edge.add(
                rwzi_coupled_model.get_node(node_id),
                rwzi_coupled_model.get_node(int(basin_node_id)),
                name=row["name"],
            )
        elif verbose:
            print(f"Geen koppeling gevonden voor RWZI-code {rwzi_code}")

    return rwzi_coupled_model


rwzi_coupled_model = terminal2junction(rwzi_coupled_model, coupling_lookup, verbose=True)


# %%
print("write coupled rwzi model")
ribasim_toml = cloud.joinpath(
    "Basisgegevens",
    "RWZI",
    "modellen",
    f"rwzi_coupled_{authorities[0]}",
    "rwzi_coupled.toml",
)
rwzi_coupled_model.write(ribasim_toml)

logging.info(f"There are {len(unmatched_rwzi_df)} RWZI's not incorporated in this model")

# %%
ribasim_path = r"c:\Program Files\Deltares\ribasim\ribasim.exe"

cloud.joinpath("Basisgegevens", "RWZI", "modellen", f"rwzi_coupled_{authorities[0]}", "readme.md").write_text(readme)

upload_model = True

# TODO: Ik krijg een error hier
# if upload_model:
#     cloud.upload_model("Basisgegevens/RWZI", model=f"rwzi_coupled_{authorities[0]}")


# result = subprocess.run(
#     [ribasim_path, ribasim_toml], capture_output=True, encoding="utf-8"
# )

# %%
