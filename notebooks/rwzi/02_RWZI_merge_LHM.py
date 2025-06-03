# %% Imports
import logging
from datetime import datetime

import geopandas as gpd
import pandas as pd
import ribasim

# import subprocess
from ribasim_nl import CloudStorage, Model, concat, prefix_index, reset_index
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.case_conversions import pascal_to_snake_case

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s", force=True)

# %%
# TODO: change the names (lhm_mode etc) to more suitable naming
# Check in the final model if we miss some RWZIs (Texel)

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
upload_model = False
# TODO:discuss what the buffer_distance should be
buffer_distance = 20  # meter outside the basin which is still clipped

RESET_TABLES = []

model_specs_to_merge = [
    {
        "authority": "Rijkswaterstaat",
        "model": "hws",
        "find_toml": False,
    },
    {
        "authority": "DeDommel",
        "model": "DeDommel",
        "find_toml": True,
    },
    {
        "authority": "WetterskipFryslan",
        "model": "WetterskipFryslan_boezemmodel",
        "find_toml": True,
    },
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

    # TODO: give the RWZI models a real code
    # waterbeheercode[model_spec["Basisgegevens/RWZI"]] = 999
    # print(waterbeheercode[model_spec["authority"]])

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
        lhm_model = model

    else:
        # concat and do not mess with original_index as it has been preserved
        lhm_model = concat([lhm_model, model], keep_original_index=True)
        readme += f"""

**{model_spec["authority"]}**: {model_spec["model"]} ({model_version.version})"""


# %%
# points_all = lhm_model.terminal.node.df
# basins = lhm_model.basin.area.df

# rwzi_codes_df = points_all[["meta_rwzi_code"]].dropna().copy()

# points_filtered = points_all[
#     points_all.meta_rwzi_code.isin(rwzi_codes_df.meta_rwzi_code)
# ].copy()

# points_filtered = points_filtered.to_crs(basins.crs)

# basins_reset = basins.reset_index()
# basins_reset = basins_reset.rename(columns={"node_id": "couple_to_basin_id"})

# joined_within = gpd.sjoin(
#     points_filtered,
#     basins_reset[["couple_to_basin_id", "geometry"]],
#     how="left",
#     predicate="within",
# )

# unmatched_points = points_filtered.loc[joined_within["couple_to_basin_id"].isna()]
# basins_buffered = basins_reset.copy()
# basins_buffered["geometry"] = basins_buffered.geometry.buffer(buffer_distance)

# joined_nearby = gpd.sjoin(
#     unmatched_points,
#     basins_buffered[["couple_to_basin_id", "geometry"]],
#     how="left",
#     predicate="within",
# )

# joined_combined = joined_within.copy()
# buffer_matches = joined_nearby[["couple_to_basin_id"]]
# joined_combined.loc[unmatched_points.index, "couple_to_basin_id"] = buffer_matches[
#     "couple_to_basin_id"
# ]


# coupling_df = joined_combined[["meta_rwzi_code", "couple_to_basin_id"]].dropna()
# coupling_lookup = dict(zip(coupling_df.meta_rwzi_code, coupling_df.couple_to_basin_id))

# unmatched_rwzi_df = joined_combined[joined_combined["couple_to_basin_id"].isna()][
#     ["meta_rwzi_code", "geometry"]
# ]


def create_rwzi_basin_coupling(lhm_model, buffer_distance):
    """
    Match RWZI points to basin polygons using spatial join.

    First tries exact match (within), then falls back to buffer match.

    Args:
        lhm_model: Object with attributes terminal.node.df and basin.area.df (GeoDataFrames).
        buffer_distance (float): Distance in CRS units to buffer basin geometries.

    Returns
    -------
        coupling_lookup (dict): Maps RWZI codes to basin IDs.
        unmatched_rwzi_df (GeoDataFrame): RWZI points not matched to any basin.
    """
    points_all = lhm_model.terminal.node.df
    basins = lhm_model.basin.area.df

    # Extract RWZI codes
    rwzi_codes_df = points_all[["meta_rwzi_code"]].dropna().copy()

    # Filter points that have a RWZI code
    points_filtered = points_all[points_all.meta_rwzi_code.isin(rwzi_codes_df.meta_rwzi_code)].copy()

    # Match CRS
    points_filtered = points_filtered.to_crs(basins.crs)

    # Prepare basins
    basins_reset = basins.reset_index().rename(columns={"node_id": "couple_to_basin_id"})

    # First join: points within basins
    joined_within = gpd.sjoin(
        points_filtered,
        basins_reset[["couple_to_basin_id", "geometry"]],
        how="left",
        predicate="within",
    )

    # Find unmatched
    unmatched_points = points_filtered.loc[joined_within["couple_to_basin_id"].isna()]

    # Buffer the basins
    basins_buffered = basins_reset.copy()
    basins_buffered["geometry"] = basins_buffered.geometry.buffer(buffer_distance)

    # Second join: buffered basins
    joined_nearby = gpd.sjoin(
        unmatched_points,
        basins_buffered[["couple_to_basin_id", "geometry"]],
        how="left",
        predicate="within",
    )

    # Combine matches
    joined_combined = joined_within.copy()
    buffer_matches = joined_nearby[["couple_to_basin_id"]]
    joined_combined.loc[unmatched_points.index, "couple_to_basin_id"] = buffer_matches["couple_to_basin_id"]

    # Build lookup and unmatched table
    coupling_df = joined_combined[["meta_rwzi_code", "couple_to_basin_id"]].dropna()
    coupling_lookup = dict(zip(coupling_df.meta_rwzi_code, coupling_df.couple_to_basin_id))

    unmatched_rwzi_df = joined_combined[joined_combined["couple_to_basin_id"].isna()][["meta_rwzi_code", "geometry"]]

    return coupling_lookup, unmatched_rwzi_df


coupling_lookup, unmatched_rwzi_df = create_rwzi_basin_coupling(lhm_model, buffer_distance)


# %% remove RWZI's (terminals) which are outside the basin area of this model
for node_id in unmatched_rwzi_df.index:
    lhm_model.remove_node(node_id, remove_edges=True)


# %% remove the flow boundaries which correspond with those terminals
flow_boundary_to_remove = lhm_model.flow_boundary.node.df[
    lhm_model.flow_boundary.node.df["meta_rwzi_code"].isin(unmatched_rwzi_df.meta_rwzi_code)
]

for node_id in flow_boundary_to_remove.index:
    lhm_model.remove_node(node_id, remove_edges=True)

# %% Chang the terminals of the RWZI's into junctions
for node_id, row in lhm_model.terminal.node.df[lhm_model.terminal.node.df.meta_rwzi_code.notna()].iterrows():
    print(node_id, row)
    lhm_model.update_node(node_id, "Junction", data=[])

# %% Connect the junctions with the corresponding basin node

for node_id, row in lhm_model.junction.node.df[lhm_model.junction.node.df.meta_rwzi_code.notna()].iterrows():
    rwzi_code = row.meta_rwzi_code
    basin_node_id = coupling_lookup.get(rwzi_code, None)

    if pd.notna(basin_node_id):
        basin_node = lhm_model.basin.node.df.loc[basin_node_id]
        print(basin_node, node_id)

        lhm_model.edge.add(
            lhm_model.get_node(node_id),
            lhm_model.get_node(int(basin_node_id)),
            name=row["name"],
        )

# %%
print("write lhm model")
ribasim_toml = cloud.joinpath("Basisgegevens", "RWZI", "modellen", "lhm_rwzi", "lhm_rwzi.toml")
lhm_model.write(ribasim_toml)

logging.info(f"There are {len(unmatched_rwzi_df)} RWZI's not incorporated in this model")

# %%

result = lhm_model.run()

# %%
# ribasim_path = r"c:\Program Files\Deltares\ribasim\ribasim.exe"
# ribasim_toml

# result = subprocess.run([ribasim_path, ribasim_toml], capture_output=True, encoding="utf-8")
# print(result.stderr)
# result.check_returncode()

# %%
cloud.joinpath("Basisgegevens", "RWZI", "modellen", "readme.md").write_text(readme)
