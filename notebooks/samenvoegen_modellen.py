# %%
import itertools
from datetime import datetime

import numpy as np
import ribasim
from bokeh.palettes import Category10
from ribasim_nl import CloudStorage
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.cloud import ModelVersion
from ribasim_nl.concat import concat

# %%
cloud = CloudStorage()
readme = f"""# Model voor het Landelijk Hydrologisch Model

Gegenereerd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Ribasim versie: {ribasim.__version__}
Getest (u kunt simuleren): Nee

** Samengevoegde modellen (beheerder: modelnaam (versie)**
"""

download_latest_model = True
upload_model = False

models = [
    {
        "authority": "Rijkswaterstaat",
        "model": "hws",
        "find_toml": False,
        "zoom_level": 0,
    },
    {
        "authority": "AmstelGooienVecht",
        "model": "AmstelGooienVecht_boezemmodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Delfland",
        "model": "Delfland_boezemmodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "HollandseDelta",
        "model": "HollandseDelta_boezemmodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "HollandsNoorderkwartier",
        "model": "HollandsNoorderkwartier_boezemmodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Rijnland",
        "model": "Rijnland_boezemmodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Rivierenland",
        "model": "Rivierenland_boezemmodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Scheldestromen",
        "model": "Scheldestromen_boezemmodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "SchielandendeKrimpenerwaard",
        "model": "SchielandendeKrimpenerwaard_boezemmodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "WetterskipFryslan",
        "model": "WetterskipFryslan_boezemmodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Zuiderzeeland",
        "model": "Zuiderzeeland_boezemmodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "AaenMaas",
        "model": "AaenMaas",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "BrabantseDelta",
        "model": "BrabantseDelta",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "DeDommel",
        "model": "DeDommel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "DrentsOverijsselseDelta",
        "model": "DrentsOverijsselseDelta",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "HunzeenAas",
        "model": "HunzeenAas",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Limburg",
        "model": "Limburg",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Noorderzijlvest",
        "model": "Noorderzijlvest",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "RijnenIJssel",
        "model": "RijnenIJssel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "StichtseRijnlanden",
        "model": "StichtseRijnlanden",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "ValleienVeluwe",
        "model": "ValleienVeluwe",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Vechtstromen",
        "model": "Vechtstromen",
        "find_toml": True,
        "zoom_level": 3,
    },
]


def get_model_path(model, model_version):
    return cloud.joinpath(model["authority"], "modellen", model_version.path_string)


# %%
for idx, model in enumerate(models):
    print(f"{model["authority"]} - {model["model"]}")

    # get version
    if "model_version" in model.keys():
        model_version = model["model_version"]

    else:
        model_versions = [
            i
            for i in cloud.uploaded_models(model["authority"])
            if i.model == model["model"]
        ]
        if model_versions:
            model_version = sorted(model_versions, key=lambda x: x.sorter)[-1]
        else:
            raise ValueError(f"No models with name {model["model"]} in the cloud")

    model_path = get_model_path(model, model_version)

    # download model if not yet downloaded
    if not model_path.exists():
        if download_latest_model:
            print(f"Downloaden versie: {model_version.version}")
            url = cloud.joinurl(
                model["authority"], "modellen", model_version.path_string
            )
            cloud.download_content(url)
        else:
            model_versions = sorted(
                model_versions, key=lambda x: x.version, reverse=True
            )
            model_paths = (get_model_path(model, i) for i in model_versions)
            model_path = next((i for i in model_paths if i.exists()), None)
            if model_path is None:
                raise ValueError(f"No models with name {model["model"]} on local drive")

    # find toml
    if model["find_toml"]:
        tomls = list(model_path.glob("*.toml"))
        if len(tomls) > 1:
            raise ValueError(
                f"User provided more than one toml-file: {len(tomls)}, remove one!"
            )
        else:
            model_path = tomls[0]
    else:
        model_path = model_path.joinpath(f"{model["model"]}.toml")

    # read model
    ribasim_model = ribasim.Model.read(model_path)

    # zoom_level to model
    for node_type in ribasim_model.node_table().df.node_type.unique():
        ribasim_node = getattr(ribasim_model, pascal_to_snake_case(node_type))
        ribasim_node.node.df.loc[:, "meta_zoom_level"] = model["zoom_level"]
        ribasim_node.node.df.loc[:, "meta_waterbeheerder"] = model["authority"]

    ribasim_model.edge.df.loc[:, "meta_zoom_level"] = model["zoom_level"]
    ribasim_model.edge.df.loc[:, "meta_waterbeheerder"] = model["authority"]

    if idx == 0:
        lhm_model = ribasim_model
    else:
        if "meta_categorie" in ribasim_model.edge.df.columns:
            # tric to use higher zoom-level for primary waters

            ribasim_model.edge.df.loc[
                ribasim_model.edge.df["meta_categorie"] == "hoofdwater",
                "meta_zoom_level",
            ] = 2
            df = ribasim_model.edge.df[ribasim_model.edge.df["meta_zoom_level"] == 2]
            if not df.empty:
                node_ids = set(
                    np.concatenate(
                        ribasim_model.edge.df[
                            ribasim_model.edge.df["meta_zoom_level"] == 2
                        ][["from_node_id", "to_node_id"]].to_numpy()
                    )
                )
                # zoom_level to model
                for node_type in ribasim_model.node_table().df.node_type.unique():
                    ribasim_node = getattr(
                        ribasim_model, pascal_to_snake_case(node_type)
                    )
                    ribasim_node.node.df.loc[
                        ribasim_node.node.df["node_id"].isin(node_ids),
                        "meta_zoom_level",
                    ] = 2

        cols = [i for i in lhm_model.edge.df.columns if i != "meta_index"]
        lhm_model.edge.df = lhm_model.edge.df[cols]
        try:
            lhm_model = concat([lhm_model, ribasim_model])
        except KeyError:
            print(f"model {model['authority']} is waarschijnlijk niet aan de haak")
            pass

        readme += f"""
    **{model["authority"]}**: {model["model"]} ({model_version.version})"""

# %% color
# color_cycle = itertools.cycle(Category10[10])
# lhm_model.basin.area.df.loc[
#     lhm_model.basin.area.df["meta_streefpeil"] == "Onbekend streefpeil",
#     "meta_streefpeil",
# ] = None
# lhm_model.basin.area.df.loc[:, "meta_streefpeil"] = lhm_model.basin.area.df[
#     "meta_streefpeil"
# ].astype(float)

# lhm_model.basin.area.df.loc[:, "meta_color"] = [
#     next(color_cycle) for _ in range(len(lhm_model.basin.area.df))
# ]

# %%
print("write lhm model")
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "lhm.toml")
lhm_model.write(ribasim_toml)
cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "readme.md").write_text(readme)
# %%
if upload_model:
    cloud.upload_model("Rijkswaterstaat", model="lhm")
