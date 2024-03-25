# %%
from datetime import datetime

import ribasim
from ribasim_nl import CloudStorage
from ribasim_nl.concat import concat

# %%
cloud = CloudStorage()
readme = f"""# Model voor het Landelijk Hydrologisch Model

Gegenereerd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Ribasim versie: {ribasim.__version__}
Getest (u kunt simuleren): Nee

** Samengevoegde modellen (beheerder: modelnaam (versie)**
"""

download_latest_model = False
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
        "model": "AmstelGooienVecht_poldermodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Delfland",
        "model": "Delfland_poldermodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "HollandseDelta",
        "model": "HollandseDelta_poldermodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "HollandsNoorderkwartier",
        "model": "HollandsNoorderkwartier_poldermodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Rijnland",
        "model": "Rijnland_poldermodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Rivierenland",
        "model": "Rivierenland_poldermodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Scheldestromen",
        "model": "Scheldestromen_poldermodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "SchielandendeKrimpenerwaard",
        "model": "SchielandendeKrimpenerwaard_poldermodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "WetterskipFryslan",
        "model": "WetterskipFryslan_poldermodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Zuiderzeeland",
        "model": "Zuiderzeeland_poldermodel",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "AaenMaas",
        "model": "ribasim_model",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "BrabantseDelta",
        "model": "ribasim_model",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "DeDommel",
        "model": "ribasim_model",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "DrentsOverijsselseDelta",
        "model": "ribasim_model",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "HunzeenAas",
        "model": "ribasim_model",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Limburg",
        "model": "ribasim_model",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Noorderzijlvest",
        "model": "ribasim_model",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "RijnenIJssel",
        "model": "ribasim_model",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "StichtseRijnlanden",
        "model": "ribasim_model",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "ValleienVeluwe",
        "model": "ribasim_model",
        "find_toml": True,
        "zoom_level": 3,
    },
    {
        "authority": "Vechtstromen",
        "model": "ribasim_model",
        "find_toml": True,
        "zoom_level": 3,
    },
]


def get_model_path(model, model_version):
    return cloud.joinpath(model["authority"], "modellen", model_version.path_string)


# %%
for idx, model in enumerate(models):
    print(f"{model["authority"]} - {model["model"]}")
    model_versions = [
        i
        for i in cloud.uploaded_models(model["authority"])
        if i.model == model["model"]
    ]
    if model_versions:
        model_version = sorted(model_versions, key=lambda x: x.version)[-1]
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
    ribasim_model.network.node.df.loc[:, "meta_zoom_level"] = model["zoom_level"]
    ribasim_model.network.edge.df.loc[:, "meta_zoom_level"] = model["zoom_level"]
    if idx == 0:
        lhm_model = ribasim_model
    else:
        cols = [i for i in lhm_model.network.edge.df.columns if i != "meta_index"]
        lhm_model.network.edge.df = lhm_model.network.edge.df[cols]
        ribasim_model.network.node.df.loc[:, "meta_waterbeheerder"] = model["authority"]
        ribasim_model.network.edge.df.loc[:, "meta_waterbeheerder"] = model["authority"]
        lhm_model = concat([lhm_model, ribasim_model])

    readme += f"""
**{model["authority"]}**: {model["model"]} ({model_version.version})"""

# %%
print("write lhm model")
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "lhm.toml")
lhm_model.write(ribasim_toml)
cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "readme.md").write_text(readme)
# %%
if upload_model:
    cloud.upload_model("Rijkswaterstaat", model="lhm")
