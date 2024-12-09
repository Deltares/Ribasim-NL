# %%
from datetime import datetime

import ribasim

from ribasim_nl import CloudStorage, Model
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.concat import concat
from ribasim_nl.reset_static_tables import reset_static_tables

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

RESET_TABLES = [
    "AaenMaas",
    "BrabantseDelta",
    "DrentsOverijsselseDelta",
    "HunzeenAas",
    "Limburg",
    "Noorderzijlvest",
    "RijnenIJssel",
    "StichtseRijnlanden",
    "ValleienVeluwe",
    "Vechtstromen",
]
models = [
    {
        "authority": "Rijkswaterstaat",
        "model": "hws",
        "find_toml": False,
    },
    {
        "authority": "AmstelGooienVecht",
        "model": "AmstelGooienVecht_parametrized",
        "find_toml": True,
    },
    {
        "authority": "Delfland",
        "model": "Delfland_parametrized",
        "find_toml": True,
    },
    {
        "authority": "HollandseDelta",
        "model": "HollandseDelta_parametrized",
        "find_toml": True,
    },
    {
        "authority": "HollandsNoorderkwartier",
        "model": "HollandsNoorderkwartier_parametrized",
        "find_toml": True,
    },
    {
        "authority": "Rijnland",
        "model": "Rijnland_parametrized",
        "find_toml": True,
    },
    {
        "authority": "Rivierenland",
        "model": "Rivierenland_parametrized",
        "find_toml": True,
    },
    {
        "authority": "Scheldestromen",
        "model": "Scheldestromen_parametrized",
        "find_toml": True,
    },
    {
        "authority": "SchielandendeKrimpenerwaard",
        "model": "SchielandendeKrimpenerwaard_parametrized",
        "find_toml": True,
    },
    {
        "authority": "WetterskipFryslan",
        "model": "WetterskipFryslan_parametrized",
        "find_toml": True,
    },
    {
        "authority": "Zuiderzeeland",
        "model": "Zuiderzeeland_parametrized",
        "find_toml": True,
    },
    {
        "authority": "AaenMaas",
        "model": "AaenMaas",
        "find_toml": True,
    },
    {
        "authority": "BrabantseDelta",
        "model": "BrabantseDelta",
        "find_toml": True,
    },
    {
        "authority": "DeDommel",
        "model": "DeDommel",
        "find_toml": True,
    },
    {
        "authority": "DrentsOverijsselseDelta",
        "model": "DrentsOverijsselseDelta",
        "find_toml": True,
    },
    {
        "authority": "HunzeenAas",
        "model": "HunzeenAas",
        "find_toml": True,
    },
    {
        "authority": "Limburg",
        "model": "Limburg",
        "find_toml": True,
    },
    {
        "authority": "Noorderzijlvest",
        "model": "Noorderzijlvest",
        "find_toml": True,
    },
    {
        "authority": "RijnenIJssel",
        "model": "RijnenIJssel",
        "find_toml": True,
    },
    {
        "authority": "StichtseRijnlanden",
        "model": "StichtseRijnlanden",
        "find_toml": True,
    },
    {
        "authority": "ValleienVeluwe",
        "model": "ValleienVeluwe",
        "find_toml": True,
    },
    {
        "authority": "Vechtstromen",
        "model": "Vechtstromen",
        "find_toml": True,
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
        model_versions = [i for i in cloud.uploaded_models(model["authority"]) if i.model == model["model"]]
        if model_versions:
            model_version = sorted(model_versions, key=lambda x: x.sorter)[-1]
        else:
            raise ValueError(f"No models with name {model["model"]} in the cloud")

    model_path = get_model_path(model, model_version)

    # download model if not yet downloaded
    if not model_path.exists():
        if download_latest_model:
            print(f"Downloaden versie: {model_version.version}")
            url = cloud.joinurl(model["authority"], "modellen", model_version.path_string)
            cloud.download_content(url)
        else:
            model_versions = sorted(model_versions, key=lambda x: x.version, reverse=True)
            model_paths = (get_model_path(model, i) for i in model_versions)
            model_path = next((i for i in model_paths if i.exists()), None)
            if model_path is None:
                raise ValueError(f"No models with name {model["model"]} on local drive")

    # find toml
    if model["find_toml"]:
        tomls = list(model_path.glob("*.toml"))
        if len(tomls) > 1:
            raise ValueError(f"User provided more than one toml-file: {len(tomls)}, remove one!")
        else:
            model_path = tomls[0]
    else:
        model_path = model_path.joinpath(f"{model["model"]}.toml")

    # read model
    ribasim_model = Model.read(model_path)

    # add meta_waterbeheerder
    for node_type in ribasim_model.node_table().df.node_type.unique():
        ribasim_node = getattr(ribasim_model, pascal_to_snake_case(node_type))
        ribasim_node.node.df.loc[:, "meta_waterbeheerder"] = model["authority"]

    if idx == 0:
        lhm_model = ribasim_model
    else:
        # TODO: make sure this isn't needed next round!
        if model["authority"] in RESET_TABLES:
            ribasim_model.remove_unassigned_basin_area()
            ribasim_model = reset_static_tables(ribasim_model)

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
