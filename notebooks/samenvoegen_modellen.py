# %%
from datetime import datetime

import ribasim

from ribasim_nl import CloudStorage, Model, concat, prefix_index, reset_index
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.case_conversions import pascal_to_snake_case
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

RESET_TABLES = []
INCLUDE_MODELS = ["Rijkswaterstaat", "AmstelGooienVecht", "Rijnland", "HollandseDelta", "Delfland"]

model_specs = [
    {
        "authority": "Rijkswaterstaat",
        "model": "hws",
        "find_toml": False,
    },
    {
        "authority": "AmstelGooienVecht",
        "model": "AmstelGooienVecht_parameterized",
        "find_toml": True,
    },
    {
        "authority": "Delfland",
        "model": "Delfland_parameterized",
        "find_toml": True,
    },
    {
        "authority": "HollandseDelta",
        "model": "HollandseDelta_parameterized",
        "find_toml": True,
    },
    {
        "authority": "HollandsNoorderkwartier",
        "model": "HollandsNoorderkwartier_parameterized",
        "find_toml": True,
    },
    {
        "authority": "Rijnland",
        "model": "Rijnland_parameterized",
        "find_toml": True,
    },
    {
        "authority": "Rivierenland",
        "model": "Rivierenland_parameterized",
        "find_toml": True,
    },
    {
        "authority": "Scheldestromen",
        "model": "Scheldestromen_parameterized",
        "find_toml": True,
    },
    {
        "authority": "SchielandendeKrimpenerwaard",
        "model": "SchielandendeKrimpenerwaard_parameterized",
        "find_toml": True,
    },
    {
        "authority": "WetterskipFryslan",
        "model": "WetterskipFryslan_parameterized",
        "find_toml": True,
    },
    {
        "authority": "Zuiderzeeland",
        "model": "Zuiderzeeland_parameterized",
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
if INCLUDE_MODELS:
    model_specs = [i for i in model_specs if i["authority"] in INCLUDE_MODELS]

for idx, model_spec in enumerate(model_specs):
    print(f"{model_spec['authority']} - {model_spec['model']}")

    # get version
    if "model_version" in model_spec.keys():
        model_version = model_spec["model_version"]

    else:
        model_versions = [i for i in cloud.uploaded_models(model_spec["authority"]) if i.model == model_spec["model"]]
        if model_versions:
            model_version = sorted(model_versions, key=lambda x: x.sorter)[-1]
        else:
            raise ValueError(f"No models with name {model_spec['model']} in the cloud")

    model_path = get_model_path(model_spec, model_version)

    # download model if not yet downloaded
    if not model_path.exists():
        if download_latest_model:
            print(f"Downloaden versie: {model_version.version}")
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
    else:
        model_path = model_path.joinpath(f"{model_spec['model']}.toml")

    # read model
    model = Model.read(model_path)

    # TODO: make sure this isn't needed next round!
    if model_spec["authority"] in RESET_TABLES:
        model.remove_unassigned_basin_area()
        model = reset_static_tables(model)

    # run model
    if not model.basin_outstate.filepath.exists():
        print("run model to update state")
        result = model.run()
        if result.exit_code != 0:
            raise Exception("model won't run successfully!")
    model.update_state()

    # add meta_waterbeheerder
    for node_type in model.node_table().df.node_type.unique():
        ribasim_node = getattr(model, pascal_to_snake_case(node_type))
        ribasim_node.node.df.loc[:, "meta_waterbeheerder"] = model_spec["authority"]

    # reset index of RWS model so we get subsequent ids
    if model_spec["authority"] == "Rijkswaterstaat":
        model = reset_index(model)

    # prefix index so ids will be unique
    try:
        model = prefix_index(model=model, prefix_id=waterbeheercode[model_spec["authority"]])
    except KeyError as e:
        print("Remove model results (and retry) if a node_id in Basin / state is not in node-table.")
        raise e

    if idx == 0:
        lhm_model = model
    else:
        # concat and do not mess with original_index as it has been preserved
        lhm_model = concat([lhm_model, model], keep_original_index=True)
        lhm_model._validate_model()
        readme += f"""
**{model_spec["authority"]}**: {model_spec["model"]} ({model_version.version})"""


# %%
print("write lhm model")
ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "lhm.toml")
lhm_model.write(ribasim_toml)
cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "readme.md").write_text(readme)
# %%
if upload_model:
    cloud.upload_model("Rijkswaterstaat", model="lhm")
