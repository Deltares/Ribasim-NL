from datetime import datetime
from pathlib import Path
from typing import Any

import ribasim
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.case_conversions import pascal_to_snake_case
from ribasim_nl.cloud import ModelVersion

from ribasim_nl import CloudStorage, Model, concat, prefix_index, reset_index

# %%
cloud = CloudStorage()
readme: str = f"""# Model voor het Landelijk Hydrologisch Model
Gegenereerd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Ribasim versie: {ribasim.__version__}
Getest (u kunt simuleren): Nee

** Samengevoegde modellen (beheerder: modelnaam (versie)**
"""

download_latest_model: bool = True
# Write intermediate models for debugging or scaling tests
write_intermediate_models: bool = False
upload_model: bool = False

# Remove any model from this list to skip it
INCLUDE_MODELS: list[str] = [
    "Rijkswaterstaat",
    "AmstelGooienVecht",
    "Delfland",
    "HollandsNoorderkwartier",
    "HollandseDelta",
    "Rijnland",
    "Rivierenland",
    "Scheldestromen",
    "SchielandendeKrimpenerwaard",
    "WetterskipFryslan",
    "Zuiderzeeland",
    "AaenMaas",
    "BrabantseDelta",
    "DeDommel",
    "DrentsOverijsselseDelta",
    "HunzeenAas",
    "Limburg",
    "Noorderzijlvest",
    "RijnenIJssel",
    "StichtseRijnlanden",
    "ValleienVeluwe",
    "Vechtstromen",
]

# A spec consists of the following keys:
# - authority: str; The authority responsible for the model
#     e.g. "HollandsNoorderkwartier"
# - model: str; The name of the model folder excluding the version
#     e.g. "HollandsNoorderkwartier_parameterized"
# - model_version: ModelVersion; Option
#     e.g. ModelVersion("HollandsNoorderkwartier_parameterized", 2025, 7, 1)
hws_spec: dict[str, Any] = {
    "authority": "Rijkswaterstaat",
    "model": "hws",
}

model_specs: list[dict[str, Any]] = [
    {
        "authority": "HollandsNoorderkwartier",
        "model": "HollandsNoorderkwartier_parameterized",
    },
    {
        "authority": "AmstelGooienVecht",
        "model": "AmstelGooienVecht_parameterized",
    },
    {
        "authority": "Delfland",
        "model": "Delfland_parameterized",
    },
    {
        "authority": "Rijnland",
        "model": "Rijnland_parameterized",
    },
    {
        "authority": "Rivierenland",
        "model": "Rivierenland_parameterized",
    },
    {
        "authority": "Scheldestromen",
        "model": "Scheldestromen_parameterized",
    },
    {
        "authority": "SchielandendeKrimpenerwaard",
        "model": "SchielandendeKrimpenerwaard_parameterized",
    },
    {
        "authority": "WetterskipFryslan",
        "model": "WetterskipFryslan_parameterized",
    },
    {
        "authority": "Zuiderzeeland",
        "model": "Zuiderzeeland_parameterized",
    },
    {
        "authority": "AaenMaas",
        "model": "AaenMaas",
    },
    {
        "authority": "BrabantseDelta",
        "model": "BrabantseDelta",
    },
    {
        "authority": "DeDommel",
        "model": "DeDommel",
    },
    {
        "authority": "DrentsOverijsselseDelta",
        "model": "DrentsOverijsselseDelta",
    },
    {
        "authority": "HunzeenAas",
        "model": "HunzeenAas",
    },
    {
        "authority": "Limburg",
        "model": "Limburg",
    },
    {
        "authority": "Noorderzijlvest",
        "model": "Noorderzijlvest",
    },
    {
        "authority": "RijnenIJssel",
        "model": "RijnenIJssel",
    },
    {
        "authority": "StichtseRijnlanden",
        "model": "StichtseRijnlanden",
    },
    {
        "authority": "ValleienVeluwe",
        "model": "ValleienVeluwe",
    },
    {
        "authority": "Vechtstromen",
        "model": "Vechtstromen",
    },
    {
        "authority": "HollandseDelta",
        "model": "HollandseDelta_parameterized",
    },
]


def get_model_path(model: dict[str, Any], model_version: ModelVersion) -> Path:
    return cloud.joinpath(model["authority"], "modellen", model_version.path_string)


def get_latest_model_version(model_spec: dict[str, Any]) -> ModelVersion:
    if "model_version" in model_spec.keys():
        return model_spec["model_version"]
    model_versions = [
        i
        for i in cloud.uploaded_models(model_spec["authority"])
        if i is not None and getattr(i, "model", None) == model_spec["model"]
    ]
    if model_versions:
        return sorted(model_versions, key=lambda x: getattr(x, "sorter", ""))[-1]
    raise ValueError(f"No models with name {model_spec['model']} in the cloud")


def ensure_model_downloaded(model_spec: dict[str, Any], model_version: ModelVersion) -> Path:
    model_path = get_model_path(model_spec, model_version)
    if not model_path.exists():
        if download_latest_model:
            print(f"Downloaden versie: {model_version.version}")
            url = cloud.joinurl(model_spec["authority"], "modellen", model_version.path_string)
            cloud.download_content(url)
        else:
            model_versions = sorted(
                [
                    i
                    for i in cloud.uploaded_models(model_spec["authority"])
                    if i is not None and getattr(i, "model", None) == model_spec["model"]
                ],
                key=lambda x: getattr(x, "version", ""),
                reverse=True,
            )
            model_paths = (get_model_path(model_spec, i) for i in model_versions)
            model_path = next((i for i in model_paths if i.exists()), None)
            if model_path is None:
                raise ValueError(f"No models with name {model_spec['model']} on local drive")
    return model_path


def find_toml_path(model_dir: Path) -> Path:
    tomls = list(model_dir.glob("*.toml"))
    if len(tomls) == 0:
        raise ValueError(f"No TOML file found at: {model_dir}")
    elif len(tomls) > 1:
        raise ValueError(f"User provided more than one toml-file: {len(tomls)}, remove one! {tomls}")
    return tomls[0]


def read_and_prepare_model(model_path: Path) -> Model:
    model = Model.read(model_path)
    if not model.basin_outstate.filepath.exists():
        print("run model to update state")
        model.write(model_path)  # forced migration
        result = model.run()
        if result.exit_code != 0:
            raise Exception("model won't run successfully!")
    model.update_state()
    return model


def add_meta_waterbeheerder(model: Model, authority: str) -> None:
    for node_type in model.node_table().df.node_type.unique():
        ribasim_node = getattr(model, pascal_to_snake_case(node_type))
        ribasim_node.node.df.loc[:, "meta_waterbeheerder"] = authority


def process_model_spec(
    idx: int, model_spec: dict[str, Any], lhm_model: Model | None, readme: str, write_toml: Path | None = None
) -> tuple[Model | None, str]:
    if model_spec["authority"] not in INCLUDE_MODELS:
        return lhm_model, readme
    print(f"{model_spec['authority']} - {model_spec['model']}")
    model_version = get_latest_model_version(model_spec)
    model_dir = ensure_model_downloaded(model_spec, model_version)
    model_path = find_toml_path(model_dir)
    model = read_and_prepare_model(model_path)
    add_meta_waterbeheerder(model, model_spec["authority"])
    if model_spec["authority"] == "Rijkswaterstaat":
        model = reset_index(model)
    try:
        # TODO reduce max_digits back to 4 after fixing #364
        model = prefix_index(model=model, max_digits=5, prefix_id=waterbeheercode[model_spec["authority"]])
    except KeyError as e:
        print("Remove model results (and retry) if a node_id in Basin / state is not in node-table.")
        raise e
    if lhm_model is None:
        lhm_model = model
    else:
        lhm_model = concat([lhm_model, model], keep_original_index=True)
        assert lhm_model is not None
        lhm_model._validate_model()
        version_str = getattr(model_version, "version", "unknown")
        readme += f"""
**{model_spec["authority"]}**: {model_spec["model"]} ({version_str})"""
    if write_intermediate_models and write_toml is not None:
        assert lhm_model is not None
        lhm_model.write(write_toml)
    return lhm_model, readme


lhm_model = None
readme = f"# Model voor het Landelijk Hydrologisch Model\nGegenereerd: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nRibasim versie: {ribasim.__version__}\nGetest (u kunt simuleren): Nee\n\n** Samengevoegde modellen (beheerder: modelnaam (versie)**\n"
lhm_model, readme = process_model_spec(1, hws_spec, lhm_model, readme)
for idx, model_spec in enumerate(model_specs):
    write_toml = cloud.joinpath(f"Rijkswaterstaat/modellen/lhm-scaling/lhm-{idx + 2:02}/lhm-{idx + 2:02}.toml")
    lhm_model, readme = process_model_spec(idx + 2, model_spec, lhm_model, readme, write_toml=write_toml)
# Write lhm model only if it exists
print("write lhm model")
ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/lhm_parts/lhm.toml")
if lhm_model is not None:
    lhm_model.write(ribasim_toml)
cloud.joinpath("Rijkswaterstaat/modellen/lhm_parts/readme.md").write_text(readme)
if upload_model:
    cloud.upload_model("Rijkswaterstaat", model="lhm_parts")
