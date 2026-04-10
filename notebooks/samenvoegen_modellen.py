# %%
from datetime import datetime
from pathlib import Path
from typing import Any

import ribasim
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.settings import settings

from ribasim_nl import Model, concat, prefix_index

# %%
data_dir = settings.ribasim_nl_data_dir
readme: str = f"""# Model voor het Landelijk Hydrologisch Model
Gegenereerd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Ribasim versie: {ribasim.__version__}
Getest (u kunt simuleren): Nee

** Samengevoegde modellen (beheerder: modelnaam (versie)**
"""

# Write intermediate models for debugging or scaling tests
write_intermediate_models: bool = False

# Remove any model from this list to skip it
# TODO when enabling more models, also add them to samenvoegen deps in dvc.yaml
INCLUDE_MODELS: list[str] = [
    "Rijkswaterstaat",
    # "AmstelGooienVecht",
    # "Delfland",
    # "HollandsNoorderkwartier",
    # "HollandseDelta",
    # "Rijnland",
    # "Rivierenland",
    # "Scheldestromen",
    # "SchielandendeKrimpenerwaard",
    # "WetterskipFryslan",
    # "Zuiderzeeland",
    "AaenMaas",
    # "BrabantseDelta",
    # "DeDommel",
    # "DrentsOverijsselseDelta",
    # "HunzeenAas",
    # "Limburg",
    # "Noorderzijlvest",
    # "RijnenIJssel",
    # "StichtseRijnlanden",
    # "ValleienVeluwe",
    # "Vechtstromen",
]

sub_models: dict[str, list[str]] = {
    # "GR-DR-OV_Delta": ["Noorderzijlvest", "HunzeenAas", "DrentsOverijsselseDelta"],
    # "RDO-Noord": ["Noorderzijlvest", "HunzeenAas", "WetterskipFryslan", "DrentsOverijsselseDelta"],
}


# A spec consists of the following keys:
# - authority: str; The authority responsible for the model
#     e.g. "HollandsNoorderkwartier"
# - model: str; The name of the model folder excluding the version
#     e.g. "HollandsNoorderkwartier_parameterized"
hws_spec: dict[str, Any] = {
    "authority": "Rijkswaterstaat",
    "model": "hws_transient",
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
        "model": "AaenMaas_dynamic_model",
    },
    {
        "authority": "BrabantseDelta",
        "model": "BrabantseDelta_dynamic_model",
    },
    {
        "authority": "DeDommel",
        "model": "DeDommel_dynamic_model",
    },
    {
        "authority": "DrentsOverijsselseDelta",
        "model": "DrentsOverijsselseDelta_dynamic_model",
    },
    {
        "authority": "HunzeenAas",
        "model": "HunzeenAas_dynamic_model",
    },
    {
        "authority": "Limburg",
        "model": "Limburg_dynamic_model",
    },
    {
        "authority": "Noorderzijlvest",
        "model": "Noorderzijlvest_dynamic_model",
    },
    {
        "authority": "RijnenIJssel",
        "model": "RijnenIJssel_dynamic_model",
    },
    {
        "authority": "StichtseRijnlanden",
        "model": "StichtseRijnlanden_dynamic_model",
    },
    {
        "authority": "ValleienVeluwe",
        "model": "ValleienVeluwe_dynamic_model",
    },
    {
        "authority": "Vechtstromen",
        "model": "Vechtstromen_dynamic_model",
    },
    {
        "authority": "HollandseDelta",
        "model": "HollandseDelta_parameterized",
    },
]


def get_model_dir(model_spec: dict[str, Any]) -> Path:
    return data_dir / f"{model_spec['authority']}/modellen/{model_spec['model']}"


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


def process_model_spec(
    idx: int, model_spec: dict[str, Any], lhm_model: Model | None, readme: str, write_toml: Path | None = None
) -> tuple[Model | None, str]:
    if model_spec["authority"] not in INCLUDE_MODELS:
        return lhm_model, readme
    print(f"{model_spec['authority']} - {model_spec['model']}")
    model_dir = get_model_dir(model_spec)
    model_path = find_toml_path(model_dir)
    model = read_and_prepare_model(model_path)
    model.node.df["meta_waterbeheerder"] = model_spec["authority"]
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

    readme += f"""
**{model_spec["authority"]}**: {model_spec["model"]}"""
    if write_intermediate_models and write_toml is not None:
        assert lhm_model is not None
        lhm_model.write(write_toml)
    return lhm_model, readme


lhm_model = None
readme = f"# Model voor het Landelijk Hydrologisch Model\nGegenereerd: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nRibasim versie: {ribasim.__version__}\nGetest (u kunt simuleren): Nee\n\n** Samengevoegde modellen (beheerder: modelnaam (versie)**\n"

for model_name, authorities in sub_models.items():
    print(model_name)
    lhm_model = None
    readme = f"\n\n## Submodel: {model_name}\n** Samengevoegde modellen (beheerder: modelnaam (versie)**\n"
    for authority in authorities:
        if authority == "Rijkswaterstaat":
            model_spec = hws_spec
        else:
            model_spec = next((i for i in model_specs if i["authority"] == authority), None)
        if model_spec is not None:
            lhm_model, readme = process_model_spec(0, model_spec, lhm_model, readme)
    # Write lhm model only if it exists
    assert lhm_model is not None
    ribasim_toml = data_dir / f"Rijkswaterstaat/modellen/lhm_sub_models/{model_name}/{model_name}.toml"
    lhm_model.write(ribasim_toml)
    ribasim_toml.with_name("readme.md").write_text(readme)

lhm_model, readme = process_model_spec(1, hws_spec, lhm_model, readme)
for idx, model_spec in enumerate(model_specs):
    write_toml = data_dir / f"Rijkswaterstaat/modellen/lhm-scaling/lhm-{idx + 2:02}/lhm-{idx + 2:02}.toml"
    lhm_model, readme = process_model_spec(idx + 2, model_spec, lhm_model, readme, write_toml=write_toml)
# Write lhm model only if it exists
print("write lhm model")
ribasim_toml = data_dir / "Rijkswaterstaat/modellen/lhm_parts/lhm.toml"
if lhm_model is not None:
    lhm_model.write(ribasim_toml)
(data_dir / "Rijkswaterstaat/modellen/lhm_parts/readme.md").write_text(readme)
