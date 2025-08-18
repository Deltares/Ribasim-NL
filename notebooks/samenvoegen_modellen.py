from datetime import datetime

import ribasim

from ribasim_nl import CloudStorage, Model, concat, prefix_index, reset_index
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.case_conversions import pascal_to_snake_case

# %%
cloud = CloudStorage()
readme = f"""# Model voor het Landelijk Hydrologisch Model
Gegenereerd: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
Ribasim versie: {ribasim.__version__}
Getest (u kunt simuleren): Nee

** Samengevoegde modellen (beheerder: modelnaam (versie)**
"""

download_latest_model = True
# Write each unique Regionaal Droogte Overleg area to a separate model
write_rdo = True
# Write one national model
write_lhm = True
# Write intermediate models for debugging or scaling tests
write_intermediate_models = True
upload_model = False

# Remove any model from this list to skip it
INCLUDE_MODELS = [
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
# - authority: The authority responsible for the model, used to find it
# - model: The name of the TOML model
# - rdo: The RDO (Regionaal Droogte Overleg) the model belongs to
# - find_toml: Whether to find one *.toml, or rely on the model key
hws_spec = {
    "authority": "Rijkswaterstaat",
    "model": "hws",
    "rdo": None,
    "find_toml": False,
}

model_specs = [
    {
        "authority": "AmstelGooienVecht",
        "model": "AmstelGooienVecht_parameterized",
        "rdo": "RDO-West-Midden",
        "find_toml": True,
    },
    {
        "authority": "Delfland",
        "model": "Delfland_parameterized",
        "rdo": "RDO-West-Midden",
        "find_toml": True,
    },
    {
        "authority": "HollandsNoorderkwartier",
        "model": "HollandsNoorderkwartier_parameterized",
        "rdo": "RDO-Noord",
        "find_toml": True,
    },
    {
        "authority": "Rijnland",
        "model": "Rijnland_parameterized",
        "rdo": "RDO-West-Midden",
        "find_toml": True,
    },
    {
        "authority": "Rivierenland",
        "model": "Rivierenland_parameterized",
        "rdo": "RDO-Gelderland",
        "find_toml": True,
    },
    {
        "authority": "Scheldestromen",
        "model": "Scheldestromen_parameterized",
        "rdo": "RDO-Zuid-West",
        "find_toml": True,
    },
    {
        "authority": "SchielandendeKrimpenerwaard",
        "model": "SchielandendeKrimpenerwaard_parameterized",
        "rdo": "RDO-West-Midden",
        "find_toml": True,
    },
    {
        "authority": "WetterskipFryslan",
        "model": "WetterskipFryslan_parameterized",
        "rdo": "RDO-Noord",
        "find_toml": True,
    },
    {
        "authority": "Zuiderzeeland",
        "model": "Zuiderzeeland_parameterized",
        "rdo": "RDO-Noord",
        "find_toml": True,
    },
    {
        "authority": "AaenMaas",
        "model": "AaenMaas",
        "rdo": "RDO-Zuid-Oost",
        "find_toml": True,
    },
    {
        "authority": "BrabantseDelta",
        "model": "BrabantseDelta",
        "rdo": "RDO-Zuid-West",
        "find_toml": True,
    },
    {
        "authority": "DeDommel",
        "model": "DeDommel",
        "rdo": "RDO-Zuid-Oost",
        "find_toml": True,
    },
    {
        "authority": "DrentsOverijsselseDelta",
        "model": "DrentsOverijsselseDelta",
        "rdo": "RDO-Twentekanalen",
        "find_toml": True,
    },
    {
        "authority": "HunzeenAas",
        "model": "HunzeenAas",
        "rdo": "RDO-Noord",
        "find_toml": True,
    },
    {
        "authority": "Limburg",
        "model": "Limburg",
        "rdo": "RDO-Zuid-Oost",
        "find_toml": True,
    },
    {
        "authority": "Noorderzijlvest",
        "model": "Noorderzijlvest",
        "rdo": "RDO-Noord",
        "find_toml": True,
    },
    {
        "authority": "RijnenIJssel",
        "model": "RijnenIJssel",
        "rdo": "RDO-Gelderland",
        "find_toml": True,
    },
    {
        "authority": "StichtseRijnlanden",
        "model": "StichtseRijnlanden",
        "rdo": "RDO-West-Midden",
        "find_toml": True,
    },
    {
        "authority": "ValleienVeluwe",
        "model": "ValleienVeluwe",
        "rdo": "RDO-Gelderland",
        "find_toml": True,
    },
    {
        "authority": "Vechtstromen",
        "model": "Vechtstromen",
        "rdo": "RDO-Twentekanalen",
        "find_toml": True,
    },
    {
        "authority": "HollandseDelta",
        "model": "HollandseDelta_parameterized",
        "rdo": "RDO-West-Midden",
        "find_toml": True,
    },
]


def get_model_path(model, model_version):
    return cloud.joinpath(model["authority"], "modellen", model_version.path_string)


def get_latest_model_version(model_spec):
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


def ensure_model_downloaded(model_spec, model_version):
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


def find_toml_path(model_spec, model_path):
    if model_spec["find_toml"]:
        tomls = list(model_path.glob("*.toml"))
        if len(tomls) == 0:
            raise ValueError(f"No TOML file found at: {model_path}")
        elif len(tomls) > 1:
            raise ValueError(f"User provided more than one toml-file: {len(tomls)}, remove one! {tomls}")
        return tomls[0]
    else:
        return model_path.joinpath(f"{model_spec['model']}.toml")


def read_and_prepare_model(model_path):
    model = Model.read(model_path)
    if not model.basin_outstate.filepath.exists():
        print("run model to update state")
        model.write(model_path)  # forced migration
        result = model.run()
        if result.exit_code != 0:
            raise Exception("model won't run successfully!")
    model.update_state()
    return model


def add_meta_waterbeheerder(model, authority):
    for node_type in model.node_table().df.node_type.unique():
        ribasim_node = getattr(model, pascal_to_snake_case(node_type))
        ribasim_node.node.df.loc[:, "meta_waterbeheerder"] = authority


def process_model_spec(idx, model_spec, lhm_model, readme, write_toml=None):
    if model_spec["authority"] not in INCLUDE_MODELS:
        return lhm_model, readme
    print(f"{model_spec['authority']} - {model_spec['model']}")
    model_version = get_latest_model_version(model_spec)
    model_path = ensure_model_downloaded(model_spec, model_version)
    model_path = find_toml_path(model_spec, model_path)
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
        lhm_model._validate_model()
        version_str = getattr(model_version, "version", "unknown")
        readme += f"""
**{model_spec["authority"]}**: {model_spec["model"]} ({version_str})"""
    if write_intermediate_models and write_toml is not None:
        lhm_model.write(write_toml)
    return lhm_model, readme


# --- RDO writing logic ---
if write_rdo:
    # Get all unique rdos (excluding None)
    rdos = sorted({spec["rdo"] for spec in model_specs if spec.get("rdo")})
    rdo_models = {}
    rdo_readmes = {}
    for rdo in rdos:
        # Start each rdo model with hws
        rdo_model = None
        rdo_readme = f"# Model voor RDO: {rdo}\nBegint met hws (Rijkswaterstaat)\n"
        # Add hws as first model
        rdo_model, rdo_readme = process_model_spec(1, hws_spec, rdo_model, rdo_readme)
        for idx, model_spec in enumerate(model_specs):
            if model_spec.get("rdo") == rdo:
                write_toml = cloud.joinpath(f"Rijkswaterstaat/modellen/{rdo}/{rdo}-{idx:02}/{rdo}-{idx:02}.toml")
                rdo_model, rdo_readme = process_model_spec(
                    idx + 2, model_spec, rdo_model, rdo_readme, write_toml=write_toml
                )
        # Write final rdo model
        ribasim_toml = cloud.joinpath(f"Rijkswaterstaat/modellen/{rdo}/{rdo}/{rdo}.toml")
        rdo_models[rdo] = rdo_model
        rdo_readmes[rdo] = rdo_readme
        if rdo_model is not None:
            rdo_model.write(ribasim_toml)
        cloud.joinpath(f"Rijkswaterstaat/modellen/{rdo}/{rdo}/readme.md").write_text(rdo_readme)


if write_lhm:
    lhm_model = None
    readme = f"# Model voor het Landelijk Hydrologisch Model\nGegenereerd: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\nRibasim versie: {ribasim.__version__}\nGetest (u kunt simuleren): Nee\n\n** Samengevoegde modellen (beheerder: modelnaam (versie)**\n"
    lhm_model, readme = process_model_spec(1, hws_spec, lhm_model, readme)
    for idx, model_spec in enumerate(model_specs):
        write_toml = cloud.joinpath(f"Rijkswaterstaat/modellen/lhm-scaling/lhm-{idx + 2:02}/lhm-{idx + 2:02}.toml")
        lhm_model, readme = process_model_spec(idx + 2, model_spec, lhm_model, readme, write_toml=write_toml)
    # Write lhm model only if it exists
    print("write lhm model")
    ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "lhm.toml")
    if lhm_model is not None:
        lhm_model.write(ribasim_toml)
    cloud.joinpath("Rijkswaterstaat", "modellen", "lhm", "readme.md").write_text(readme)
    if upload_model:
        cloud.upload_model("Rijkswaterstaat", model="lhm")
