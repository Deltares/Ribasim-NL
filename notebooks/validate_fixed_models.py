# %% Validate link source destination in fixed models

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

authorities = [
    "Noorderzijlvest",
    "HunzeenAas",
    "DrentsOverijsselseDelta",
    "AaenMaas",
    "BrabantseDelta",
    "StichtseRijnlanden",
    "ValleienVeluwe",
    "Vechtstromen",
    "RijnenIJssel",
    "DeDommel",
    "Limburg",
]

for authority in authorities:
    print(authority)
    ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model")
    tomls = list(ribasim_dir.glob("*.toml"))
    assert len(tomls) == 1
    ribasim_toml = tomls[0]
    model = Model.read(ribasim_toml)
    model.validate_link_source_destination()
