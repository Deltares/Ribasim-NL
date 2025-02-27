# %%
from ribasim_nl import CloudStorage

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

missing_models = []

for authority in authorities:
    ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
    if not ribasim_dir.exists():
        missing_models += [authority]

assert len(missing_models) == 0
