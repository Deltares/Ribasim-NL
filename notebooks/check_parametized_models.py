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
missing_runs = []

for authority in authorities:
    ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model")
    output_controle_gpkg = ribasim_dir.joinpath("results", "output_controle.gpkg")
    if not ribasim_dir.exists():
        missing_models += [authority]
    if not output_controle_gpkg.exists():
        missing_runs += [authority]


assert len(missing_models) == 0
assert len(missing_runs) == 0
