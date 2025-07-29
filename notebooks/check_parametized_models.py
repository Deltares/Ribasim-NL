# %%
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

missing_models = []
missing_runs = []
missing_buitenlandse_aanvoer = []

for authority in authorities:
    ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model")
    output_controle_gpkg = ribasim_dir.joinpath("results", "output_controle.gpkg")
    if not ribasim_dir.exists():
        missing_models += [authority]
    if not output_controle_gpkg.exists():
        missing_runs += [authority]
    ribasim_toml = next(ribasim_dir.glob("*.toml"))
    model = Model.read(ribasim_toml)
    if not (model.flow_boundary.node.df["meta_categorie"] == "buitenlandse aanvoer").all():
        missing_buitenlandse_aanvoer = [authority]

assert len(missing_buitenlandse_aanvoer) == 0
assert len(missing_models) == 0
assert len(missing_runs) == 0
