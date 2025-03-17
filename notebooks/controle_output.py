# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model
from ribasim_nl.check_basin_level import add_check_basin_level

cloud = CloudStorage()

qlr_path = cloud.joinpath("Basisgegevens\\QGIS_lyr\\output_controle_vaw_afvoer.qlr")

run_model = True

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
data = []

for authority in authorities:
    print(authority)
    ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model")
    if not ribasim_dir.exists():
        missing_models += [authority]
    else:
        tomls = list(ribasim_dir.glob("*.toml"))
        assert len(tomls) == 1
        ribasim_toml = tomls[0]
        model = Model.read(ribasim_toml)
        if "meta_check_basin_level" not in model.basin.node.df.columns:
            add_check_basin_level(model=model)
        model.write(ribasim_toml)
        model.validate_link_source_destination()
        if run_model:
            t0 = time.time()
            exit_code = model.run()
            assert exit_code == 0
            simulation_time = time.time() - t0
        controle_output = Control(ribasim_toml=ribasim_toml, qlr_path=qlr_path)
        indicators = controle_output.run_afvoer()

    # compute statistics
    data += [
        {
            "waterschap": authority,
            "simulatietijd": simulation_time,
            "kleine_basins": len(model.basin.area.df[model.basin.area.df.area < 10000]),
        }
    ]

assert len(missing_models) == 0
assert len(missing_runs) == 0
