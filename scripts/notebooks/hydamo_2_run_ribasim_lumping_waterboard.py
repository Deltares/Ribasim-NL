import warnings
from pathlib import Path

import pandas as pd
from ribasim_lumping_tools.run_ribasim_lumping_waterboard import run_ribasim_lumping_for_waterboard

warnings.simplefilter("ignore")
pd.options.mode.chained_assignment = None
warnings.simplefilter(action="ignore", category=UserWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)


base_dir = Path("..\\..\\Ribasim modeldata")
dx = 250.0

waterschappen = [
    "HunzeenAas",
    "DrentsOverijsselseDelta",
    "Vechtstromen",
    "RijnenIJssel",
    "ValleienVeluwe",
    "StichtseRijnlanden",
    "BrabantseDelta",
    "DeDommel",
    "AaenMaas",
    "Limburg",
]


for waterschap in waterschappen:
    run_ribasim_lumping_for_waterboard(
        base_dir=base_dir,
        waterschap=waterschap,
        dx=dx,
        buffer_distance=1.0,
        assign_unassigned_areas_to_basins=False if waterschap == "ValleienVeluwe" else True,
        remove_isolated_basins=True,
    )
