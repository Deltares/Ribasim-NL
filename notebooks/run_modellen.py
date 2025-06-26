# %%
from datetime import datetime

import pandas as pd

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

FIND_POST_FIXES = ["dynamic_model"]
SELECTION = []
INCLUDE_RESULTS = True


def get_model_dir(authority, post_fix):
    return cloud.joinpath(authority, "modellen", f"{authority}_{post_fix}")


data = {}
if len(SELECTION) == 0:
    authorities = cloud.water_authorities
else:
    authorities = SELECTION

for authority in authorities:
    # find model directory

    model_dir = next(
        (
            get_model_dir(authority, post_fix)
            for post_fix in FIND_POST_FIXES
            if get_model_dir(authority, post_fix).exists()
        ),
        None,
    )
    if model_dir is not None:
        print(authority)
        # read model
        toml_file = next(model_dir.glob("*.toml"))
        model = Model.read(toml_file)

        # time model-run
        run_specs = model.run()
        data[authority] = {
            "basins": len(model.basin.node.df),
            "exit_code": run_specs.exit_code,
            "simulation_time": run_specs.simulation_time,
            "starttime": model.starttime,
            "endtime": model.endtime,
        }


pd.DataFrame.from_dict(data, orient="index").to_excel(
    cloud.joinpath(f"simulation_efficiency_{datetime.today().strftime('%Y%m%d')}.xlsx")
)
