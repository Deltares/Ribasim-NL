# %%
import sys
from datetime import datetime

from ribasim_nl.parametrization.basin_tables import add_basin_time_synthetic

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
starttime = datetime(2017, 1, 1)
endtime = datetime(2018, 1, 1)

FIND_POST_FIXES = ["full_control_model"]
# pass authorities as arguments, or edit list here
SELECTION: set = {"AaenMaas"}
REBUILD = True


def get_model_dir(authority, post_fix):
    return cloud.joinpath(authority, "modellen", f"{authority}_{post_fix}")


valid_authorities = set(cloud.water_authorities)

# We make a list of authorities:
# 1. provided as arguments
authorities = set(sys.argv[1:]) & valid_authorities
# 2. provided in global SELECTION
if len(authorities) == 0:
    authorities = SELECTION & valid_authorities
# 3. all authorities
if len(authorities) == 0:
    authorities = valid_authorities
# %%
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
        toml_file = next(model_dir.glob("*.toml"))
        model = Model.read(toml_file)

        # write dynamic model
        dst_model_dir = model_dir.with_name(f"{authority}_synthetic_forcing_model")
        dst_toml_file = dst_model_dir / toml_file.name

        if (not dst_toml_file.exists()) or REBUILD:
            # update state so we start smooth/empty
            if not model.results_path.exists():
                model.run()
            model.update_state()

            # add forcing
            add_basin_time_synthetic(
                model, precipitation_mm_per_day=2, evaporation_mm_per_day=1, start_time=starttime, end_time=endtime
            )

            # run model
            model.write(dst_toml_file)
            model.run()
