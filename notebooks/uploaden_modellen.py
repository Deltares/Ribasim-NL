# %%
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

FIND_POST_FIXES = ["bergend", "fix_model_area", "fix_model_network"]
SELECTION = []


def get_model_dir(authority, post_fix):
    return cloud.joinpath(authority, "modellen", f"{authority}_{post_fix}")


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
        print(f"uploading copy for {authority}")
        # read model
        toml_file = next(model_dir.glob("*.toml"))
        model = Model.read(toml_file)

        # write a local copy in root
        toml_file = toml_file.parents[1].joinpath(authority, toml_file.name)
        model.write(toml_file)

        # create version and upload
        cloud.upload_model(authority=authority, model=authority)
