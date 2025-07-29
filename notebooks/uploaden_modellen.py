# %%
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

FIND_POST_FIXES = ["dynamic_model"]
SELECTION = []
INCLUDE_RESULTS = False


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
        print(authority)
        # read model
        toml_file = next(model_dir.glob("*.toml"))
        model = Model.read(toml_file)

        # write a local copy in root
        print(f"copy {toml_file}")
        toml_file = toml_file.parents[1].joinpath(authority, toml_file.name)
        model.write(toml_file)

        # run model
        print("simulating...")
        result = model.run()
        assert result.exit_code == 0

        # create version and upload
        print("uploading...")
        version = cloud.upload_model(authority=authority, model=authority, include_results=INCLUDE_RESULTS)
        print(f"uploaded version {version.version}")
