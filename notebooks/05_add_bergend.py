# %%
from ribasim_nl import CloudStorage, Model
from ribasim_nl.berging import VdGaastBerging

cloud = CloudStorage()

FIND_POST_FIXES = ["full_control_model"]
SELECTION: list[str] = ["StichtseRijnlanden"]
INCLUDE_RESULTS = False
REBUILD = True


def get_model_dir(authority, post_fix):
    return cloud.joinpath(authority, "modellen", f"{authority}_{post_fix}")


if len(SELECTION) == 0:
    authorities = cloud.water_authorities
else:
    authorities = SELECTION
# %%
link_data = []
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
        dst_model_dir = model_dir.with_name(f"{authority}_bergend_model")
        dst_toml_file = dst_model_dir / toml_file.name

        if (not dst_toml_file.exists()) or REBUILD:
            # add berging
            add_berging = VdGaastBerging(model=model, cloud=cloud, use_add_api=False)
            add_berging.add()

            # run model
            model.write(dst_toml_file)
            result = model.run()
            assert result.exit_code == 0
