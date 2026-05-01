# %%
from ribasim.delwaq import generate, parse, run_delwaq

from ribasim_nl import CloudStorage, Model, settings

cloud = CloudStorage()

FIND_POST_FIXES = ["dynamic_model"]
SELECTION = ["RijnenIJssel"]
INCLUDE_RESULTS = True
RUN_DELWAQ: bool = False


def get_model_dir(authority, post_fix):
    return cloud.joinpath(authority, "modellen", f"{authority}_{post_fix}")


authorities = cloud.water_authorities if len(SELECTION) == 0 else SELECTION

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

        # run DELWAQ
        if RUN_DELWAQ:
            delwaq_dir = model.toml_path.with_name("delwaq")
            print(f"generate DELWAQ model in {delwaq_dir}")
            graph, substances = generate(model, output_path=delwaq_dir)

            # run DELWAQ model
            print("run DELWAQ")
            run_delwaq(
                model_dir=delwaq_dir,
                d3d_home=settings.d3d_home,
            )

            # parse DELWAQ results in model
            print("parse DELWAQ results in Ribasim-model")
            parse(model, graph, substances, output_folder=delwaq_dir, to_input=True)

        # create version and upload
        print("uploading...")
        version = cloud.upload_model(authority=authority, model=authority, include_results=INCLUDE_RESULTS)
        print(f"uploaded version {version.version}")
