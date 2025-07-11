# %%
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

FIND_POST_FIXES = ["full_control_model", "parameterized_model"]
SELECTION = []


def get_model_dir(authority, post_fix):
    return cloud.joinpath(authority, "modellen", f"{authority}_{post_fix}")


if len(SELECTION) == 0:
    authorities = cloud.water_authorities
else:
    authorities = SELECTION


to_couple_boundaries = {}


for authority in authorities:
    model_dir = next(
        (
            get_model_dir(authority, post_fix)
            for post_fix in FIND_POST_FIXES
            if get_model_dir(authority, post_fix).exists()
        ),
        None,
    )
    if model_dir is not None:
        print(model_dir.name)
        toml_file = next(model_dir.glob("*.toml"))
        model = Model.read(toml_file)
        node_ids = model.level_boundary.node.df[
            model.level_boundary.node.df["meta_couple_authority"].isna()
        ].index.to_list()
        to_couple_boundaries[authority] = model.level_boundary.node.df[
            model.level_boundary.node.df["meta_couple_authority"].isna()
        ].index.to_list()
