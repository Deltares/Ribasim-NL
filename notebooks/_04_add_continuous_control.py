# %%
from peilbeheerst_model import ribasim_parametrization
from ribasim_nl import CloudStorage, Model

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

        # dst toml-file
        dst_model_dir = model_dir.with_name(f"{authority}_continuous_control_model")
        dst_toml_file = dst_model_dir / toml_file.name

        model = Model.read(toml_file)

        # add columns we need to run add_continouws_control
        model.basin.state.df["meta_categorie"] = model.basin.state.df["node_id"].apply(
            lambda x: model.basin.node.df.at[x, "meta_categorie"]
        )  # parameterization-script wants meta_categorie in basin.state
        model.basin.area.df["meta_node_id"] = model.basin.area.df[
            "node_id"
        ]  # parameterization-script wants meta_categorie in basin.area.df
        model.outlet.static.df.loc[model.outlet.static.df["node_id"] == 887, "meta_aanvoer"] = 1
        ribasim_parametrization.add_continuous_control(model, dy=-200, numerical_tolerance=0.05)

        # drop the columns we don't want to keep
        model.basin.area.df.drop(columns="meta_node_id", inplace=True)
        model.basin.state.df.drop(columns="meta_categorie", inplace=True)

        model.write(dst_toml_file)
        model.run()
