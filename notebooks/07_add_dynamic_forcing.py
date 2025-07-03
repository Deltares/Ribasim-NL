# %%
from datetime import datetime

from ribasim_nl import CloudStorage, Model, SetDynamicForcing
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets

cloud = CloudStorage()
starttime = datetime(2017, 1, 1)
endtime = datetime(2018, 1, 1)


def add_forcing(model, cloud, starttime, endtime):
    forcing = SetDynamicForcing(
        model=model,
        cloud=cloud,
        startdate=starttime,
        enddate=endtime,
    )

    model = forcing.add()

    # Add dynamic groundwater
    offline_budgets = AssignOfflineBudgets()
    offline_budgets.compute_budgets(model)


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
        model = Model.read(toml_file)

        # write dynamic model
        dst_model_dir = model_dir.with_name(f"{authority}_dynamic_model")
        dst_toml_file = dst_model_dir / toml_file.name

        if (not dst_toml_file.exists()) or REBUILD:
            # update state so we start smooth/empty
            model.update_state()

            # add categorie to basin / state
            series = model.basin.node.df.loc[model.basin.state.df["node_id"].to_numpy()]["meta_categorie"]
            assert series.notna().all()
            model.basin.state.df["meta_categorie"] = series.to_numpy()

            # add forcing
            add_forcing(model, cloud, starttime, endtime)

            # run model
            model.write(dst_toml_file)
            model.run()
