# %%
import sys
from datetime import datetime

from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets

from ribasim_nl import CloudStorage, Model, SetDynamicForcing

cloud = CloudStorage()
starttime = datetime(2017, 1, 1)
endtime = datetime(2018, 1, 1)
write_budgets: bool = False


def add_forcing(model, cloud, starttime, endtime):

    lhm_budget_path = cloud.joinpath("Basisgegevens/LHM/4.3/results/LHM_433_budget.zip")
    precipitation_path = cloud.joinpath("Basisgegevens/WIWB/Meteobase.Precipitation.nc")
    evaporation_path = cloud.joinpath("Basisgegevens/WIWB/Meteobase.Evaporation.Makkink.nc")
    cloud.synchronize(filepaths=[lhm_budget_path, precipitation_path, evaporation_path], overwrite=False)

    # compute forcing
    forcing = SetDynamicForcing(
        model=model,
        cloud=cloud,
        startdate=starttime,
        enddate=endtime,
    )

    model = forcing.add()

    # Add dynamic groundwater
    offline_budgets = AssignOfflineBudgets(lhm_budget_path)
    _, budgets_df = offline_budgets.compute_budgets(model)
    return budgets_df


FIND_POST_FIXES = ["bergend_model"]
# pass authorities as arguments, or edit list here
SELECTION: set = {"AaenMaas"}
INCLUDE_RESULTS = False
REBUILD = True


def get_model_dir(authority, post_fix):
    return cloud.joinpath(authority, "modellen", f"{authority}_{post_fix}")


def check_build(toml_file):
    # check if we want to rebuild any way
    build = REBUILD

    # we build if model does not exist
    if not build:
        build = not toml_file.exists()

    # we build if we don't have tabulated_rating_curves
    if not build:
        model = Model.read(toml_file)
        # pyrefly: ignore[missing-attribute]
        build = model.tabulated_rating_curve.node.df is None

    # we build if tabulated rating curves don't have a meta_cateogrie colummn
    if not build:
        # pyrefly: ignore[unbound-name, missing-attribute]
        build = "meta_categorie" not in model.tabulated_rating_curve.node.df.columns

    # we build if we don't have any bergend in meta_categorie
    if not build:
        # pyrefly: ignore[missing-attribute]
        build = not (model.tabulated_rating_curve.node.df["meta_categorie"] == "bergend").any()

    return build


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

        # check if we need to (re)do the model

        # write dynamic model
        dst_model_dir = model_dir.with_name(f"{authority}_dynamic_model")
        dst_toml_file = dst_model_dir / toml_file.name

        if check_build(dst_toml_file):
            model = Model.read(toml_file)
            # update state so we start smooth/empty
            model.update_state()

            # add categorie to basin / state
            # pyrefly: ignore[missing-attribute]
            series = model.basin.node.df["meta_categorie"]
            uncategorized_basins = series[series.isna()].index.values
            if len(uncategorized_basins) > 0:
                print(f"uncategorized basins: {uncategorized_basins}, will be set to doorgaand")
                # pyrefly: ignore[missing-attribute]
                model.node.df.loc[uncategorized_basins, "meta_categorie"] = "doorgaand"

            # add forcing
            budgets_df = add_forcing(model, cloud, starttime, endtime)

            # run model
            model.write(dst_toml_file)
            if write_budgets:
                budgets_df.to_feather(dst_toml_file.with_name("budgets.arrow"))
                budgets_df.to_csv(dst_toml_file.with_name("budgets.csv.zip"), compression="zip")
            model.run()
