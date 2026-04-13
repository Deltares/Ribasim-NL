# %%
import sys
from datetime import datetime

from ribasim.delwaq import generate, parse, run_delwaq
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets

from ribasim_nl import CloudStorage, Model, SetDynamicForcing, settings

cloud = CloudStorage()
starttime = datetime(2017, 1, 1)
endtime = datetime(2018, 1, 1)
write_budgets: bool = True  # write mfms_budgets.arrow for later verification
assing_fractions: bool = True  # compute (sub-) fractions from budgets-table
compute_fractions: bool = True


# LHM4.3 mfma budgets to be assign to primary/secondary drainage/surface_runoff columns
primary_budgets: set[str] = {"bdgriv_sys1", "bdgriv_sys4", "bdgriv_sys5"}
secondary_budgets: set[str] = {
    "bdgriv_sys2",
    "bdgriv_sys3",
    "bdgriv_sys6",
    "bdgdrn_sys1",
    "bdgdrn_sys2",
    "bdgdrn_sys3",
    "bdgpssw",
}
surface_runoff_budgets: set[str] = {"bdgqrun"}


def add_forcing(model, cloud, starttime, endtime, assign_fractions, fraction_prefix):

    # sync files so we're good to go!
    lhm_budget_path = cloud.joinpath("Basisgegevens/LHM/4.3/results/LHM_433_budget.zip")
    _precipitation = cloud.joinpath("Basisgegevens/WIWB/Meteobase.Precipitation.nc")
    _evaporation = cloud.joinpath("Basisgegevens/WIWB/Meteobase.Evaporation.Makkink.nc")
    cloud.synchronize(filepaths=[lhm_budget_path, _precipitation, _evaporation], overwrite=False)

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
    _, budgets_df = offline_budgets.compute_budgets(
        model,
        primary_budgets=primary_budgets,
        secondary_budgets=secondary_budgets,
        surface_runoff_budgets=surface_runoff_budgets,
        assign_fractions=assign_fractions,
        fraction_prefix=fraction_prefix,
    )  # budgets_df is used to compute basin_fractions
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
        build = model.tabulated_rating_curve.node.df is None

    # we build if tabulated rating curves don't have a meta_cateogrie colummn
    if not build:
        build = "meta_categorie" not in model.tabulated_rating_curve.node.df.columns

    # we build if we don't have any bergend in meta_categorie

    if not build:
        build = not (model.tabulated_rating_curve.node.df["meta_categorie"] == "bergend").any()

    return build


# We make a list of authorities:
# 1. provided as arguments
authorities = set(sys.argv[1:]) & set(cloud.water_authorities)
# 2. provided in global SELECTION
if len(authorities) == 0:
    authorities = set(SELECTION) & set(cloud.water_authorities)
# 3. all authorities
if len(authorities) == 0:
    authorities = set(cloud.water_authorities)

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
            series = model.basin.node.df.loc[model.basin.state.df["node_id"].to_numpy()]["meta_categorie"]
            uncategorized_basins = series[series.isna()].index.values
            if len(uncategorized_basins) > 0:
                print(f"uncategorized basins: {uncategorized_basins}, will be set to doorgaand")
                model.node.df.loc[uncategorized_basins, "meta_categorie"] = "doorgaand"

            # add forcing
            budgets_df = add_forcing(
                model, cloud, starttime, endtime, assing_fractions, fraction_prefix=waterbeheercode[authority]
            )

            # run model
            model.write(dst_toml_file)
            if write_budgets:
                budgets_df.to_feather(dst_toml_file.with_name("mfms_budgets.arrow"))  # for later reference
            model.run()

            # DELWAQ(!)
            if compute_fractions:
                # generate DELWAQ model
                delwaq_dir = model.toml_path.with_name("delwaq")
                print(f"🗀 generate DELWAQ model in {delwaq_dir}")
                graph, substances = generate(model, output_path=delwaq_dir)

                # run DELWAQ model
                print("⚙️ run DELWAQ")
                run_delwaq(
                    model_dir=delwaq_dir,
                    d3d_home=settings.d3d_home,
                )

                # parse DELWAQ results in model
                print("📖 parse DELWAQ results in Ribasim-model")
                model = parse(model, graph, substances, output_folder=delwaq_dir, to_input=True)
                model.write(dst_toml_file)
