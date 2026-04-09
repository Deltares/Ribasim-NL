# %%
from datetime import datetime
from pathlib import Path

from ribasim.delwaq import generate, parse

# from ribasim.delwaq import run_delwaq
from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets
from ribasim_nl.run_delwaq import run_delwaq

from ribasim_nl import CloudStorage, Model, SetDynamicForcing

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
    # compute forcing
    forcing = SetDynamicForcing(
        model=model,
        cloud=cloud,
        startdate=starttime,
        enddate=endtime,
    )

    model = forcing.add()

    # Add dynamic groundwater
    lhm_budget_path = cloud.joinpath("Basisgegevens/LHM/4.3/results/LHM_433_budget.zip")
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
SELECTION: list[str] = ["AaenMaas"]
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
            assert series.notna().all()
            model.basin.state.df["meta_categorie"] = series.to_numpy()

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
                graph, substances = generate(model, output_path=delwaq_dir)

                # run DELWAQ model
                # run_delwaq(
                #     model_dir=delwaq_dir,
                #     d3d_home=Path(
                #         "c:/Program Files/Deltares/D-HYDRO Suite 2025.02 1D2D/plugins/DeltaShell.Dimr/kernels/x64"
                #     ),
                # )

                run_delwaq(
                    dimr_config=delwaq_dir.joinpath("dimr_config.xml"),
                    run_dimr_bat=Path(
                        "c:/Program Files/Deltares/D-HYDRO Suite 2025.02 1D2D/plugins/DeltaShell.Dimr/kernels/x64/bin/run_dimr.bat"
                    ),
                )

                # parse DELWAQ results in model
                model = parse(model, graph, substances, output_folder=delwaq_dir, to_input=True)
                model.write(dst_toml_file)
