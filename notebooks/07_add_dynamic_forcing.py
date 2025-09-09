# %%
from datetime import datetime

from ribasim_nl import CloudStorage, Model, SetDynamicForcing
from ribasim_nl.analyse_results import CompareOutputMeasurements
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


FIND_POST_FIXES = ["bergend_model"]
SELECTION: list[str] = [
    # "AaenMaas",
    # "BrabantseDelta",
    # "DeDommel",
    # "DrentsOverijsselseDelta",
    # "HunzeenAas",
    # "Limburg",
    # "Noorderzijlvest",
    # "RijnenIJssel",
    # "StichtseRijnlanden",
    # "ValleienVeluwe",
    # "Vechtstromen",
]
REBUILD = False
COMPARE_MEASEUREMENTS = True


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


# specify koppeltabel and meas_folder
if COMPARE_MEASEUREMENTS:
    loc_koppeltabel = cloud.joinpath(
        "Landelijk", "resultaatvergelijking", "koppeltabel", "Transformed_koppeltabel_test_met_suggestie.xlsx"
    )
    meas_folder = cloud.joinpath("Landelijk", "resultaatvergelijking", "meetreeksen")
    cloud.synchronize([loc_koppeltabel, meas_folder])


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

            # add categorie to basin / state
            series = model.basin.node.df.loc[model.basin.state.df["node_id"].to_numpy()]["meta_categorie"]
            assert series.notna().all()
            model.basin.state.df["meta_categorie"] = series.to_numpy()

            # add forcing
            add_forcing(model, cloud, starttime, endtime)

            # run model
            model.write(dst_toml_file)
            model.run()
            # update state so we start smooth/empty
            model.update_state()

        else:
            # update state so we have a reasonable start
            model = Model.read(dst_toml_file)

        if COMPARE_MEASEUREMENTS:
            model.update_state()
            model.write(dst_toml_file)
            model.run()

            model_folder = model.filepath.parent
            compare = CompareOutputMeasurements(
                loc_koppeltabel=loc_koppeltabel,
                meas_folder=meas_folder,
                model_folder=model_folder,
                apply_for_water_authority=authority,
            )
