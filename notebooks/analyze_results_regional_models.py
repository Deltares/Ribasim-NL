# %%

from ribasim_nl.analyse_results import CompareOutputMeasurements

from ribasim_nl import CloudStorage

# %%
cloud = CloudStorage()
water_authorities = [
    # "AaenMaas",
    # "BrabantseDelta",
    # "DeDommel",
    # "DrentsOverijsselseDelta",
    # "HunzeenAas",
    "Limburg",
    # "Noorderzijlvest",
    # "RijnenIJssel",
    # "StichtseRijnlanden",
    # "ValleienVeluwe",
    # "Vechtstromen",
]

# specify koppeltabel and meas_folder

# OUD
# loc_koppeltabel = cloud.joinpath(
#     "Basisgegevens/resultaatvergelijking/koppeltabel/Transformed_koppeltabel_20260202.xlsx"
# )

# NEW
base_koppeltabel = cloud.joinpath("Basisgegevens/resultaatvergelijking/koppeltabel_2026")
loc_koppeltabel = cloud.joinpath(
    base_koppeltabel, "Transformed_koppeltabel_versie_Limburg_2026_4_0_Feedback_Verwerkt_HydroLogic.xlsx"
)
loc_specifieke_bewerking = cloud.joinpath(base_koppeltabel, "Specifiek_bewerking_versieLimburg_2026_4_0.xlsx")

# meas_folder = cloud.joinpath("Basisgegevens/resultaatvergelijking/meetreeksen")

meas_folder = cloud.joinpath("Basisgegevens/resultaatvergelijking/meetreeksen_2026")

cloud.synchronize([loc_koppeltabel, meas_folder, loc_specifieke_bewerking])

for water_authority in water_authorities:
    print(water_authority)
    # get latest coupled LHM model
    # model_folder = cloud.joinpath(water_authority, "modellen", f"{water_authority}_dynamic_model")

    # CompareOutputMeasurements(
    #     loc_koppeltabel=loc_koppeltabel,
    #     loc_specifics=loc_specifieke_bewerking,
    #     meas_folder=meas_folder,
    #     model_folder=model_folder,
    #     apply_for_water_authority=water_authority,
    #     save_results_combined=True,
    # )

    cloud = CloudStorage()
    base_koppeltabel = cloud.joinpath("Basisgegevens/resultaatvergelijking/koppeltabel_2026")

    loc_koppeltabel = cloud.joinpath(
        base_koppeltabel, "Transformed_koppeltabel_versie_Limburg_2026_4_0_Feedback_Verwerkt_HydroLogic.xlsx"
    )
    loc_specifieke_bewerking = cloud.joinpath(base_koppeltabel, "Specifiek_bewerking_versieLimburg_2026_4_0.xlsx")
    waterboard = "Limburg"
    waterboard_model_versions = cloud.uploaded_models(authority=waterboard)

    latest_model_version = sorted(
        [i for i in waterboard_model_versions if i.model == waterboard], key=lambda x: getattr(x, "sorter", "")
    )[-1]

    model_folder = cloud.joinpath(f"{waterboard}/modellen", latest_model_version.path_string)

    CompareOutputMeasurements(
        loc_koppeltabel=loc_koppeltabel,
        loc_specifics=loc_specifieke_bewerking,
        meas_folder=meas_folder,
        model_folder=model_folder,
        apply_for_water_authority=water_authority,
        filetype="flow",
        save_results_combined=True,
        output_is_feather=False,
        output_is_nc=True,
        resample_to_daily=True,
    )

# %%
