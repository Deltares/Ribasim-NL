# %%

from ribasim_nl.analyse_results import CompareOutputMeasurements

from ribasim_nl import CloudStorage

cloud = CloudStorage()
water_authorities = [
    # "AaenMaas",
    # "BrabantseDelta",
    # "DeDommel",
    # "DrentsOverijsselseDelta",
    # "HunzeenAas",
    # "Limburg",
    "Noorderzijlvest",
    # "RijnenIJssel",
    "StichtseRijnlanden",
    # "ValleienVeluwe",
    # "Vechtstromen",
]

# specify koppeltabel and meas_folder

loc_koppeltabel = cloud.joinpath(
    "Basisgegevens/resultaatvergelijking/koppeltabel/Transformed_koppeltabel_20260202.xlsx"
)

loc_specifieke_bewerking = cloud.joinpath(
    "Basisgegevens/resultaatvergelijking/koppeltabel/Specifiek_bewerking_versielhm_coupled_2025_9_0.xlsx"
)

meas_folder = cloud.joinpath("Basisgegevens/resultaatvergelijking/meetreeksen")
cloud.synchronize([loc_koppeltabel, meas_folder, loc_specifieke_bewerking])

for water_authority in water_authorities:
    print(water_authority)
    # get latest coupled LHM model
    model_folder = cloud.joinpath(water_authority, "modellen", f"{water_authority}_dynamic_model")

    compare = CompareOutputMeasurements(
        loc_koppeltabel=loc_koppeltabel,
        loc_specifics=loc_specifieke_bewerking,
        meas_folder=meas_folder,
        model_folder=model_folder,
        apply_for_water_authority=water_authority,
        save_results_combined=True,
    )

# %%
