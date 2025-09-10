# %%
from ribasim_nl import CloudStorage
from ribasim_nl.analyse_results import CompareOutputMeasurements

cloud = CloudStorage()
water_authorities = [
    # "AaenMaas",
    # "BrabantseDelta",
    # "DeDommel",
    # "DrentsOverijsselseDelta",
    # "HunzeenAas",
    # "Limburg",
    # "Noorderzijlvest",
    # "RijnenIJssel",
    "StichtseRijnlanden",
    # "ValleienVeluwe",
    # "Vechtstromen",
]

# specify koppeltabel and meas_folder

loc_koppeltabel = cloud.joinpath(
    "Landelijk", "resultaatvergelijking", "koppeltabel", "Transformed_koppeltabel_test_met_suggestie.xlsx"
)
meas_folder = cloud.joinpath("Landelijk", "resultaatvergelijking", "meetreeksen")
cloud.synchronize([loc_koppeltabel, meas_folder])

for water_authority in water_authorities:
    print(water_authority)
    # get latest coupled LHM model
    model_folder = cloud.joinpath(water_authority, "modellen", f"{water_authority}_dynamic_model")

    # synchronize paths

    compare = CompareOutputMeasurements(
        loc_koppeltabel=loc_koppeltabel,
        meas_folder=meas_folder,
        model_folder=model_folder,
        apply_for_water_authority=water_authority,
    )
