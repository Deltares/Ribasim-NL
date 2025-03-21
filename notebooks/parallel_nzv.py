# %%
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Noorderzijlvest", "modellen", "Noorderzijlvest_2025_3_1", "nzv.toml")
model = Model.read(ribasim_toml)

# model = prefix_index(model=model, prefix_id=waterbeheercode[model_spec["authority"]])


# lhm_model = concat([lhm_model, model], keep_original_index=True)
