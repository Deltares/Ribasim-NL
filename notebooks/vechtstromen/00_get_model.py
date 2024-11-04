# %%
from ribasim_nl import CloudStorage

cloud = CloudStorage()

model_url = cloud.joinurl("Vechtstromen", "modellen", "Vechtstromen_2024_6_3")

# %%
cloud.download_content(model_url)


# %% rename, so we can seperate in QGIS
ribasim_toml = cloud.joinpath("Vechtstromen", "modellen", "Vechtstromen_2024_6_3", "model.toml")
if ribasim_toml.exists():
    ribasim_toml.rename(ribasim_toml.with_name("vechtstromen.toml"))
