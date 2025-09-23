# %%
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_transient", "hws.toml")
model = Model.read(ribasim_toml)
model.write(cloud.joinpath("Rijkswaterstaat", "modellen", "hws", "hws.toml"))
model.run()

# %%
cloud.upload_model("Rijkswaterstaat", "hws", include_results=True, include_plots=True)
# %%
