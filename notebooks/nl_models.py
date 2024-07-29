# %%
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
waterbeheerder = "Rijkswaterstaat"

ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_2024_4_4", "hws.toml")
model = Model.read(ribasim_toml)

df = model.basin_results.df

# %% cloud later be later model.basin_results.get_series(node_id=63)
df[df["node_id"] == 63].plot()
