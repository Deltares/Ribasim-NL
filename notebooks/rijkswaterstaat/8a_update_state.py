# %%
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_demand", "hws.toml")
model = Model.read(ribasim_toml)
exit_code = model.run()
assert exit_code == 0
model.update_state()

model.write(ribasim_toml)

# %%
