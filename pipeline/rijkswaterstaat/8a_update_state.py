# %%
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_demand/hws.toml")
model = Model.read(ribasim_toml)
result = model.run()
assert result.exit_code == 0
model.update_state()

model.write(ribasim_toml)

# %%
