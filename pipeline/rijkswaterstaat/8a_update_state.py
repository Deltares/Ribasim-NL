# %%
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Rijkswaterstaat/modellen/hws_demand/hws.toml")
model = Model.read(ribasim_toml)
result = model.run()
assert result.exit_code == 0
# The levels at the last saved time, rather than the final state in basin_state.nc: the Ribasim core of
# `install_ribasim_core.py` can write a wrong final level for a Basin fed by a FlowBoundary, like -115 m below Lobith
model.update_state(time_stamp=model.basin_results.df.index.max())

model.write(ribasim_toml)

# %%
