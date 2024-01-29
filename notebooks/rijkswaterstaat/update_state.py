# %% init
import ribasim
from ribasim_nl import CloudStorage

cloud = CloudStorage()

ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws", "hws.toml")
model = ribasim.Model.read(ribasim_toml)

# %% # 1m above bottom or 0 NAP
state = model.basin.profile.df.groupby("node_id").min()["level"].reset_index()
state.loc[:, ["level"]] = state["level"].apply(lambda x: max(x + 1, 0))
model.basin.state.df = state
model.write(ribasim_toml)
# %%
