# %%
import pandas as pd
from ribasim import Model

from ribasim_nl import CloudStorage

cloud = CloudStorage()


ribasim_toml = cloud.joinpath("Rijkswaterstaat", "modellen", "hws_sturing", "hws.toml")
model = Model.read(ribasim_toml)

basin_df = pd.read_feather(model.filepath.parent.joinpath(model.results_dir, "basin.arrow"))

model.basin.state.df.loc[:, ["level"]] = basin_df[basin_df.time == basin_df.time.max()]["level"].to_numpy()

model.write(ribasim_toml)

# %%
