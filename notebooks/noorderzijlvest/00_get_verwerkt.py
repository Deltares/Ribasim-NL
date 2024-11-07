# %%
from ribasim_nl import CloudStorage

cloud = CloudStorage()

data_url = cloud.joinurl("Noorderzijlvest", "verwerkt")

# %%
cloud.download_content(data_url)

# %%
