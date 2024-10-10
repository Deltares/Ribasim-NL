# %%
from ribasim_nl import CloudStorage

cloud = CloudStorage()

data_url = cloud.joinurl("DeDommel", "verwerkt")

# %%
cloud.download_content(data_url)
