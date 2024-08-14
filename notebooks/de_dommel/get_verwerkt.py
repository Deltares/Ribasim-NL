# %%
from ribasim_nl import CloudStorage

cloud = CloudStorage()

dommel_url = cloud.joinurl("DeDommel", "verwerkt")

# %%
cloud.download_content(dommel_url)
