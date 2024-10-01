# %%
from ribasim_nl import CloudStorage

cloud = CloudStorage()

model_url = cloud.joinurl("DrentsOverijsselseDelta", "modellen", "DrentsOverijsselseDelta_2024_6_3")

# %%
cloud.download_content(model_url)
