# %%
from ribasim_nl import CloudStorage

cloud = CloudStorage()

aaenmaas_url = cloud.joinurl("AaenMaas", "modellen", "AaenMaas_2024_6_3")

# %%
cloud.download_content(aaenmaas_url)
