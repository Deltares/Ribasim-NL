# %%
from ribasim_nl import CloudStorage

cloud = CloudStorage()

cloud.upload_model("Rijkswaterstaat", "hws", include_results=True, include_plots=True)
# %%
