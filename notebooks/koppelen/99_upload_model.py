# %%
from ribasim_nl import CloudStorage

cloud = CloudStorage()

cloud.upload_model("Rijkswaterstaat", "lhm", include_results=True, include_plots=False)

# %%
