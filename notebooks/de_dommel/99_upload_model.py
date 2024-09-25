# %%
from ribasim_nl import CloudStorage

cloud = CloudStorage()

cloud.upload_model("DeDommel", "DeDommel", include_results=True, include_plots=False)
