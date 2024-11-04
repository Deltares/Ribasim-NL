# %%
from ribasim_nl import CloudStorage

cloud = CloudStorage()

authority = "Noorderzijlvest"
short_name = "nzv"

cloud = CloudStorage()

model_url = cloud.joinurl(authority, "modellen", f"{authority}_boezemmodel_2024_10_3")
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_boezemmodel_2024_10_3", "ribasim.toml")
if not ribasim_toml.exists():
    cloud.download_content(model_url)

if ribasim_toml.exists():  # get a short_name version to differentiate QGIS layergroup
    ribasim_toml.with_name(f"{short_name}.toml").write_text(ribasim_toml.read_text())
