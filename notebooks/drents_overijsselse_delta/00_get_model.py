# %%
from ribasim_nl import CloudStorage

authority = "DrentsOverijsselseDelta"
short_name = "dod"

cloud = CloudStorage()

model_url = cloud.joinurl(authority, "modellen", f"{authority}_2024_6_3")

cloud.download_content(model_url)

ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_2024_6_3", "model.toml")
if ribasim_toml.exists():
    ribasim_toml.replace(ribasim_toml.with_name(f"{short_name}.toml"))
