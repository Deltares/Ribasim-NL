# %%
from ribasim_nl import CloudStorage

cloud = CloudStorage()

SELECTION = []


def get_model_dir(authority, post_fix):
    return cloud.joinpath(authority, "modellen", f"{authority}_{post_fix}")


if len(SELECTION) == 0:
    authorities = cloud.water_authorities
else:
    authorities = SELECTION

for authority in authorities:
    static_data_xlsx = cloud.joinpath(f"{authority}/verwerkt/parameters/static_data.xlsx")
    if static_data_xlsx.exists():
        print(f"uploading {static_data_xlsx}")
        cloud.create_dir(authority, "verwerkt", "parameters")  # make sure remote url is created
        cloud.upload_file(static_data_xlsx)
