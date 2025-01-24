# %%
import time

from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "Noorderzijlvest"
short_name = "nzv"


static_data_xlsx = cloud.joinpath(
    "Noorderzijlvest",
    "verwerkt",
    "parameters",
    "static_data.xlsx",
)


# %%

# read
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_fix_model", f"{short_name}.toml")
model = Model.read(ribasim_toml)

start_time = time.time()
# %%
# parameterize
model.parameterize(static_data_xlsx=static_data_xlsx, precipitation_mm_per_day=10)
print("Elapsed Time:", time.time() - start_time, "seconds")

# %%

# Write model
ribasim_toml = cloud.joinpath(authority, "modellen", f"{authority}_parameterized_model", f"{short_name}.toml")
model.write(ribasim_toml)

# %%

# test-run
assert model.run() == 0
