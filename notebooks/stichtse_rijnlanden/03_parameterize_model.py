# %%
import time

from peilbeheerst_model.controle_output import Control
from ribasim_nl import CloudStorage, Model

cloud = CloudStorage()
authority = "StichtseRijnlanden"
short_name = "hdsr"

static_data_xlsx = cloud.joinpath(
    authority,
    "verwerkt",
    "parameters",
    "static_data.xlsx",
)
ribasim_dir = cloud.joinpath(authority, "modellen", f"{authority}_prepare_model")
ribasim_toml = ribasim_dir / f"{short_name}.toml"

# # you need the excel, but the model should be local-only by running 01_fix_model.py
# cloud.synchronize(filepaths=[static_data_xlsx])
# cloud.synchronize(filepaths=[ribasim_dir], check_on_remote=False)

# %%

# read
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

# run model
exit_code = model.run()
assert exit_code == 0

# %%
controle_output = Control(ribasim_toml=ribasim_toml)
indicators = controle_output.run_all()
# %%
