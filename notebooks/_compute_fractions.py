# %%
import pandas as pd

from ribasim_nl import Model, settings

AUTHORITY = "AaenMaas"

model_dir = settings.ribasim_nl_data_dir.joinpath(rf"{AUTHORITY}\modellen\AaenMaas_dynamic_model")
model = Model.read(model_dir.joinpath("aam.toml"))

budgets_df = pd.read_feather(model_dir.joinpath("budgets.arrow"))
