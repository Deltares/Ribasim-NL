"""Build and write the Ribasim model with prepared DELWAQ inputs."""

# %%
from pathlib import Path

import pandas as pd
from ribasim import Model

# %%
ROOT = Path(__file__).resolve().parents[2]
MODEL_NAME = "lhm_coupled_full"
TOML_NAME = "lhm_coupled.toml"
model_path = ROOT / "data" / "Rijkswaterstaat" / "modellen" / MODEL_NAME
toml_path = model_path / TOML_NAME
prepared_data_path = model_path / "delwaq_input"

model = Model.read(toml_path)
ANIMO_and_ER_loads_df = pd.read_parquet(prepared_data_path / "ANIMO_and_ER_loads.parquet")
IM_and_Zinfo_df = pd.read_parquet(prepared_data_path / "IM_and_Zinfo_concentrations.parquet")

# %%
model.basin.mass_load = ANIMO_and_ER_loads_df
model.flow_boundary.concentration = pd.concat(
    [model.flow_boundary.concentration.df, IM_and_Zinfo_df],
    ignore_index=True,
)
model.write(toml_path)
