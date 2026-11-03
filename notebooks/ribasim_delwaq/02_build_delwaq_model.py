"""Build and write the Ribasim model with prepared DELWAQ inputs."""

# %%
from pathlib import Path

import pandas as pd
from config import load_delwaq_config
from ribasim import Model

# %%
ROOT = Path(__file__).resolve().parents[2]
delwaq_config = load_delwaq_config(Path(__file__).resolve().parent / "config.toml")
model_path = delwaq_config.model_path
toml_path = delwaq_config.model_toml
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
