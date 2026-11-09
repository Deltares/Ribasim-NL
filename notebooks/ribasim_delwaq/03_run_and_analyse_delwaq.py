"""Generate, run, and parse the DELWAQ model."""

# %%
from pathlib import Path

from config import load_delwaq_config
from ribasim_nl.delwaq import compute_delwaq

# %%
OUTPUT_FOLDER = "delwaq_all_sources"

delwaq_config = load_delwaq_config(Path(__file__).resolve().parent / "config.toml")
toml_path = delwaq_config.model_toml
output_path = delwaq_config.model_path / OUTPUT_FOLDER

# %% Generate, run and parse DELWAQ, with the Delft3D installation of D3D_HOME
nmodel = compute_delwaq(toml_path, to_input=True, output_path=output_path)
