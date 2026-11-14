"""Generate, run, and parse the DELWAQ model."""

# %%
from pathlib import Path

from config import load_delwaq_config
from ribasim_nl.delwaq import CONCENTRATION_FILENAME, compute_delwaq

# %%
OUTPUT_FOLDER = "delwaq_all_sources"

delwaq_config = load_delwaq_config(Path(__file__).resolve().parent / "config.toml")
toml_path = delwaq_config.model_toml
output_path = delwaq_config.model_path / OUTPUT_FOLDER

# %% Generate, run and parse DELWAQ, with the Delft3D installation of D3D_HOME.
# The concentrations of Delwaq go next to those of the core, which the model viewer shows both.
nmodel = compute_delwaq(
    toml_path,
    to_input=True,
    output_path=output_path,
    concentration_path=delwaq_config.model_path / "results" / CONCENTRATION_FILENAME,
)
