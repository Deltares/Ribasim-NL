"""Explore results from an existing DELWAQ run."""

# %%
from pathlib import Path

from config import load_delwaq_config
from ribasim.delwaq import parse

# %% Configuration
ROOT = Path(__file__).resolve().parents[2]
OUTPUT_FOLDER = "delwaq_all_sources"

# Set this to a copied model's DELWAQ results directory when analysing a run
# outside the repository. Leave it as None to use the configured model.
OUTPUT_PATH_OVERRIDE: Path | None = None

delwaq_config = load_delwaq_config(Path(__file__).resolve().parent / "config.toml")
toml_path = delwaq_config.model_toml
output_path = OUTPUT_PATH_OVERRIDE or delwaq_config.model_path / OUTPUT_FOLDER

if not toml_path.is_file():
    raise FileNotFoundError(f"Configured model TOML does not exist: {toml_path}")
if not output_path.is_dir():
    raise FileNotFoundError(f"DELWAQ results directory does not exist: {output_path}")

print(f"Reading model: {toml_path}")
print(f"Reading DELWAQ results: {output_path}")

# %% Parse existing results
# to_input=False keeps this notebook read-only: it does not write parsed
# concentrations or other results back to the Ribasim model.
nmodel = parse(toml_path, output_folder=output_path, to_input=False)

# %% Inspect and visualise
# Add exploratory plots below. For example, inspect the parsed result object:
print(type(nmodel))
print(nmodel)

# %%
# Add selected plots here, for example:
#
# import matplotlib.pyplot as plt
#
# values = ...
# values.plot()
# plt.show()
