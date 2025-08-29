### officieel test bestand voor delwaq-ER koppeling

# %% importing general libraries
import os
from pathlib import Path

import pandas as pd
from ribasim.delwaq import generate

# %% set path of Ribasim model

# this step should be preceded by importing and running a fully functioning ribasim model
# right now this is done in notebooks/rwzi/add_rwzi_model.py
# model_path uses the environment variable "RIBASIM_NL_DATA_DIR" to find the model
# a later version of this script should access a model with results directly from goodcloud

model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "Rijkswaterstaat/modellen/lhm_vrij_coupled_2025_6_1/"
toml_path = model_path / "lhm.toml"
assert toml_path.is_file()

# %% check if ribasim model has been run correctly and has results
pd.read_feather(model_path / "results/basin.arrow", dtype_backend="pyarrow")
# should incl


# %% generate delwaq input files
output_path = model_path / "delwaq"
graph, substances = generate(toml_path, output_path)


# %% check network nodes
node2node = pd.DataFrame(
    [(k, v["id"], v["type"]) for k, v in graph.nodes(data=True)], columns=["delwaq_id", "ribasim_id", "type"]
)


# %% write boundlist for loads in delwaq
basins = node2node[node2node["type"] == "Basin"]
with open(output_path / "delwaq_bndlist.inc", "w") as f:
    f.write(f"{len(basins)}; Number of loads\n")

    for _, row in basins.iterrows():
        line = f"{row['delwaq_id']} 'Basin_{row['ribasim_id']}' ' ' '{row['type']}'\n"
        f.write(line)


## Manually edit the delwaq input files before running model (see README.txt for detailed instructions)
## example check of bdlist: node2node.loc[(node2node["ribasim_id"] == 99991010) & (node2node["type"] == "Drainage")]

# %%
