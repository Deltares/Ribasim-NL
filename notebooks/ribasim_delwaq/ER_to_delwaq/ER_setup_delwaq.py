### officieel test bestand voor delwaq-ER koppeling

# %% importing general libraries
from pathlib import Path
import os

import pandas as pd
from ribasim.delwaq import generate

# %% set path of Ribasim model

# this step should be preceded by importing and running a fully functioning ribasim model
# right now this is done in notebooks/rwzi/add_rwzi_model.py
# model_path uses the environment variable "RIBASIM_NL_DATA_DIR" to find the model

model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "DeDommel/modellen/DeDommel_2025_7_0"
toml_path = model_path / "dommel.toml"
assert toml_path.is_file()


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
