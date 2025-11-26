"""
10-11-2025 - Jesse van Leeuwen

This notebook sets up the delwaq input files for an LWKM simulation based on an LHM simulation using the ribasim.delwaq.generate function.

Make sure that the ribasim model and results are saved under the location specified by the environment variable RIBASIM_NL_DATA_DIR

It requires manual editing of the generated delwaq input file before running the model (see README.txt for detailed instructions)

This script also creates the loadswq.id file required for delwaq to link loads to Ribasim nodes
The coupling file for concentrations on Flowboundary nodes (transboundary and RWZI) is already created in the generate function
"""

# %% Import necessary libraries
import os
from pathlib import Path

import pandas as pd
from ribasim.delwaq import generate

# %% set path of Ribasim model

model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "modellen/lhm_coupled_2025_9_0"
toml_path = model_path / "lhm.toml"
assert toml_path.is_file()

# %% generate delwaq input files
output_path = model_path / "delwaq"
graph, substances = generate(toml_path, output_path)

# %% check network nodes
node2node = pd.DataFrame(
    [(k, v["id"], v["type"]) for k, v in graph.nodes(data=True)], columns=["delwaq_id", "ribasim_id", "type"]
)

# %% inspect node2node dataframe (only for testing purposes)

##  show full dataframe

# with pd.option_context("display.max_rows", None):
#     display(node2node)

##  example check of a node:

# node2node.loc[(node2node["ribasim_id"] == 99991010) & (node2node["type"] == "Drainage")]

# %% write loadswq.id for loads in delwaq TODO: include in generate function
basins = node2node[node2node["type"] == "Basin"]
with open(output_path / "loadswq.id", "w") as f:
    f.write(f"{len(basins)}; Number of loads\n")

    for _, row in basins.iterrows():
        line = f"{row['delwaq_id']} '{row['ribasim_id']}' ' ' '{row['type']}'\n"
        f.write(line)

# %%
