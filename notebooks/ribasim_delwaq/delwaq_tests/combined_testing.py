"""
5-12-2025 Jesse van Leeuwen

- Test script to generate delwaq input files from Ribasim model, run delwaq simulation and check results
- At this moment includes manual steps to modify delwaq.inp and add input timeseries files
- For testing purposes, the delwaq simulation itself uses a different delwaq folder than the standard generated one
- Here the manual adjustments have already been made
"""

# %% Import necessary libraries
import os
import subprocess
from pathlib import Path

import pandas as pd
from ribasim.delwaq import generate, parse, plot_fraction

# %% set path of Ribasim model
model_name = "lhm_coupled_2025_9_0"

model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "modellen" / model_name
toml_path = model_path / "lhm.toml"
assert toml_path.is_file()


# %% generate delwaq input files and graph variable
output_folder = "delwaq"

output_path = model_path / output_folder
graph, substances = generate(toml_path, output_path)


# %% manually edit delwaq.inp
# TODO: include in generate function
"""
Block 1:
    - Add substances NO3, NH4, OON, PO4, AAP, OOP to the list of substances
    - Change number of substances accordingly
Block 5:
    - keep: INCLUDE 'B5_bounddata.inc'    (already present, existing substances/tracers)
    - add:  INCLUDE 'BOUNDWQ_rwzi.DAT'    (timeseries for new substances on FlowBoundary nodes)
    - add:  INCLUDE 'BOUNDWQ_ba.DAT'      (timeseries for new substances on FlowBoundary nodes)

Block 6:
    - add:  INCLUDE 'loadswq.id'           (links ribasim basins to delwaq segments)
    - add:  INCLUDE 'B6_loads.inc'         (timeseries loads for new substances on Basin nodes)

Block 8:
    - Change contents to (without indentation):
            INITIALS Continuity Drainage FlowBoundary Initial LevelBoundary Precipitation SurfaceRunoff UserDemand NO3 NH4 OON PO4 AAP OOP
            DEFAULTS 1.0 0.0 0.0 1.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0 0.0

    (instead of specifying the initials for every node separately)

"""

# %% write loadswq.id for loads in delwaq (based on graph variable)
# TODO: include in generate function
node2node = pd.DataFrame(
    [(k, v["id"], v["type"]) for k, v in graph.nodes(data=True)], columns=["delwaq_id", "ribasim_id", "type"]
)

basins = node2node[node2node["type"] == "Basin"]

with open(output_path / "loadswq.id", "w") as f:
    f.write(f"{len(basins)}; Number of loads\n")

    for _, row in basins.iterrows():
        line = f"{row['delwaq_id']} '{row['ribasim_id']}' ' ' '{row['type']}'\n"
        f.write(line)

"""
format of loadswq.id (without indentation):

    20494; Number of loads
    1 '200001' ' ' 'Basin'
    2 '200002' ' ' 'Basin'
    ...

<delwaq_segnr> '<ribasim_nodeid>' ' ' '<ribasim_nodetype>'

"""

# %% manually add input timeseries (boundwq.dat & b6_loads.inc) to delwaq folder

# TODO: (lower priority) add data to ribasim model beforehand so generate handles them automatically
# requires timeseries to be in long format with columns ['ribasim_id', 'time', 'substance', 'value']
# requires modification of generate.py for loads on Basin nodes UPDATE: this has been done as of 1-12-2025, accessible through editable install?

# %% Set path of Ribasim model
output_folder = "delwaq_combined_testing"  # changed folder name with delwaq.inp modifications and added files: boundwq_rwzi.dat, boundwq_ba.dat, loadswq.id, b6_loads.inc

output_path = model_path / output_folder
assert toml_path.is_file()


# %% run delwaq from python code
dimr_path = Path(os.environ["DIMR_PATH"])
dimr_config_path = output_path / "dimr_config.xml"

result = subprocess.run([dimr_path, dimr_config_path], cwd=output_path, capture_output=True, encoding="utf-8")
print(result.stdout)
print(result.stderr)
result.check_returncode()


# %% before parsing model: include manually added substance/load
# TODO: include in generate function
substances.add("NO3")
substances.add("NH4")
substances.add("OON")
substances.add("PO4")
substances.add("AAP")
substances.add("OOP")

# %% parse delwaq results
nmodel = parse(toml_path, graph, substances, output_folder=output_path)

# %% check added loads in specified Ribasim nodes
plot_fraction(nmodel, 700970, ["NO3"])  # node downstream of BA 700008; see lhm.toml in QGIS

# fraction exceeds 1, units unclear
