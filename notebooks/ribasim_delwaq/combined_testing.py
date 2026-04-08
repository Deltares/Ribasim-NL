"""
5-12-2025 Jesse van Leeuwen

- Test script to generate delwaq input files from Ribasim model, run delwaq simulation and check results
- At this moment includes manual steps to modify delwaq.inp and add input timeseries files
- For testing purposes, the delwaq simulation itself uses a different delwaq folder than the standard generated one
- Here the manual adjustments have already been made
"""

# %%
# Import necessary libraries
import os
import subprocess
from pathlib import Path

import pandas as pd
from ribasim.delwaq import generate, parse, plot_fraction
from ribasim_nl.model import Model

# %%
# set path of Ribasim model
model_name = "lhm_coupled_2025_9_0"

model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / model_name
toml_path = model_path / "lhm.toml"
assert toml_path.is_file()

# %%
"""

Pseudo-code of envisioned workflow to set up a delwaq simulation automatically

# configuration of LWKM run:
# substance or tracer (mode 1 or 2 as done in ANIMO2Delwaq.py)
# which substances to include (e.g. only N and P or also OON, AAP, OOP)
# which emission sources to include (ER, ANIMO, RWZI, BA)
# LHM model version (name)
# reuse existing WQ input (parquet files) or generate new ones from raw data (necessary for new LHM version)

# specify postprocessing preferences:
    # aggregation to WFD waterbodies
    # visualization (geopandas)
    # comparison to monitoring / validation

** Add emission sources to Ribasim data structure #$ new, not yet tested **

# download Ribasim model

cloud.synchronize(lhm)

model = Model.read(toml_path)

# either run coupling scripts and save parquet files or use existing parquet files (write or read, specify per emission source)

if write:

    cloud.synchronize(raw data for ER, ANIMO, RWZI, BA)

    export to cloud (or just save locally): true / false

    run ER2delwaq (model_name, optional args: frac_bergend, specific emissions) --> save parquet
    run ANIMO2delwaq (model_name, optional args: animo version no., mode 1(substance) or mode 2(tracer)) --> save parquet
    run BoundWQ2delwaq (model_name, args=?) --> save parquet

else continue

cloud.synchronize(parquet files)

ER_loads_df = pd.read_parquet(model_path / "ER_loads.pq")
ANIMO_loads_df = pd.read_parquet(model_path / "ANIMO_loads.pq")
RWZI_loads_df = pd.read_parquet(model_path / "RWZI_loads.pq")
BA_loads_df = pd.read_parquet(model_path / "BA_loads.pq")

# process into long format: <nodeid> <time> <substance> <value>
# optional: select which substances to keep (these will be used in delwaq simulation)

model.basin.loads = pd.concat([ER_loads_long_df, ANIMO_loads_long_df], ignore_index=True)
model.flow_boundary.concentration = pd.concat([RWZI_loads_long_df, BA_loads_long_df], ignore_index=True)

model.write(toml_path)  # write Ribasim model with WQ data added

"""

# %%
# generate delwaq input files and graph variable
output_folder = "delwaq"

output_path = model_path / output_folder
graph, substances = generate(toml_path, output_path)

# %%
# manually edit `delwaq.inp`

# %%
# write loadswq.id for loads in delwaq (based on graph variable)
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
# %%
# pause here and proceed with steps in README.md

# %%
# manually add input timeseries (boundwq.dat & b6_loads.inc) to delwaq folder
# %%
########## RUNNING DELWAQ SIMULATION ##########

# Define path of Ribasim model again
output_folder = "delwaq"  # change folder name with delwaq.inp modifications and added files: boundwq_rwzi.dat, boundwq_ba.dat, loadswq.id, b6_loads.inc to prevent overwriting

output_path = model_path / output_folder
assert toml_path.is_file()

# %%
# run delwaq from python code
dimr_path = Path(os.environ["DIMR_PATH"])
dimr_config_path = output_path / "dimr_config.xml"

result = subprocess.run([dimr_path, dimr_config_path], cwd=output_path, capture_output=True, encoding="utf-8")
print(result.stdout)
print(result.stderr)
result.check_returncode()

# %%
# before parsing model: include manually added substance/load
# TODO: include in generate function
substances.add("NO3")
substances.add("NH4")
substances.add("OON")
substances.add("PO4")
substances.add("AAP")
substances.add("OOP")

# %%
# parse delwaq results
nmodel = parse(toml_path, graph, substances, output_folder=output_path)

# %% check added loads in specified Ribasim nodes
plot_fraction(nmodel, 700970, ["NO3"])  # node downstream of BA 700008; see lhm.toml in QGIS
# fraction exceeds 1, units unclear

# %% just to check if model can be read again
model = Model.read(toml_path)
model.basin.concentration  # should include new substances
model.basin.concentration_external

# %%
# save results for QGIS visualization
nmodel.basin.concentration_external._write_arrow("concentration.arrow", nmodel.filepath.parent, nmodel.results_dir)
nmodel.write(model_path / "lhm_test.toml")

# %%
# additional inspection
nmodel.basin.concentration_external.filepath = "results/concentration.arrow"
# display(nmodel.basin.concentration_external)
