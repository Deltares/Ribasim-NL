### officieel test bestand voor delwaq-ER koppeling

# %% importing general libraries
from pathlib import Path

import pandas as pd
from ribasim.delwaq import generate

# %% set path of Ribasim model

# would ideally be preceded by importing a fully functioning goodcloud model
# could use an instance on public drive but local is better for testing
# model_path should use the environment variable "RIBASIM_NL_DATA_DIR"
# example: model_path = Path(os.environ["RIBASIM_NL_DATA_DIR"]) / "DeDommel/modellen/DeDommel_2025_7_0"

model_path = Path("c:/Users/leeuw_je/Projecten/LWKM_Ribasim/lhm_rwzi_delwaq_Dommel")
toml_path = model_path / "lhm_rwzi_delwaq.toml"
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
