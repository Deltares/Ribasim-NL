"""Extract the node and link IDs of the input models of the level controlled water authorities of 2024.

The feedback forms and scripts refer to the nodes and links of the input models `<authority>_boezemmodel_<version>` of
2024 by ID. `pipeline/00e_crossings_to_ribasim.py` numbers the nodes and links of the input model after these, with
`ribasim_nl.crossings.renumber`. This script writes them to `<authority>/verwerkt/boezemmodel_ids.gpkg`, from the input
model of 2024, which `data/<authority>/modellen/<model>.dvc` tracks at commit `1115d284`. It was run once; see
`docs/dev/reproducing-models.qmd`.

Usage: pixi run python scripts/extract_boezemmodel_ids.py <directory with <authority>/<model>/ribasim.toml>
"""

import sys
from pathlib import Path

from ribasim_nl.crossings.renumber import Reference

from ribasim_nl import CloudStorage, Model

MODELS = {
    "AmstelGooienVecht": "AmstelGooienVecht_boezemmodel_2024_12_0",
    "Delfland": "Delfland_boezemmodel_2024_12_0",
    "HollandseDelta": "HollandseDelta_boezemmodel_2024_12_3",
    "HollandsNoorderkwartier": "HollandsNoorderkwartier_boezemmodel_2024_12_0",
    "Rijnland": "Rijnland_boezemmodel_2024_12_3",
    "Rivierenland": "Rivierenland_boezemmodel_2024_12_0",
    "Scheldestromen": "Scheldestromen_boezemmodel_2024_12_0",
    "SchielandendeKrimpenerwaard": "SchielandendeKrimpenerwaard_boezemmodel_2024_12_1",
    "WetterskipFryslan": "WetterskipFryslan_boezemmodel_2025_5_1",
    "Zuiderzeeland": "Zuiderzeeland_boezemmodel_2024_12_0",
}

if __name__ == "__main__":
    cloud = CloudStorage()
    source = Path(sys.argv[1])
    for authority, name in MODELS.items():
        reference = Reference.from_model(Model.read(source / authority / name / "ribasim.toml"))
        reference.write(cloud.joinpath(authority, "verwerkt", "boezemmodel_ids.gpkg"))
        print(authority, len(reference.nodes), "nodes", len(reference.links), "links")
