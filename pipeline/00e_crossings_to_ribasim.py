# %%
"""Convert the crossings of a level controlled water authority into its input model, `modellen/<authority>_boezemmodel_<version>`.

The peilgebieden or aggregation areas of `verwerkt/postprocessed.gpkg` become Basins, the crossings of
`verwerkt/crossings.gpkg` connector nodes, and the links to the boezems follow the paths of `verwerkt/shortest_path.gpkg`.
The nodes and links are numbered after the input model of 2024, `verwerkt/boezemmodel_ids.gpkg`, to which the feedback
forms and scripts refer. Writes the model and `database_checks.gpkg`, see `ribasim_nl.crossings.model` and
`ribasim_nl.crossings.renumber`.

Usage: pixi run python pipeline/00e_crossings_to_ribasim.py <authority>
"""

import sys

from ribasim_nl.crossings.model import Options, boezem_model
from ribasim_nl.crossings.renumber import Reference, renumber_checks, renumber_nodes

from ribasim_nl import CloudStorage

cloud = CloudStorage()

# the version of the input model of each authority, and how it was made, see `Options`
MODELS = {
    "AmstelGooienVecht": ("2024_12_0", Options()),
    "Delfland": ("2024_12_0", Options()),
    "HollandseDelta": ("2024_12_3", Options()),
    "HollandsNoorderkwartier": (
        "2024_12_0",
        Options(
            link_changes=(
                ((447, 3), (83, 447)),
                ((395, 3), (68, 395)),
                ((571, 3), (126, 571)),
                ((1287, 3), (226, 1287)),
                ((911, 3), (101, 911)),
                ((383, 3), (65, 383)),
                ((817, 3), (119, 817)),
                ((366, 3), (57, 366)),
                ((717, 3), (13, 717)),
                ((487, 3), (98, 487)),
                ((481, 3), (96, 481)),
                ((1027, 3), (197, 1027)),
            )
        ),
    ),
    "Rijnland": ("2024_12_3", Options()),
    "Rivierenland": ("2024_12_0", Options()),
    "Scheldestromen": ("2024_12_0", Options()),
    "SchielandendeKrimpenerwaard": ("2024_12_1", Options(refill_streefpeil=True)),
    # made in May 2025, with the code of then
    "WetterskipFryslan": (
        "2025_5_1",
        Options(all_layers=True, unknown_pump_function="afvoer", add_names=True, endtime="2025-01-01"),
    ),
    "Zuiderzeeland": ("2024_12_0", Options()),
}

authorities = sys.argv[1:]
assert authorities, "pass the authorities as arguments"

for authority in authorities:
    print(authority)
    version, options = MODELS[authority]
    verwerkt = cloud.joinpath(authority, "verwerkt")
    model, checks = boezem_model(
        verwerkt / "postprocessed.gpkg", verwerkt / "crossings.gpkg", verwerkt / "shortest_path.gpkg", options
    )
    mapping = renumber_nodes(model, Reference.read(verwerkt / "boezemmodel_ids.gpkg"))
    renumber_checks(checks, mapping)
    changed = sum(old != new for old, new in mapping.items())
    print(f"{changed} of {len(mapping)} nodes renumbered after the input model of 2024")
    model_dir = cloud.model_dir(authority, f"boezemmodel_{version}")
    model.write(model_dir / "ribasim.toml")
    for name, layer in checks.items():
        layer.to_file(model_dir / "database_checks.gpkg", layer=name)
