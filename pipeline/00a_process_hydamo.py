# %%
"""Apply the manual changes to the HyDAMO data of a water authority, which the input model is lumped from.

Writes `verwerkt/4_ribasim/hydamo.gpkg` from `verwerkt/2_voorbewerking/hydamo.gpkg`, without the objects of
`verwerkt/3_input/hydamo_verwijderen.gpkg` and with those of `verwerkt/3_input/hydamo_toevoegen.gpkg`. For Aa en Maas,
De Dommel and Vallei en Veluwe, `4_ribasim/hydamo.gpkg` was made otherwise, and is source data.

Usage: pixi run python pipeline/00a_process_hydamo.py <authority>
"""

import sys

from ribasim_nl.lumping.hydamo import apply_hydamo_changes

from ribasim_nl import CloudStorage

cloud = CloudStorage()
authorities = sys.argv[1:]
assert authorities, "pass the authorities as arguments"

for authority in authorities:
    cloud.validate_authority(authority)
    print(authority)
    verwerkt = cloud.joinpath(authority, "verwerkt")
    apply_hydamo_changes(
        source=verwerkt / "2_voorbewerking/hydamo.gpkg",
        removals=verwerkt / "3_input/hydamo_verwijderen.gpkg",
        additions=verwerkt / "3_input/hydamo_toevoegen.gpkg",
        target=verwerkt / "4_ribasim/hydamo.gpkg",
    )
