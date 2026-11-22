# %%
"""Apply the manual changes to the HyDAMO data of a water authority, which the input model is lumped from.

Writes `verwerkt/4_ribasim/hydamo.gpkg` from `verwerkt/2_voorbewerking/hydamo.gpkg`, without the objects of
`verwerkt/3_input/hydamo_verwijderen.gpkg` and with those of `verwerkt/3_input/hydamo_toevoegen.gpkg`. For Aa en Maas,
De Dommel and Vallei en Veluwe, `4_ribasim/hydamo.gpkg` was made otherwise, and is source data.

Where `pipeline/00_gkw_hydamo.py` prepared the HyDAMO data from the GKW-HyDAMO package, the hydroobjects are connected
again after the changes, so an added hydroobject connects where it ends near another one.

Usage: pixi run python pipeline/00a_process_hydamo.py <authority>
"""

import sys

from ribasim_nl.lumping.gkw_hydamo import CONNECT_DISTANCE
from ribasim_nl.lumping.hydamo import apply_hydamo_changes

from ribasim_nl import CloudStorage

cloud = CloudStorage()

# The water authorities with HyDAMO data from the GKW-HyDAMO package
GKW_AUTHORITIES = {"BrabantseDelta"}

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
        connect_distance=CONNECT_DISTANCE if authority in GKW_AUTHORITIES else None,
    )
