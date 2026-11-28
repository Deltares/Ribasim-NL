# %%
"""Prepare the HyDAMO data of a water authority from the national GKW-HyDAMO package.

Writes `verwerkt/2_voorbewerking/hydamo.gpkg` from the objects of the authority in
`Basisgegevens/GKW-HyDAMO/LHM_HyDAMO.gpkg`, see `ribasim_nl.lumping.gkw_hydamo`. Its hydroobjects are those of the
categories in `CATEGORIES`, with the others in `CODES`, made into lines that connect, see
`ribasim_nl.lumping.gkw_hydamo.prepare_hydroobjects`. The structures are those of the package. New hydroobjects that
bypass a structure are handled by the lumping, see `ribasim_nl.lumping.hydamo.read_network_without_bypasses`.

Usage: pixi run python pipeline/00_gkw_hydamo.py <authority>
"""

import logging
import sys

import pyogrio
from ribasim_nl.lumping.gkw_hydamo import prepare_hydroobjects, read_authority
from ribasim_nl.lumping.hydamo import STRUCTURE_LAYERS

from ribasim_nl import CloudStorage

cloud = CloudStorage()

# The categories of the hydroobjects that the network of a water authority is lumped from
CATEGORIES = {"BrabantseDelta": ["primair"]}

# The hydroobjects of other categories that the network needs as well, by their code
CODES = {
    "BrabantseDelta": [
        # the supply routes of https://github.com/Deltares/Ribasim-NL/issues/1055: OWL00015 from inlet KST02534 at the
        # Wilhelminakanaal, and OWL00537 with weir KST02867, which the old Donge supplies
        "OWL00015",
        "OWL00537",
    ],
}

logging.basicConfig(level=logging.INFO, format="%(message)s")
assert len(sys.argv) == 2, "pass the authority as argument"
authority = sys.argv[1]
cloud.validate_authority(authority)
assert authority in CATEGORIES, f"no categories of hydroobjects for {authority}"

target = cloud.joinpath(authority, "verwerkt/2_voorbewerking/hydamo.gpkg")
target.parent.mkdir(parents=True, exist_ok=True)
target.unlink(missing_ok=True)

hydroobject = prepare_hydroobjects(
    read_authority(authority, "hydroobject"),
    CATEGORIES[authority],
    codes=CODES.get(authority, ()),
)
pyogrio.write_dataframe(hydroobject, target, layer="hydroobject", promote_to_multi=False)
for layer in STRUCTURE_LAYERS:
    structures = read_authority(authority, layer)
    if len(structures) > 0:
        pyogrio.write_dataframe(structures, target, layer=layer)
