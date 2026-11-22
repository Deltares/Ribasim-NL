# %%
"""Prepare the HyDAMO data of a water authority from its extract of the national GKW-HyDAMO package.

Writes `verwerkt/2_voorbewerking/hydamo.gpkg` from `verwerkt/1_ontvangen_data/gkw_hydamo_<date>.gpkg`, the extract that
`scripts/extract_gkw_hydamo.py` made of the package of that date. Its hydroobjects are those of the categories in
`CATEGORIES`, with the others in `CODES` and without those in `EXCLUDED`, made into lines that connect, see
`ribasim_nl.lumping.gkw_hydamo.prepare_hydroobjects`. The structures are those of the extract.

Usage: pixi run python pipeline/00_gkw_hydamo.py <authority> <date>
"""

import logging
import sys

import geopandas as gpd
import pyogrio
from ribasim_nl.lumping.gkw_hydamo import prepare_hydroobjects
from ribasim_nl.lumping.hydamo import STRUCTURE_LAYERS

from ribasim_nl import CloudStorage

cloud = CloudStorage()

# The categories of the hydroobjects that the network of a water authority is lumped from
CATEGORIES = {"BrabantseDelta": ["primair"]}

# The hydroobjects of other categories that the network needs as well, by their code
CODES = {
    "BrabantseDelta": [
        # in the delivery of 2023 that the model was built on, since then secundair or tertiair with a new code
        "OWL00272", "OWL00536", "OWL00537", "OWL00538", "OWL27191", "OWL40023", "OWL40054", "OWL40055", "OWL40056",
        "OWL40106", "OWL40127", "OWL40128", "OWL40340", "OWL40342", "OWL40347", "OWL40354", "OWL40377", "OWL40433",
        "OWL41194", "OWL41213", "OWL41215", "OWL41216", "OWL41228", "OWL41237", "OWL41238", "OWL41244", "OWL41245",
        "OWL41275", "OWL41276", "OWL41277", "OWL41302", "OWL41303", "OWL41304", "OWL41305", "OWL41306", "OWL41307",
        "OWL41308", "OWL41309", "OWL41310", "OWL41311", "OWL41324", "OWL41326", "OWL41377", "OWL41380", "OWL41387",
        "OWL41388", "OWL41389", "OWL41438", "OWL41439", "OWL41440", "OWL41462", "OWL41465", "OWL41467", "OWL41468",
        "OWL41469", "OWL41471", "OWL41472", "OWL41473", "OWL41474", "OWL41475", "OWL41476", "OWL41477", "OWL41478",
        "OWL41500", "OWL41501", "OWL41502", "OWL41540", "OWL41562", "OWL41573", "OWL41574", "OWL41597",
    ],
}  # fmt: skip

# The hydroobjects of these categories that the network is not lumped from, by their code. These are new since the
# delivery of 2023 that the model was built on, and bypass a structure that the model has a node on, or connect two
# of its Basins without a structure. To use one, give it a split node in `verwerkt/3_input/ribasim_input.gpkg`.
EXCLUDED = {
    "BrabantseDelta": [
        # fish passages around a weir or pumping station
        "OVK12628", "OVK12629", "OVK12630", "OVK12631", "OVK12633", "OVK12637", "OVK12638", "OVK12640",
        "OVK12641", "OVK12642", "OVK12643", "OVK12672", "OVK12812", "OVK12825", "OVK12826", "OVK12827",
        "OVK50059", "OVK50062",
        # other watercourses around a structure, or between two Basins
        "OVK08708", "OVK12366", "OVK12567", "OVK12627", "OVK12632", "OVK12708", "OVK12718", "OVK12719",
        "OVK12809", "OVK12828", "OVK21910", "OVK22014", "OVK22020",
        # Westelijke Langstraat, where the watercourses were laid out anew, with new weirs
        "OVK12563", "OVK12565", "OVK12566", "OVK12569", "OVK12570", "OVK12571", "OVK12580", "OVK12581",
        "OVK12582", "OVK12584", "OVK12585", "OVK12587", "OVK12588", "OVK12589", "OVK12593", "OVK12594",
        "OVK12605", "OVK12606", "OVK12613", "OVK12614", "OVK12626", "OVK12634", "OVK12686", "OVK12687",
        "OVK12768",
        # Moerdijk, around (103700, 411000), where new watercourses along the edges of an area replace those within it
        "OVK12789", "OVK12790", "OVK12830", "OVK12897", "OVK12898", "OVK12899", "OVK12900",
        # the old Donge, the supply route of https://github.com/Deltares/Ribasim-NL/issues/1055
        "OVK06910", "OVK12689",
    ],
}  # fmt: skip

logging.basicConfig(level=logging.INFO, format="%(message)s")
assert len(sys.argv) == 3, "pass the authority and the date of its extract as arguments"
authority, date = sys.argv[1:]
cloud.validate_authority(authority)
assert authority in CATEGORIES, f"no categories of hydroobjects for {authority}"

verwerkt = cloud.joinpath(authority, "verwerkt")
extract = verwerkt / f"1_ontvangen_data/gkw_hydamo_{date}.gpkg"
target = verwerkt / "2_voorbewerking/hydamo.gpkg"
target.parent.mkdir(parents=True, exist_ok=True)
target.unlink(missing_ok=True)

hydroobject = prepare_hydroobjects(
    gpd.read_file(extract, layer="hydroobject"),
    CATEGORIES[authority],
    codes=CODES.get(authority, ()),
    excluded=EXCLUDED.get(authority, ()),
)
pyogrio.write_dataframe(hydroobject, target, layer="hydroobject", promote_to_multi=False)
layers = set(pyogrio.list_layers(extract)[:, 0])
for layer in STRUCTURE_LAYERS:
    if layer in layers:
        pyogrio.write_dataframe(gpd.read_file(extract, layer=layer), target, layer=layer, promote_to_multi=False)
