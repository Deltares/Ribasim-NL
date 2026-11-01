# %%
"""Make the postprocessed data of Rivierenland, `verwerkt/postprocessed.gpkg`, from its deliveries.

A port of `peilbeheerst_model/preprocess_data/Rivierenland.py` and `postprocess_data/post-process_WSRL.py` by HKV, the
version of 2024. The peilgebieden get the corrections of HKV, `verwerkt/corrections.gpkg`: LNG014-P is split in two, and
LHA201-P to LHA205-P and TLW635-P to TLW637-P are merged into two peilgebieden without a level, while LHA205-P remains.
HKV made the aggregation areas by hand from the supply areas delivered in January 2024, without the boezems of the
Alblasserwaard; the corrections remove 16 supply areas, replace the Nederwaard by 7 of its parts, and add the Overwaard
in two parts and Weeneb with eind a15 stuw. See `docs/dev/reproducing-models.qmd`.
"""

import zipfile

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from ribasim_nl.crossings.postprocess import apply_edits, main_water_system, with_main_water_system_levels, write_layers

from ribasim_nl import CloudStorage

cloud = CloudStorage()

authority = "Rivierenland"
delivery = cloud.joinpath(authority, "aangeleverd")
hydamo = (delivery / "Eerste_levering/WSRL_HyDAMO.zip").as_posix()
overig = (delivery / "Eerste_levering/OverigeGegevens.zip").as_posix()
corrections = cloud.joinpath(authority, "verwerkt/corrections.gpkg")

# the levels of the peilgebieden, the first given in this order. The preprocessing filled them by their type of level
# management first, but those chained assignments on a copy had no effect.
LEVELS = ["STREEFPEIL", "VASTPEIL", "ZOMERPEIL", "MIN_PEIL", "WINTERPEIL", "MAX_PEIL"]
# the boezems: the Linge (LNG), those of the Alblasserwaard (NDW100-P and OVW200-P), and NDB004-P
BOEZEMS = [
    "LNG014-P",
    "LNG014-P_extra",
    "NDB004-P",
    "LNG013-P",
    "LNG012-P",
    "LNG011-P",
    "LNG010-P",
    "LNG009-P",
    "LNG008-P",
    "LNG007-P",
    "LNG006-P",
    "LNG005-P",
    "LNG304-P",
    "LNG002-P",
    "LNG001-P",
    "NDW100-P",
    "OVW200-P",
]
# the boezems of the Alblasserwaard, which HKV cut out of the supply areas
ALBLASSERWAARD = ["NDW100-P", "OVW200-P"]

# %% preprocessing
# a HyDAMO GeoPackage per catchment, in the order of their names, as the preprocessing walked them on Windows
with zipfile.ZipFile(hydamo) as zf:
    members = sorted(name for name in zf.namelist() if name.endswith("/HyDAMO.gpkg"))
assert len(members) == 9, f"expected 9 catchments, found {members}"


def read_catchments(layer: str) -> gpd.GeoDataFrame:
    """A layer of all catchments, one after the other."""
    parts = [gpd.read_file(f"/vsizip/{hydamo}/{member}", layer=layer) for member in members]
    assert all(part.crs == parts[0].crs for part in parts), f"{layer} has several projections"
    return gpd.GeoDataFrame(pd.concat(parts, ignore_index=True), crs=parts[0].crs)


columns = ["code", "globalid", "nen3610id", "geometry"]
stuw = read_catchments("Stuw")[columns]
hydroobject = read_catchments("Hydroobject")[columns]
duikersifonhevel = read_catchments("Duikersifonhevel")[columns]

# the function of the gemalen follows from their name, and is afvoer if it does not
gemaal = read_catchments("Gemaal")
name = gemaal["naam"].astype(object).map(str)
gemaal["func_afvoer"] = name.str.contains("Afvoer|Onbekend|Af-|Onderbemaling")
gemaal["func_aanvoer"] = name.str.contains("Aanvoer|Opmaling")
gemaal["func_circulatie"] = name.str.contains("Doorspoelgemaal")
gemaal.loc[~(gemaal["func_afvoer"] | gemaal["func_aanvoer"] | gemaal["func_circulatie"]), "func_afvoer"] = True
gemaal = gemaal[["code", "globalid", "nen3610id", "func_afvoer", "func_aanvoer", "func_circulatie", "geometry"]]

peilgebied = gpd.read_file(f"/vsizip/{overig}/OverigeGegevens/OverigeGegevens.gdb", layer="PeilgebiedenPraktijk")
peilgebied = apply_edits(peilgebied, gpd.read_file(corrections, layer="PeilgebiedenPraktijk"), "CODE")
levels = peilgebied[LEVELS].astype(float)
# the levels are in cm, with the noise of float32 (0.69999999 for 0.7), which the data of 2024 had not
noise = np.abs(levels.to_numpy() - levels.round(2).to_numpy())
assert np.nanmax(noise) < 1e-6, "levels that are not in cm"
waterhoogte = levels.round(2).bfill(axis=1).iloc[:, 0]

# %% postprocessing
peilgebied_cat = peilgebied["CODE"].isin(BOEZEMS).astype(int)
alblasserwaard = shapely.union_all(peilgebied.loc[peilgebied["CODE"].isin(ALBLASSERWAARD)].geometry.to_numpy())

i = peilgebied.index.astype(str)
globalid = "dummy_globalid_peilgebied_" + i
streefpeil = gpd.GeoDataFrame(
    {"waterhoogte": waterhoogte, "globalid": globalid}, geometry=gpd.GeoSeries([None] * len(i)), crs=peilgebied.crs
)
peilgebied = gpd.GeoDataFrame(
    {
        "globalid": globalid,
        "code": "dummy_code_peilgebied_" + i,
        "nen3610id": "dummy_nen3610id_peilgebied_" + i,
        "peilgebied_cat": peilgebied_cat,
    },
    geometry=peilgebied.geometry,
    crs=peilgebied.crs,
)
assert peilgebied["globalid"].is_unique

# the main water system only adds missing streefpeilen, as in 2024
hws = main_water_system(
    gpd.read_file(cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")),
    gpd.read_file(cloud.joinpath("Rijkswaterstaat/verwerkt/krw_basins_vlakken.gpkg")),
    "Waterschap Rivierenland",
)
streefpeil = with_main_water_system_levels(streefpeil, hws)

# %% aggregation areas, numbered in order, as HKV did
supply = gpd.read_file(delivery / "Na_levering/Wateraanvoer/Aanvoergebieden_detail.shp")
supply = apply_edits(supply, gpd.read_file(corrections, layer="Aanvoergebieden_detail"), "GAFIDENT")
# the difference with all supply areas, which also starts their rings at the vertex of 2024
supply["geometry"] = supply.difference(alblasserwaard)
i = supply.index.astype(str)
aggregation_area = gpd.GeoDataFrame(
    {
        "code": supply["GAFNAAM"] + "_dummy_id_" + i,
        "nen3610id": "dummy_nen3610id_agg_area_" + i,
        "globalid": "dummy_globalid_agg_area_" + i,
        "meta_name": supply["GAFNAAM"],
    },
    geometry=supply.geometry,
    crs=supply.crs,
)
# the aggregation areas that HKV changed after numbering them
aggregation_area = apply_edits(aggregation_area, gpd.read_file(corrections, layer="aggregation_area"), "code")
assert aggregation_area["code"].is_unique

write_layers(
    {
        "stuw": stuw,
        "gemaal": gemaal,
        "hydroobject": hydroobject,
        "duikersifonhevel": duikersifonhevel,
        "peilgebied": peilgebied,
        "streefpeil": streefpeil,
        "aggregation_area": aggregation_area,
    },
    cloud.joinpath(authority, "verwerkt/postprocessed.gpkg"),
)
