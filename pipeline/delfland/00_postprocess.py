# %%
"""Make the postprocessed data of Delfland, `verwerkt/postprocessed.gpkg`, from its deliveries.

A port of `peilbeheerst_model/preprocess_data/Delfland.py` and `postprocess_data/post-process_delfland.py` by HKV, the
version of 2024. The aggregation areas are the delivered polders. HKV replaced the boezem, the peilgebied `BZM 1` and the
polder `BZM`, by 12 parts, one or more per KRW water body, which are peilgebieden and aggregation areas. No delivered
layer has them, so they are read from `verwerkt/aggregation_area.gpkg`, which has them from the postprocessed data of
2024. See `docs/dev/reproducing-models.qmd`.
"""

from pathlib import Path

import geopandas as gpd
import pandas as pd
from ribasim_nl.crossings.postprocess import main_water_system, with_main_water_system_levels, write_layers

from ribasim_nl import CloudStorage

cloud = CloudStorage()

authority = "Delfland"
delivery = cloud.joinpath(authority, "aangeleverd/Na_levering")


def read_delivery(path: Path, layer: str | None = None) -> gpd.GeoDataFrame:
    """A delivered layer in RD New; the deliveries are in RD New with NAP heights."""
    return gpd.read_file(path, layer=layer).to_crs("EPSG:28992")


# %% preprocessing
watersysteem = delivery / "Watersysteem.gdb"
stuw = read_delivery(watersysteem, "Stuw")
gemaal = read_delivery(watersysteem, "Gemaal")
watergang = read_delivery(watersysteem, "Watergang")
duikersifonhevel = read_delivery(watersysteem, "DuikerSifonHevel")
peilgebiedpraktijk = read_delivery(watersysteem, "PeilgebiedPraktijk")

stuw = stuw[["CODE", "GLOBALID", "geometry"]].rename(columns={"CODE": "code", "GLOBALID": "globalid"})
stuw["nen3610id"] = "dummy_nen3610id_stuw_" + stuw.index.astype(str)

# the function of the gemalen; the script of 2024 meant to fill a missing function with the kind of gemaal, but did not
function = gemaal["FUNCTIEGEMAAL_resolved"]
gemaal["func_afvoer"] = function.str.contains("Onbekend|Onderbemaling|Afvoergemaal|Af-", na=False)
gemaal["func_aanvoer"] = function.str.contains("Opmaling|Aanvoer", na=False)
gemaal["func_circulatie"] = function.str.contains("Overig|circulatie", na=False)
gemaal.loc[~(gemaal["func_afvoer"] | gemaal["func_aanvoer"] | gemaal["func_circulatie"]), "func_afvoer"] = True
gemaal = gemaal[["GLOBALID", "func_afvoer", "func_aanvoer", "func_circulatie", "geometry"]]
gemaal = gemaal.rename(columns={"GLOBALID": "globalid"})
gemaal["code"] = "dummy_code_gemaal_" + gemaal.index.astype(str)
gemaal["nen3610id"] = "dummy_nen3610id_gemaal_" + gemaal.index.astype(str)

hydroobject = watergang[["GLOBALID", "geometry"]].rename(columns={"GLOBALID": "globalid"})
hydroobject["code"] = "dummy_code_hydroobject_" + hydroobject.index.astype(str)
hydroobject["nen3610id"] = "dummy_nen3610id_hydroobject_" + hydroobject.index.astype(str)

duikersifonhevel = duikersifonhevel[["CODE", "GLOBALID", "geometry"]]
duikersifonhevel = duikersifonhevel.rename(columns={"CODE": "code", "GLOBALID": "globalid"})
# their codes are not unique
duikersifonhevel["code"] = "dummy_code_duikersifonhevel_" + duikersifonhevel.index.astype(str)
duikersifonhevel["nen3610id"] = "dummy_nen3610id_duikersifonhevel_" + duikersifonhevel.index.astype(str)

peilgebied = peilgebiedpraktijk[["WS_HOOGPEIL", "CODE", "GLOBALID", "geometry"]]
peilgebied = peilgebied.rename(columns={"WS_HOOGPEIL": "streefpeil", "CODE": "code", "GLOBALID": "globalid"})
peilgebied["nen3610id"] = "dummy_nen3610id_peilgebiedpraktijk_" + peilgebied.index.astype(str)

# HKV replaced the boezem by its parts, after the preprocessing; the parts follow the peilgebieden
parts = gpd.read_file(cloud.joinpath(authority, "verwerkt/aggregation_area.gpkg"))
boezem = peilgebied["code"] == "BZM 1"
assert boezem.sum() == 1, "no single boezem BZM 1"
parts["streefpeil"] = peilgebied.loc[boezem, "streefpeil"].iat[0]
peilgebied = pd.concat([peilgebied[~boezem], parts[peilgebied.columns]], ignore_index=True)
assert peilgebied["code"].notna().all(), "peilgebieden without code"
assert peilgebied["globalid"].is_unique, "peilgebieden with the same globalid"

# %% postprocessing
# the boezems: the peilgebieden with a code BZM, and the parts of BZM 1, which HKV marked as boezem by hand
boezem = peilgebied["code"].str.startswith("BZM") | peilgebied["globalid"].isin(parts["globalid"])
peilgebied["peilgebied_cat"] = boezem.astype(int)

# The peilgebieden in the main water system (category 2) were to be left out, but none were, since the script of 2024
# took the overlap after subtracting it. The main water system only adds missing streefpeilen.
hws = main_water_system(
    gpd.read_file(cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")),
    gpd.read_file(cloud.joinpath("Rijkswaterstaat/verwerkt/krw_basins_vlakken.gpkg")),
    "Hoogheemraadschap van Delfland",
)
streefpeil = gpd.GeoDataFrame(
    {"waterhoogte": peilgebied["streefpeil"], "globalid": peilgebied["globalid"]},
    geometry=gpd.GeoSeries([None] * len(peilgebied), index=peilgebied.index),
    crs=peilgebied.crs,
)
streefpeil = with_main_water_system_levels(streefpeil, hws, prefix="dummy_globalid_hws_")
peilgebied = peilgebied[["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]

# the delivered polders are the aggregation areas, but for the boezem, which its parts replace
polders = read_delivery(delivery / "Wateraanvoer/Aanvoergebied_Afvoergebied_polders.gpkg")
assert (polders["CODE"] == "BZM").sum() == 1, "no single boezem BZM"
polders = polders[polders["CODE"] != "BZM"]
aggregation_area = gpd.GeoDataFrame(
    {
        "code": polders["CODE"],
        "nen3610id": polders["NAAM"],
        "globalid": "dummy_globalid_agg_area_" + polders.index.astype(str),
        "meta_name": polders["NAAM"],
    },
    geometry=polders.geometry,
)
aggregation_area = pd.concat([aggregation_area, parts[aggregation_area.columns]], ignore_index=True)

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
