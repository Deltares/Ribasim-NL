# %%
"""Make the postprocessed data of Zuiderzeeland, `verwerkt/postprocessed.gpkg`, from its deliveries.

A port of `peilbeheerst_model/preprocess_data/Zuiderzeeland.py` and `postprocess_data/post-process_zuiderzeeland.py` by
HKV, the version of 2024. The peilvakken get the corrections of HKV, `verwerkt/corrections.gpkg`, and every peilgebied is
an aggregation area. See `docs/dev/reproducing-models.qmd`.
"""

import geopandas as gpd
import numpy as np
import pandas as pd
from ribasim_nl.crossings.postprocess import apply_edits, main_water_system, with_main_water_system_levels, write_layers

from ribasim_nl import CloudStorage

cloud = CloudStorage()

authority = "Zuiderzeeland"
delivery = cloud.joinpath(authority, "aangeleverd/Na_levering")
corrections = cloud.joinpath(authority, "verwerkt/corrections.gpkg")

# %% preprocessing
gemaal = gpd.read_file(delivery / "gemalen.gpkg")
kunstwerken = gpd.read_file(delivery / "overigekunstwerken.gpkg")
peilvakken = gpd.read_file(delivery / "peilvakken_nalevering.gpkg")
peilvakken = apply_edits(peilvakken, gpd.read_file(corrections, layer="peilvakken_nalevering"), "GPGIDENT")
duikers = gpd.read_file(delivery / "Duikers.gpkg").set_crs("EPSG:28992", allow_override=True)
# the coordinates of the watergangen are in WGS84, whatever their projection file says
watergangen = gpd.read_file(delivery / "zzl_watergangen_nalevering/zzl_Watergangen.shp")
watergangen = watergangen.set_crs("WGS84", allow_override=True).to_crs("EPSG:28992")

# the stuwen are among the other structures, by their kind
stuw = kunstwerken.loc[kunstwerken["KWKSOORT"].isin(["Constructie", "inlaat", "uitlaat", "keerwand"])]
stuw = stuw.reset_index(drop=True)
stuw["geometry"] = stuw.centroid
hevels = kunstwerken.loc[kunstwerken["KWKSOORT"] == "Hevel"].reset_index(drop=True)

# the function of the gemalen, from their function or else their kind
function = gemaal["KGMFUNC"].astype(object).map(str)
function = function.where(function != "onbekend", np.nan).fillna(gemaal["KGMSOORT"])
assert function.notna().all(), "gemalen without function or kind"
gemaal["func_aanvoer"] = function.str.contains("aanvoergemaal|opmaling")
gemaal["func_afvoer"] = function.str.contains("af-|afvoer|onderbemaling")
gemaal["func_circulatie"] = function.str.contains("circulatie")
gemaal.loc[~(gemaal["func_aanvoer"] | gemaal["func_afvoer"] | gemaal["func_circulatie"]), "func_afvoer"] = True
gemaal = gemaal[["KGMIDENT", "GLOBALID", "func_aanvoer", "func_afvoer", "func_circulatie", "geometry"]]
gemaal = gemaal.rename(columns={"KGMIDENT": "code", "GLOBALID": "globalid"})
gemaal["nen3610id"] = "dummy_nen3610id_gemaal_" + gemaal.index.astype(str)

hydroobject = watergangen[["OWAIDENT", "GLOBALID", "geometry"]]
hydroobject = hydroobject.rename(columns={"OWAIDENT": "code", "GLOBALID": "globalid"})
hydroobject["nen3610id"] = "dummy_nen3610id_hydroobject_" + hydroobject.index.astype(str)

duikersifonhevel = duikers[["KDUIDENT", "GLOBALID", "geometry"]].rename(
    columns={"KDUIDENT": "code", "GLOBALID": "globalid"}
)
duikersifonhevel["nen3610id"] = "dummy_nen3610id_duikersifonhevel_" + duikersifonhevel.index.astype(str)
hevels = hevels[["KWKIDENT", "GLOBALID", "geometry"]].rename(columns={"KWKIDENT": "code", "GLOBALID": "globalid"})
hevels["nen3610id"] = "dummy_nen3610id_hevels_" + hevels.index.astype(str)
duikersifonhevel = gpd.GeoDataFrame(pd.concat([duikersifonhevel, hevels]), crs="EPSG:28992")

stuw = stuw[["KWKIDENT", "GLOBALID", "geometry", "KWKSOORT"]]
stuw = stuw.rename(columns={"KWKIDENT": "code", "GLOBALID": "globalid", "KWKSOORT": "KWKsoort"})
stuw["nen3610id"] = "dummy_nen3610id_stuw_" + stuw.index.astype(str)

peilgebied = peilvakken[["DHYDRO_ZMRPL", "GPGIDENT", "geometry"]].rename(
    columns={"DHYDRO_ZMRPL": "waterhoogte", "GPGIDENT": "code"}
)
peilgebied["nen3610id"] = "dummy_nen3610id_peilgebied_" + peilgebied.index.astype(str)
peilgebied["globalid"] = "dummy_globalid_peilgebied_" + peilgebied.index.astype(str)
streefpeil = gpd.GeoDataFrame(
    peilgebied[["waterhoogte", "globalid"]],
    geometry=gpd.GeoSeries([None] * len(peilgebied), index=peilgebied.index),
    crs=peilgebied.crs,
)

# %% postprocessing
# the boezems
boezem = peilgebied["code"].map(
    lambda code: any(part in str(code) for part in ["LVA.01", "3.01", "LAGE AFDELING", "HOGE AFDELING"])
)
peilgebied["peilgebied_cat"] = boezem.astype(int)
peilgebied = peilgebied[["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]

# the main water system only adds missing streefpeilen, as in 2024
hws = main_water_system(
    gpd.read_file(cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")),
    gpd.read_file(cloud.joinpath("Rijkswaterstaat/verwerkt/krw_basins_vlakken.gpkg")),
    "Waterschap Zuiderzeeland",
)
streefpeil = with_main_water_system_levels(streefpeil, hws)

# every peilgebied is an aggregation area
aggregation_area = peilgebied[["code", "globalid", "nen3610id", "geometry"]].copy()
aggregation_area["meta_name"] = aggregation_area["code"]
aggregation_area["code"] = aggregation_area["code"] + "_dummy_id_" + aggregation_area.index.astype(str)
aggregation_area["globalid"] = "dummy_globalid_agg_area_" + aggregation_area.index.astype(str)
aggregation_area = aggregation_area[["code", "globalid", "nen3610id", "meta_name", "geometry"]]

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
