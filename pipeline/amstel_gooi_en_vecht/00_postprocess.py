# %%
"""Make the postprocessed data of Amstel, Gooi en Vecht, `verwerkt/postprocessed.gpkg`, from its deliveries.

A port of `peilbeheerst_model/preprocess_data/AmstelGooienVecht.py` and `postprocess_data/post-process_agv.py` by HKV,
the version of 2024, which HKV partly committed in November 2024 on the branch `next`. The structures and hydro-objects
come from the data dump of March 2024, all of them rather than those marked for the LHM, and the peilgebieden from the
improved areas of May 2024, with their streefpeilen rounded to cm. Every peilgebied is an aggregation area. See
`docs/dev/reproducing-models.qmd`.
"""

import geopandas as gpd
import pandas as pd
from ribasim_nl.crossings.postprocess import main_water_system, with_main_water_system_levels, write_layers

from ribasim_nl import CloudStorage

cloud = CloudStorage()

authority = "AmstelGooienVecht"
delivery = cloud.joinpath(authority, "aangeleverd")
# pyogrio reads a file in a zip after the exclamation mark
dump = delivery / "aanlevering_6maart24/data dump 6 maart LHM AGV.zip!/data dump 6 maart LHM AGV"

# %% preprocessing
# The preprocessing also reads the CSV files of the first delivery and `vigerende_peilgebieden.gpkg`, but the data dump
# replaces them, except `afsluitmiddel`, which the postprocessing of 2024 left out.
stuw = gpd.read_file(dump / "Stuw.shp")
gemaal = gpd.read_file(dump / "Gemaal.shp")
duikersifonhevel = gpd.read_file(dump / "DuikerSifonHevel.shp")
hydrovakken = gpd.read_file(dump / "LHM_hydrovakken.shp")
# The 239 peilgebieden are in Web Mercator; the coordinates of 2024 differ up to 8e-9 m, from another reprojection.
# GDAL 3.13 splits the self-touching ring of Polder Groot Wilnis Vinkeveen, Overig into an outer ring and a hole, which
# makes it valid; in 2024 it was one ring, which gives the same crossings.
gebieden = gpd.read_file(delivery / "verbeterde_LHM_gebieden_geactualiseerd/LHM_gebieden.shp").to_crs("EPSG:28992")

# a level of 0 is a missing level; the streefpeil is the fixed level, or the summer level if that is lower
levels = gebieden[["vast", "zomer"]].where(gebieden[["vast", "zomer"]] != 0)
waterhoogte = levels.min(axis=1, skipna=True)
assert waterhoogte.notna().all(), "peilgebieden without streefpeil"
# the levels were float32, written as -2.0999999 for -2.1, and the data of 2024 has them rounded to cm
assert (waterhoogte.round(2) - waterhoogte).abs().max() < 1e-6, "levels that are not in cm"
waterhoogte = waterhoogte.round(2)

# AGV confirmed that the name of a gemaal tells its function; gemalen without a known function drain
name = gemaal["naam"].fillna("")
gemaal["func_afvoer"] = name.str.contains("fvoer|nderbemaling|f-|oodpomp")
gemaal["func_aanvoer"] = name.str.contains("anvoergemaal|pmaling|an-|p-|pvoer")
gemaal["func_circulatie"] = name.str.contains("irculatie")
gemaal.loc[~(gemaal["func_afvoer"] | gemaal["func_aanvoer"] | gemaal["func_circulatie"]), "func_afvoer"] = True

# the structures are in RD with NAP heights, EPSG:7415
stuw = stuw[["code", "geometry"]].to_crs("EPSG:28992")
stuw["nen3610id"] = "dummy_nen3610id_stuw_" + stuw.index.astype(str)
stuw["globalid"] = "dummy_globalid_stuw_" + stuw.index.astype(str)

gemaal = gemaal[["code", "func_afvoer", "func_aanvoer", "func_circulatie", "geometry"]].to_crs("EPSG:28992")
gemaal["nen3610id"] = "dummy_nen3610id_gemaal_" + gemaal.index.astype(str)
gemaal["globalid"] = "dummy_globalid_gemaal_" + gemaal.index.astype(str)

duikersifonhevel = duikersifonhevel[["code", "geometry"]].to_crs("EPSG:28992")
duikersifonhevel["nen3610id"] = "dummy_nen3610id_duikersifonhevel_" + duikersifonhevel.index.astype(str)
duikersifonhevel["globalid"] = "dummy_globalid_duikersifonhevel_" + duikersifonhevel.index.astype(str)

# the hydro-objects include the culverts
assert hydrovakken.crs == "EPSG:28992", f"hydro-objects in {hydrovakken.crs}"
hydroobject = hydrovakken[["geometry"]].copy()
hydroobject["code"] = "dummy_code_hydroobject_" + hydroobject.index.astype(str)
hydroobject["nen3610id"] = "dummy_nen3610id_hydroobject_" + hydroobject.index.astype(str)
hydroobject["globalid"] = "dummy_globalid_hydroobject_" + hydroobject.index.astype(str)
hydroobject = gpd.GeoDataFrame(pd.concat([hydroobject, duikersifonhevel]), crs="EPSG:28992")

peilgebied = gebieden[["GAFNAAM", "geometry"]].rename(columns={"GAFNAAM": "code"})
assert peilgebied["code"].notna().all(), "peilgebieden without name"
peilgebied["nen3610id"] = "dummy_nen3610id_peilgebied_" + peilgebied.index.astype(str)
peilgebied["globalid"] = "dummy_globalid_peilgebied_" + peilgebied.index.astype(str)
# a streefpeil has the globalid of its peilgebied
streefpeil = gpd.GeoDataFrame(
    {
        "waterhoogte": waterhoogte,
        "globalid": peilgebied["globalid"],
        "code": "dummy_code_streefpeil_" + peilgebied.index.astype(str),
    },
    geometry=gpd.GeoSeries([None] * len(peilgebied), index=peilgebied.index),
    crs=peilgebied.crs,
)

# every peilgebied is an aggregation area
aggregation_area = peilgebied[["code", "nen3610id", "globalid", "geometry"]].copy()
aggregation_area["globalid"] = "dummy_globalid_agg_area_" + aggregation_area.index.astype(str)
aggregation_area["meta_name"] = aggregation_area["code"]
aggregation_area["code"] = aggregation_area["code"] + "_dummy_id_" + aggregation_area.index.astype(str)

# %% postprocessing
# the boezems
boezem = peilgebied["code"].str.contains("Oosterpark|Vechtboezem|Stadsboezem Amsterdam")
peilgebied["peilgebied_cat"] = boezem.astype(int)
peilgebied = peilgebied[["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]

# the main water system only adds missing streefpeilen, as in 2024
hws = main_water_system(
    gpd.read_file(cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")),
    gpd.read_file(cloud.joinpath("Rijkswaterstaat/verwerkt/krw_basins_vlakken.gpkg")),
    "Waterschap Amstel, Gooi en Vecht",
)
streefpeil = with_main_water_system_levels(streefpeil, hws)

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
