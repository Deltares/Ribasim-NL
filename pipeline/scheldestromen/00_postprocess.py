# %%
"""Make the postprocessed data of Scheldestromen, `verwerkt/postprocessed.gpkg`, from its deliveries.

A port of `peilbeheerst_model/preprocess_data/Scheldestromen.py` and `postprocess_data/post-processing_scheldestromen.py`
by HKV, as they made the data of 2024, which used more deliveries than these scripts read: the stuwen and culverts are
those of the HyDAMO data of the first delivery, the hydro-objects, peilgebieden and gemalen those of the delivery of
March 2024, and the peilgebieden get their code and streefpeil from `Praktijkpeilgebieden_20231204.shp`. The
aggregation areas are the regional drainage units of the peilgebieden, `RWE_ID`, see
`Toelichting_Oplevering_20240328.docx`. GEOS 3.12 made them as in 2024, later versions move some vertices by 1e-8 m.
See `docs/dev/reproducing-models.qmd`.
"""

import geopandas as gpd
import pandas as pd
import shapely
from ribasim_nl.crossings.postprocess import main_water_system, with_main_water_system_levels, write_layers

from ribasim_nl import CloudStorage

cloud = CloudStorage()

authority = "Scheldestromen"
delivery = cloud.joinpath(authority, "aangeleverd")
# pyogrio reads a file in a zip after the exclamation mark
hydamo = delivery / "Eerste_levering/Levering_20230323.zip!/Levering_20230323/Hydamo_scheldestromen_20230323.gpkg"
oplevering = delivery / "Na_levering/Oplevering_Scheldestromen_20240328/Oplevering_20240328.gdb"

# %% preprocessing
stuw = gpd.read_file(hydamo, layer="Stuw")
duikersifonhevel = gpd.read_file(hydamo, layer="Duikersifonhevel")
hydroobjecten = gpd.read_file(oplevering, layer="main_Hydroobject_regionaal")
peilgebieden = gpd.read_file(oplevering, layer="main_Peilgebiedpraktijk_regionaal")
gemalen = gpd.read_file(oplevering, layer="Gemalen")
praktijkpeilgebieden = gpd.read_file(
    delivery / "Na_levering/Praktijkpeilgebieden_20231204/Praktijkpeilgebieden_20231204.shp"
)

# the stuwen are multipoints of one point
assert (stuw.geometry.count_geometries() == 1).all(), "stuwen of several points"
stuw["geometry"] = stuw.centroid
stuw = stuw[["code", "nen3610id", "globalid", "naam", "geometry"]].rename(columns={"naam": "meta_name"})

# the culverts are 2D, and got a z of 0 in 2024, like the hydro-objects have
duikersifonhevel = duikersifonhevel[["code", "nen3610id", "globalid", "geometry"]]
assert not duikersifonhevel.has_z.any(), "culverts with z"
duikersifonhevel["geometry"] = duikersifonhevel.geometry.force_3d(0.0)

# the function of the gemalen, by the domain table of `Toelichting_Oplevering_20240328.docx`: 1 aanvoergemaal,
# 2 afvoergemaal, 3 opmaling, 4 onderbemaling, 7 doorvoergemaal, 8 doorspoelgemaal, 98 overig and 99 onbekend. As in
# 2024, 98 drains and circulates, and the gemalen without function have none.
function = gemalen["KGMFUNCT"]
assert function.dropna().isin([1, 2, 3, 4, 7, 8, 98, 99]).all(), "gemalen with an unknown function"
gemaal = gpd.GeoDataFrame(
    {
        "code": gemalen["KGMNAAM"],
        "KGM_GPGIN": gemalen["KGM_GPGIN"],
        "globalid": gemalen["GLOBALID"],
        "func_afvoer": function.isin([2, 4, 7, 98, 99]),
        "func_aanvoer": function.isin([1, 3]),
        "func_circulatie": function.isin([8, 98]),
        "meta_name": gemalen["KGMNAAM"],
        "meta_capaciteit": gemalen["KGMCOMCAP"],
    },
    geometry=gemalen.geometry,
    crs=gemalen.crs,
)

# the regional hydro-objects, numbered by their row in the delivery, without the connections that are only used for
# extreme discharges or in winter, and all culverts, as in 2024
regional = [
    "Ja",
    "Ja (+ aanvoertraject)",
    "Koppeling aanvoer",
    "Koppeling afvoer (energiebesparing)",
    "Koppeling afvoer (zomer)",
]
hydroobject = hydroobjecten.loc[hydroobjecten["regionaal"].isin(regional), ["geometry"]]
number = hydroobject.index.astype(str)
hydroobject.insert(0, "code", "dummy_code_hydroobject_" + number)
hydroobject.insert(1, "nen3610id", "dummy_nen3610id_hydroobject_" + number)
hydroobject.insert(2, "globalid", "dummy_globalid_hydroobject_" + number)
hydroobject = gpd.GeoDataFrame(pd.concat([hydroobject, duikersifonhevel], ignore_index=True), crs=hydroobjecten.crs)

# the peilgebieden take the code, globalid and streefpeil of their name in the shapefile of December 2023; 12 are not in
# it, and get the code 'nan' and no streefpeil, as in 2024
assert praktijkpeilgebieden["GPGIDENT"].is_unique, "peilgebieden with the same code"
attributes = praktijkpeilgebieden[["GPGIDENT", "GLOBALID", "GPGZP"]]
peilgebieden = peilgebieden.merge(attributes, how="left", left_on="naam", right_on="GPGIDENT", validate="many_to_one")
number = peilgebieden.index.astype(str)
peilgebied = gpd.GeoDataFrame(
    {
        "code": peilgebieden["GPGIDENT"].fillna("nan") + "_dummy_id_" + number,
        "nen3610id": "dummy_nen3610id_peilgebied_" + number,
        "globalid": "dummy_globalid_peilgebied_" + number,
        "meta_name": peilgebieden["naam"],
    },
    geometry=peilgebieden.geometry,
    crs=peilgebieden.crs,
)
streefpeil = gpd.GeoDataFrame(
    {"waterhoogte": peilgebieden["GPGZP"], "globalid": peilgebied["globalid"]},
    geometry=gpd.GeoSeries([None] * len(peilgebied)),
    crs=peilgebied.crs,
)

# an aggregation area is a regional drainage unit, with the first code, nen3610id and globalid of its peilgebieden that
# have one; the units are sorted by their ID as text
units = gpd.GeoDataFrame(
    {
        "RWE_ID": peilgebieden["RWE_ID"],
        "code": peilgebieden["GPGIDENT"],
        "nen3610id": peilgebieden["nen3610id"],
        "globalid": peilgebieden["GLOBALID"],
    },
    geometry=peilgebieden.geometry,
    crs=peilgebieden.crs,
)
assert pd.api.types.is_string_dtype(units["RWE_ID"]) and units["RWE_ID"].notna().all(), "peilgebieden without unit"
aggregation_area = units.dissolve(by="RWE_ID", aggfunc="first").reset_index()
aggregation_area["geometry"] = gpd.GeoSeries(
    [shapely.MultiPolygon([p]) if isinstance(p, shapely.Polygon) else p for p in aggregation_area.geometry],
    crs=aggregation_area.crs,
)
aggregation_area["meta_name"] = aggregation_area["RWE_ID"]
aggregation_area = aggregation_area[["RWE_ID", "code", "nen3610id", "globalid", "meta_name", "geometry"]]

# %% postprocessing
# the boezems; 2024 named GPG862 by its row, 549
boezem = peilgebied["meta_name"].isin(["GPG862", "GPG437"])
assert boezem.sum() == 2, "not one peilgebied per boezem"
peilgebied["peilgebied_cat"] = boezem.astype(int)
peilgebied = peilgebied[["globalid", "code", "nen3610id", "peilgebied_cat", "meta_name", "geometry"]]

# the main water system only adds missing streefpeilen, as in 2024
hws = main_water_system(
    gpd.read_file(cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")),
    gpd.read_file(cloud.joinpath("Rijkswaterstaat/verwerkt/krw_basins_vlakken.gpkg")),
    "Waterschap Scheldestromen",
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
