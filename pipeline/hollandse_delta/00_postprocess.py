# %%
"""Make the postprocessed data of Hollandse Delta, `verwerkt/postprocessed.gpkg`, from its deliveries.

A port of `peilbeheerst_model/preprocess_data/Hollandse_Delta.py` and `postprocess_data/post-processing_HD.py` by
HKV, the version of 2024. The postprocessed data of 2024 differs from what these scripts make, and the port follows it:

- Each delivered peilgebied, with its deviations (AP) dissolved, is a peilgebied once, with the attributes of its
  praktijk peilgebied rather than of its deviations, and its row number is appended to its code.
- HKV added the parts of 24 KRW water bodies within 21 peilgebieden as peilgebieden of their own, which overlap those
  peilgebieden, with a streefpeil of their own. The corrections `verwerkt/corrections.gpkg` turn the delivered water
  bodies into these parts.
- The aggregation areas are the peilgebieden, with the code of their praktijk peilgebied. Those of the 21 lose the
  water bodies within them, which become aggregation areas of their own.

See `docs/dev/reproducing-models.qmd`.
"""

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from ribasim_nl.crossings.postprocess import apply_edits, main_water_system, with_main_water_system_levels, write_layers

from ribasim_nl import CloudStorage

cloud = CloudStorage()

authority = "HollandseDelta"
delivery = cloud.joinpath(authority, "aangeleverd/Na_levering")
hydamo = delivery / "Peilgebieden_HD_nalevering/Hollandse_Delta_aangepast.gpkg"
shapefiles = delivery / "Nalvering_tweede/Shapefiles"
corrections = cloud.joinpath(authority, "verwerkt/corrections.gpkg")

# the peilgebieden whose aggregation areas HKV split together into the KRW water bodies within them and the rest, and
# the one that HKV split by itself
KRW_PEILGEBIEDEN = [
    "P-D21.006",
    "P-D27.006",
    "P-G12.008",
    "P-G13.004",
    "P-G22.005",
    "P-G32.007",
    "P-G33.008",
    "P-G42.001",
    "P-G43.006",
    "P-G44.005",
    "P-H02.001",
    "P-H03.002",
    "P-H04.002",
    "P-H17.009",
    "P-H18.001",
    "P-H18.002",
    "P-V01.010",
    "P-V01.022",
    "P-V05.008",
    "P-V06.010",
]
KRW_PEILGEBIED_ALONE = "P-V03.005"
BOEZEMS = [
    "Zuiderdiepboezem_164",
    "Zuiderdiepboezem_163",
    "Zoetwaterboezem_571",
    "Kanaal door Voorne_570",
    "Binnenbedijkte Maas_290",
    "Boezemloozende door Strijensas_333",
    "Kreekkade_660",
    "Zwijndrechtse Waard_703",
]


def difference_2024(df1: gpd.GeoDataFrame, df2: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """`gpd.overlay(df1, df2, how="difference")` as before geopandas 1, which applied `buffer(0)` to the differences."""
    idx1, idx2 = df2.sindex.query(df1.geometry, predicate="intersects", sort=True)
    geometry = []
    for i, geom in enumerate(df1.geometry):
        for other in df2.geometry.iloc[idx2[idx1 == i]]:
            geom = shapely.difference(geom, other)
        geometry.append(geom)
    result = df1.set_geometry(gpd.GeoSeries(geometry, index=df1.index, crs=df1.crs).buffer(0))
    assert not result.is_empty.any(), "an aggregation area lies within the KRW water bodies"
    return result


# %% preprocessing
stuw = gpd.read_file(hydamo, layer="Stuwen")
gemaal = gpd.read_file(hydamo, layer="Gemalen")
afsluitmiddel = gpd.read_file(hydamo, layer="Afsluitmiddelen")
sluis = gpd.read_file(hydamo, layer="Sluizen")
hydroobject = gpd.read_file(hydamo, layer="HydroObjectWatergangtype")
duikersifonhevel = gpd.read_file(hydamo, layer="HydroObjectKunstwerkvakken")

# the function of the gemalen, or their name if it is "Onbekend"; a missing function becomes a string, as with
# astype(str) in pandas 2, so it is not replaced
function = gemaal["FUNCTIEGEM"].astype(object).map(str)
function = function.where(function != "Onbekend", gemaal["NAAM"].astype(object).map(str))
gemaal["func_afvoer"] = function.str.contains("f-|fvoer|nderbemaling")
gemaal["func_aanvoer"] = function.str.contains("anvoergemaal|pmaling")
gemaal["func_circulatie"] = function.str.contains("Doorspoel")
gemaal.loc[~(gemaal["func_afvoer"] | gemaal["func_aanvoer"] | gemaal["func_circulatie"]), "func_afvoer"] = True

stuw = stuw[["CODE", "GLOBALID", "geometry"]].rename(columns={"CODE": "code", "GLOBALID": "globalid"})
stuw["nen3610id"] = "dummy_nen3610id_stuw_" + stuw.index.astype(str)

gemaal = gemaal[["CODE", "GLOBALID", "func_afvoer", "func_aanvoer", "func_circulatie", "geometry"]]
gemaal = gemaal.rename(columns={"CODE": "code", "GLOBALID": "globalid"})
gemaal["nen3610id"] = "dummy_nen3610id_gemaal_" + gemaal.index.astype(str)

hydroobject = hydroobject[["CODE", "GLOBALID", "geometry"]].rename(columns={"CODE": "code", "GLOBALID": "globalid"})
hydroobject["nen3610id"] = "dummy_nen3610id_hydroobject_" + hydroobject.index.astype(str)

sluis = sluis[["CODE", "GLOBALID", "geometry"]].rename(columns={"GLOBALID": "globalid"})
sluis["nen3610id"] = "dummy_nen3610id_sluis_" + sluis.index.astype(str)

duikersifonhevel = duikersifonhevel[["CODE", "GLOBALID", "geometry"]]
duikersifonhevel = duikersifonhevel.rename(columns={"CODE": "code", "GLOBALID": "globalid"})
duikersifonhevel["nen3610id"] = "dummy_nen3610id_duikersifonhevel_" + duikersifonhevel.index.astype(str)

afsluitmiddel = afsluitmiddel[["CODE", "geometry"]].rename(columns={"CODE": "code"})
afsluitmiddel["nen3610id"] = "dummy_nen3610id_afsluitmiddel_" + afsluitmiddel.index.astype(str)
afsluitmiddel["globalid"] = "dummy_globalid_afsluitmiddel_" + afsluitmiddel.index.astype(str)

# the peilgebieden with their deviations (AP) dissolved by `Dis`, with the attributes of their praktijk peilgebied;
# one of them, P-Y07-003, has deviations only
praktijk = gpd.read_file(shapefiles / "PeilgebiedenPraktijk.shp")
praktijk = praktijk.loc[praktijk["AFWIJKENDO"] == "Peilgebied praktijk"].drop(columns="geometry")
assert praktijk["Dis"].is_unique
delivered = gpd.read_file(shapefiles / "PeilgebiedenGeenAP.shp").merge(praktijk, on="Dis", how="left")

# the parts of the KRW water bodies that HKV added as peilgebieden, in their order
krw = gpd.read_file(shapefiles / "KRW-vlak.shp")
krw_bodies = apply_edits(krw, gpd.read_file(corrections, layer="KRW-vlak"), "OWMIDENT")
krw_bodies = krw_bodies.sort_values("KRW_body", kind="stable")
assert (krw_bodies["KRW_body"] == np.arange(len(krw_bodies))).all() and krw_bodies["REKENPEIL_"].notna().all()
krw_bodies["NAAM_BEMAL"] = "KRW_body_" + krw_bodies["KRW_body"].astype(int).astype(str)

peilgebied = pd.concat(
    [delivered[["NAAM_BEMAL", "REKENPEIL_", "geometry"]], krw_bodies[["NAAM_BEMAL", "REKENPEIL_", "geometry"]]],
    ignore_index=True,
)
peilgebied = gpd.GeoDataFrame(peilgebied, geometry="geometry", crs=delivered.crs)
# a missing name becomes "nan", as with astype(str) in pandas 2
peilgebied["code"] = peilgebied["NAAM_BEMAL"].fillna("nan") + "_" + peilgebied.index.astype(str)
peilgebied["nen3610id"] = "dummy_nen3610id_peilgebied_" + peilgebied.index.astype(str)
peilgebied["globalid"] = "dummy_globalid_peilgebied_" + peilgebied.index.astype(str)
assert peilgebied["globalid"].is_unique
streefpeil = gpd.GeoDataFrame(
    peilgebied[["REKENPEIL_", "globalid"]].rename(columns={"REKENPEIL_": "waterhoogte"}),
    geometry=gpd.GeoSeries([None] * len(peilgebied), index=peilgebied.index, crs=peilgebied.crs),
)

# the aggregation areas
aggregation_area = gpd.GeoDataFrame(
    {
        "code": delivered["CODE"],
        "nen3610id": delivered["NAAM_BEMAL"],
        "globalid": "dummy_globalid_agg_area_" + delivered.index.astype(str),
    },
    geometry=delivered.geometry,
    crs=delivered.crs,
)
together = aggregation_area["code"].isin(KRW_PEILGEBIEDEN)
alone = aggregation_area["code"] == KRW_PEILGEBIED_ALONE
assert together.sum() == len(KRW_PEILGEBIEDEN) and alone.sum() == 1
# The KRW water bodies within the 21 become aggregation areas of their own, in the order of `gpd.clip`, which follows
# the spatial index. The 20 split together come first, then the water bodies. P-V03.005 was split by itself: it keeps
# its place, and its water bodies are cut out in their delivered order, as in its basin area in the input model of
# 2024. The crossings files of 2024 lack this aggregation area.
krw_areas = gpd.clip(krw, aggregation_area.loc[together | alone])
krw_areas = gpd.GeoDataFrame(
    {
        "code": krw_areas["OWMIDENT"],
        "nen3610id": krw_areas["OWMNAAM"],
        "globalid": "dummy_globalid_KRW_" + krw_areas.index.astype(str),
    },
    geometry=krw_areas.geometry,
    crs=krw_areas.crs,
)
cut = difference_2024(aggregation_area.loc[together], krw_areas)
aggregation_area.loc[alone, "geometry"] = difference_2024(aggregation_area.loc[alone], krw_areas.sort_index()).geometry
# the part of Havenkanaal Goedereede is a sliver with a line, cut out but not kept
krw_areas = krw_areas.loc[krw_areas.geom_type.isin(["Polygon", "MultiPolygon"])]
aggregation_area = gpd.GeoDataFrame(
    pd.concat([cut, krw_areas, aggregation_area.loc[~together]], ignore_index=True), crs=aggregation_area.crs
)
assert aggregation_area["code"].dropna().is_unique

# %% postprocessing
peilgebied["peilgebied_cat"] = peilgebied["code"].isin(BOEZEMS).astype(int)
assert peilgebied["peilgebied_cat"].sum() == len(BOEZEMS)
peilgebied = peilgebied[["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]

# the main water system only adds missing streefpeilen, as in 2024
hws = main_water_system(
    gpd.read_file(cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")),
    gpd.read_file(cloud.joinpath("Rijkswaterstaat/verwerkt/krw_basins_vlakken.gpkg")),
    "Waterschap Hollandse Delta",
)
streefpeil = with_main_water_system_levels(streefpeil, hws)

write_layers(
    {
        "afsluitmiddel": afsluitmiddel,
        "sluis": sluis,
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
