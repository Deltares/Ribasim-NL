# %%
"""Make the postprocessed data of Schieland en de Krimpenerwaard, `verwerkt/postprocessed.gpkg`, from its deliveries.

A port of `peilbeheerst_model/preprocess_data/HHSK.py` and `postprocess_data/post-processing_HHSK.py` by HKV, as they
were run in the summer of 2024, which differs from the scripts in git. The hydro-objects and culverts come from the
delivery of May 2024, `LHM_06052024_hhsk.gpkg`, rather than from the HyDAMO data and the pressure pipes of
`LHM_hhsk_nalevering.gpkg`. The peilgebieden are the basin areas that HHSK delivered in July 2024, with the code, names
and level of the vigerend peilgebied of March 2024 that they lie in, and every peilgebied is an aggregation area. The
main water system adds nothing, as in 2024. See `docs/dev/reproducing-models.qmd`.
"""

import geopandas as gpd
import pandas as pd
import shapely
from ribasim_nl.crossings.postprocess import write_layers

from ribasim_nl import CloudStorage

cloud = CloudStorage()

authority = "SchielandendeKrimpenerwaard"
delivery = cloud.joinpath(authority, "aangeleverd/Na_levering")

# %% preprocessing
hydamo = delivery / "Wateraanvoer/HyDamo_metWasverzachter_20230905.gpkg"
stuw = gpd.read_file(hydamo, layer="stuw")
gemaal = gpd.read_file(hydamo, layer="gemaal")
lhm = delivery / "poldergebieden_en_duikersifonhevels_persleidingen/LHM_06052024_hhsk.gpkg"
watergangen = gpd.read_file(lhm, layer="hydroobject")
duikers = gpd.read_file(lhm, layer="duikersifonhevel")
vigerend = gpd.read_file(delivery / "hhsklevering_07032024.gpkg", layer="VigerendPeilgebiedZPP")
basin_area = gpd.read_file(delivery / "20240701_losseshapes/basin_area_hhsk.shp")
for df in [stuw, gemaal, watergangen, duikers, vigerend, basin_area]:
    assert df.crs is not None and df.crs.to_epsg() == 28992, f"unexpected CRS {df.crs}"

# only the objects in use
stuw = stuw.loc[stuw["statusobject"].astype(str) == "3", ["code", "nen3610id", "globalid", "geometry"]]
stuw = stuw.reset_index(drop=True)
gemaal = gemaal.loc[gemaal["statusobject"].astype(str) == "3"]

# the function of the gemalen; in git, the pattern of aanvoer is "1|3|5|", which matches every function
function = gemaal["functiegemaal"].astype(str)
gemaal["func_afvoer"] = function.str.contains("2|4|5|6|7|99", na=False)
gemaal["func_aanvoer"] = function.str.contains("1|3|5", na=False)
gemaal["func_circulatie"] = function.str.contains("8", na=False)
gemaal.loc[~(gemaal["func_afvoer"] | gemaal["func_aanvoer"] | gemaal["func_circulatie"]), "func_afvoer"] = True
gemaal = gemaal[["code", "nen3610id", "globalid", "func_afvoer", "func_aanvoer", "func_circulatie", "geometry"]]
gemaal = gemaal[~gemaal.is_empty].reset_index(drop=True)

# the hydro-objects and culverts in use, numbered before the selection
watergangen = watergangen.rename(columns={"CODE": "code", "GLOBALID": "globalid"})
watergangen["nen3610id"] = "dummy_nen3610id_" + watergangen.index.astype(str)
watergangen = watergangen.loc[watergangen["STATUSOBJECT"] == 3]
duikers = duikers.rename(columns={"CODE": "code", "GLOBALID": "globalid"})
duikers["nen3610id"] = "dummy_nen3610id_" + duikers.index.astype(str)
duikers = duikers.loc[duikers["STATUSOBJECT"] == 3]

# the culverts, inlets, gemalen and siphons among the hydro-objects are culverts as well
columns = ["code", "nen3610id", "globalid", "geometry"]
kunstwerken = watergangen.loc[watergangen["code"].isin(["Duiker", "Inlaat", "Gemaal", "Sifon"]), columns]
duikersifonhevel = gpd.GeoDataFrame(pd.concat([duikers[columns], kunstwerken], ignore_index=True), crs=duikers.crs)
# those among the hydro-objects share their globalid with them, so all culverts get a new one
duikersifonhevel["globalid"] = "dummy_globalid_duikersifonhevel_" + duikersifonhevel.index.astype(str)

# the primary hydro-objects and those without category, such as the structures, with the culverts
hydroobject = watergangen.loc[
    watergangen["CATEGORIEOPPWATERLICHAAM"] != 98, ["code", "globalid", "nen3610id", "geometry"]
]
hydroobject = gpd.GeoDataFrame(pd.concat([hydroobject, duikersifonhevel]), crs=duikers.crs)
assert hydroobject["globalid"].is_unique, "hydro-objects with the same globalid"

# HHSK delivered the peilgebieden in July 2024, as the basin areas of a model, which get the code, names and levels of
# the vigerend peilgebied that a point inside them lies in
inside = gpd.GeoDataFrame(geometry=basin_area.representative_point(), crs=basin_area.crs)
inside = gpd.sjoin(inside, vigerend, how="left", predicate="within")
assert inside.index.is_unique and inside["CODE"].notna().all(), "basin areas not in a single vigerend peilgebied"


def numbered(name: pd.Series) -> pd.Series:
    """The names with their row number, and with underscores for commas, as in 2024."""
    return name.str.replace(",", "_") + "_" + name.index.astype(str)


# zero is a missing level in this delivery
levels = inside[["ONDERPEIL", "VASTPEIL", "BOVENPEIL", "SCHOUWPEIL"]]
levels = levels.where(levels != 0)
waterhoogte = levels["ONDERPEIL"].fillna(levels["VASTPEIL"]).fillna(levels["BOVENPEIL"]).fillna(levels["SCHOUWPEIL"])
assert waterhoogte.notna().all(), "peilgebieden without level"

naam = inside["NAAM"].where(inside["NAAM"].str.strip() != "", inside["PEILBESLUI"])
peilgebied = gpd.GeoDataFrame(
    {
        "code": numbered(inside["CODE"]),
        "nen3610id": numbered(inside["PEILBESLUI"]),
        "globalid": numbered(naam),
        "waterhoogte": waterhoogte,
    },
    geometry=[shapely.MultiPolygon([g]) if g.geom_type == "Polygon" else g for g in basin_area.geometry],
    crs="EPSG:28992",
)
assert peilgebied[["code", "nen3610id", "globalid"]].notna().all().all(), "peilgebieden without name"
no_geometry = gpd.GeoSeries([None] * len(peilgebied), index=peilgebied.index)
streefpeil = gpd.GeoDataFrame(peilgebied[["waterhoogte", "globalid"]], geometry=no_geometry, crs=peilgebied.crs)

# %% postprocessing
# without duplicate geometries, such as those of the culverts that are hydro-objects as well
hydroobject["geometry"] = hydroobject.make_valid().force_2d()
hydroobject = hydroobject[~hydroobject.is_empty]
hydroobject = hydroobject.drop_duplicates(subset="geometry", keep="first").reset_index(drop=True)

# the boezems
boezem = peilgebied["code"].str.contains("GPG-399|GPG-1005|GPG-1360|GPG-1012")
peilgebied["peilgebied_cat"] = boezem.astype(int)
peilgebied = peilgebied[["code", "nen3610id", "globalid", "peilgebied_cat", "geometry"]]

# every peilgebied is an aggregation area
aggregation_area = peilgebied[["code", "nen3610id", "globalid", "geometry"]].copy()

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
