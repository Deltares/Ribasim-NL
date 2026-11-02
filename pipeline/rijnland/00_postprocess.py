# %%
"""Make the postprocessed data of Rijnland, `verwerkt/postprocessed.gpkg`, from its deliveries.

A port of `peilbeheerst_model/preprocess_data/Rijnland.py` and `postprocess_data/post-process_rijnland.py` by HKV, as
they made the data of 2024; the preprocessing then differed from the version in the repository, and is reconstructed
from that data. The boezem is replaced by its parts in the aggregation layer of April 2024, which also gives the
aggregation areas, and the vigerend peilgebieden are burned into the praktijk ones.

`verwerkt/corrections.gpkg` has the corrections of HKV to the praktijk peilgebieden and to a part of the boezem, in the
layers named after them. What remains of the vigerend peilgebieden are mostly slivers along the boundaries of the
praktijk ones, which GEOS 3.14 makes otherwise than GEOS 3.12 did in 2024; its layers `remnants` and `boezem` have the
results of 2024 where they differ. See `docs/dev/reproducing-models.qmd`.
"""

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from ribasim_nl.crossings.postprocess import apply_edits, main_water_system, with_main_water_system_levels, write_layers

from ribasim_nl import CloudStorage

cloud = CloudStorage()

authority = "Rijnland"
delivery = cloud.joinpath(authority, "aangeleverd/Eerste_levering/DataRijnland.zip")
aggregation = cloud.joinpath(authority, "aangeleverd/Na_levering/20240402_gegevensRL_nalevering.gdb")
corrections = cloud.joinpath(authority, "verwerkt/corrections.gpkg")
POLYGONS = ["Polygon", "MultiPolygon"]


def read(layer: str) -> gpd.GeoDataFrame:
    """A layer of the first delivery, read from its zip file."""
    return gpd.read_file(f"/vsizip/{delivery}/DataRijnland.gpkg", layer=layer)


def difference(df1: gpd.GeoDataFrame, df2: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """The rows of `df1` without the polygons of `df2`, as `geopandas.overlay` made them before geopandas 0.14.

    The intersecting polygons are taken off one by one, and the polygonal results are fixed with `buffer(0)`, which
    decides the order of their parts. Empty results are left out.
    """
    others = df2.geometry.to_numpy()
    tree = shapely.STRtree(others)
    geometries = []
    for geometry in df1.geometry.to_numpy():
        for other in others[np.sort(tree.query(geometry, predicate="intersects"))]:
            geometry = shapely.difference(geometry, other)
        geometries.append(geometry)
    result = gpd.GeoSeries(geometries, index=df1.index, crs=df1.crs)
    polygons = result.geom_type.isin(POLYGONS)
    result[polygons] = result[polygons].buffer(0)
    out = df1[~result.is_empty].copy()
    out["geometry"] = result[~result.is_empty]
    return out


def duplicates(geometries: np.ndarray) -> np.ndarray:
    """The positions of the geometries that overlap an earlier one by more than 90 % of both their areas.

    As in 2024, a geometry that is itself a duplicate does not make the later ones duplicates.
    """
    area = shapely.area(geometries)
    assert (area > 0).all(), "peilgebieden without area"
    first, second = shapely.STRtree(geometries).query(geometries, predicate="intersects")
    pairs = first < second
    first, second = first[pairs], second[pairs]
    overlap = shapely.area(shapely.intersection(geometries[first], geometries[second]))
    duplicate = (overlap / area[first] * 100 > 90) & (overlap / area[second] * 100 > 90)
    removed: set[int] = set()
    for i in np.unique(first[duplicate]):
        if i not in removed:
            removed.update(second[duplicate & (first == i)].tolist())
    return np.array(sorted(removed), dtype=int)


# %% preprocessing
columns = ["code", "nen3610id", "globalid", "geometry"]
stuw = read("Stuw")[columns]
gemaal = read("Gemaal")
duikersifonhevel = read("Duikersifonhevel")[columns]
hydroobject = read("Hydroobject")[columns]
vigerend = read("Peilgebiedvigerend")[columns]
praktijk = read("Peilgebiedpraktijk")[columns]
streefpeil = read("Streefpeil")
agg = gpd.read_file(aggregation, layer="Peilgebiedenpraktijk_aggregatie_RL")

# the function of the gemalen: noodpompen circulate, and those with another or no function drain
function = gemaal["functiegemaal"].fillna("")
gemaal["func_aanvoer"] = function.str.contains("Aanvoergemaal|Opmaling")
gemaal["func_circulatie"] = function.str.contains("Doorspoelgemaal|Noodpomp")
gemaal["func_afvoer"] = ~(gemaal["func_aanvoer"] | gemaal["func_circulatie"])
gemaal = gemaal[["code", "nen3610id", "globalid", "func_aanvoer", "func_afvoer", "func_circulatie", "geometry"]]

# the duikers are hydro-objects too, with their row number in their own layer in their code
hydroobject = pd.concat([hydroobject, duikersifonhevel])
hydroobject["code"] = hydroobject["code"] + "_" + hydroobject.index.astype(str)
hydroobject = hydroobject.reset_index(drop=True)

# the aggregation areas join the peilgebieden of the aggregation layer by their aggregation, else their afwatering
agg["geometry"] = shapely.force_2d(agg.geometry.to_numpy())
key = agg["RL_aggregatie_id"].fillna(agg["CODE_AFW"])
aggregation_area = agg.assign(key=key).dissolve(by="key", as_index=False)
aggregation_area["code"] = aggregation_area["code"] + "_dummy_id_" + aggregation_area.index.astype(str)
aggregation_area = aggregation_area[["code", "nen3610id", "CODE_AFW", "globalid", "geometry"]]

# burn the vigerend peilgebieden into the praktijk ones: what remains of them, which got the results of 2024 where GEOS
# 3.14 makes other slivers, and the praktijk ones but the boezem, which its parts replace below
vigerend["geometry"] = vigerend.buffer(0)
praktijk["geometry"] = praktijk.buffer(0)
remnants = apply_edits(difference(vigerend, praktijk), gpd.read_file(corrections, layer="remnants"), "code")
peilgebied = pd.concat([remnants, praktijk[praktijk["code"] != "RIJNLANDSBOEZEM"]], ignore_index=True)
peilgebied = peilgebied[peilgebied.geom_type.isin(POLYGONS)]

# the first streefpeil of each peilgebied: HKV meant to keep the summer levels and lower bounds as well, but `isin`
# matches none of them
streefpeil["peilgebiedpraktijkid"] = streefpeil["peilgebiedpraktijkid"].fillna(streefpeil["peilgebiedvigerendid"])
kept = streefpeil["soortstreefpeil"].isin(["omer", "ondergrens"])
streefpeil = pd.concat([streefpeil[kept], streefpeil[~kept].drop_duplicates(subset="peilgebiedpraktijkid")])
streefpeil = streefpeil[["peilgebiedpraktijkid", "waterhoogte"]]
pg_sp = peilgebied.merge(streefpeil, left_on="globalid", right_on="peilgebiedpraktijkid")
pg_sp = pg_sp.drop(columns="peilgebiedpraktijkid").assign(kind="peilgebied", number=lambda df: df.index)

# HKV corrected the praktijk peilgebieden after the burning in
pg_sp = apply_edits(pg_sp, gpd.read_file(corrections, layer="Peilgebiedpraktijk"), "code")

# the boezem consists of its corrected parts in the aggregation layer, with the level of 2024, 0.61 m, though its
# delivered summer level is -0.61 m; HKV took the code and number of the part Gouwe from its aggregation area
boezem = agg.loc[agg["code"] == "RIJNLANDSBOEZEM", ["code", "nen3610id", "CODE_AFW", "geometry"]]
assert len(boezem) == 17
boezem = boezem.assign(kind="boezem", number=boezem.index, waterhoogte=0.61)
boezem = apply_edits(boezem, gpd.read_file(corrections, layer="Peilgebiedenpraktijk_aggregatie_RL"), "CODE_AFW")
gouwe = aggregation_area.index[aggregation_area["CODE_AFW"] == "Gouwe"]
assert len(gouwe) == 1
boezem.loc[boezem["CODE_AFW"] == "Gouwe", ["code", "number"]] = [aggregation_area.at[gouwe[0], "code"], gouwe[0]]

# the aanvullingen of the boezem and its parts get a margin of 1 m, and come first; HKV took their symmetric
# difference with the other peilgebieden by position, which joins the first 21 of these to them, as duplicates. They get
# the results of 2024 where GEOS 3.14 differs.
in_boezem = pg_sp["code"].str.contains("RIJNLANDSBOEZEM")
aanvulling = pg_sp[in_boezem & pg_sp["code"].str.contains("Aanvulling")]
rest = pg_sp[~in_boezem]
assert len(aanvulling) == 4, f"expected 4 aanvullingen of the boezem, found {len(aanvulling)}"
new = pd.concat([aanvulling, boezem.drop(columns="CODE_AFW")], ignore_index=True)
new["geometry"] = shapely.symmetric_difference(new.buffer(1).to_numpy(), rest.geometry.to_numpy()[: len(new)])
new = apply_edits(new, gpd.read_file(corrections, layer="boezem"), "code")

# every part of a peilgebied is a peilgebied, with the number of its row and its own in its ID
peilgebied = gpd.GeoDataFrame(pd.concat([new, rest]), geometry="geometry", crs=praktijk.crs).explode(ignore_index=True)
part = peilgebied.index.astype(str)
peilgebied["globalid"] = "dummy_globalid_" + peilgebied["kind"] + "_" + peilgebied["number"].astype(str) + "_" + part
peilgebied["code"] = peilgebied["code"] + "_" + part
streefpeil = peilgebied[["waterhoogte", "globalid", "geometry"]]

# %% postprocessing
# HKV left out the third peilgebied, but not its streefpeil
peilgebied = peilgebied.drop(index=2)
peilgebied["peilgebied_cat"] = peilgebied["code"].str.contains("RIJNLANDSBOEZEM").astype(int)
peilgebied = peilgebied[["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]

# the main water system only adds missing streefpeilen, as in 2024
hws = main_water_system(
    gpd.read_file(cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg")),
    gpd.read_file(cloud.joinpath("Rijkswaterstaat/verwerkt/krw_basins_vlakken.gpkg")),
    "Hoogheemraadschap van Rijnland",
)
streefpeil = with_main_water_system_levels(streefpeil, hws)

# remove the duplicate peilgebieden but the first, and their streefpeilen, by row number; HKV skipped three rows for the
# two of the main water system, and so left out the first peilgebied
removed = peilgebied.index[1:][duplicates(peilgebied.geometry.to_numpy()[1:])]
peilgebied = peilgebied[~peilgebied.index.isin(removed)]
streefpeil = streefpeil[~streefpeil.index.isin(removed)]

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
