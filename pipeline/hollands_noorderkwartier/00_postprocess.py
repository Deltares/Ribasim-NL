# %%
"""Make the postprocessed data of Hollands Noorderkwartier, `verwerkt/postprocessed.gpkg`, from its deliveries.

A port of `peilbeheerst_model/preprocess_data/HHNK.py` and `postprocess_data/post-processing_HHNK.py` by HKV, as they
made the data of 2024, which used other deliveries than these scripts read: the peilgebieden and their streefpeil are
those of the delivery of June 2024, rather than the BWN ruimtekaart and the boezems of `BZM_HWS_HHNK.shp`, and the
aggregation areas are the polders of that delivery, as the union of the peilgebieden in each. The hydro-objects and
culverts are those of the HyDAMO data of the first delivery, the gemalen and stuwen those of March 2024. See
`docs/dev/reproducing-models.qmd`.
"""

import struct
from pathlib import Path

import geopandas as gpd
import numpy as np
import shapely
from ribasim_nl.crossings.postprocess import main_water_system, with_main_water_system_levels, write_layers
from ribasim_nl.lumping.utils import overlay_intersection

from ribasim_nl import CloudStorage

cloud = CloudStorage()

authority = "HollandsNoorderkwartier"
delivery = cloud.joinpath(authority, "aangeleverd")
# pyogrio reads a file in a zip after the exclamation mark
hydamo = delivery / "Eerste_levering/20230413_aanlevering_hyDAMO_HHNK.zip!/20230413_HyDAMO_HHNK.gpkg"
# GDAL does not recognize this geodatabase by its name, but opens its tables
gemalen_stuwen = delivery / "Na_levering/Na_levering_HHNK_gemalen_stuwen_20240321.gdb - Copy"
peilgebieden_polders = delivery / "Na_levering/20240618_peilgebieden_en_polders"

# the peilgebieden that HKV removed, in the dunes of Texel (8071) and between Petten and Den Helder (2040), all but one
# without streefpeil
REMOVED = ["8071-01", "8071-02", "2040-2060", "2040-2030-02", "2040-2700", "2040-2020"]
# the peilgebieden of the boezems, as in 2024: Schermerboezem, Schagerkogge, Amstelmeer and Raaksmaats- en Niedorperkogge
BOEZEMS = ["1000-01", "03010-01", "1700-01", "1800-01"]


def polygons_as_stored(path: Path) -> list[shapely.Polygon | shapely.MultiPolygon]:
    """The polygons of a shapefile with their rings as stored, as GDAL 3.10 and before read them.

    GDAL 3.13 splits a ring that touches itself into an outer ring and holes, and orders the parts anew. Here a clockwise
    ring starts a polygon, and the counterclockwise rings after it are its holes.
    """
    data = path.read_bytes()
    polygons = []
    offset = 100  # the header
    while offset < len(data):
        (length,) = struct.unpack(">i", data[offset + 4 : offset + 8])
        record = data[offset + 8 : offset + 8 + 2 * length]
        offset += 8 + 2 * length
        (shape_type,) = struct.unpack("<i", record[:4])
        assert shape_type == 5, f"a record of shape type {shape_type}, not a polygon"
        n_parts, n_points = struct.unpack("<2i", record[36:44])
        starts = struct.unpack(f"<{n_parts}i", record[44 : 44 + 4 * n_parts])
        xy = np.frombuffer(record, dtype="<f8", count=2 * n_points, offset=44 + 4 * n_parts).reshape(-1, 2)
        parts: list[list[np.ndarray]] = []
        for ring in np.split(xy, starts[1:]):
            x, y = ring[:, 0], ring[:, 1]
            if np.sum(x[:-1] * y[1:] - x[1:] * y[:-1]) < 0:
                parts.append([ring])
            else:
                assert parts, "a hole before its outer ring"
                parts[-1].append(ring)
        shapes = [shapely.Polygon(part[0], part[1:]) for part in parts]
        polygons.append(shapes[0] if len(shapes) == 1 else shapely.MultiPolygon(shapes))
    return polygons


# %% preprocessing
gemalen = gpd.read_file(gemalen_stuwen / "a00000009.gdbtable", layer="Gemaal")
stuwen = gpd.read_file(gemalen_stuwen / "a0000000a.gdbtable", layer="Stuw")
hydroobject = gpd.read_file(hydamo, layer="hydroobject")
duikersifonhevel = gpd.read_file(hydamo, layer="duikersifonhevel")
peilgebieden_path = peilgebieden_polders / "Peilgebieden_export_2024-06-18.shp"
peilgebieden = gpd.read_file(peilgebieden_path)
polders = gpd.read_file(peilgebieden_polders / "Polders_export_2024-06-18.shp")

# the function codes of the gemalen are floats, such as 4.0, which the preprocessing compared as text with "4". None
# matched, so all gemalen drain. It meant 2, 4, 5, 6 and 903 to drain, 1, 3, 5, 902 and 903 to supply, and 904 to
# circulate, and to leave out 901, not in use.
names = gemalen["NAAM"].where(gemalen["NAAM"] != "NVT")
gemaal = gpd.GeoDataFrame(
    {
        "code": gemalen["CODE"],
        "globalid": gemalen["GLOBALID"],
        "nen3610id": "dummy_nen3610id_" + gemalen.index.astype(str),
        "func_afvoer": True,
        "func_aanvoer": False,
        "func_circulatie": False,
        "meta_name": names.fillna(gemalen["CODE"]),
        "meta_capaciteit": gemalen["MAXIMALECAPACITEIT"],
    },
    # HKV moved the gemalen by 1 cm
    geometry=gemalen.translate(0.01, 0.01),
    crs=gemalen.crs,
)

stuw = gpd.GeoDataFrame(
    {
        "code": stuwen["CODE"],
        "globalid": stuwen["GLOBALID"],
        "nen3610id": "dummy_nen3610id_" + stuwen.index.astype(str),
        "meta_name": stuwen["CODE"],
    },
    geometry=stuwen.geometry,
    crs=stuwen.crs,
)

# one code occurs twice, so the row number makes them unique
hydroobject["code"] = hydroobject["code"] + "_" + hydroobject.index.astype(str)
hydroobject = hydroobject[["code", "globalid", "nen3610id", "geometry"]]
duikersifonhevel = duikersifonhevel[["code", "globalid", "nen3610id", "geometry"]]

# the peilgebieden as GDAL read them in 2024, some with rings that touch themselves
stored = gpd.GeoSeries(polygons_as_stored(peilgebieden_path), index=peilgebieden.index, crs=peilgebieden.crs)
assert np.allclose(stored.area, peilgebieden.area), "the stored polygons differ from those GDAL reads"
peilgebieden = peilgebieden.set_geometry(stored)
assert peilgebieden["CODE"].is_unique, "peilgebieden with the same code"
assert peilgebieden["CODE"].isin(REMOVED).sum() == len(REMOVED), "removed peilgebieden that are not delivered"
peilgebieden = peilgebieden.loc[~peilgebieden["CODE"].isin(REMOVED)].reset_index(drop=True)

# the polder of a peilgebied is the polder its representative point lies in; 5170-8 lies in none, so in no aggregation
# area
points = gpd.GeoDataFrame(geometry=peilgebieden.representative_point(), crs=peilgebieden.crs)
polder = gpd.sjoin(points, polders[["CODE", "NAAM", "geometry"]], how="left", predicate="within")
assert polder.index.equals(peilgebieden.index), "peilgebieden in several polders"
assert peilgebieden["CODE"].isin(BOEZEMS).sum() == len(BOEZEMS), "boezems that are not delivered"
peilgebied = gpd.GeoDataFrame(
    {
        "code": "peilgebied_" + peilgebieden["CODE"],
        "nen3610id": polder["NAAM"] + "_" + peilgebieden.index.astype(str),
        "globalid": "globalid_" + peilgebieden["OBJECTID"].astype(str),
        "meta_name": polder["CODE"],
        "HWS_BZM": peilgebieden["CODE"].isin(BOEZEMS),
    },
    geometry=peilgebieden.geometry,
    crs=peilgebieden.crs,
)

# the streefpeil is the first of these levels that applies, is not 0. The peilgebieden with a seasonal dynamic
# peilbeheer have their levels in the columns after these, so they got none.
levels = peilgebieden[["VAST", "WINTER", "ZOMER", "STREEFPEIL", "ONDERGRENS"]]
streefpeil = gpd.GeoDataFrame(
    {"waterhoogte": levels.where(levels != 0).bfill(axis=1).iloc[:, 0], "globalid": peilgebied["globalid"]},
    geometry=gpd.GeoSeries([None] * len(peilgebied), index=peilgebied.index),
    crs=peilgebied.crs,
)

# the peilgebieden of each polder together are its aggregation area, in the order of their codes
aggregation_area = peilgebied.dissolve(by="meta_name", aggfunc="first").reset_index()
aggregation_area["polder_id"] = aggregation_area["meta_name"]
aggregation_area = aggregation_area[["polder_id", "code", "nen3610id", "globalid", "meta_name", "geometry"]]

# %% postprocessing
# the peilgebieden clipped by the boundary of the water authority, with the overlay of geopandas < 1. The boundary of
# 2024, `Waterschapsgrenzen.geojson`, is not available; this one differs by up to 3 cm from it north of Amsterdam.
boundaries = gpd.read_file(cloud.joinpath("Basisgegevens/RWS_waterschaps_grenzen/waterschap.gpkg"))
name = "Hoogheemraadschap Hollands Noorderkwartier"
boundary = boundaries.loc[boundaries["naam"].str.strip() == name, ["geometry"]]
assert len(boundary) == 1, f"no single boundary named {name}"
boundary = boundary.set_geometry(boundary.geometry.force_2d().buffer(0))
peilgebied = overlay_intersection(peilgebied.set_geometry(peilgebied.buffer(0)), boundary)
assert peilgebied["globalid"].tolist() == streefpeil["globalid"].tolist(), "peilgebieden outside the boundary"

# the boezems, of category 1
peilgebied["peilgebied_cat"] = peilgebied["HWS_BZM"].astype(int)
peilgebied = peilgebied[["globalid", "code", "nen3610id", "peilgebied_cat", "geometry"]]

# the main water system only adds missing streefpeilen, as in 2024
hws = main_water_system(
    boundaries, gpd.read_file(cloud.joinpath("Rijkswaterstaat/verwerkt/krw_basins_vlakken.gpkg")), name
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
