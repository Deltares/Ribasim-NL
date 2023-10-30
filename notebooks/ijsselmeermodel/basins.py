# %%
import os
from pathlib import Path

import geopandas as gpd
from geometry_utils import cut_basin

DATA_DIR = Path(os.getenv("RIBASIM_NL_DATA_DIR"))
MODEL_DIR = Path(os.getenv("RIBASIM_NL_MODEL_DIR")) / "ijsselmeer"


def add_basin(**kwargs):
    global basins
    basins += [kwargs]


# %% Toevoegen RWSKRW-lichamen
krw_ids = [
    "NL92_IJSSELMEER",
    "NL92_MARKERMEER",
    "NL92_RANDMEREN_ZUID",
    "NL92_RANDMEREN_OOST",
    "NL92_KETELMEER_VOSSEMEER",
    "NL92_ZWARTEMEER",
]
rws_krw_gpkg = DATA_DIR / r"KRW/krw-oppervlaktewaterlichamen-nederland-vlakken.gpkg"
rws_krw_gdf = gpd.read_file(rws_krw_gpkg).set_index("owmident")

basins = []

# rws_krw_gdf.loc[krw_ids].explore()

krw_cutlines_gdf = gpd.read_file(MODEL_DIR / "model_data.gpkg", layer="krw_cutlines")


for row in rws_krw_gdf.loc[krw_ids].itertuples():
    # row = next(rws_krw_gdf.loc[krw_ids].itertuples())
    krw_id = row.Index
    basin_polygon = row.geometry
    if krw_id in krw_cutlines_gdf.owmident.to_numpy():
        if krw_id in krw_cutlines_gdf.owmident.to_numpy():
            if basin_polygon.geom_type == "Polygon":
                for cut_line in (
                    krw_cutlines_gdf[krw_cutlines_gdf.owmident == row.Index]
                    .sort_values("cut_order")
                    .geometry
                ):
                    # cut_line = krw_cutlines_gdf[krw_cutlines_gdf.owmident == row.Index].sort_values("cut_order").geometry[0]
                    basin_multi_polygon = cut_basin(basin_polygon, cut_line)
                    geometry = basin_multi_polygon.geoms[0]
                    add_basin(krw_id=krw_id, geometry=geometry)
                    basin_polygon = basin_multi_polygon.geoms[1]
                add_basin(krw_id=krw_id, geometry=basin_polygon)
            else:
                raise TypeError(
                    f"basin_polygon not of correct type {basin_polygon.geom_type}"
                )
    else:
        add_basin(krw_id=krw_id, geometry=basin_polygon)


# %% N.G. Toevoegen Peilgebieden Zuiderzeeland

gdf = gpd.read_file(DATA_DIR / r"Zuiderzeeland/Oplevering LHM/peilgebieden.gpkg")

# Define your selection criteria
criteria = (
    (gdf["GPGIDENT"] == "LVA.01")
    | (gdf["GPGIDENT"] == "3.01")
    | (gdf["GPGIDENT"] == "LAGE AFDELING")
    | (gdf["GPGIDENT"] == "HOGE AFDELING")
)

# Select the polygons based on the criteria
sel_peilgebieden_gdf = gdf[criteria]
# Explode the MultiPolygons into individual Polygons
# exploded_gdf = sel_peilgebieden_gdf.explode()


# Process the selected polygons and add centroids and attributes
for index, row in sel_peilgebieden_gdf.iterrows():
    add_basin(**row)

# Also, save the selected polygons to the new GeoPackage
# "sel_peilgebieden_gdf.to_file(output_geopackage, driver="GPKG")

basins_gdf = gpd.GeoDataFrame(basins, crs=28992)
basins_gdf.to_file(MODEL_DIR / "basins.gpkg", layer="basins_areas")

# %%  D.T. Aanmaken Ribasim-basins

# %% Toevoegen kunstwerken Ijsselmeer en Zuiderzeeland
"""
N.G. opzoeken data
D.T. toevoegen Ribasim-objecten
"""

# %% Toevoegen netwerk
"""
N.G. netwerk tekenen in QGIS
D.T. Toevoegen Ribasim-objecten
"""

# %% Toevoegen randvoorwaarden (D.T.)