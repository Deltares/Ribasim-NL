# %%
import copy
import os
from pathlib import Path

import geopandas as gpd
from geometry_utils import cut_basin
from shapely.ops import polylabel

DATA_DIR = Path(os.getenv("RIBASIM_NL_DATA_DIR"))
MODEL_DIR = Path(os.getenv("RIBASIM_NL_MODEL_DIR")) / "ijsselmeer"


def add_basin(**kwargs):
    global basin_areas
    global basins

    basin_areas += [copy.copy(kwargs)]

    polygon = kwargs["geometry"]
    point = polygon.centroid
    if not point.within(polygon):
        point = polylabel(polygon)
    kwargs["geometry"] = point
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

basin_areas = []
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


gpd.GeoDataFrame(basins, crs=28992).to_file(MODEL_DIR / "basins.gpkg", layer="basins")
gpd.GeoDataFrame(basin_areas, crs=28992).to_file(
    MODEL_DIR / "basins.gpkg", layer="basin_areas"
)


# %% Toevoegen Peilgebieden Zuiderzeeland
