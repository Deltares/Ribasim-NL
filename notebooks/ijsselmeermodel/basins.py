# %%
import os
from pathlib import Path

import geopandas as gpd
import pandas as pd

from hydamo import code_utils
from ribasim_nl import settings
from ribasim_nl.utils.geometry import cut_basin, drop_z
from ribasim_nl.utils.geoseries import basins_to_points

DATA_DIR = settings.ribasim_nl_data_dir
MODEL_DIR = Path(os.getenv("RIBASIM_NL_MODEL_DIR")) / "ijsselmeer"

DEFAULT_AREA = [0.01, 1000.0]
DEFAULT_LEVEL = [0.0, 1.0]
DEFAULT_EVAPORATION = 0
DEFAULT_PRECIPITATION = 0.002 / 86400
basins = []


def add_basin(**kwargs):
    global basins
    kwargs["geometry"] = drop_z(kwargs["geometry"])
    basins += [kwargs]


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

# rws_krw_gdf.loc[krw_ids].explore()

krw_cutlines_gdf = gpd.read_file(MODEL_DIR / "model_data.gpkg", layer="krw_cutlines")


def strip_code(code):
    return code.split("_", 1)[-1]


def user_id(code, wbh_code, code_postfix=None):
    code = strip_code(code)
    if code_postfix:
        code = f"{code}_{code_postfix}"
    return code_utils.generate_model_id(code, "basin", wbh_code=wbh_code)


for row in rws_krw_gdf.loc[krw_ids].itertuples():
    # row = next(rws_krw_gdf.loc[krw_ids].itertuples())
    code = row.Index
    basin_polygon = row.geometry
    if code in krw_cutlines_gdf.owmident.to_numpy():
        if code in krw_cutlines_gdf.owmident.to_numpy():
            if basin_polygon.geom_type == "Polygon":
                for cut_line in (
                    krw_cutlines_gdf[krw_cutlines_gdf.owmident == row.Index].sort_values("cut_order").itertuples()
                ):
                    # cut_line = krw_cutlines_gdf[krw_cutlines_gdf.owmident == row.Index].sort_values("cut_order").geometry[0]
                    basin_multi_polygon = cut_basin(basin_polygon, cut_line.geometry)
                    geometry = basin_multi_polygon.geoms[0]
                    add_basin(
                        user_id=user_id(code, "80", cut_line.cut_order),
                        geometry=geometry,
                        rijkswater=strip_code(code),
                    )
                    basin_polygon = basin_multi_polygon.geoms[1]
                add_basin(
                    user_id=user_id(code, "80", cut_line.cut_order + 1),
                    geometry=basin_polygon,
                    rijkswater=strip_code(code),
                )
            else:
                raise TypeError(f"basin_polygon not of correct type {basin_polygon.geom_type}")
    else:
        add_basin(
            user_id=user_id(code, "80"),
            geometry=basin_polygon,
            rijkswater=strip_code(code),
        )

gdf = gpd.read_file(DATA_DIR / r"Zuiderzeeland/Oplevering LHM/peilgebieden.gpkg")

# Define your selection criteria
mask = (
    (gdf["GPGIDENT"] == "LVA.01")
    | (gdf["GPGIDENT"] == "3.01")
    | (gdf["GPGIDENT"] == "LAGE AFDELING")
    | (gdf["GPGIDENT"] == "HOGE AFDELING")
)

# Process the selected polygons and add centroids and attributes
for index, row in gdf[mask].iterrows():
    add_basin(
        user_id=user_id(row.GPGIDENT, "37"),
        peilvak=user_id(row.GPGIDENT, "37"),
        geometry=row.geometry,
    )

# Also, save the selected polygons to the new GeoPackage
# "sel_peilgebieden_gdf.to_file(output_geopackage, driver="GPKG")


basins_gdf = gpd.GeoDataFrame(basins, crs=28992)
basins_gdf.to_file(MODEL_DIR / "model_data.gpkg", layer="basin_area")
basins_gdf.loc[:, "geometry"] = basins_to_points(basins_gdf["geometry"])
basins_gdf.to_file(MODEL_DIR / "model_data.gpkg", layer="basin")

## %% generate profiles
data = []
for row in basins_gdf.itertuples():
    data += list(
        zip(
            [row.node_id] * len(DEFAULT_AREA),
            [row.user_id] * len(DEFAULT_AREA),
            DEFAULT_AREA,
            DEFAULT_LEVEL,
        )
    )
    profile_df = pd.DataFrame(data, columns=["node_id", "user_id", "area", "level"])

## %% generate static
static_df = basins_gdf[["node_id", "user_id"]].copy()
static_df["drainage"] = 0
static_df["potential_evaporation"] = DEFAULT_EVAPORATION
static_df["infiltration"] = 0
static_df["precipitation"] = DEFAULT_PRECIPITATION
static_df["urban_runoff"] = 0

## %%
profile_df["remarks"] = profile_df["user_id"]
static_df["remarks"] = static_df["user_id"]
