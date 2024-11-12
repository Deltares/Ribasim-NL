import os
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point

# # Voorbereiding
#
# Globale variabelen
# `DATA_DIR`: De locale directory waar project-data staat opgeslagen
# `EXCEL_FILE`: Het Excel-bestand dat moet worden ingelezen
# `CRS`: De projectile waarin de ruimtelijke data moet worden opgeslagen (28992 = Rijksdriehoekstelsel)


# environmnt variables
DATA_DIR = os.getenv("RIBASIM_NL_DATA_DIR")
RIBASIM_NL_CLOUD_PASS = os.getenv("RIBASIM_NL_CLOUD_PASS")
assert DATA_DIR is not None
assert RIBASIM_NL_CLOUD_PASS is not None

EXCEL_FILE = r"# Overzicht kunstwerken primaire keringen waterschappen_ET.xlsx"
CRS = 28992
RIBASIM_NL_CLOUD_USER = "nhi_api"
WEBDAV_URL = "https://deltares.thegood.cloud/remote.php/dav"
BASE_URL = f"{WEBDAV_URL}/files/{RIBASIM_NL_CLOUD_USER}/D-HYDRO modeldata"

# file-paths
kunstwerken_xlsx = Path(DATA_DIR) / EXCEL_FILE
kunstwerken_gpkg = kunstwerken_xlsx.parent / "nl_kunstwerken.gpkg"


def upload_file(url, path):
    with open(path, "rb") as f:
        r = requests.put(url, data=f, auth=(RIBASIM_NL_CLOUD_USER, RIBASIM_NL_CLOUD_PASS))
    r.raise_for_status()


# ## Inlezen NL kunstwerken vanuit data dir
# We lezen de geleverde excel in en:
# - skippen de eerste 6 regels boven de header
# - gooien, voor dit project, irrelevante kolommen weg
# - hernoemen de kolom met organisatie-naam, x en y coordinaat
# - transformeren de x en y coordinaat; wordt NaN wanneer data dat niet toelaat (text of missend)


# skip first rows
kunstwerken_df = pd.read_excel(kunstwerken_xlsx, skiprows=6)

# drop irrelevant columns
columns = kunstwerken_df.columns[1:13]
kunstwerken_df = kunstwerken_df.loc[:, columns]

# rename columns into our liking
kunstwerken_df.rename(
    columns={
        "Unnamed: 1": "organisatie",
        "Y coördinaat RD": "y",
        "X coördinaat RD": "x",
    },
    inplace=True,
)

# drop no-data rows
kunstwerken_df = kunstwerken_df[~kunstwerken_df["organisatie"].isna()]

# convert x/y to numeric
kunstwerken_df["x"] = pd.to_numeric(kunstwerken_df["x"], errors="coerce")
kunstwerken_df["y"] = pd.to_numeric(kunstwerken_df["y"], errors="coerce")


# ## Aanmaken niet-ruimtelijke table
# Waar x/y coordinaten mizzen maken we een niet-ruimtelijke table die we wegschrijven in geen GeoPackage


# make a non-spatial GeoDataFrame where x/y are missing
kunstwerken_non_spatial_df = kunstwerken_df[kunstwerken_df["x"].isna() | kunstwerken_df["y"].isna()]
kunstwerken_non_spatial_gdf = gpd.GeoDataFrame(kunstwerken_non_spatial_df, geometry=gpd.GeoSeries(), crs=28992)

# writ to GeoPackage
kunstwerken_non_spatial_gdf.to_file(kunstwerken_gpkg, layer="kunstwerken (geen coordinaten)")


# ## Aanmaken ruimtelijke table
# Waar x/y coordinaten beschikbaar zijn maken we een ruimtelijke table die we wegschrijven in geen GeoPackage


# make a spatial GeoDataFrame where x/y exist
kunstwerken_spatial_df = kunstwerken_df[~kunstwerken_df["x"].isna() & ~kunstwerken_df["y"].isna()]
geometry_series = gpd.GeoSeries(kunstwerken_spatial_df.apply((lambda x: Point(x.x, x.y)), axis=1))
kunstwerken_spatial_gdf = gpd.GeoDataFrame(kunstwerken_spatial_df, geometry=geometry_series, crs=CRS)

# write to GeoPackage
kunstwerken_spatial_gdf.to_file(kunstwerken_gpkg, layer="kunstwerken")


# ## Upload geopackage


to_url = f"{BASE_URL}/Rijkswaterstaat/{kunstwerken_gpkg.name}"
upload_file(to_url, kunstwerken_gpkg)
