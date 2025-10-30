# import packages and functions
import os

import fiona
import geopandas as gpd
import numpy as np
import pandas as pd
from peilbeheerst_model.general_functions import *

pd.set_option("display.max_columns", None)


# define relative paths
waterschap = "Zuiderzeeland"
path_zzl = "..\..\Data_preprocessed\Waterschappen\Zuiderzeeland"
output_gpkg_path = "../../Data_postprocessed/Waterschappen/Zuiderzeeland"


# # Zuiderzeeland


Zuiderzeeland = {}

Zuiderzeeland["gemaal"] = gpd.read_file(path_zzl + "\gemalen.gpkg")
Zuiderzeeland["hevels"] = gpd.read_file(path_zzl + "\overigekunstwerken.gpkg")
# Zuiderzeeland['peilgebied'] = gpd.read_file(path_zzl + '\peilgebieden.gpkg')
Zuiderzeeland["peilgebied"] = gpd.read_file(path_zzl + "\peilvakken_nalevering.gpkg")


# use fiona for the duikersifonhevels and watergangen due to unexpted geometry types
with fiona.open(path_zzl + "/Duikers.gpkg", "r") as file:
    # Read the contents and store them in the GeoDataFrame
    Zuiderzeeland["duikersifonhevel"] = gpd.GeoDataFrame.from_features(file, crs="EPSG:28992")

with fiona.open(path_zzl + "/zzl_watergangen_nalevering/zzl_Watergangen.shp", "r") as file:
    # Read the contents and store them in the GeoDataFrame
    Zuiderzeeland["hydroobject"] = gpd.GeoDataFrame.from_features(file)


Zuiderzeeland["hydroobject"] = Zuiderzeeland["hydroobject"].set_crs(crs="WGS84", allow_override=True)
Zuiderzeeland["hydroobject"] = Zuiderzeeland["hydroobject"].to_crs(crs="EPSG:28992")

ZZL: stuwen in KWKSOORT in overigekunstwerken.gpkg


KWKSOORT_stuw = ["Constructie", "inlaat", "uitlaat", "keerwand"]  # gebasseerd op de geleverde data van Zuiderzeeland

Zuiderzeeland["stuw"] = (
    Zuiderzeeland["hevels"].loc[Zuiderzeeland["hevels"]["KWKSOORT"].isin(KWKSOORT_stuw)].reset_index(drop=True)
)
Zuiderzeeland["stuw"].geometry = Zuiderzeeland["stuw"].centroid  # prevent pointZ geometries


# distinguish multiple parameters from the same gpkg
Zuiderzeeland["afsluitmiddel"] = (
    Zuiderzeeland["hevels"].loc[Zuiderzeeland["hevels"]["KWKSOORT"] == "Afsluitmiddel (groot)"].reset_index(drop=True)
)
Zuiderzeeland["hevels"] = (
    Zuiderzeeland["hevels"].loc[Zuiderzeeland["hevels"]["KWKSOORT"] == "Hevel"].reset_index(drop=True)
)


# determine aanvoer en afvoer gemalen
(
    Zuiderzeeland["gemaal"]["func_aanvoer"],
    Zuiderzeeland["gemaal"]["func_afvoer"],
    Zuiderzeeland["gemaal"]["func_circulatie"],
) = False, False, False  # default is False
Zuiderzeeland["gemaal"]["functiegemaal"] = Zuiderzeeland["gemaal"]["KGMFUNC"].astype(str)
Zuiderzeeland["gemaal"].loc[Zuiderzeeland["gemaal"]["functiegemaal"] == "onbekend", "functiegemaal"] = (
    np.nan
)  # replace onbekend with nan, will be filled up later see one line below
Zuiderzeeland["gemaal"]["functiegemaal"].fillna(
    Zuiderzeeland["gemaal"]["KGMSOORT"], inplace=True
)  # some additional is given in this column

Zuiderzeeland["gemaal"].loc[
    Zuiderzeeland["gemaal"].functiegemaal.str.contains("af-|afvoer|onderbemaling"), "func_afvoer"
] = True
Zuiderzeeland["gemaal"].loc[
    Zuiderzeeland["gemaal"].functiegemaal.str.contains("aanvoergemaal|opmaling"), "func_aanvoer"
] = True
Zuiderzeeland["gemaal"].loc[Zuiderzeeland["gemaal"].functiegemaal.str.contains("circulatie"), "func_circulatie"] = True
Zuiderzeeland["gemaal"].loc[
    (Zuiderzeeland["gemaal"].func_afvoer is False)
    & (Zuiderzeeland["gemaal"].func_aanvoer is False)
    & (Zuiderzeeland["gemaal"].func_circulatie is False),
    "func_afvoer",
] = True  # set to afvoergemaal is there the function is unknown


# Gemaal
Zuiderzeeland["gemaal"] = Zuiderzeeland["gemaal"][
    ["KGMIDENT", "GLOBALID", "func_aanvoer", "func_afvoer", "func_circulatie", "geometry"]
]
Zuiderzeeland["gemaal"] = Zuiderzeeland["gemaal"].rename(columns={"KGMIDENT": "code", "GLOBALID": "globalid"})
Zuiderzeeland["gemaal"]["nen3610id"] = "dummy_nen3610id_gemaal_" + Zuiderzeeland["gemaal"].index.astype(str)

# Hydroobject
Zuiderzeeland["hydroobject"] = Zuiderzeeland["hydroobject"][["OWAIDENT", "GLOBALID", "geometry"]]
Zuiderzeeland["hydroobject"] = Zuiderzeeland["hydroobject"].rename(columns={"OWAIDENT": "code", "GLOBALID": "globalid"})
Zuiderzeeland["hydroobject"]["nen3610id"] = "dummy_nen3610id_hydroobject_" + Zuiderzeeland["hydroobject"].index.astype(
    str
)

# duikersifonhevel
Zuiderzeeland["duikersifonhevel"] = Zuiderzeeland["duikersifonhevel"][["KDUIDENT", "GLOBALID", "geometry"]]
Zuiderzeeland["duikersifonhevel"] = Zuiderzeeland["duikersifonhevel"].rename(
    columns={"KDUIDENT": "code", "GLOBALID": "globalid"}
)
Zuiderzeeland["duikersifonhevel"]["nen3610id"] = "dummy_nen3610id_duikersifonhevel_" + Zuiderzeeland[
    "duikersifonhevel"
].index.astype(str)

# hevels
Zuiderzeeland["hevels"] = Zuiderzeeland["hevels"][["KWKIDENT", "GLOBALID", "geometry"]]
Zuiderzeeland["hevels"] = Zuiderzeeland["hevels"].rename(columns={"KWKIDENT": "code", "GLOBALID": "globalid"})
Zuiderzeeland["hevels"]["nen3610id"] = "dummy_nen3610id_hevels_" + Zuiderzeeland["hevels"].index.astype(str)
# add to the duikersifonhevel
Zuiderzeeland["duikersifonhevel"] = gpd.GeoDataFrame(
    pd.concat((Zuiderzeeland["duikersifonhevel"], Zuiderzeeland["hevels"]))
)

# stuw
Zuiderzeeland["stuw"] = Zuiderzeeland["stuw"][["KWKIDENT", "GLOBALID", "geometry", "KWKSOORT"]]
Zuiderzeeland["stuw"] = Zuiderzeeland["stuw"].rename(
    columns={"KWKIDENT": "code", "GLOBALID": "globalid", "KWKSOORT": "KWKsoort"}
)
Zuiderzeeland["stuw"] = Zuiderzeeland["stuw"].set_crs("EPSG:28992")
Zuiderzeeland["stuw"]["nen3610id"] = "dummy_nen3610id_stuw_" + Zuiderzeeland["stuw"].index.astype(str)

# afsluitmiddel
Zuiderzeeland["afsluitmiddel"] = Zuiderzeeland["afsluitmiddel"][["KWKIDENT", "GLOBALID", "geometry"]]
Zuiderzeeland["afsluitmiddel"] = Zuiderzeeland["afsluitmiddel"].rename(
    columns={"KWKIDENT": "code", "GLOBALID": "globalid"}
)
Zuiderzeeland["afsluitmiddel"]["nen3610id"] = "dummy_nen3610id_hevels_" + Zuiderzeeland["afsluitmiddel"].index.astype(
    str
)

# peilgebied
Zuiderzeeland["peilgebied"] = Zuiderzeeland["peilgebied"][["DHYDRO_ZMRPL", "GPGIDENT", "geometry"]]
Zuiderzeeland["peilgebied"]["nen3610id"] = "dummy_nen3610id_peilgebied_" + Zuiderzeeland["peilgebied"].index.astype(str)
Zuiderzeeland["peilgebied"]["globalid"] = "dummy_globalid_peilgebied_" + Zuiderzeeland["peilgebied"].index.astype(str)
Zuiderzeeland["peilgebied"] = Zuiderzeeland["peilgebied"].rename(
    columns={"DHYDRO_ZMRPL": "streefpeil", "GPGIDENT": "code"}
)
Zuiderzeeland["peilgebied"]["globalid"] = "dummy_globalid_peilgebied_" + Zuiderzeeland["peilgebied"].index.astype(str)

# streefpeil
Zuiderzeeland["streefpeil"] = Zuiderzeeland["peilgebied"][["streefpeil", "globalid"]]
Zuiderzeeland["streefpeil"]["geometry"] = np.nan
Zuiderzeeland["streefpeil"].rename(columns={"streefpeil": "waterhoogte"}, inplace=True)
Zuiderzeeland["streefpeil"] = gpd.GeoDataFrame(Zuiderzeeland["streefpeil"], geometry="geometry")

# delete the streefpeil in the peilgebied for consistency
Zuiderzeeland["peilgebied"] = Zuiderzeeland["peilgebied"][["code", "globalid", "nen3610id", "geometry"]]


# ### Check for the correct keys and columns


show_layers_and_columns(waterschap=Zuiderzeeland)


# ### Store data


# Check if the directory exists. If it doesn't exist, create it

if not os.path.exists(output_gpkg_path):
    os.makedirs(output_gpkg_path)

store_data(waterschap=Zuiderzeeland, output_gpkg_path=output_gpkg_path + "/Zuiderzeeland")
