# import packages and functions
import geopandas as gpd
import numpy as np
import pandas as pd

from peilbeheerst_model.general_functions import *

# # Hollands Noorderkwartier


# define relative paths
gpkg_path_HHNK = "../../Data_preprocessed/Waterschappen/HHNK/Noorderkwartier.gpkg"
gdb_path_HHNK = "../../Data_preprocessed/Waterschappen/HHNK/Watersysteemanalyse_BWN2.gdb"
gdb_path_HHNK_nalevering = "../../Data_preprocessed/Waterschappen/HHNK/Na_levering_HHNK_gemalen_stuwen_20240321.gdb"
output_gpkg_path_HHNK = "../../Data_postprocessed/Waterschappen/HHNK/Noorderkwartier"
interim_results = "../../Interim_results/Waterschappen/HHNK/Noorderkwartier_IR"


# retrieve the data
HHNK = read_gpkg_layers(
    gpkg_path=gpkg_path_HHNK,
    variables=[
        # 'stuw', #nalevering
        # 'gemaal', #nalevering
        "afsluitmiddel",
        "hydroobject",
        "duikersifonhevel",
    ],
)
# 'peilafwijkinggebied',
# 'peilgebiedpraktijk',
# 'pomp'])
# 'streefpeil'])

# retrieve data from a gdb, as the gpkg of HHNK does not contain all relevant data
data_gdb = gpd.read_file(gdb_path_HHNK, layer="BWN_ruimtekaart")
HHNK_nalevering = read_gpkg_layers(gpkg_path=gdb_path_HHNK_nalevering, variables=["stuw", "gemaal"])  # nalevering

HHNK["stuw"] = HHNK_nalevering["stuw"]
HHNK["gemaal"] = HHNK_nalevering["gemaal"]


# determine aanvoer en afvoer gemalen
HHNK["gemaal"]["func_aanvoer"], HHNK["gemaal"]["func_afvoer"], HHNK["gemaal"]["func_circulatie"] = (
    False,
    False,
    False,
)  # default is False
HHNK["gemaal"]["functiegemaal"] = HHNK["gemaal"]["FUNCTIEGEMAAL"].astype(str)
HHNK["gemaal"].loc[HHNK["gemaal"]["functiegemaal"] == "onbekend", "functiegemaal"] = (
    np.nan
)  # replace onbekend with nan, will be filled up later see one line below
HHNK["gemaal"].loc[HHNK["gemaal"]["functiegemaal"] == "99", "functiegemaal"] = (
    np.nan
)  # replace onbekend with nan, will be filled up later see one line below
HHNK["gemaal"]["functiegemaal"].fillna(
    HHNK["gemaal"]["OPMERKING"], inplace=True
)  # some additional is given in this column
HHNK["gemaal"] = HHNK["gemaal"].loc[
    HHNK["gemaal"]["functiegemaal"] != "niet meer in gebruik"
]  # filter the gemalen out which are not in use
HHNK["gemaal"] = HHNK["gemaal"].loc[
    HHNK["gemaal"]["functiegemaal"] != "901"
]  # filter the gemalen out which are not in use

# HHNK['gemaal'].loc[HHNK['gemaal'].functiegemaal.str.contains('onderbemaling|afvoer|af-'), 'func_afvoer'] = True
# HHNK['gemaal'].loc[HHNK['gemaal'].functiegemaal.str.contains('trekker|opmaling|op-|wateraanvoer|aanvoer'), 'func_aanvoer'] = True #aannamen: trekkerpompen vooral voor wateraanvoer
# HHNK['gemaal'].loc[HHNK['gemaal'].functiegemaal.str.contains('doorspoelpomp'), 'func_circulatie'] = True

afvoer_values = ["2", "4", "5", "6", "903"]
aanvoer_values = ["1", "3", "5", "902", "903"]  # aannamen: trekkerpompen vooral voor wateraanvoer
circulatie_values = ["904"]


HHNK["gemaal"].loc[HHNK["gemaal"]["functiegemaal"].isin(afvoer_values), "func_afvoer"] = True
HHNK["gemaal"].loc[HHNK["gemaal"]["functiegemaal"].isin(aanvoer_values), "func_aanvoer"] = True
HHNK["gemaal"].loc[HHNK["gemaal"]["functiegemaal"].isin(circulatie_values), "func_circulatie"] = True

HHNK["gemaal"].loc[
    (HHNK["gemaal"].func_afvoer is False)
    & (HHNK["gemaal"].func_aanvoer is False)
    & (HHNK["gemaal"].func_circulatie is False),
    "func_afvoer",
] = True  # set to afvoergemaal is there the function is unknown


# gemaal
HHNK["gemaal"].rename(columns={"CODE": "code", "GLOBALID": "globalid"}, inplace=True)
HHNK["gemaal"]["nen3610id"] = "dummy_nen3610id_" + HHNK["gemaal"].index.astype(
    str
)  # create a string as the globalid is usually a str as well

# stuw
HHNK["stuw"].rename(columns={"CODE": "code", "GLOBALID": "globalid"}, inplace=True)
HHNK["stuw"]["nen3610id"] = "dummy_nen3610id_" + HHNK["stuw"].index.astype(
    str
)  # create a string as the globalid is usually a str as well


# ### GPKG


# discard irrelevant dataHHNK
HHNK["stuw"] = HHNK["stuw"][["code", "globalid", "nen3610id", "geometry"]]
HHNK["gemaal"] = HHNK["gemaal"][
    ["code", "globalid", "nen3610id", "func_afvoer", "func_aanvoer", "func_circulatie", "geometry"]
]
HHNK["hydroobject"] = HHNK["hydroobject"][["code", "globalid", "nen3610id", "geometry"]]
HHNK["afsluitmiddel"] = HHNK["afsluitmiddel"][["code", "globalid", "nen3610id", "geometry"]]
HHNK["duikersifonhevel"] = HHNK["duikersifonhevel"][["code", "globalid", "nen3610id", "geometry"]]


# ### .GDB


data_gdb = data_gdb[["streefpeil", "geometry"]]
data_gdb["globalid"] = "dummy_globalid_" + data_gdb.index.astype(
    str
)  # create a string as the globalid is usually a str as well
streefpeil = data_gdb[["streefpeil", "globalid"]]
peilgebied = data_gdb[["globalid", "geometry"]]

# add the data to the dictionary
HHNK["streefpeil"] = streefpeil
HHNK["peilgebied"] = peilgebied

# hand made changes
HHNK["peilgebied"] = HHNK["peilgebied"][
    HHNK["peilgebied"]["globalid"] != 1725
]  # not a correct shape. Basically only lines, with 36 seperate segments


HHNK["streefpeil"] = HHNK["streefpeil"].rename(columns={"streefpeil": "waterhoogte"})
HHNK["streefpeil"]["geometry"] = None
HHNK["streefpeil"] = gpd.GeoDataFrame(HHNK["streefpeil"], geometry="geometry")


# HHNK['streefpeil']['code'] = 'dummy_code_streefpeil_' + HHNK['streefpeil'].index.astype(str)
# HHNK['streefpeil']['nen3610id'] = 'dummy_nen3610id_streefpeil_' + HHNK['streefpeil'].index.astype(str)

HHNK["peilgebied"]["code"] = "dummy_code_" + HHNK["peilgebied"].index.astype(str)
HHNK["peilgebied"]["nen3610id"] = "dummy_nen3610id_" + HHNK["peilgebied"].index.astype(str)
HHNK["peilgebied"]["HWS_BZM"] = False


# ### Check for the correct keys and columns


show_layers_and_columns(waterschap=HHNK)


# # Add the boezem and hoofdwatersysteem

# Some changes by hand have been made. The resulting shapefile contains the bordering BZM and HWS shapes, including streefpeil


path_HWS_BZM = "..\..\Scripts\Aggregeren\Hoofdwatersysteem\BZM_HWS_HHNK.shp"
HWS_BZM = gpd.read_file(path_HWS_BZM)


HWS_BZM["code"] = "dummy_code_" + (HWS_BZM.index + max(HHNK["peilgebied"].index) + 1).astype(str)
HWS_BZM["globalid"] = "dummy_globalid_" + (HWS_BZM.index + max(HHNK["peilgebied"].index) + 1).astype(str)
HWS_BZM["nen3610id"] = "dummy_nen3610id_" + (HWS_BZM.index + max(HHNK["peilgebied"].index) + 1).astype(str)
HWS_BZM["waterhoogte"] = HWS_BZM["zomerpeil"]
HWS_BZM["HWS_BZM"] = True
HWS_BZM = HWS_BZM[["code", "globalid", "nen3610id", "waterhoogte", "HWS_BZM", "geometry"]]

HWS_BZM_peilgebied = HWS_BZM[["code", "globalid", "nen3610id", "HWS_BZM", "geometry"]]
HWS_BZM_streefpeil = HWS_BZM[["waterhoogte", "globalid", "geometry"]]

HHNK["peilgebied"] = gpd.GeoDataFrame(pd.concat([HHNK["peilgebied"], HWS_BZM_peilgebied])).reset_index(drop=True)
HHNK["streefpeil"] = gpd.GeoDataFrame(pd.concat([HHNK["streefpeil"], HWS_BZM_streefpeil])).reset_index(drop=True)


# ### Store data


store_data(waterschap=HHNK, output_gpkg_path=output_gpkg_path_HHNK)

# Toevoegen aan notities:

# Duikersifonhevel and hydroobject have a type of multicurvedZ, the peilgebieden a MultiSurfaceZ, which geopandas can not handle. I have manually exported these to single shapes, which automatically converts it to regular MultiStrings. Then these layers have been packed together to a geopackage again.

# Peilmerk is geometrisch gekoppeld aan peilgebieden, niet administratief. Daarnaast zijn bij een aantal beschikbaar of deze gekoppeld zijn met een gemaal, stuw, duikersifonhevel (wel administratief). Wel is er een streefpeil tabel beschikbaar, die wel administratief gekoppeld is. Ga kijken wat het verschil is.

# In de streefpeilen kaart zijn er verschillende soorten peilen:
# - winter
# - zomer
# - vast
# - dynamische bovengrens
# - dynamische ondergrens
