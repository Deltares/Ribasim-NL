# import packages and functions
import os

import geopandas as gpd
import pandas as pd
from general_functions import *

# # Delfland


# define relative paths
waterschap = "Delfland"
gdb_path = "../../Data_preprocessed/Waterschappen/Delfland/Watersysteem.gdb"
output_gpkg_path = "../../Data_postprocessed/Waterschappen/Delfland"


Delfland = read_gpkg_layers(
    gpkg_path=gdb_path, variables=["stuw", "gemaal", "watergang", "duikersifonhevel", "peilgebiedpraktijk", "keerschot"]
)
# 'peilafwijkinggebied',
# 'pomp'])
# 'streefpeil'])
# change names
Delfland["hydroobject"] = Delfland.pop("watergang")


# ### Adjust column names


# discard irrelevant data of Delfland Delfland, and create a uniform dataset compared to the other waterschappen
# Stuw
Delfland["stuw"] = Delfland["stuw"][["CODE", "GLOBALID", "geometry"]]
Delfland["stuw"] = Delfland["stuw"].rename(columns={"CODE": "code", "GLOBALID": "globalid"})
Delfland["stuw"]["nen3610id"] = "dummy_nen3610id_stuw_" + Delfland["stuw"].index.astype(str)

# Gemaal

# determine aanvoer en afvoer gemalen
Delfland["gemaal"]["func_aanvoer"], Delfland["gemaal"]["func_afvoer"], Delfland["gemaal"]["func_circulatie"] = (
    False,
    False,
    False,
)  # default is False
Delfland["gemaal"].FUNCTIEGEMAAL_resolved.fillna(
    Delfland["gemaal"].WS_SOORTGEMAAL
)  # sometimes recircualtie is located in another column, but sometimes they are different. Only fill in for NaN
Delfland["gemaal"]["FUNCTIEGEMAAL_resolved"] = Delfland["gemaal"]["FUNCTIEGEMAAL_resolved"].astype(str)

Delfland["gemaal"].loc[
    Delfland["gemaal"].FUNCTIEGEMAAL_resolved.str.contains("Onbekend|Onderbemaling|Afvoergemaal|Af-"), "func_afvoer"
] = True
Delfland["gemaal"].loc[Delfland["gemaal"].FUNCTIEGEMAAL_resolved.str.contains("Opmaling|Aanvoer"), "func_aanvoer"] = (
    True
)
Delfland["gemaal"].loc[
    Delfland["gemaal"].FUNCTIEGEMAAL_resolved.str.contains("Overig|circulatie"), "func_circulatie"
] = True
Delfland["gemaal"].loc[
    (Delfland["gemaal"].func_afvoer is False)
    & (Delfland["gemaal"].func_aanvoer is False)
    & (Delfland["gemaal"].func_circulatie is False),
    "func_afvoer",
] = True  # set to afvoergemaal is there the function is unknown

Delfland["gemaal"] = Delfland["gemaal"][["GLOBALID", "func_afvoer", "func_aanvoer", "func_circulatie", "geometry"]]
Delfland["gemaal"] = Delfland["gemaal"].rename(columns={"GLOBALID": "globalid"})
Delfland["gemaal"]["code"] = "dummy_code_gemaal_" + Delfland["gemaal"].index.astype(str)
Delfland["gemaal"]["nen3610id"] = "dummy_nen3610id_gemaal_" + Delfland["gemaal"].index.astype(str)

# Hydroobject
Delfland["hydroobject"] = Delfland["hydroobject"][["GLOBALID", "geometry"]]
Delfland["hydroobject"] = Delfland["hydroobject"].rename(columns={"GLOBALID": "globalid"})
Delfland["hydroobject"]["code"] = "dummy_code_hydroobject_" + Delfland["hydroobject"].index.astype(str)
Delfland["hydroobject"]["nen3610id"] = "dummy_nen3610id_hydroobject_" + Delfland["hydroobject"].index.astype(str)

# Keerschot
Delfland["keerschot"] = Delfland["keerschot"][["GLOBALID", "geometry"]]
Delfland["keerschot"] = Delfland["keerschot"].rename(columns={"GLOBALID": "globalid"})
Delfland["keerschot"]["code"] = "dummy_code_keerschot_" + Delfland["keerschot"].index.astype(str)
Delfland["keerschot"]["nen3610id"] = "dummy_nen3610id_keerschot_" + Delfland["keerschot"].index.astype(str)

# duikersifonhevel
Delfland["duikersifonhevel"] = Delfland["duikersifonhevel"][["CODE", "GLOBALID", "geometry"]]
Delfland["duikersifonhevel"] = Delfland["duikersifonhevel"].rename(columns={"CODE": "code", "GLOBALID": "globalid"})
Delfland["duikersifonhevel"]["code"] = "dummy_code_duikersifonhevel_" + Delfland["duikersifonhevel"].index.astype(str)
Delfland["duikersifonhevel"]["nen3610id"] = "dummy_nen3610id_duikersifonhevel_" + Delfland[
    "duikersifonhevel"
].index.astype(str)

# afsluitmiddel
# niet geleverd

# Peilgebiedpraktijk
Delfland["peilgebiedpraktijk"] = Delfland["peilgebiedpraktijk"][["WS_HOOGPEIL", "CODE", "GLOBALID", "geometry"]]
Delfland["peilgebiedpraktijk"]["nen3610id"] = "dummy_nen3610id_peilgebiedpraktijk_" + Delfland[
    "peilgebiedpraktijk"
].index.astype(str)
Delfland["peilgebiedpraktijk"] = Delfland["peilgebiedpraktijk"].rename(
    columns={"WS_HOOGPEIL": "streefpeil", "CODE": "code", "GLOBALID": "globalid"}
)

# Streefpeil
Delfland["streefpeil"] = pd.DataFrame()
Delfland["streefpeil"]["waterhoogte"] = Delfland["peilgebiedpraktijk"]["streefpeil"]
Delfland["streefpeil"]["globalid"] = Delfland["peilgebiedpraktijk"]["globalid"]
Delfland["streefpeil"]["geometry"] = None
Delfland["streefpeil"] = gpd.GeoDataFrame(Delfland["streefpeil"], geometry="geometry")

Delfland["peilgebied"] = Delfland["peilgebiedpraktijk"]


# ### Add column to determine the HWS_BZM


Delfland["peilgebied"]["HWS_BZM"] = False
Delfland["peilgebied"].loc[Delfland["peilgebied"].code == "BZM 1", "HWS_BZM"] = True  # looked up manually


# delete irrelvant data
variables = ["peilgebiedpraktijk"]

for variable in variables:
    if str(variable) in Delfland:
        del Delfland[variable]


# ### Check for the correct keys and columns


show_layers_and_columns(waterschap=Delfland)


# ### Store data


# Check if the directory exists
if not os.path.exists(output_gpkg_path):
    # If it doesn't exist, create it
    os.makedirs(output_gpkg_path)

store_data(waterschap=Delfland, output_gpkg_path=output_gpkg_path + "/Delfland")
