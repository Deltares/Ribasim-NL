import warnings
from pathlib import Path

import fiona
import geopandas as gpd
import pandas as pd
import requests
from shapely.geometry import Point

from hydamo import HyDAMO, code_utils
from ribasim_nl import settings

warnings.simplefilter("ignore", UserWarning)


# # Voorbereiding
#
# Globale variabelen
# - `DATA_DIR`: De locale directory waar project-data staat opgeslagen
# - `EXCEL_FILE`: Het Excel-bestand dat moet worden ingelezen
# - `CRS`: De projectile waarin de ruimtelijke data moet worden opgeslagen (28992 = Rijksdriehoekstelsel)


# environment variables
DATA_DIR = settings.ribasim_nl_data_dir
RIBASIM_NL_CLOUD_PASS = settings.ribasim_nl_cloud_pass

DATA_DIR = Path(DATA_DIR)
EXCEL_FILE = r"uitlaten_inlaten.xlsx"
CRS = 28992
RIBASIM_NL_CLOUD_USER = "nhi_api"
WEBDAV_URL = "https://deltares.thegood.cloud/remote.php/dav"
BASE_URL = f"{WEBDAV_URL}/files/{RIBASIM_NL_CLOUD_USER}/D-HYDRO modeldata"

# file-paths
kunstwerken_xlsx = Path(DATA_DIR) / EXCEL_FILE
kunstwerken_gpkg = kunstwerken_xlsx.parent / f"{kunstwerken_xlsx.stem}.gpkg"


def upload_file(url, path):
    with open(path, "rb") as f:
        r = requests.put(url, data=f, auth=(RIBASIM_NL_CLOUD_USER, RIBASIM_NL_CLOUD_PASS))
    r.raise_for_status()


# ## Inlezen NL kunstwerken vanuit data dir
# - Inlezen Excel
# - Nu masken we (nog) op kunstwerken die we uit files kunnen trekken


kunstwerken_df = pd.read_excel(kunstwerken_xlsx)
kunstwerken_df["user_id"] = None
files_mask = ~kunstwerken_df["damo_bestand"].isna()
data = {}


# ## Toevoegen kunstwerken op XY-locatie


for kwk_row in kunstwerken_df[~files_mask].itertuples():
    layer = kwk_row.hydamo_object
    if layer not in data.keys():
        data[layer] = []
    name = kwk_row.dm_naam
    geometry = Point(kwk_row.x, kwk_row.y)
    code = code_utils.code_from_geometry(geometry)
    result = {"code": code, "naam": name, "geometry": geometry}

    kunstwerken_df.loc[kwk_row.Index, ["user_id"]] = code_utils.generate_model_id(
        result["code"], layer, bgt_code=kwk_row.bgt_code
    )

    data[layer] += [result]


# ## Ophalen kunstwerken
# - Aanmaken lege data-dict
# - loopen over kunstwerken/lagen en daar de relevante kunstwerken uit halen


# create data-dict for every layer
for layer in kunstwerken_df["hydamo_object"].unique():
    if layer not in data.keys():
        data[layer] = []

# group by file and check if exists
for file, file_df in kunstwerken_df[files_mask].groupby("damo_bestand"):
    file = DATA_DIR.joinpath(file)
    assert file.exists()

    # open per layer, and check if specified layer xists
    for layer, layer_df in file_df.groupby("damo_laag"):
        file_layers = fiona.listlayers(file)
        if (
            len(file_layers) == 1
        ):  # in case single-layer files, users don't understand a `layer-property` and make mistakes
            layer = file_layers[0]
        if layer not in fiona.listlayers(file):
            raise ValueError(f"layer '{layer}' not a layer in '{file}'. Specify one of {fiona.listlayers(file)}")
        print(f"reading {file.name}, layer {layer}")
        gdf = gpd.read_file(file, layer=layer)
        gdf.columns = [i.lower() for i in gdf.columns]
        gdf.to_crs(CRS, inplace=True)

        # read every row this file-layer group and get the source-info
        for kwk_row in layer_df.itertuples():
            # get the index from the used code or name column
            damo_index = kwk_row.damo_ident_kolom
            src_index = getattr(kwk_row, f"damo_{damo_index}_kolom").strip().lower()
            index_value = str(kwk_row.damo_waarde)

            # read the source
            if src_index in gdf.columns:
                if index_value in gdf[src_index].to_numpy():
                    src_row = gdf.set_index(src_index).loc[index_value]
                else:
                    raise KeyError(f"'{index_value}' not a value in '{file}', layer '{layer}', column '{src_index}'")
            else:
                raise KeyError(
                    f"'{src_index}' not a column in '{file}', layer '{layer}' (searching value '{index_value}')"
                )

            # populate the result
            result = {}

            # populate code and naam fields
            for damo_att in ["code", "naam"]:
                if damo_att == damo_index:
                    result[damo_att] = index_value
                else:
                    column = getattr(kwk_row, f"damo_{damo_att}_kolom")
                    if pd.isna(column) and (damo_att == "code"):
                        result[damo_att] = code_utils.code_from_geometry(src_row.geometry)
                    else:
                        column = getattr(kwk_row, f"damo_{damo_att}_kolom").strip().lower()
                        result[damo_att] = str(getattr(src_row, column))

            # get the geometry. We get the centroid to avoid flatten all kinds of mult-features
            result["geometry"] = src_row.geometry.centroid

            # add it to our data dictionary
            data[kwk_row.hydamo_object] += [result]

            # update Escel with user-id
            kunstwerken_df.loc[kwk_row.Index, ["user_id"]] = code_utils.generate_model_id(
                result["code"], kwk_row.hydamo_object, bgt_code=kwk_row.bgt_code
            )
            kunstwerken_df.loc[kwk_row.Index, ["x"]] = result["geometry"].x
            kunstwerken_df.loc[kwk_row.Index, ["y"]] = result["geometry"].y


# ## Wegschrijven HyDAMO
# - lokaal
# - op TheGoodCloud


hydamo = HyDAMO("2.2.1")
for layer in data.keys():
    if layer != "duikersifonhevel":
        gdf = gpd.GeoDataFrame(data[layer], crs=CRS)
        getattr(hydamo, layer).set_data(gdf, check_columns=False)

hydamo.to_geopackage(kunstwerken_gpkg)

kunstwerken_df.to_excel(kunstwerken_xlsx, index=False, sheet_name="kunstwerken")

for file in [kunstwerken_xlsx, kunstwerken_gpkg]:
    to_url = f"{BASE_URL}/HyDAMO_geconstrueerd/{file.name}"
    upload_file(to_url, file)
