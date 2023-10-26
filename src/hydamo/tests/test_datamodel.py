import os
from pathlib import Path

import hydamo
import requests
import shapely

# basic auth for https://deltares.thegood.cloud/
RIBASIM_NL_CLOUD_USER = "nhi_api"
RIBASIM_NL_CLOUD_PASS = os.getenv("RIBASIM_NL_CLOUD_PASS")
assert RIBASIM_NL_CLOUD_PASS is not None
WEBDAV_URL = "https://deltares.thegood.cloud/remote.php/dav"
BASE_URL = f"{WEBDAV_URL}/files/{RIBASIM_NL_CLOUD_USER}/D-HYDRO modeldata"


def download_file(url, path):
    r = requests.get(url, auth=(RIBASIM_NL_CLOUD_USER, RIBASIM_NL_CLOUD_PASS))
    r.raise_for_status()
    with open(path, "wb") as f:
        f.write(r.content)


def upload_file(url, path):
    with open(path, "rb") as f:
        r = requests.put(
            url, data=f, auth=(RIBASIM_NL_CLOUD_USER, RIBASIM_NL_CLOUD_PASS)
        )
    r.raise_for_status()


def test_initialize():
    damo = hydamo.HyDAMO()
    assert damo.version == "2.2"
    assert "stuw" in damo.layers
    assert isinstance(damo.stuw, hydamo.ExtendedGeoDataFrame)
    assert "kruinbreedte" in damo.stuw.columns
    # no data is loaded
    assert len(damo.stuw) == 0
    # no file is written
    path = Path("hydamo.gpkg")
    damo.to_geopackage(path)
    assert not path.is_file()


def test_download(tmp_path):
    """Download a HyDAMO GeoPackage, check contents and upload the export"""
    # download file
    filename = "uitlaten_inlaten.gpkg"
    url = f"{BASE_URL}/HyDAMO_geconstrueerd/{filename}"
    path = tmp_path / filename
    download_file(url, path)
    assert path.is_file()

    # load the data
    damo = hydamo.HyDAMO.from_geopackage(path)
    assert damo.data_layers == ["gemaal", "stuw"]
    # check the content
    rozema = damo.gemaal.loc[damo.gemaal.naam == "Gemaal Rozema"].squeeze()
    assert rozema.code == "KGM-O-11700"
    rozema.geometry
    assert isinstance(rozema.geometry, shapely.Point)

    assert len(damo.stuw) == 1
    assert damo.stuw.naam[0] == "CASPARGOUW STUW"

    # export file
    to_path = path.with_suffix(".to.gpkg")
    damo.to_geopackage(to_path)
    assert to_path.is_file()

    # upload file
    to_url = url = f"{BASE_URL}/HyDAMO_geconstrueerd/{to_path.name}"
    upload_file(to_url, to_path)
