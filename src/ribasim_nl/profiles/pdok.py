"""Download PDOK data."""

import io
import json
import logging
import pathlib
import time
import zipfile

import geopandas as gpd
import requests
import shapely
from tornado.httpclient import HTTPError

BASE_URL = "https://api.pdok.nl"
DATA = {
    "featuretypes": ["waterdeel"],
    "format": "citygml",
}

LOG = logging.getLogger(__name__)


def download_pdok_water(geo_filter: shapely.Polygon | shapely.MultiPolygon = None, **kwargs) -> gpd.GeoDataFrame:
    # optional arguments
    fn: str = kwargs.get("fn", "pdok_waterdeel.gpkg")
    sleep_time: float = kwargs.get("sleep_time", 1)
    wd: pathlib.Path = kwargs.get("wd")

    # API URL
    __full_custom_url = "lv/bgt/download/v1_0/full/custom"

    # download description
    if geo_filter is None:
        data = DATA.copy()
    else:
        data = {**DATA, **{"geofilter": str(geo_filter)}}
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    # download request
    post_response = requests.post(f"{BASE_URL}/{__full_custom_url}", headers=headers, data=json.dumps(data))
    if post_response.status_code == 202:
        download_request_id = post_response.json()["downloadRequestId"]
        LOG.debug(f"{download_request_id=}")
    else:
        raise HTTPError(post_response.status_code, "Download request failed")

    # request download
    get_url = f"{BASE_URL}/{__full_custom_url}/{download_request_id}/status"
    while True:
        get_response = requests.get(get_url, headers=headers)
        match get_response.status_code:
            case 200:
                LOG.debug(f"Download not yet ready; sleep {sleep_time} seconds")
                time.sleep(sleep_time)
            case 201:
                break
            case _:
                raise HTTPError(get_response.status_code, "Download denied")

    # download URL
    relative_download_url = get_response.json()["_links"]["download"]["href"]
    download_url = BASE_URL + relative_download_url
    LOG.debug(f"{download_url=}")
    download_response = requests.get(download_url)
    LOG.debug(f"{download_response.status_code=}")

    # download PDOK-data
    if download_response.status_code == 200:
        with zipfile.ZipFile(io.BytesIO(download_response.content)) as zip_file:
            (gml_fn,) = zip_file.namelist()
            with zip_file.open(gml_fn) as gml_file:
                gdf = gpd.read_file(gml_file)
                # > write PDOK-data
                if wd is not None:
                    gdf.to_file(wd / fn)
    else:
        raise HTTPError(download_response.status_code, "Download failed")

    # return PDOK-data
    return gdf


def get_water_surfaces(
    wd: pathlib.Path, geo_filter: shapely.Polygon | shapely.MultiPolygon, **kwargs
) -> gpd.GeoDataFrame:
    # optional arguments
    fn: str = kwargs.get("fn", "pdok_waterdeel.gpkg")
    force: bool = kwargs.get("force", False)
    write: bool = kwargs.get("write", True)

    # read pre-downloaded PDOK-data
    if (wd / fn).exists() and not force:
        gdf = gpd.read_file(wd / fn)
        LOG.info(f"Used downloaded PDOK-data: {wd / fn}")
    # download PDOK-data
    else:
        gdf = download_pdok_water(geo_filter=geo_filter, wd=(wd if write else None), fn=fn)
        LOG.info(f"Downloaded PDOK-data ({write=})" + (f": {wd / fn}" if write else ""))

    # return water surfaces
    return gdf
