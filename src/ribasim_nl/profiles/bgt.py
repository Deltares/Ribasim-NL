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

from ribasim_nl import CloudStorage

BASE_URL = "https://api.pdok.nl"
DATA = {
    "featuretypes": ["waterdeel"],
    "format": "citygml",
}

LOG = logging.getLogger(__name__)


def download_bgt_water(geo_filter: shapely.Polygon | shapely.MultiPolygon = None, **kwargs) -> gpd.GeoDataFrame:
    # optional arguments
    fn: str = kwargs.get("fn", "bgt_water.gpkg")
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
        raise ValueError(f"{post_response.status_code=}: Download request failed")

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
                raise ValueError(f"{post_response.status_code=}: Download denied")

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
        raise ValueError(f"{post_response.status_code=}: Download failed")

    # return BGT-data
    return gdf


def get_water_surfaces(
    wd: pathlib.Path, geo_filter: shapely.Polygon | shapely.MultiPolygon, **kwargs
) -> gpd.GeoDataFrame:
    # optional arguments
    fn: str = kwargs.get("fn", "bgt_water.gpkg")
    force: bool = kwargs.get("force", False)
    write: bool = kwargs.get("write", True)

    # read pre-downloaded BGT-data
    if (wd / fn).exists() and not force:
        gdf = gpd.read_file(wd / fn)
        LOG.info(f"Used downloaded BGT-data: {wd / fn}")
    # download BGT-data
    else:
        gdf = download_bgt_water(geo_filter=geo_filter, wd=(wd if write else None), fn=fn)
        LOG.info(f"Downloaded BGT-data ({write=})" + (f": {wd / fn}" if write else ""))

    # return water surfaces
    return gdf


def upload_bgt_water(authority: str, cloud: CloudStorage = CloudStorage(), **kwargs) -> None:
    # optional arguments
    basins_fn: str = kwargs.get("basins_fn", f"{authority}.gpkg")
    basins_layer: str = kwargs.get("basins_layer", "peilgebied")
    mkdir: bool = kwargs.get("mkdir", True)
    overwrite: bool = kwargs.get("overwrite", True)
    sync: bool = kwargs.get("sync", True)

    # validate optional arguments
    if not basins_fn.endswith(".gpkg"):
        basins_fn += ".gpkg"

    # sync 'verwerkt'-directory
    if sync:
        cloud.download_verwerkt(authority, overwrite=overwrite)
        paths = [cloud.joinpath(authority, "verwerkt", "Parametrisatie_data")]
        cloud.synchronize(paths)

    # create geo-filter from basins
    basins = gpd.read_file(cloud.joinpath(authority, "verwerkt", "Parametrisatie_data", basins_fn), layer=basins_layer)
    geo_filter = shapely.MultiPolygon(basins.explode().geometry.values).convex_hull
    if geo_filter.has_z:
        assert isinstance(geo_filter, shapely.Polygon)
        geo_filter = shapely.Polygon([(x, y) for x, y, _ in geo_filter.exterior.coords])

    # download BGT-data
    fn_bgt = cloud.joinpath(authority, "verwerkt", "BGT", f"bgt_{authority}_water.gpkg")
    if mkdir:
        fn_bgt.parent.mkdir(parents=True, exist_ok=True)
    _ = get_water_surfaces(fn_bgt.parent, geo_filter, fn=fn_bgt.name, force=overwrite, write=True)

    # upload BGT-data
    cloud.create_dir(authority, "verwerkt", "BGT")
    cloud.upload_file(fn_bgt)
