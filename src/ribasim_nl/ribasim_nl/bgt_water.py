"""The water areas of the BGT (Basisregistratie Grootschalige Topografie), as one national GeoParquet file.

The BGT `waterdeel` maps all surface water as areas, with decimetre accuracy. `scripts/download_bgt_water.py`
downloads it from PDOK in tiles with `download_tile`, keeps the current objects with `current_objects`, and writes
`BGT_WATER` with `write_bgt_water`. Stages read the part they need with `read_bgt_water`.
"""

import io
import logging
import time
import zipfile
from pathlib import Path

import geopandas as gpd
import pyogrio
import requests
from shapely.geometry.base import BaseGeometry

from ribasim_nl.settings import settings

LOG = logging.getLogger(__name__)

BGT_WATER = settings.ribasim_nl_data_dir / "Basisgegevens/BGT/bgt_water.parquet"

PDOK = "https://api.pdok.nl"
FULL_CUSTOM_URL = f"{PDOK}/lv/bgt/download/v1_0/full/custom"

# The columns that are kept, by their name in the GML-light download. `lokaalID` identifies an object in the BGT,
# `bronhouder` is the organisation that maintains it, and `tijdstipRegistratie` the time of its current version.
COLUMNS = {
    "identificatie.lokaalID": "lokaalID",
    "bronhouder": "bronhouder",
    "bgt-type": "bgt-type",
    "plus-type": "plus-type",
    "relatieveHoogteligging": "relatieveHoogteligging",
    "inOnderzoek": "inOnderzoek",
    "objectBeginTijd": "objectBeginTijd",
    "tijdstipRegistratie": "tijdstipRegistratie",
}

# The same types in each tile, also where a download lacks an optional column
DTYPES = {
    "lokaalID": "string",
    "bronhouder": "string",
    "bgt-type": "string",
    "plus-type": "string",
    "relatieveHoogteligging": "Int8",
    "inOnderzoek": "boolean",
    "objectBeginTijd": "datetime64[ms]",
    "tijdstipRegistratie": "datetime64[ms]",
}
assert set(DTYPES) == set(COLUMNS.values())


def download_tile(
    geofilter: BaseGeometry | None, file_format: str = "gmllight", poll_interval: float = 2.0
) -> gpd.GeoDataFrame:
    """Download the BGT `waterdeel` objects that intersect `geofilter`, with all their versions.

    Objects are not clipped to `geofilter`. The download contains the history of each object: its earlier versions
    have an `eindRegistratie`, see `current_objects`.

    Args:
        geofilter: area to download, or None for the whole of the Netherlands
        file_format: format of the PDOK download, "gmllight", "citygml" or "stufgeo"
        poll_interval: seconds between checks whether PDOK has prepared the download

    Returns
    -------
        the `waterdeel` objects, in EPSG:28992
    """
    request = {"featuretypes": ["waterdeel"], "format": file_format}
    if geofilter is not None:
        request["geofilter"] = geofilter.wkt
    response = requests.post(FULL_CUSTOM_URL, json=request, timeout=300)
    if response.status_code != 202:
        raise ValueError(f"BGT download request failed with {response.status_code}: {response.text[:500]}")
    status_url = f"{FULL_CUSTOM_URL}/{response.json()['downloadRequestId']}/status"
    while (status := requests.get(status_url, timeout=300)).status_code == 200:
        time.sleep(poll_interval)
    if status.status_code != 201:
        raise ValueError(f"BGT download failed with {status.status_code}: {status.text[:500]}")
    download = requests.get(PDOK + status.json()["_links"]["download"]["href"], timeout=600)
    download.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(download.content)) as zip_file:
        (name,) = zip_file.namelist()
        gml = zip_file.read(name)
    # an area without water, such as at sea, has no layer
    if len(pyogrio.list_layers(io.BytesIO(gml))) == 0:
        return gpd.GeoDataFrame({"identificatie.lokaalID": [], "bgt-status": []}, geometry=[], crs=28992)
    gdf = gpd.read_file(io.BytesIO(gml))
    return gdf.set_crs(28992, allow_override=True) if gdf.crs is None else gdf.to_crs(28992)


def current_objects(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """The current version of the existing objects of a GML-light download, with the columns of `COLUMNS`.

    Versions that were replaced or ended have an `eindRegistratie`. Objects that lie in several downloaded tiles occur
    in each of them, so duplicates by `lokaalID` are dropped. Optional columns that a download lacks, because none of
    its objects has them, are empty.
    """
    missing = {"identificatie.lokaalID", "bgt-status"} - set(gdf.columns)
    assert not missing, f"BGT download misses columns {sorted(missing)}"
    # optional attributes are left out of a download where no object has them
    gdf = gdf.reindex(columns=[*dict.fromkeys([*gdf.columns, *COLUMNS, "eindRegistratie"])])
    current = gdf[gdf["eindRegistratie"].isna() & gdf["bgt-status"].eq("bestaand")]
    current = current[[*COLUMNS, "geometry"]].rename(columns=COLUMNS).astype(DTYPES)
    return current.drop_duplicates("lokaalID").reset_index(drop=True)


def write_bgt_water(gdf: gpd.GeoDataFrame, path: Path = BGT_WATER, row_group_size: int = 50_000) -> None:
    """Write BGT water objects as GeoParquet, sorted along a Hilbert curve so that reading an area is fast.

    The geometries are GeoArrow encoded, and each row has its bounding box, see `read_bgt_water`.
    """
    assert gdf["lokaalID"].is_unique, "lokaalID should identify each object"
    gdf = gdf.iloc[gdf.geometry.hilbert_distance().argsort()].reset_index(drop=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(
        path,
        index=False,
        compression="zstd",
        geometry_encoding="geoarrow",
        write_covering_bbox=True,
        row_group_size=row_group_size,
    )


def read_bgt_water(
    mask: BaseGeometry | None = None, path: Path = BGT_WATER, columns: list[str] | None = None
) -> gpd.GeoDataFrame:
    """Read the BGT water objects that intersect `mask`, or all of them.

    Only the row groups whose bounding box overlaps that of `mask` are read.
    """
    if mask is None:
        return gpd.read_parquet(path, columns=columns)
    gdf = gpd.read_parquet(path, columns=columns, bbox=mask.bounds)
    return gdf[gdf.intersects(mask)].reset_index(drop=True)
