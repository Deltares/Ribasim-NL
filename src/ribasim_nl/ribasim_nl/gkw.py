# %%
import shutil
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

import geopandas as gpd
import pandas as pd
import requests

from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.cloud import CloudStorage

cloud = CloudStorage()

PDOK_URL = r"https://service.pdok.nl/hwh/kunstwerkenimwa/atom/waterschappen_kunstwerken_imwa.xml"
NAMESPACES = {"atom": "http://www.w3.org/2005/Atom", "georss": "http://www.georss.org/georss"}
GKW_ROOT_PATH = cloud.joinpath("Basisgegevens", "GKW")


def download_geopackage(url: str, save_dir: Path):
    """Download a geopackage from an url to a save_dir

    Args:
        url (str): url with GeoPackage
        save_dir (Path): directory to save the file
    """
    if Path(url).suffix == ".gpkg":
        try:
            # Get the filename from the URL
            filepath = save_dir / Path(url).name

            # Make the GET request
            response = requests.get(url, headers={"Accept": "application/geopackage+sqlite3"}, stream=True)

            # Check if the response is successful
            if response.status_code == 200:
                content_type = response.headers.get("Content-Type", "unknown")

                # Check if the response is a Geopackage file
                if "application/geopackage+sqlite3" in content_type:
                    # Save the file locally
                    with open(filepath, "wb") as file:
                        for chunk in response.iter_content(chunk_size=8192):
                            file.write(chunk)
                    print(f"Downloaded: {filepath}")
                else:
                    print(f"Expected Geopackage file, but got {content_type}. Skipping...")
            else:
                print(f"Failed to download {url}: HTTP {response.status_code}")
                if response.history:
                    print(f"Redirected to: {response.url}")

        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")


def download_from_pdok(force_update: bool = False, upload_to_cloud_storage: bool = False) -> Path:
    """Download latest GKW-data from PDOK.

    Args:
        force_update (bool, optional): force update even if folder with latest GPKGs exist. Defaults to True.
        upload_to_cloud_storage (bool, optional): upload to CloudStorage. Defaults to False.
    """
    # get atom-feed
    print(f"Downloading GKW-data from {PDOK_URL}")
    response = requests.get(PDOK_URL)
    response.raise_for_status()

    # updated datetime and define gkw_source_dir
    root = ET.fromstring(response.text)
    updated = datetime.fromisoformat(root.find(".//atom:entry/atom:updated", NAMESPACES).text)
    folder = updated.strftime("%Y%m%d")
    gkw_source_dir = GKW_ROOT_PATH / folder

    # download if gkw_source_dir doesn't exist or we force an update
    if (not gkw_source_dir.exists()) | force_update:
        # (re)make the gkw_source_dir
        if gkw_source_dir.exists():
            shutil.rmtree(gkw_source_dir)
        gkw_source_dir.mkdir(parents=True)

        # get download_urls and download files
        download_urls = [link.attrib["href"] for link in root.findall(".//atom:entry/atom:link", NAMESPACES)]
        for url in download_urls:
            download_geopackage(url=url, save_dir=gkw_source_dir)
    else:
        print(f"Local data is up-to-date with PDOK : {updated}")

    # upload to cloud
    if upload_to_cloud_storage:
        overwrite = force_update
        if folder not in cloud.content(cloud.joinurl(cloud.relative_path(GKW_ROOT_PATH).as_posix())):
            gkw_source_url = cloud.joinurl(cloud.relative_path(gkw_source_dir).as_posix())
            cloud.create_dir(gkw_source_url)
        cloud.upload_content(gkw_source_dir, overwrite=overwrite)

    return gkw_source_dir


def get_gkw_source_dir() -> Path | None:
    """Get latest gkw_source_data path if exists."""
    dirs = [i for i in GKW_ROOT_PATH.glob("*") if i.is_dir]
    if dirs:
        return [i for i in GKW_ROOT_PATH.glob("*") if i.is_dir][-1]
    else:
        print("No GKW-data local, download latest using 'download_from_cloud()' or 'download_from_pdok()'")
        return None


def get_gkw_source_url() -> str | None:
    """Get latest gkw_source_data path if exists."""
    dirs = cloud.content(cloud.joinurl(cloud.relative_path(GKW_ROOT_PATH).as_posix()))
    if dirs:
        return cloud.joinurl((cloud.relative_path(GKW_ROOT_PATH) / max(dirs)).as_posix())
    else:
        print("No GKW-data in cloud-storage, download latest using 'download_from_pdok()'")
        return None


def download_from_cloud() -> Path | None:
    """Download latest GKW-data from CloudStorage. Return local Path after download"""
    url = get_gkw_source_url()
    print(f"Downloading GKW-data from {url}")
    if url is None:
        return None
    else:
        cloud.download_content(url)
        return cloud.joinpath(cloud.relative_url(url))


def get_data_from_gkw(layers: list[str], authority: str | None = None):
    # get gkw_source_dir (and data if not any)
    gkw_source_dir = get_gkw_source_dir()  # get source_dir if exists locally
    if gkw_source_dir is None:
        gkw_source_dir = download_from_cloud()  # get source_dir if exists in CloudStorage
        if gkw_source_dir is None:
            gkw_source_dir = download_from_pdok()  # download from PDOK

    # reader for geopackages
    dfs = []
    nen3610id = f"NL.WBHCODE.{waterbeheercode[authority]}"
    for layer in layers:
        filepath = gkw_source_dir / f"{layer}.gpkg"
        if not filepath.exists():  # check if file exists
            raise FileNotFoundError(f"file for layer {layer} does not exist {filepath}")
        else:
            df = gpd.read_file(gkw_source_dir / f"{layer}.gpkg", layer=layer)
            if authority is not None:
                df = df[df.nen3610id.str.startswith(nen3610id)]  # nen3610id includes reference to authority
            df["layer"] = layer  # add layer so we can discriminate later
            dfs += [df]

    # return in 1 dataframe
    return pd.concat(dfs, ignore_index=True)
