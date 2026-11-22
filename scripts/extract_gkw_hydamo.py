"""Extract the HyDAMO data of water authorities from the national GKW-HyDAMO package, see `ribasim_nl.lumping.gkw_hydamo`.

The package is downloaded into `--cache`, about 1.8 GB that unpacks to 5.4 GB, unless it is there already. It has no
versions, so each extract is named after the date on which the package was last modified:
`<authority>/verwerkt/1_ontvangen_data/gkw_hydamo_<date>.gpkg`. Track a new extract with `dvc add`, and give its date
to the stage `gkw_hydamo` of the water authority in `dvc.yaml`.

Usage: pixi run python scripts/extract_gkw_hydamo.py AUTHORITY [AUTHORITY ...] [--cache DIR]
"""

import argparse
import logging
import zipfile
from datetime import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
from ribasim_nl.lumping.gkw_hydamo import PACKAGE_GPKG, PACKAGE_URL, extract_authority

from ribasim_nl import CloudStorage

LOG = logging.getLogger(__name__)
CHUNK_SIZE = 8 * 1024 * 1024


def download_package(cache: Path) -> tuple[Path, datetime]:
    """Download and unpack the package into `cache`, unless that has the current one. Returns it with its date."""
    response = requests.head(PACKAGE_URL, timeout=60)
    response.raise_for_status()
    modified = parsedate_to_datetime(response.headers["Last-Modified"])
    folder = cache / modified.strftime("%Y%m%d")
    gpkg = folder / PACKAGE_GPKG
    if gpkg.exists():
        LOG.info(f"Using {gpkg}")
        return gpkg, modified
    folder.mkdir(parents=True, exist_ok=True)
    archive = folder / Path(PACKAGE_URL).name
    LOG.info(f"Downloading {PACKAGE_URL} of {modified:%Y-%m-%d}")
    with requests.get(PACKAGE_URL, stream=True, timeout=600) as download:
        download.raise_for_status()
        with archive.open("wb") as file:
            for chunk in download.iter_content(CHUNK_SIZE):
                file.write(chunk)
    with zipfile.ZipFile(archive) as package:
        package.extract(PACKAGE_GPKG, folder)
    archive.unlink()
    return gpkg, modified


def main() -> None:
    """Extract the HyDAMO data of the given water authorities."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("authorities", nargs="+", metavar="AUTHORITY", help="water authority, such as BrabantseDelta")
    parser.add_argument("--cache", type=Path, default=Path(".cache/gkw_hydamo"), help="folder of the package")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    cloud = CloudStorage()
    for authority in args.authorities:
        cloud.validate_authority(authority)
    gpkg, modified = download_package(args.cache)
    for authority in args.authorities:
        target = cloud.joinpath(authority, f"verwerkt/1_ontvangen_data/gkw_hydamo_{modified:%Y%m%d}.gpkg")
        target.parent.mkdir(parents=True, exist_ok=True)
        extract_authority(gpkg, authority, target)
        LOG.info(f"Wrote {target}")


if __name__ == "__main__":
    main()
