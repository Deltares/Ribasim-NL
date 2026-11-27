"""Download the national GKW-HyDAMO package of NHI, see `ribasim_nl.lumping.gkw_hydamo`.

The package, about 1.8 GB, is unpacked to `ribasim_nl.lumping.gkw_hydamo.GKW_HYDAMO`, 5.4 GB, to be tracked with
`dvc add`. The package has no versions, so put the date on which it was published, which this script logs, in the
`desc` of the `.dvc` file. `dvc add` keeps it.

Not a DVC stage: run it to update the dataset, and commit the updated `.dvc` file.

Usage: pixi run python scripts/download_gkw_hydamo.py
"""

import logging
import zipfile
from email.utils import parsedate_to_datetime
from pathlib import Path

import requests
from ribasim_nl.lumping.gkw_hydamo import GKW_HYDAMO, PACKAGE_GPKG, PACKAGE_URL

LOG = logging.getLogger(__name__)
CHUNK_SIZE = 8 * 1024 * 1024


def download(url: str, path: Path) -> None:
    """Download `url` to `path`, in chunks."""
    with requests.get(url, stream=True, timeout=600) as response:
        response.raise_for_status()
        with path.open("wb") as file:
            for chunk in response.iter_content(CHUNK_SIZE):
                file.write(chunk)


def main() -> None:
    """Download the package, and unpack its GeoPackage to `GKW_HYDAMO`."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    response = requests.head(PACKAGE_URL, timeout=60)
    response.raise_for_status()
    published = parsedate_to_datetime(response.headers["Last-Modified"])
    LOG.info(f"Downloading {PACKAGE_URL}, published {published:%Y-%m-%d}")

    GKW_HYDAMO.parent.mkdir(parents=True, exist_ok=True)
    archive = GKW_HYDAMO.with_name(Path(PACKAGE_URL).name)
    unpacked = GKW_HYDAMO.with_suffix(".gpkg.part")
    try:
        download(PACKAGE_URL, archive)
        with zipfile.ZipFile(archive) as package, package.open(PACKAGE_GPKG) as source, unpacked.open("wb") as target:
            while chunk := source.read(CHUNK_SIZE):
                target.write(chunk)
        # a new file, rather than writing into the old one, which may be linked to the DVC cache
        unpacked.replace(GKW_HYDAMO)
    finally:
        archive.unlink(missing_ok=True)
        unpacked.unlink(missing_ok=True)
    LOG.info(
        f"Wrote {GKW_HYDAMO}; track it with `dvc add {GKW_HYDAMO}`, and give the date {published:%Y-%m-%d} in the "
        "desc of its .dvc file"
    )


if __name__ == "__main__":
    main()
