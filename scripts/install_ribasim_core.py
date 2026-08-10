"""Download, install, and archive the Ribasim core binary.

The release zip is fetched from either GitHub releases or a MinIO S3 bucket,
depending on the SOURCE setting below. By default it is installed into
``RIBASIM_HOME``; ``--archive`` copies that installation to
``/p/ribasim-nl/bin/<NAME>``.
"""

from __future__ import annotations

import argparse
import os
import platform
import shutil
import subprocess
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

from minio import Minio
from ribasim_nl.settings import settings

# ── Configuration ──────────────────────────────────────────────────────────────
# SOURCE: "github" to download from GitHub releases, "minio" to download from S3.
# NAME: For github, a release tag like "v2026.1.0".
#       For minio, a folder name like "storage-formulation".
# Make sure the version is compatible with the Ribasim Python version in pixi.toml.
SOURCE = "minio"
NAME = "storage-continuous"
# ───────────────────────────────────────────────────────────────────────────────

GITHUB_RELEASE_URL = "https://github.com/Deltares/Ribasim/releases/download/{name}/{asset}"
MINIO_SERVER = "s3.deltares.nl"
MINIO_BUCKET = "ribasim-nl"
MINIO_FOLDER = "bin"
HPC_BIN_DIR = Path("/p/ribasim-nl/bin")


def _ribasim_home() -> Path:
    """Return RIBASIM_HOME from the environment."""
    value = os.environ.get("RIBASIM_HOME")
    if not value:
        raise RuntimeError("RIBASIM_HOME environment variable is not set")
    return Path(value)


def _asset_name() -> str:
    system = platform.system()
    if system == "Windows":
        return "ribasim_windows.zip"
    if system == "Linux":
        return "ribasim_linux.zip"
    raise RuntimeError(
        f"No Ribasim core binary is published for platform '{system}'. Build it from source or run on Windows/Linux."
    )


def _download_github(dest: Path) -> None:
    asset = _asset_name()
    url = GITHUB_RELEASE_URL.format(name=NAME, asset=asset)
    print(f"Downloading {url}")
    with urllib.request.urlopen(url) as response, dest.open("wb") as out:  # noqa: S310
        shutil.copyfileobj(response, out)


def _download_minio(dest: Path) -> None:
    if not settings.minio_access_key or not settings.minio_secret_key:
        raise RuntimeError("MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in .env")

    asset = _asset_name()
    object_name = f"{MINIO_FOLDER}/{NAME}/{asset}"
    print(f"Downloading s3://{MINIO_BUCKET}/{object_name}")
    client = Minio(MINIO_SERVER, access_key=settings.minio_access_key, secret_key=settings.minio_secret_key)
    client.fget_object(MINIO_BUCKET, object_name, str(dest))


def _extract(zip_path: Path, dest: Path) -> None:
    """Extract a zip archive, preserving symlinks and permissions."""
    if platform.system() != "Windows":
        subprocess.run(["unzip", "-o", str(zip_path), "-d", str(dest)], check=True)
    else:
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(dest)


def _install(ribasim_home: Path) -> None:
    ribasim_home.parent.mkdir(parents=True, exist_ok=True)
    asset = _asset_name()
    with tempfile.TemporaryDirectory(dir=ribasim_home.parent) as temp_dir:
        temp_path = Path(temp_dir)
        zip_path = temp_path / asset
        if SOURCE == "github":
            _download_github(zip_path)
        elif SOURCE == "minio":
            _download_minio(zip_path)
        else:
            raise RuntimeError(f"Unknown SOURCE: {SOURCE!r}. Use 'github' or 'minio'.")

        _extract(zip_path, temp_path)
        extracted_home = temp_path / "ribasim"
        if not extracted_home.is_dir():
            raise RuntimeError(f"Archive does not contain the expected 'ribasim' directory: {asset}")

        if ribasim_home.exists():
            shutil.rmtree(ribasim_home)
        shutil.move(str(extracted_home), ribasim_home)


def _archive(ribasim_home: Path) -> Path:
    if not ribasim_home.is_dir():
        raise RuntimeError(f"Ribasim core is not installed at {ribasim_home}")

    archived_home = HPC_BIN_DIR / NAME
    archived_home.parent.mkdir(parents=True, exist_ok=True)
    if archived_home.exists():
        return archived_home
    shutil.copytree(ribasim_home, archived_home, symlinks=True)
    return archived_home


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive", action="store_true", help="copy RIBASIM_HOME to /p/ribasim-nl/bin/<NAME>")
    parser.add_argument("--print-name", action="store_true", help="print NAME and exit")
    args = parser.parse_args()

    if args.print_name:
        print(NAME)
        return 0

    ribasim_home = _ribasim_home()
    if args.archive:
        archived_home = _archive(ribasim_home)
        print(f"Archived Ribasim core ({SOURCE}: {NAME}) at {archived_home}")
        return 0

    _install(ribasim_home)

    print(f"Installed Ribasim core ({SOURCE}: {NAME}) to {ribasim_home}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
