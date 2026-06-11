"""Download and install the Ribasim core binary into ``bin/ribasim``.

The release zip is fetched from either GitHub releases or a MinIO S3 bucket,
depending on the SOURCE setting below. The zip is extracted into ``bin/``
(which contains a top-level ``ribasim`` folder).
"""

from __future__ import annotations

import os
import platform
import shutil
import subprocess
import sys
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
NAME = "storage-pass"
# ───────────────────────────────────────────────────────────────────────────────

GITHUB_RELEASE_URL = "https://github.com/Deltares/Ribasim/releases/download/{name}/{asset}"
MINIO_SERVER = "s3.deltares.nl"
MINIO_BUCKET = "ribasim-nl"
MINIO_FOLDER = "bin"


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


def main() -> int:
    ribasim_home = _ribasim_home()
    bin_dir = ribasim_home.parent
    bin_dir.mkdir(parents=True, exist_ok=True)

    if ribasim_home.exists():
        shutil.rmtree(ribasim_home)

    asset = _asset_name()
    zip_path = bin_dir / asset
    try:
        if SOURCE == "github":
            _download_github(zip_path)
        elif SOURCE == "minio":
            _download_minio(zip_path)
        else:
            raise RuntimeError(f"Unknown SOURCE: {SOURCE!r}. Use 'github' or 'minio'.")

        _extract(zip_path, bin_dir)
    finally:
        zip_path.unlink(missing_ok=True)

    print(f"Installed Ribasim core ({SOURCE}: {NAME}) to {ribasim_home}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
