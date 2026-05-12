"""Download and install the Ribasim core binary into ``bin/ribasim``.

The Ribasim core version is read from ``pixi.toml`` so that the installed
binary stays in sync with the ``ribasim`` Python package version pinned there.
The release zip is fetched from GitHub releases and extracted on top of
``bin/ribasim`` (i.e. ``RIBASIM_HOME``).
"""

from __future__ import annotations

import platform
import shutil
import sys
import tempfile
import urllib.request
import zipfile
from pathlib import Path

import tomli

REPO_ROOT = Path(__file__).resolve().parent.parent
PIXI_TOML = REPO_ROOT / "pixi.toml"
RIBASIM_HOME = REPO_ROOT / "bin" / "ribasim"
RELEASE_URL = "https://github.com/Deltares/Ribasim/releases/download/v{version}/{asset}"


def _read_core_version() -> str:
    """Return the Ribasim core version pinned in ``pixi.toml``."""
    with PIXI_TOML.open("rb") as f:
        data = tomli.load(f)
    spec = data.get("dependencies", {}).get("ribasim")
    if not isinstance(spec, str):
        raise RuntimeError("Could not find 'ribasim' dependency in pixi.toml")
    # Strip leading comparison operators like '==', '>=', '~=' etc.
    return spec.lstrip("=<>~! ").strip()


def _asset_name() -> str:
    system = platform.system()
    if system == "Windows":
        return "ribasim_windows.zip"
    if system == "Linux":
        return "ribasim_linux.zip"
    raise RuntimeError(
        f"No Ribasim core binary is published for platform '{system}'. Build it from source or run on Windows/Linux."
    )


def _installed_version() -> str | None:
    readme = RIBASIM_HOME / "README.md"
    if not readme.exists():
        return None
    for line in readme.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("version"):
            # e.g. version = "2026.1.0"
            _, _, value = line.partition("=")
            return value.strip().strip('"').strip("'") or None
    return None


def _download(url: str, dest: Path) -> None:
    print(f"Downloading {url}")
    with urllib.request.urlopen(url) as response, dest.open("wb") as out:  # noqa: S310
        shutil.copyfileobj(response, out)


def _extract(zip_path: Path, target: Path) -> None:
    print(f"Extracting to {target}")
    if target.exists():
        shutil.rmtree(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        with zipfile.ZipFile(zip_path) as zf:
            zf.extractall(tmp)
        # The release zip contains a single top-level 'ribasim' folder.
        entries = [p for p in tmp.iterdir() if p.is_dir()]
        if len(entries) == 1:
            shutil.move(str(entries[0]), str(target))
        else:
            shutil.move(str(tmp), str(target))
    # Restore executable bits on POSIX (zip drops them).
    if platform.system() != "Windows":
        for exe in ("ribasim", "bin/ribasim"):
            p = target / exe
            if p.exists():
                p.chmod(0o755)


def main() -> int:
    version = _read_core_version()
    current = _installed_version()
    if current == version:
        print(f"Ribasim core {version} already installed at {RIBASIM_HOME}")
        return 0
    if current is not None:
        print(f"Replacing Ribasim core {current} with {version}")

    asset = _asset_name()
    url = RELEASE_URL.format(version=version, asset=asset)

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / asset
        _download(url, zip_path)
        _extract(zip_path, RIBASIM_HOME)

    print(f"Installed Ribasim core {version} to {RIBASIM_HOME}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
