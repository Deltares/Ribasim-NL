import os
from pathlib import Path

import pytest
from ribasim_nl.cloud import WATER_AUTHORITIES
from ribasim_nl.settings import Settings, settings

from ribasim_nl import CloudStorage


@pytest.fixture
def cloud(tmp_path):
    return CloudStorage(tmp_path)


def test_initialize(cloud):
    """Test if cloud still has same structure"""
    assert cloud.data_dir.exists()

    # check if we have the correct directories
    directories = cloud.dirs()
    assert len(directories) >= 20  # RWS, water authorities + pytest directory
    for directory in [*cloud.water_authorities, "Basisgegevens"]:
        assert directory in directories


def test_download(cloud):
    """Check if we can download."""
    global WATER_AUTHORITIES

    authority = "pytest"
    WATER_AUTHORITIES += [authority]
    local_dir = cloud.data_dir.joinpath(authority, "aangeleverd")
    remote_url = cloud.joinurl(authority, "aangeleverd")

    # check if local_dir does not exist
    assert authority in cloud.water_authorities
    assert not local_dir.exists()

    # download data aangeleverd and check if exists
    cloud.download_aangeleverd(authority)
    assert local_dir.exists()

    # check if content is the same
    remote_content = cloud.content(remote_url)
    for item in remote_content:
        assert local_dir.joinpath(item).exists()


def test_source(cloud):
    # check if sources are not deleted
    ref_sources = ["KRW", "LHM", "LKM", "Top10NL", "documenten"]
    available_sources = cloud.source_data

    for source in ref_sources:
        assert source in available_sources


def test_propfind_distinguishes_dirs_and_files(cloud):
    """Test that _propfind correctly identifies directories vs files using WebDAV resourcetype.

    The LHM zarr store 'LHM_433_budget.zip' is a directory (not a zip file), and zarr metadata
    files like '.zgroup' and '.zmetadata' are files (not directories). The old heuristic based on
    file extensions got both of these wrong.
    """
    url = cloud.joinurl("Basisgegevens/LHM/4.3/results/LHM_433_budget.zip")
    items, dir_names = cloud._propfind(url)

    # zarr dotfiles must be recognized as files, not directories
    zarr_dotfiles = {".zgroup", ".zmetadata", ".zattrs"}
    for dotfile in zarr_dotfiles & set(items):
        assert dotfile not in dir_names, f"{dotfile} should be a file, not a directory"

    # data variable subdirectories (e.g. bdgriv_sys1) should be directories
    for item in items:
        if item.startswith("bdg"):
            assert item in dir_names, f"{item} should be a directory"


def test_models(cloud):
    # check if we can find uploaded models
    models = cloud.uploaded_models("Rijkswaterstaat")
    assert any((i.model == "ijsselmeer" for i in models))  # noqa: UP034


def test_settings():
    assert isinstance(settings, Settings)

    os.environ["RIBASIM_NL_CLOUD_PASS"] = "test"  # noqa: S105
    nsettings = Settings(_env_file="foo.env", ribasim_home=Path("custom_ribasim"))
    assert nsettings.ribasim_home == Path("custom_ribasim")
    assert nsettings.ribasim_nl_data_dir == Path("data")
    assert nsettings.ribasim_nl_cloud_pass == "test"  # noqa: S105
