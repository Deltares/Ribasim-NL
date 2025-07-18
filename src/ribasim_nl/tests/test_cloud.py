import os
from pathlib import Path

import pytest

from ribasim_nl import CloudStorage
from ribasim_nl.cloud import WATER_AUTHORITIES  # noqa: F401
from ribasim_nl.settings import Settings, settings


@pytest.fixture
def cloud(tmp_path):
    return CloudStorage(tmp_path)


def test_initialize(cloud):
    """Test if cloud still has same structure"""
    assert cloud.data_dir.exists()

    # check if we have the correct directories
    directories = cloud.dirs(cloud.url)
    assert len(directories) >= 20  # RWS, water authorities + pytest directory
    for directory in cloud.water_authorities + ["Basisgegevens"]:
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


def test_models(cloud):
    # check if we can find uploaded models
    models = cloud.uploaded_models("Rijkswaterstaat")
    assert any((i.model == "ijsselmeer" for i in models))  # noqa: UP034


def test_settings():
    assert isinstance(settings, Settings)

    os.environ["RIBASIM_NL_CLOUD_PASS"] = "test"
    nsettings = Settings(_env_file="foo.env")
    assert nsettings.ribasim_exe == Path("ribasim")
    assert nsettings.ribasim_nl_data_dir == Path("data")
    assert nsettings.ribasim_nl_cloud_pass == "test"
