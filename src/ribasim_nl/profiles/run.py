"""Definition of profiles per water authority."""

import logging

from ribasim_nl import CloudStorage

LOG = logging.getLogger(__name__)


def _fn_crossings(water_authority: str, cloud: CloudStorage = CloudStorage()) -> str:
    wd = cloud.joinpath(water_authority, "verwerkt", "Crossings")
    files = sorted(wd.glob("*.gpkg"))
    return files[-1]


def _fn_selection(water_authority: str, cloud: CloudStorage = CloudStorage()) -> str:
    wd = cloud.joinpath(water_authority, "verwerkt", "profiles")
    return str(wd / "source.gpkg")


def create_sub_selection(water_authority: str, cloud: CloudStorage = CloudStorage(), **kwargs) -> str:
    # optional arguments
    fn_xs: str = kwargs.get("fn_xs")
    sync: bool = kwargs.get("sync", True)

    # cross-sections data
    file_xs = cloud.joinpath(water_authority, fn_xs)

    # sync cloud
    if sync:
        cloud.download_verwerkt(water_authority)

    # get relevant datasets
    basin_file = {
        "filename": cloud.joinpath(water_authority, "modellen", f"{water_authority}_parameterized", "database.gpkg"),
        "layer": "Basin / area",
    }
    crossings_file = {
        "filename": _fn_crossings(water_authority, cloud=cloud),
        "layer": "crossings_hydroobject_filtered",
    }
    hydro_objects_file = {
        "filename": _fn_crossings(water_authority, cloud=cloud),
        "layer": "hydroobject",
    }
    cross_sections_file = {"filename": "", "layer": "profielpunt"}

    return ", ".join(
        [
            file_xs,
            basin_file["filename"],
            crossings_file["filename"],
            hydro_objects_file["filename"],
            cross_sections_file["filename"],
        ]
    )
