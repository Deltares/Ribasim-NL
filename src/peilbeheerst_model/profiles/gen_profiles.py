"""Generation of profiles."""

import pathlib
import typing

import geopandas as gpd
import pandas as pd
from ribasim_nl.profiles import hydrotopes, run

from ribasim_nl import CloudStorage


def main(
    water_authority: str,
    fn_network: str,
    *,
    export_profile_tables: bool = True,
    sync: bool = True,
    overwrite: bool = False,
    export_intermediate_output: bool = False,
    fn_water_bodies: gpd.GeoDataFrame | None = None,
) -> None:
    """Execute profile table generator.

    This is the generalised profile-generator in which all common files, directories, etc. are coded to minimise
    overlapping code.

    :param water_authority: water authority
    :param fn_network: filename with geospatial data of crossings and hydro-objects
        File is considered to be located at '<GoodCloud>/<water_authority>/verwerkt/Crossings/<fn_network>
    :param export_profile_tables: export generated profile tables, defaults to True
    :param sync: sync with GoodCloud's 'verwerkt'- and 'Basisgegevens/Hydrotypen'-folders, defaults to True
    :param overwrite: overwrite GoodCloud's 'verwerkt'- and 'Basisgegevens/Hydrotypen'-folders, defaults to False
    :param export_intermediate_output: export intermediate output steps (for debugging), defaults to False
    :param fn_water_bodies: filename with water bodies with specific, user-defined representative depths used to
        overwrite the determined representative depths per hydro-object, defaults to None
        When `water_bodies` (polygons), hydro-objects within the polygon(s) have their representative depth overwritten
        by the depth value(s) in `water_bodies`.
    """
    # sync with the GoodCloud
    cloud = CloudStorage()
    if sync:
        _sync(cloud, water_authority, overwrite, fn_water_bodies)

    # read files
    gdf_basins = _read_basins(cloud, water_authority)
    _fn_network = cloud.joinpath(water_authority, "verwerkt", "Crossings", fn_network)
    gdf_hydro_objects = gpd.read_file(_fn_network, layer="hydroobject")
    gdf_crossings = gpd.read_file(_fn_network, layer="crossings_hydroobject_filtered")
    gdf_target_levels = _read_target_levels(cloud, water_authority, fn_network)
    gdf_cross_sections = _read_cross_sections(cloud, water_authority)
    gdf_water_bodies = _read_water_bodies(cloud, water_authority, fn_water_bodies)
    table = _read_hydrotope_table(cloud)

    # setting paths
    fn_bgt = _bgt_path(cloud, water_authority)
    wd_int = _int_output_path(cloud, water_authority, export_intermediate_output)

    # execute profile generation
    profile_tables = run.main(
        gdf_basins,
        gdf_hydro_objects,
        gdf_crossings,
        gdf_cross_sections,
        hydrotope_table=table,
        target_levels=gdf_target_levels,
        cloud=cloud,
        fn_bgt=fn_bgt,
        wd_intermediate_output=wd_int,
        water_bodies=gdf_water_bodies,
    )

    # export profile table
    if export_profile_tables:
        _export_profiles(cloud, water_authority, profile_tables, export_intermediate_output)


def flagged_hydro_objects(
    water_authority: str,
    fn_target_levels: pathlib.Path | str,
    fn_hydro_objects: pathlib.Path | str,
    col_flag: str,
    *,
    val_flag: typing.Any = True,
    layer_hydro_objects: str = "hydroobjects",
    export_profile_tables: bool = True,
    sync: bool = True,
    overwrite: bool = False,
    export_intermediate_output: bool = False,
    fn_water_bodies: pathlib.Path | str | tuple[pathlib.Path, str] | tuple[str, str] | None = None,
) -> None:
    """Execute profile table generator with user-defined main-routing.

    This is a variant of `main(...)` in which the main route is predefined based on a flag in the hydro-objects.

    :param water_authority: water authority
    :param fn_target_levels: filename with geospatial data of target levels
    :param fn_hydro_objects: filename with geospatial data of hydro-objects
        File is considered to be located at '<GoodCloud>/<water_authority>/fn_hydro_objects'
    :param col_flag: column-name containing the main route flag
    :param val_flag: value(s) flagging hydro-object as part of the main route, defaults to True
    :param layer_hydro_objects: layer-name with hydro-objects data, defaults to "hydroobjects"
    :param export_profile_tables: export generated profile tables, defaults to True
    :param sync: sync with GoodCloud's 'verwerkt'- and 'Basisgegevens/Hydrotypen'-folders, defaults to True
    :param overwrite: overwrite GoodCloud's 'verwerkt'- and 'Basisgegevens/Hydrotypen'-folders, defaults to False
    :param export_intermediate_output: export intermediate output steps (for debugging), defaults to False
    :param fn_water_bodies: filename with water bodies with specific, user-defined representative depths used to
        overwrite the determined representative depths per hydro-object, defaults to None
        When `water_bodies` (polygons), hydro-objects within the polygon(s) have their representative depth overwritten
        by the depth value(s) in `water_bodies`.
    """
    # sync with the GoodCloud
    cloud = CloudStorage()
    if sync:
        _sync(cloud, water_authority, overwrite, fn_hydro_objects, fn_water_bodies)

    # read files
    gdf_basins = _read_basins(cloud, water_authority)
    gdf_hydro_objects = gpd.read_file(cloud.joinpath(water_authority, fn_hydro_objects), layer=layer_hydro_objects)
    gdf_target_levels = _read_target_levels(cloud, water_authority, fn_target_levels)
    gdf_cross_sections = _read_cross_sections(cloud, water_authority)
    gdf_water_bodies = _read_water_bodies(cloud, water_authority, fn_water_bodies)
    table = _read_hydrotope_table(cloud)

    # settings paths
    fn_bgt = _bgt_path(cloud, water_authority)
    wd_int = _int_output_path(cloud, water_authority, export_intermediate_output)

    # execute profile generation
    profiles_tables = run.main(
        gdf_basins,
        gdf_hydro_objects,
        gdf_cross_sections,
        hydrotope_table=table,
        col_ho_main_route=col_flag,
        val_ho_main_route=val_flag,
        target_levels=gdf_target_levels,
        cloud=cloud,
        fn_bgt=fn_bgt,
        wd_intermediate_output=wd_int,
        water_bodies=gdf_water_bodies,
    )

    # export profile table
    if export_profile_tables:
        _export_profiles(cloud, water_authority, profiles_tables, export_intermediate_output)


def _sync(cloud: CloudStorage, water_authority: str, overwrite: bool, *extra_file: str | None) -> None:
    """Sync with the GoodCloud.

    :param cloud: the GoodCloud
    :param water_authority: water authority
    :param overwrite: overwrite files on the GoodCloud
    :param extra_file: extra files to sync, other than the folders "Verwerkt" and "Basisgegevens/Hydrotypen"
    """
    print("Syncing with the GoodCloud...", end="", flush=True)
    cloud.download_verwerkt(water_authority, overwrite=overwrite)
    cloud.download_basisgegevens(["Hydrotypen"], overwrite=overwrite)
    for f in extra_file:
        if f is None:
            continue
        cloud.download_file(cloud.joinurl(water_authority, f))
    print(f"\rSynced with the GoodCloud: {water_authority}")


def _read_basins(cloud: CloudStorage, water_authority: str) -> gpd.GeoDataFrame:
    """Read geospatial dataset with basins (polygons).

    :param cloud: the GoodCloud
    :param water_authority: water authority

    :return: basin dataset (polygons)
    """
    fn = cloud.joinpath(
        water_authority, "verwerkt", "Work_dir", f"{water_authority}_parameterized", "input", "database.gpkg"
    )
    gdf = gpd.read_file(fn, layer="Basin / area")
    return gdf[gdf["node_id"] == gdf["meta_node_id"]]


def _read_cross_sections(cloud: CloudStorage, water_authority: str) -> gpd.GeoDataFrame:
    """Read geospatial dataset with cross-sections (lines).

    :param cloud: the GoodCloud
    :param water_authority: water authority

    :return: cross-section dataset (lines)
    """
    fn = cloud.joinpath(water_authority, "verwerkt", "profielen", "intermediate", "lines_z.gpkg")
    return gpd.read_file(fn)


def _read_target_levels(cloud: CloudStorage, water_authority: str, fn_target_levels: str) -> gpd.GeoDataFrame:
    """Get polygons with target levels from source-data.

    :param cloud: the GoodCloud
    :param water_authority: water authority
    :param fn_target_levels: filename with target level (geospatial) data

    :return: polygons with target levels
    """
    fn = cloud.joinpath(water_authority, "verwerkt", "Crossings", fn_target_levels)
    polygons = gpd.read_file(fn, layer="peilgebied")
    levels = gpd.read_file(fn, layer="streefpeil")
    out = polygons.assign(meta_streefpeil=polygons["globalid"].map(levels.set_index("globalid")["waterhoogte"]))
    return out


def _read_hydrotope_table(cloud: CloudStorage) -> hydrotopes.HydrotopeTable:
    """Read hydrotope classification table.

    :param cloud: the GoodCloud

    :return: hydrotope-table
    """
    fn = cloud.joinpath("Basisgegevens", "Hydrotypen", "vdGaast_water_depth.csv")
    return hydrotopes.HydrotopeTable.from_csv(fn)


def _read_water_bodies(
    cloud: CloudStorage, water_authority: str, fn_water_bodies: pathlib.Path | str | tuple | None
) -> gpd.GeoDataFrame | None:
    """Read geospatial dataset with water bodies (polygons).

    :param cloud: the GoodCloud
    :param water_authority: water authority
    :param fn_water_bodies: filename of geo-dataset

    :return: water bodies dataset (polygons)
    """
    if fn_water_bodies is None:
        return None
    if isinstance(fn_water_bodies, tuple):
        assert len(fn_water_bodies) == 2
        return gpd.read_file(cloud.joinpath(water_authority, fn_water_bodies[0]), layer=fn_water_bodies[1])
    return gpd.read_file(cloud.joinpath(water_authority, fn_water_bodies))


def _bgt_path(cloud: CloudStorage, water_authority: str) -> pathlib.Path:
    """Absolute file-path (GoodCloud) with BGT-data.

    :param cloud: the GoodCloud
    :param water_authority: water authority

    :return: BGT-path
    """
    return cloud.joinpath(water_authority, "verwerkt", "BGT", f"bgt_{water_authority}_water.gpkg")


def _int_output_path(cloud: CloudStorage, water_authority: str, export: bool) -> pathlib.Path | None:
    """Absolute folder-path (GoodCloud) for intermediate output.

    If intermediate output is not exported, no path is returned (None). This translates in the `run.main(..)`-function
    to not exporting the intermediate output.

    :param cloud: the GoodCloud
    :param water_authority: water authority
    :param export: export intermediate output

    :return: working directory for intermediate output (optional)
    """
    if export:
        return cloud.joinpath(water_authority, "verwerkt", "profielen", "intermediate")


def _export_profiles(
    cloud: CloudStorage,
    water_authority: str,
    profile_tables: tuple[gpd.GeoDataFrame, gpd.GeoDataFrame],
    export_intermediate_output: bool,
) -> None:
    """Export profile tables.

    :param cloud: the GoodCloud
    :param water_authority: water authority
    :param profile_tables: tables with profile data ("doorgaand" & "bergend")
    :param export_intermediate_output: export intermediate output
    """
    # set working directory
    wd = cloud.joinpath(water_authority, "verwerkt", "profielen")
    wd.mkdir(exist_ok=True)

    # export profile tables
    for table, name in zip(profile_tables, ("doorgaand", "bergend"), strict=True):
        fn = wd / f"profielen_{name}.csv"
        table = pd.DataFrame(table[[c for c in table.columns if c != "geometry"]])
        table.to_csv(fn, index=False)
        print(f"File saved: {fn}")

    # upload files to the GoodCloud
    print("Uploading to the GoodCloud...", end="", flush=True)
    cloud.upload_content(wd, overwrite=True)
    if export_intermediate_output:
        (wd / "intermediate").mkdir(exist_ok=True)
        cloud.upload_content(wd / "intermediate", overwrite=True)
    print("\rFiles uploaded to the GoodCloud")
