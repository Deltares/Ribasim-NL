"""Generation of profiles."""

import typing
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ribasim_nl import CloudStorage
from ribasim_nl.profiles import hydrotopes, run

# A file name, or a file name and the name of a layer in it
type FileName = Path | str
type LayeredFileName = FileName | tuple[FileName, str]


def main(
    water_authority: str,
    *,
    hydro_objects: gpd.GeoDataFrame | None = None,
    export_profile_tables: bool = True,
    export_intermediate_output: bool = False,
    cross_sections_available: bool = True,
    fn_water_bodies: LayeredFileName | None = None,
    **kwargs,
) -> None:
    """Execute profile table generator.

    This is the generalised profile-generator in which all common files, directories, etc. are coded to minimise
    overlapping code.

    The crossings, target levels and by default the hydro-objects are read from `<water_authority>/verwerkt/crossings.gpkg`,
    see `pipeline/00c_parse_crossings.py`.

    :param water_authority: water authority
    :param hydro_objects: the hydro-objects, defaults to the layer "hydroobject" of the crossings
    :param export_profile_tables: export generated profile tables, defaults to True
    :param export_intermediate_output: export intermediate output steps (for debugging), defaults to False
    :param cross_sections_available: flag whether cross-sections dataset is available/representative, defaults to True
    :param fn_water_bodies: filename with water bodies with specific, user-defined representative depths used to
        overwrite the determined representative depths per hydro-object, defaults to None
        When `water_bodies` (polygons), hydro-objects within the polygon(s) have their representative depth overwritten
        by the depth value(s) in `water_bodies`.
    :param kwargs: (additional) optional arguments passed on to `.run.main()`
    """
    # read files
    cloud = CloudStorage()
    fn_crossings = _crossings_path(cloud, water_authority)
    gdf_basins = _read_basins(cloud, water_authority)
    gdf_hydro_objects = gpd.read_file(fn_crossings, layer="hydroobject") if hydro_objects is None else hydro_objects
    gdf_crossings = gpd.read_file(fn_crossings, layer="crossings_hydroobject_filtered")
    gdf_target_levels = _read_target_levels(fn_crossings)
    gdf_cross_sections = _read_cross_sections(cloud, water_authority, cross_sections_available)
    gdf_water_bodies = _read_water_bodies(cloud, water_authority, fn_water_bodies)
    table = _read_hydrotope_table(cloud)

    # setting paths
    wd_int = _int_output_path(cloud, water_authority, export_intermediate_output)
    wd_out = _profiles_output_path(cloud, water_authority)

    # select datasets
    data = [gdf_basins, gdf_hydro_objects, gdf_crossings]
    if cross_sections_available and gdf_cross_sections is not None:
        data.append(gdf_cross_sections)
    assert len(data) in (3, 4)

    # execute profile generation
    profile_tables = run.main(
        *data,
        hydrotope_table=table,
        target_levels=gdf_target_levels,
        cloud=cloud,
        wd_intermediate_output=wd_int,
        wd_output=wd_out,
        water_bodies=gdf_water_bodies,
        **kwargs,
    )

    # export profile table
    if export_profile_tables:
        _export_profiles(cloud, water_authority, profile_tables)


def flagged_hydro_objects(
    water_authority: str,
    fn_hydro_objects: Path | str,
    col_flag: str,
    *,
    val_flag: typing.Any = True,
    layer_hydro_objects: str | None = "hydroobjects",
    export_profile_tables: bool = True,
    export_intermediate_output: bool = False,
    cross_sections_available: bool = True,
    fn_water_bodies: LayeredFileName | None = None,
    **kwargs,
) -> None:
    """Execute profile table generator with user-defined main-routing.

    This is a variant of `main(...)` in which the main route is predefined based on a flag in the hydro-objects. The
    target levels are read from `<water_authority>/verwerkt/crossings.gpkg`.

    :param water_authority: water authority
    :param fn_hydro_objects: filename with geospatial data of hydro-objects
        File is considered to be located at '<GoodCloud>/<water_authority>/fn_hydro_objects'
    :param col_flag: column-name containing the main route flag
    :param val_flag: value(s) flagging hydro-object as part of the main route, defaults to True
    :param layer_hydro_objects: layer-name with hydro-objects data, defaults to "hydroobjects"
    :param export_profile_tables: export generated profile tables, defaults to True
    :param export_intermediate_output: export intermediate output steps (for debugging), defaults to False
    :param cross_sections_available: flag whether cross-sections dataset is available/representative, defaults to True
    :param fn_water_bodies: filename with water bodies with specific, user-defined representative depths used to
        overwrite the determined representative depths per hydro-object, defaults to None
        When `water_bodies` (polygons), hydro-objects within the polygon(s) have their representative depth overwritten
        by the depth value(s) in `water_bodies`.
    :param kwargs: (additional) optional arguments passed on to `.run.main()`
    """
    # read files
    cloud = CloudStorage()
    gdf_basins = _read_basins(cloud, water_authority)
    gdf_hydro_objects = gpd.read_file(cloud.joinpath(water_authority, fn_hydro_objects), layer=layer_hydro_objects)
    gdf_target_levels = _read_target_levels(_crossings_path(cloud, water_authority))
    gdf_cross_sections = _read_cross_sections(cloud, water_authority, cross_sections_available)
    gdf_water_bodies = _read_water_bodies(cloud, water_authority, fn_water_bodies)
    table = _read_hydrotope_table(cloud)

    # settings paths
    wd_int = _int_output_path(cloud, water_authority, export_intermediate_output)
    wd_out = _profiles_output_path(cloud, water_authority)

    # select datasets
    data = [gdf_basins, gdf_hydro_objects]
    if cross_sections_available and gdf_cross_sections is not None:
        data.append(gdf_cross_sections)
    assert len(data) in (2, 3)

    # execute profile generation
    profiles_tables = run.main(
        *data,
        hydrotope_table=table,
        col_ho_main_route=col_flag,
        val_ho_main_route=val_flag,
        target_levels=gdf_target_levels,
        cloud=cloud,
        wd_intermediate_output=wd_int,
        wd_output=wd_out,
        water_bodies=gdf_water_bodies,
        **kwargs,
    )

    # export profile table
    if export_profile_tables:
        _export_profiles(cloud, water_authority, profiles_tables)


def _crossings_path(cloud: CloudStorage, water_authority: str) -> Path:
    """The crossings of the hydro-objects with the peilgebieden, with the postprocessed data they were found in.

    :param cloud: the GoodCloud
    :param water_authority: water authority

    :return: path of the crossings
    """
    return cloud.joinpath(water_authority, "verwerkt", "crossings.gpkg")


def _read_basins(cloud: CloudStorage, water_authority: str) -> gpd.GeoDataFrame:
    """Read geospatial dataset with basins (polygons).

    :param cloud: the GoodCloud
    :param water_authority: water authority

    :return: basin dataset (polygons)
    """
    # read data
    fn = cloud.joinpath(water_authority, "modellen", f"{water_authority}_feedback", "input", "database.gpkg")
    nodes = gpd.read_file(fn, layer="Node", fid_as_index=True, use_arrow=True)
    basins = gpd.read_file(fn, layer="Basin / area")

    # exclude storing basins
    non_storing = nodes[nodes["meta_categorie"] != "bergend"].index
    return basins[basins["node_id"].isin(non_storing)]


def _read_cross_sections(cloud: CloudStorage, water_authority: str, available: bool) -> gpd.GeoDataFrame | None:
    """Read geospatial dataset with cross-sections (lines).

    :param cloud: the GoodCloud
    :param water_authority: water authority

    :return: cross-section dataset (lines), if existing (None otherwise)
    """
    # cross-sections flagged as unavailable/non-representative
    if not available:
        return None

    # read cross-sections
    fn = cloud.joinpath(water_authority, "verwerkt", "profielen", "intermediate", "lines_z.gpkg")
    out = gpd.read_file(fn)

    # return cross-sections, if not empty
    if out.empty:
        return None
    return out


def _read_target_levels(fn: Path) -> gpd.GeoDataFrame:
    """Get polygons with target levels from source-data.

    :param fn: GeoPackage with the layers "peilgebied" and "streefpeil"

    :return: polygons with target levels
    """
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
    cloud: CloudStorage, water_authority: str, fn_water_bodies: LayeredFileName | None
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


def _int_output_path(cloud: CloudStorage, water_authority: str, export: bool) -> Path | None:
    """Absolute folder-path (GoodCloud) for intermediate output.

    If intermediate output is not exported, no path is returned (None). This translates in the `run.main(...)`-function
    to not exporting the intermediate output.

    :param cloud: the GoodCloud
    :param water_authority: water authority
    :param export: export intermediate output

    :return: working directory for intermediate output (optional)
    """
    if export:
        return cloud.joinpath(water_authority, "verwerkt", "profielen", "intermediate")
    return None


def _profiles_output_path(cloud: CloudStorage, water_authority: str) -> Path:
    """Absolute folder-path (GoodCloud) for first-class profile outputs.

    Unlike the intermediate output, this directory is always used: it holds the regular profile
    outputs (e.g. the profile tables and the hydro-object network consumed by the network-snapping
    step).

    :param cloud: the GoodCloud
    :param water_authority: water authority

    :return: working directory for profile outputs
    """
    return cloud.joinpath(water_authority, "verwerkt", "profielen")


def _export_profiles(
    cloud: CloudStorage,
    water_authority: str,
    profile_tables: tuple[gpd.GeoDataFrame, gpd.GeoDataFrame],
) -> None:
    """Export profile tables.

    :param cloud: the GoodCloud
    :param water_authority: water authority
    :param profile_tables: tables with profile data ("doorgaand" & "bergend")
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
