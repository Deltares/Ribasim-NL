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
    """
    # get files from the cloud
    cloud = CloudStorage()
    if sync:
        print("\rSyncing with the GoodCloud...", end="", flush=True)
        cloud.download_verwerkt(water_authority, overwrite=overwrite)
        cloud.download_basisgegevens(["Hydrotypen"], overwrite=overwrite)
        print("Synced with the GoodCloud: 'Verwerkt' & 'Basisgegevens/Hydrotypen'")

    # read files
    # > basins
    fn_basins = cloud.joinpath(
        water_authority, "verwerkt/Work_dir", f"{water_authority}_parameterized", "input/database.gpkg"
    )
    gdf_basins = gpd.read_file(fn_basins, layer="Basin / area")
    # FIXME: Circular usage of basin-data
    gdf_basins = gdf_basins[gdf_basins["node_id"] == gdf_basins["meta_node_id"]]
    # > crossings, hydro-objects & target levels
    _fn_network = cloud.joinpath(water_authority, "verwerkt", "Crossings", fn_network)
    gdf_hydro_objects = gpd.read_file(_fn_network, layer="hydroobject")
    gdf_crossings = gpd.read_file(_fn_network, layer="crossings_hydroobject_filtered")
    gdf_target_levels = run.target_level_polygons(_fn_network)
    # > cross-sections
    fn_cross_sections = cloud.joinpath(water_authority, "verwerkt", "profielen", "intermediate", "lines_z.gpkg")
    gdf_cross_sections = gpd.read_file(fn_cross_sections)
    # > BGT-data & hydrotopes
    fn_bgt = cloud.joinpath(water_authority, "verwerkt", "BGT", f"bgt_{water_authority}_water.gpkg")
    fn_hydrotopes = cloud.joinpath("Basisgegevens", "Hydrotypen", "vdGaast_water_depth.csv")

    # load hydrotope-table
    table = hydrotopes.HydrotopeTable.from_csv(fn_hydrotopes)

    # intermediate output: working directory
    wd_int = (
        cloud.joinpath(water_authority, "verwerkt", "profielen", "intermediate") if export_intermediate_output else None
    )

    # execute profile generation
    profiles_tables = run.main(
        gdf_basins,
        gdf_hydro_objects,
        gdf_crossings,
        gdf_cross_sections,
        hydrotope_table=table,
        target_levels=gdf_target_levels,
        cloud=cloud,
        fn_bgt=fn_bgt,
        export_intermediate_output=export_intermediate_output,
        wd_intermediate_output=wd_int,
    )

    # export profile table
    if export_profile_tables:
        wd_table = cloud.joinpath(water_authority, "verwerkt", "profielen")
        wd_table.parent.mkdir(exist_ok=True)
        for table, name in zip(profiles_tables, ("doorgaand", "bergend")):
            fn_table = wd_table / f"profielen_{name}.csv"
            table = pd.DataFrame(table[[c for c in table.columns if c != "geometry"]])
            table.to_csv(fn_table, index=False)
            print(f"File saved: {fn_table}")

        # upload profile files
        print("Uploading to the GoodCloud...", end="", flush=True)
        cloud.upload_content(wd_table, overwrite=True)
        if export_intermediate_output:
            cloud.upload_content(wd_table / "intermediate", overwrite=True)
        print("\rFiles uploaded to the GoodCloud")


def flagged_hydro_objects(
    water_authority: str,
    fn_target_levels: str,
    fn_hydro_objects: str | pathlib.Path,
    col_flag: str,
    *,
    val_flag: typing.Any = True,
    layer_hydro_objects: str = "hydroobjects",
    export_profile_tables: bool = True,
    sync: bool = True,
    overwrite: bool = False,
    export_intermediate_output: bool = False,
) -> None:
    """Execute profile table generator with user-defined main-routing.

    This is a variant of `main(..)` in which the main route is predefined based on a flag in the hydro-objects.

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
    """
    # get files from the cloud
    cloud = CloudStorage()
    if sync:
        print("\rSyncing with the GoodCloud...", end="", flush=True)
        cloud.download_verwerkt(water_authority, overwrite=overwrite)
        cloud.download_basisgegevens(["Hydrotypen"], overwrite=overwrite)
        cloud.download_file(cloud.joinurl(water_authority, str(fn_hydro_objects)))
        print("Synced with the GoodCloud: 'Verwerkt', 'Basisgegevens/Hydrotypen' & `fn_hydro_objects`")

    # read files
    # > basins
    fn_basins = cloud.joinpath(
        water_authority, "verwerkt/Work_dir", f"{water_authority}_parameterized", "input/database.gpkg"
    )
    gdf_basins = gpd.read_file(fn_basins, layer="Basin / area")
    # FIXME: Circular usage of basin-data
    gdf_basins = gdf_basins[gdf_basins["node_id"] == gdf_basins["meta_node_id"]]
    # > hydro-objects
    gdf_hydro_objects = gpd.read_file(cloud.joinpath(water_authority, fn_hydro_objects), layer=layer_hydro_objects)
    # > target levels
    _fn_target_levels = cloud.joinpath(water_authority, "verwerkt", "Crossings", fn_target_levels)
    gdf_target_levels = run.target_level_polygons(_fn_target_levels)
    # > cross-sections
    fn_cross_sections = cloud.joinpath(water_authority, "verwerkt", "profielen", "intermediate", "lines_z.gpkg")
    gdf_cross_sections = gpd.read_file(fn_cross_sections)
    # > BGT-data & hydrotopes
    fn_bgt = cloud.joinpath(water_authority, "verwerkt", "BGT", f"bgt_{water_authority}_water.gpkg")
    fn_hydrotopes = cloud.joinpath("Basisgegevens", "Hydrotypen", "vdGaast_water_depth.csv")

    # load hydrotope-table
    table = hydrotopes.HydrotopeTable.from_csv(fn_hydrotopes)

    # intermediate output: working directory
    wd_int = (
        cloud.joinpath(water_authority, "verwerkt", "profielen", "intermediate") if export_intermediate_output else None
    )

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
        export_intermediate_output=export_intermediate_output,
        wd_intermediate_output=wd_int,
    )

    # export profile table
    if export_profile_tables:
        wd_table = cloud.joinpath(water_authority, "verwerkt", "profielen")
        wd_table.parent.mkdir(exist_ok=True)
        for table, name in zip(profiles_tables, ("doorgaand", "bergend")):
            fn_table = wd_table / f"profielen_{name}.csv"
            table = pd.DataFrame(table[[c for c in table.columns if c != "geometry"]])
            table.to_csv(fn_table, index=False)
            print(f"File saved: {fn_table}")

        # upload profile files
        print("Uploading to the GoodCloud...", end="", flush=True)
        cloud.upload_content(wd_table, overwrite=True)
        if export_intermediate_output:
            cloud.upload_content(wd_table / "intermediate", overwrite=True)
        print("\rFiles uploaded to the GoodCloud")
