"""Generation of profiles table for Hollandse Delta."""

import geopandas as gpd
import pandas as pd

from src.ribasim_nl.profiles import run
from src.ribasim_nl.ribasim_nl.cloud import CloudStorage


def main(
    *, export_profile_tables: bool = True, overwrite: bool = False, export_intermediate_output: bool = False
) -> None:
    """Execute profile table generator for Hollandse Delta.

    :param export_profile_tables: export profile tables as *.csv-files, defaults to True
    :param overwrite: overwrite GoodCloud-data, defaults to False
    :param export_intermediate_output: export intermediate output for checking/debugging, defaults to False

    :type export_profile_tables: bool, optional
    :type overwrite: bool, optional
    :type export_intermediate_output: bool, optional
    """
    water_authority = "HollandseDelta"

    # get files from the cloud
    cloud = CloudStorage()
    cloud.download_verwerkt(water_authority, overwrite)
    cloud.download_basisgegevens(["Hydrotypen"], overwrite)

    # read files
    # > basins
    fn_basins = cloud.joinpath(water_authority, "modellen", f"{water_authority}_parameterized", "database.gpkg")
    gdf_basins = gpd.read_file(fn_basins, layer="Basin / area")
    # > crossings
    fn_crossings = cloud.joinpath(water_authority, "verwerkt", "Crossings", "hd_crossings_v06.gpkg")
    gdf_crossings = gpd.read_file(fn_crossings, layer="crossings_hydroobject_filtered")
    # > hydro-objects
    fn_hydro_objects = cloud.joinpath(water_authority, "verwerkt", "Crossings", "hd_crossings_v06.gpkg")
    gdf_hydro_objects = gpd.read_file(fn_hydro_objects, layer="hydroobject")
    # > cross-sections
    fn_cross_sections = cloud.joinpath(water_authority, "verwerkt", "profielen", "intermediate", "lines_z.gpkg")
    gdf_cross_sections = gpd.read_file(fn_cross_sections)
    # > BGT-data
    fn_bgt = cloud.joinpath(water_authority, "verwerkt", "BGT", f"bgt_{water_authority}_water.gpkg")
    # > hydrotopes
    fn_hydrotopes = cloud.joinpath("Basisgegevens", "Hydrotypen", "vdGaast_water_depth.csv")

    # execute profile generation
    profiles_tables = run.main(
        gdf_basins,
        gdf_crossings,
        gdf_hydro_objects,
        gdf_cross_sections,
        cloud=cloud,
        fn_bgt=fn_bgt,
        fn_hydrotopes=fn_hydrotopes,
        export_intermediate_output=export_intermediate_output,
        wd_intermediate_output=cloud.joinpath(water_authority, "verwerkt", "profielen", "intermediate"),
    )

    # export profile table
    if export_profile_tables:
        wd_table = cloud.joinpath(water_authority, "verwerkt", "profielen")
        wd_table.parent.mkdir(exist_ok=True)
        for table, name in zip(profiles_tables, ("doorgaande", "bergend")):
            fn_table = wd_table / f"profielen_{name}.csv"
            table = pd.DataFrame(table[[c for c in table.columns if c != "geometry"]])
            table.to_csv(fn_table, index=False)

        # upload profile files
        cloud.upload_content(wd_table, overwrite=True)
        if export_intermediate_output:
            cloud.upload_content(wd_table / "intermediate", overwrite=True)


if __name__ == "__main__":
    main(export_profile_tables=True, export_intermediate_output=True)
