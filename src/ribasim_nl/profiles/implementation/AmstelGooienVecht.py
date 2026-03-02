"""Generation of profiles table for Amstel, Gooi en Vecht."""

import geopandas as gpd
import pandas as pd

from profiles import run
from ribasim_nl import CloudStorage


def main(
    *, export_profile_table: bool = True, overwrite: bool = False, export_intermediate_output: bool = False
) -> None:
    water_authority = "AmstelGooienVecht"

    # get files from the cloud
    cloud = CloudStorage()
    cloud.download_verwerkt(water_authority, overwrite)
    cloud.download_basisgegevens(["Hydrotypen"], overwrite)

    # read files
    # > basins
    fn_basins = cloud.joinpath(water_authority, "modellen", f"{water_authority}_parameterized", "database.gpkg")
    gdf_basins = gpd.read_file(fn_basins, layer="Basin / area")
    # > crossings
    fn_crossings = cloud.joinpath(water_authority, "verwerkt", "Crossings", "agv_crossings_v05.gpkg")
    gdf_crossings = gpd.read_file(fn_crossings, layer="crossings_hydroobject_filtered")
    # > hydro-objects
    fn_hydro_objects = cloud.joinpath(water_authority, "verwerkt", "Crossings", "agv_crossings_v05.gpkg")
    gdf_hydro_objects = gpd.read_file(fn_hydro_objects, layer="hydroobject")
    # # > cross-sections
    # fn_cross_sections = None
    # gdf_cross_sections = gpd.read_file(fn_cross_sections, layer='profielpunt')
    # > BGT-data
    fn_bgt = cloud.joinpath(water_authority, "verwerkt", "BGT", f"bgt_{water_authority}_water.gpkg")
    # > hydrotopes
    fn_hydrotopes = cloud.joinpath("Basisgegevens", "Hydrotypen", "vdGaast_water_depth.csv")

    # execute profile generation
    profiles_table = run.main(
        gdf_basins,
        gdf_crossings,
        gdf_hydro_objects,
        cloud=cloud,
        fn_bgt=fn_bgt,
        fn_hydrotopes=fn_hydrotopes,
        export_intermediate_output=export_intermediate_output,
        wd_intermediate_output=cloud.joinpath(water_authority, "verwerkt", "profielen", "intermediate"),
    )
    if sum(profiles_table["area"].isna()) > 0:
        print("NaN-values present in profile-table!")
        if not input("Continue? [y/n] ") == "y":
            raise KeyboardInterrupt

    # export profile table
    if export_profile_table:
        fn_table = cloud.joinpath(water_authority, "verwerkt", "profielen", "profiel_tabel.csv")
        fn_table.parent.mkdir(exist_ok=True)
        table = pd.DataFrame(profiles_table[[c for c in profiles_table.columns if c != "geometry"]])
        table.to_csv(fn_table, index=False)


if __name__ == "__main__":
    main(export_profile_table=True, export_intermediate_output=True)
