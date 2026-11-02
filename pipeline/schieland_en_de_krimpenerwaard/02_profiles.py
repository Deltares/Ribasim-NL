"""Generation of profiles table for Schieland en de Krimpenerwaard."""

import geopandas as gpd
from ribasim_nl.profiles import gen_profiles, implement

from ribasim_nl import CloudStorage, Model

if __name__ == "__main__":
    waterschap = "SchielandendeKrimpenerwaard"
    cloud = CloudStorage()
    # all hydro-objects, rather than the primary hydro-objects and culverts of the crossings
    hydro_objects = gpd.read_file(
        cloud.joinpath(waterschap, "verwerkt/Parametrisatie_data/HHSK.gpkg"), layer="hydroobject_original"
    )
    gen_profiles.main(
        waterschap,
        hydro_objects=hydro_objects,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
    src_toml = f"data/{waterschap}/modellen/{waterschap}_feedback/ribasim.toml"
    dst_toml = f"data/{waterschap}/modellen/{waterschap}_profiles/ribasim.toml"
    model = Model.read(src_toml)
    print(f"Adding Basin profiles to '{src_toml}'")
    implement.set_basin_profiles(model, waterschap)
    print(f"Writing '{dst_toml}'")
    model.write(dst_toml)
