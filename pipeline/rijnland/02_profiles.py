"""Generation of profiles table for Rijnland."""

import geopandas as gpd
import pandas as pd
from ribasim_nl.profiles import gen_profiles, implement

from ribasim_nl import CloudStorage, Model

if __name__ == "__main__":
    waterschap = "Rijnland"
    cloud = CloudStorage()
    delivery = "aangeleverd/Na_levering/20260422_MerenPlassen"
    fn_water_bodies = f"{delivery}/Rijnland_meren_plassen_diepte.geojson"
    # the hydro-objects, with the connections of the inlets to the boezem that Rijnland delivered in April 2026
    hydro_objects = gpd.read_file(cloud.joinpath(waterschap, "verwerkt/crossings.gpkg"), layer="hydroobject")
    connections = gpd.read_file(cloud.joinpath(waterschap, delivery, "verbindingen_inlaat_boezem.geojson"))
    connections = connections.drop(columns="fid")
    assert connections.crs == hydro_objects.crs
    hydro_objects = pd.concat([hydro_objects, connections], ignore_index=True)
    gen_profiles.main(
        waterschap,
        hydro_objects=hydro_objects,
        export_profile_tables=True,
        export_intermediate_output=True,
        fn_water_bodies=fn_water_bodies,
        col_wb_depth="DIEPTE",
    )
    src_toml = f"data/{waterschap}/modellen/{waterschap}_feedback/ribasim.toml"
    dst_toml = f"data/{waterschap}/modellen/{waterschap}_profiles/ribasim.toml"
    model = Model.read(src_toml)
    print(f"Adding Basin profiles to '{src_toml}'")
    implement.set_basin_profiles(model, waterschap)
    print(f"Writing '{dst_toml}'")
    model.write(dst_toml)
