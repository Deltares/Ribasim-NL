"""Generation of profiles table for Rijnland."""

from ribasim_nl.profiles import implement

from profiles import gen_profiles
from ribasim_nl import Model

if __name__ == "__main__":
    waterschap = "Rijnland"
    fn_water_bodies = "aangeleverd/Na_levering/20260422_MerenPlassen/Rijnland_meren_plassen_diepte.geojson"
    gen_profiles.main(
        waterschap,
        "rijnland_crossings_v05.gpkg",
        sync=True,
        export_profile_tables=True,
        export_intermediate_output=True,
        fn_water_bodies=fn_water_bodies,
        col_wb_depth="DIEPTE",
    )
    src_toml = f"data/{waterschap}/modellen/{waterschap}_feedback/ribasim.toml"
    dst_toml = f"data/{waterschap}/modellen/{waterschap}_profiles/ribasim.toml"
    model = Model.read(src_toml)
    print(f"Adding Basin profiles to '{src_toml}'")
    implement.set_basin_profiles(model, waterschap, min_area=1000)
    print(f"Writing '{dst_toml}'")
    model.write(dst_toml)
