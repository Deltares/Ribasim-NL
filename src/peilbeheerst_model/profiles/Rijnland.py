"""Generation of profiles table for Rijnland."""

from profiles import gen_profiles

if __name__ == "__main__":
    fn_water_bodies = "aangeleverd/Na_levering/20260422_MerenPlassen/Rijnland_meren_plassen_diepte.geojson"
    gen_profiles.main(
        "Rijnland",
        "rijnland_crossings_v05.gpkg",
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
        fn_water_bodies=fn_water_bodies,
    )
