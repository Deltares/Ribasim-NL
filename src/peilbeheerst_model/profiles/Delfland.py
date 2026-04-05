"""Generation of profiles table for Delfland."""

from profiles import gen_profiles

if __name__ == "__main__":
    gen_profiles.main(
        "Delfland",
        "delfland_crossings_v08.gpkg",
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
