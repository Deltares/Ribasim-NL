"""Generation of profiles table for Scheldestromen."""

from profiles import gen_profiles

if __name__ == "__main__":
    gen_profiles.main(
        "Scheldestromen",
        "scheldestromen_crossings_v02.gpkg",
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
