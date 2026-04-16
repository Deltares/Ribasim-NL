"""Generation of profiles table for Hollandse Delta."""

from profiles import gen_profiles

if __name__ == "__main__":
    gen_profiles.main(
        "HollandseDelta",
        "hd_crossings_v06.gpkg",
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
