"""Generation of profiles table for Rijnland."""

from profiles import gen_profiles

if __name__ == "__main__":
    gen_profiles.main(
        "Rijnland",
        "rijnland_crossings_v04.gpkg",
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
