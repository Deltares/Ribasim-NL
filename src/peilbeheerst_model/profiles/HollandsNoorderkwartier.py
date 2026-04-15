"""Generation of profiles table for Hollands Noorderkwartier."""

from profiles import gen_profiles

if __name__ == "__main__":
    gen_profiles.main(
        "HollandsNoorderkwartier",
        "hhnk_crossings_v26.gpkg",
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
