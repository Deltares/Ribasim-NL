"""Generation of profiles table for Amstel, Gooi en Vecht."""

from profiles import gen_profiles

if __name__ == "__main__":
    gen_profiles.main(
        "AmstelGooienVecht",
        "agv_crossings_v05.gpkg",
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
