"""Generation of profiles table for Schieland en de Krimpenerwaard."""

from profiles import gen_profiles

if __name__ == "__main__":
    gen_profiles.main(
        "SchielandendeKrimpenerwaard",
        "hhsk_crossings_voor_profielen_met_OG_hydroobjecten.gpkg",
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
