"""Generation of profiles table for Schieland en de Krimpenerwaard."""

from ribasim_nl.profiles import implement

from profiles import gen_profiles
from ribasim_nl import Model

if __name__ == "__main__":
    waterschap = "SchielandendeKrimpenerwaard"
    gen_profiles.main(
        waterschap,
        "hhsk_crossings_voor_profielen_met_OG_hydroobjecten.gpkg",
        sync=True,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
    src_toml = f"data/{waterschap}/modellen/{waterschap}_feedback/ribasim.toml"
    dst_toml = f"data/{waterschap}/modellen/{waterschap}_profiles/ribasim.toml"
    model = Model.read(src_toml)
    print(f"Adding Basin profiles to '{src_toml}'")
    implement.set_basin_profiles(model, waterschap, min_area=10)
    print(f"Writing '{dst_toml}'")
    model.write(dst_toml)
