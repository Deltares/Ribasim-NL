"""Generation of profiles table for Delfland."""

from ribasim_nl.profiles import implement

from profiles import gen_profiles
from ribasim_nl import Model

if __name__ == "__main__":
    waterschap = "Delfland"
    gen_profiles.main(
        waterschap,
        "delfland_crossings_v08.gpkg",
        export_profile_tables=True,
        export_intermediate_output=True,
    )
    src_toml = f"data/{waterschap}/modellen/{waterschap}_feedback/ribasim.toml"
    dst_toml = f"data/{waterschap}/modellen/{waterschap}_profiles/ribasim.toml"
    model = Model.read(src_toml)
    print(f"Adding Basin profiles to '{src_toml}'")
    implement.set_basin_profiles(model, waterschap)
    print(f"Writing '{dst_toml}'")
    model.write(dst_toml)
