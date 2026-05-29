"""Generation of profiles table for Hollands Noorderkwartier."""

from ribasim_nl.profiles import implement

from profiles import gen_profiles
from ribasim_nl import Model

if __name__ == "__main__":
    waterschap = "HollandsNoorderkwartier"
    gen_profiles.main(
        waterschap,
        "hhnk_crossings_v26.gpkg",
        cross_sections_available=False,
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
