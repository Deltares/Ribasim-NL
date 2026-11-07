"""
Generation of profiles table for Zuiderzeeland.

The generation of profiles for Zuiderzeeland cannot be executed with the generalised approach as defined in
`src.peilberheest.profiles.gen_profiles.main()`. The `main()`-function below does have a lot in common with this
function, but contains subtle yet crucial differences.
"""

from ribasim_nl.profiles import gen_profiles, implement

from ribasim_nl import Model

if __name__ == "__main__":
    waterschap = "Zuiderzeeland"
    gen_profiles.main(
        waterschap,
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
