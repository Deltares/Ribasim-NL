"""
Generation of profiles table for Zuiderzeeland.

The generation of profiles for Zuiderzeeland cannot be executed with the generalised approach as defined in
`src.peilberheest.profiles.gen_profiles.main()`. The `main()`-function below does have a lot in common with this
function, but contains subtle yet crucial differences.
"""

from profiles import gen_profiles

if __name__ == "__main__":
    gen_profiles.main(
        "Zuiderzeeland",
        "zzl_crossings_v05.gpkg",
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
