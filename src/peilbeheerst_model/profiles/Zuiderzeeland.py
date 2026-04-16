"""
Generation of profiles table for Zuiderzeeland.

The generation of profiles for Zuiderzeeland should not be executed with the generalised approach as defined in
`src.peilberheest.profiles.gen_profiles.main()`: Instead, its main route is user-defined. Therefore, the built-in
flagged-based method is implemented: `src.peilbeheerst.profiles.gen_profiles.flagged_hydro_objects()`.
"""

from profiles import gen_profiles

if __name__ == "__main__":
    gen_profiles.flagged_hydro_objects(
        "Zuiderzeeland",
        "zzl_crossings_v05.gpkg",
        "aangeleverd/Na_levering/zzl_watergangen_flagged/zzl_watergangen_flagged.gpkg",
        "OWASRTKN",
        val_flag=(10, 31, 27, 73, 8, 74),
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
