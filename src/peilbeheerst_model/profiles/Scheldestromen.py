"""
Generation of profiles table for Scheldestromen.

The generation of profiles for Scheldestromen should not be executed with the generalised approach as defined in
`src.peilberheest.profiles.gen_profiles.main()`: Instead, its main route is user-defined. Therefore, the built-in
flagged-based method is implemented: `src.peilbeheerst.profiles.gen_profiles.flagged_hydro_objects()`.
"""

from profiles import gen_profiles

if __name__ == "__main__":
    gen_profiles.flagged_hydro_objects(
        "Scheldestromen",
        "scheldestromen_crossings_v02.gpkg",
        "aangeleverd/Na_levering/Oplevering_Scheldestromen_20240328/Oplevering_20240328.gpkg",
        "categorieoppwaterlichaam",
        val_flag="primair",
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
