"""Generation of profiles table for Hollandse Delta."""

from profiles import gen_profiles

if __name__ == "__main__":
    gen_profiles.flagged_hydro_objects(
        "HollandseDelta",
        "hd_crossings_v06.gpkg",
        "aangeleverd/Eerste_levering/watergangen/HydroObjectWatergangtype.shp",
        "SOORT_OPPE",
        val_flag="hoofdwaterloop",
        layer_hydro_objects=None,
        sync=False,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
