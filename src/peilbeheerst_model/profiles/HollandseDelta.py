"""Generation of profiles table for Hollandse Delta."""

from ribasim_nl.profiles import implement

from profiles import gen_profiles
from ribasim_nl import Model

if __name__ == "__main__":
    waterschap = "HollandseDelta"
    gen_profiles.flagged_hydro_objects(
        waterschap,
        "hd_crossings_v06.gpkg",
        "aangeleverd/Eerste_levering/watergangen/HydroObjectWatergangtype.shp",
        "SOORT_OPPE",
        val_flag="hoofdwaterloop",
        layer_hydro_objects=None,
        sync=True,
        export_profile_tables=True,
        export_intermediate_output=True,
    )
    src_toml = f"data/{waterschap}/modellen/{waterschap}_feedback/ribasim.toml"
    dst_toml = f"data/{waterschap}/modellen/{waterschap}_profiles/ribasim.toml"
    model = Model.read(src_toml)
    print(f"Adding Basin profiles to '{src_toml}'")
    implement.set_basin_profiles(model, waterschap, min_area=1000)
    print(f"Writing '{dst_toml}'")
    model.write(dst_toml)
