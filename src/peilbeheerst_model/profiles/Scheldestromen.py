"""
Generation of profiles table for Scheldestromen.

The generation of profiles for Scheldestromen should not be executed with the generalised approach as defined in
`src.peilberheest.profiles.gen_profiles.main()`: Instead, its main route is user-defined. Therefore, the built-in
flagged-based method is implemented: `src.peilbeheerst.profiles.gen_profiles.flagged_hydro_objects()`.
"""

from ribasim_nl.profiles import implement

from profiles import gen_profiles
from ribasim_nl import Model

if __name__ == "__main__":
    waterschap = "Scheldestromen"
    gen_profiles.flagged_hydro_objects(
        waterschap,
        "scheldestromen_crossings_v02.gpkg",
        "aangeleverd/Na_levering/Oplevering_Scheldestromen_20240328/Oplevering_20240328.gpkg",
        "categorieoppwaterlichaam",
        val_flag="primair",
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
