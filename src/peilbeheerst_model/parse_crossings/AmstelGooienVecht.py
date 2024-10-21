# %%

from peilbeheerst_model import ParseCrossings, waterschap_data
from ribasim_nl import CloudStorage

# %%
waterschap = "AmstelGooienVecht"
waterschap_struct = waterschap_data[waterschap]

cloud = CloudStorage()
verwerkt_dir = cloud.joinpath(waterschap, "verwerkt")
cloud.download_verwerkt(waterschap)
cloud.download_basisgegevens()

# %%

crossing_settings = waterschap_struct["find_crossings_with_peilgebieden"]
init_settings = waterschap_struct["init"]

init_settings["gpkg_path"] = verwerkt_dir / "postprocessed.gpkg"
init_settings["krw_path"] = cloud.joinpath("Basisgegevens/KRW/KRW_lichamen_per_waterschap.gpkg")
init_settings["output_path"] = verwerkt_dir / "crossings.gpkg"
init_settings["logfile"] = verwerkt_dir / "crossings.log"

# Crossings class initializeren
cross = ParseCrossings(**init_settings)

# Crossings bepalen en wegschrijven
if crossing_settings["filterlayer"] is None:
    df_hydro = cross.find_crossings_with_peilgebieden("hydroobject", **crossing_settings)
    cross.write_crossings(df_hydro)
else:
    df_hydro, df_dsf, df_hydro_dsf = cross.find_crossings_with_peilgebieden("hydroobject", **crossing_settings)
    cross.write_crossings(df_hydro, crossing_settings["filterlayer"], df_dsf, df_hydro_dsf)
