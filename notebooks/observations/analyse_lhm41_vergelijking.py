# %%
"""Vergelijking Ribasim LHM vs. LHM 4.1 op decade-basis.

Interactief notebook om AnalyseLHM41Vergelijking uit te voeren voor één waterschap.
Zie ribasim_nl.lhm41_vergelijking voor de volledige implementatie.
"""

from pathlib import Path

from ribasim_nl.analyse_results import AnalyseLHM41Vergelijking

from ribasim_nl import CloudStorage

# %%

cloud = CloudStorage()

waterboard = "RijnenIJssel"
waterboard_model_versions = cloud.uploaded_models(authority=waterboard)
latest_model_version = sorted(
    [i for i in waterboard_model_versions if i.model == waterboard],
    key=lambda x: getattr(x, "sorter", ""),
)[-1]

model_folder = cloud.joinpath(f"{waterboard}/modellen", latest_model_version.path_string)
meas_folder = cloud.joinpath("Basisgegevens/resultaatvergelijking/meetreeksen_2026")
lhm41_folder = cloud.joinpath("Basisgegevens/resultaatvergelijking/LHM4_1_reeksen")
gpkg_koppellaag = cloud.joinpath(
    "Basisgegevens/resultaatvergelijking/Koppeltabel_LHM4_1/Koppeltabel_met_toegevoegde_data_v28_04_2026.gpkg"
)
layer_naam = "koppeling_lhm4_1_reeksen"

# %%
# Synchroniseer model en meetreeksen vanuit de cloud

cloud.synchronize([lhm41_folder, gpkg_koppellaag])

# %%

CRITERIA_LHM41 = {
    ("Afvoer", ">=5"): [
        ("NSE", 0.2, 80, "NSE > 0.2"),
        ("NSE", 0.5, 60, "NSE > 0.5"),
        ("CumJaar", 25, 80, "Cum. jaarafvoer dif. < 25%"),
    ],
    ("Afvoer", "<5"): [
        ("NSE", 0.2, 80, "NSE > 0.2"),
        ("NSE", 0.5, 60, "NSE > 0.5"),
        ("CumJaar", 33, 80, "Cum. jaarafvoer dif. < 33%"),
    ],
    ("Aanvoer", ">=5"): [
        ("NSE", 0.2, 80, "NSE > 0.2"),
        ("NSE", 0.5, 60, "NSE > 0.5"),
        ("CumZomer", 25, 80, "Cum. zomer aanvoer dif. < 25%"),
    ],
    ("Aanvoer", "<5"): [
        ("NSE", 0.2, 80, "NSE > 0.2"),
        ("NSE", 0.5, 60, "NSE > 0.5"),
        ("CumZomer", 50, 80, "Cum. zomer aanvoer dif. < 50%"),
    ],
}


# Tekencorrectie per LHM 4.1 CSV-bestand.
# Gebruik -1 als de LHM-reeks een andere tekenconventie heeft dan Ribasim en de meting.
LHM41_TEKEN_CORRECTIE: dict[str, float] = {
    "Aanvoer Gemaal Winsemius_LHM_reeks.csv": -1.0,
}

output_path = Path(model_folder) / "results" / "Validatie_criteria_lhm4_1.xlsx"

AnalyseLHM41Vergelijking(
    gpkg_koppellaag=gpkg_koppellaag,
    layer_naam=layer_naam,
    model_folder=model_folder,
    meas_folder=meas_folder,
    lhm41_folder=lhm41_folder,
    criteria_lhm41=CRITERIA_LHM41,
    LHM41_TEKEN_CORRECTIE=LHM41_TEKEN_CORRECTIE,
    output_path=output_path,
)

# %%
# ── Upload resultaten naar de cloud ──
#!TODO: nog niet getest
# UPLOAD_cloud = False

# if UPLOAD_cloud:
#     results = Path(model_folder) / "results"
#     if output_path.exists():
#         cloud.upload_file(output_path)
#     gpkg_lhm41 = results / "Validatie_resultaten_lhm41.gpkg"
#     if gpkg_lhm41.exists():
#         cloud.upload_file(gpkg_lhm41)
#     figures_lhm41 = results / "figures_lhm4_1_vergelijking"
#     if figures_lhm41.exists():
#         cloud.upload_content(figures_lhm41)

# %%
