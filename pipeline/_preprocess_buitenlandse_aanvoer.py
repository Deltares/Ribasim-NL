# %%
"""Derive the unmeasured Bovenmark 2 inflow, and set the constants of the measured foreign inflows to their medians.

Not a DVC stage: run it after changing the measurements in `data/Basisgegevens/BuitenlandseAanvoer`, and commit the
updated `buitenlandse_aanvoer.csv`. See `ribasim_nl.transboundary_inflow`.
"""

import pandas as pd
from ribasim_nl.transboundary_inflow import (
    OVERVIEW_CSV,
    TIMESERIES_DIR,
    median_flow_rates,
    read_overview,
    read_timeseries,
)

# %%
# DM and LSM take the Belgian inflow of the Mark as 0.92 times its flow at Blauwe Kamer (Ulvenhout), which is
# downstream of both Bovenmark 1 and 2 (#892). Bovenmark 1 is measured, Bovenmark 2 gets the remainder.
# The Wielhoef term of the DM relation is the inflow "Aa of Weerijs Belgie", so it is not counted again here.
blauwe_kamer = read_timeseries("aangeleverd/JCARATRACE_uur.csv", "BlauweKamer")
bovenmark_1 = read_timeseries("aangeleverd/BrabantseDelta_dag.csv", "Bovenmark 1 Belgie")
bovenmark_2 = (0.92 * blauwe_kamer - bovenmark_1).dropna().clip(lower=0).round(3).rename("Bovenmark 2 Belgie")
bovenmark_2_csv = TIMESERIES_DIR / "verwerkt/Bovenmark_2_Belgie.csv"
bovenmark_2_csv.parent.mkdir(exist_ok=True)
bovenmark_2.to_csv(bovenmark_2_csv, index_label="Datum", date_format="%Y-%m-%d %H:%M:%S")

# %%
overview = read_overview()
measured = overview.loc[overview["timeseries"].notna()]
constants = pd.DataFrame.from_dict(
    {
        inflow_id: median_flow_rates(read_timeseries(timeseries, column))
        for inflow_id, timeseries, column in zip(
            measured.index, measured["timeseries"], measured["timeseries_column"], strict=True
        )
    },
    orient="index",
)
print(constants.join(measured["name"]))
overview.update(constants)
overview.to_csv(OVERVIEW_CSV)
