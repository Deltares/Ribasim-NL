# %%
"""Set the constants of the measured foreign inflows in `buitenlandse_aanvoer.csv` to the medians of their measurements.

Not a DVC stage: run it after changing the measurements in `data/Basisgegevens/BuitenlandseAanvoer`, and commit the
updated overview. See `ribasim_nl.transboundary_inflow`.
"""

import pandas as pd
from ribasim_nl.transboundary_inflow import OVERVIEW_CSV, median_flow_rates, read_overview, read_timeseries

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
