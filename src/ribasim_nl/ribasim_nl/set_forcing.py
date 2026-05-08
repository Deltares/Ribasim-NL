"""Assign dynamic precipitation and evaporation forcing from LHM zarr budgets to Ribasim Basin nodes."""

import imod
import numpy as np
import pandas as pd
import xarray as xr

from ribasim_nl import Model
from ribasim_nl.assign_offline_budgets import _compute_budgets_per_basin, _crop_to_gdf

# Makkink to open water evaporation factor, depending on the month of the year (rows)
# and the decade in the month, starting at day 1, 11, 21 (cols). As used in Mozart.
EVAP_FACTOR = np.array(
    [
        [0.00, 0.50, 0.70],
        [0.80, 1.00, 1.00],
        [1.20, 1.30, 1.30],
        [1.30, 1.30, 1.30],
        [1.31, 1.31, 1.31],
        [1.30, 1.30, 1.30],
        [1.29, 1.27, 1.24],
        [1.21, 1.19, 1.18],
        [1.17, 1.17, 1.17],
        [1.00, 0.90, 0.80],
        [0.80, 0.70, 0.60],
        [0.00, 0.00, 0.00],
    ]
)


def _open_water_factor(times: np.ndarray) -> np.ndarray:
    """Return an array of open water evaporation factors for each timestep."""
    ts = pd.DatetimeIndex(times)
    months = ts.month - 1  # 0-based row index
    decades = np.where(ts.day < 11, 0, np.where(ts.day < 21, 1, 2))
    return EVAP_FACTOR[months, decades]


# Variable names in the LHM zarr store
PRECIPITATION_VAR = "precipitation_mmd"
EVAPORATION_VAR = "evaporation_mmd"  # not yet available; zeros used as placeholder


class SetDynamicForcing:
    def __init__(
        self,
        model: Model,
        budgets: xr.Dataset,
        startdate: str,
        enddate: str,
    ) -> None:
        self.model = model
        self.budgets = budgets
        self.startdate = startdate
        self.enddate = enddate

    def add(self) -> Model:
        """Compute basin-averaged precipitation and evaporation from LHM zarr and add to model."""
        basins = self.model.basin.area.df
        basin_definition = basins[["node_id", "geometry"]].copy()

        basin_mask = imod.prepare.rasterize(
            basin_definition,
            column="node_id",
            like=_crop_to_gdf(self.budgets[PRECIPITATION_VAR].isel(time=0, drop=True), basin_definition),
            fill=-999,
            dtype=np.int32,
        )

        # Build precipitation dataset
        precip_ds = _crop_to_gdf(self.budgets[[PRECIPITATION_VAR]], basin_definition)

        # Build evaporation dataset: use real data if available, else zeros
        if EVAPORATION_VAR in self.budgets:
            evap_ds = _crop_to_gdf(self.budgets[[EVAPORATION_VAR]], basin_definition)
        else:
            # Placeholder: zeros shaped like precipitation
            evap_da = xr.zeros_like(precip_ds[PRECIPITATION_VAR])
            evap_da.name = EVAPORATION_VAR
            evap_ds = evap_da.to_dataset()

        # Compute basin-averaged values (mm/day per basin)
        # _compute_budgets_per_basin sums over cells; divide by cell count to get the mean
        print("compute precipitation per basin")
        precip_df = _compute_budgets_per_basin(precip_ds, basin_mask)
        print("compute evaporation per basin")
        evap_df = _compute_budgets_per_basin(evap_ds, basin_mask)

        # Count cells per basin for averaging
        mask_flat = basin_mask.values.reshape(-1)
        valid = np.isfinite(mask_flat) & (mask_flat != -999)
        ids = mask_flat[valid].astype(np.int64)
        unique_ids, counts = np.unique(ids, return_counts=True)
        cell_counts = pd.Series(counts, index=unique_ids, name="count")
        # Broadcast cell counts to match the MultiIndex (node_id, time)
        node_ids = precip_df.index.get_level_values("node_id")
        counts_per_row = node_ids.map(cell_counts).values

        # Convert summed mm/day -> averaged mm/day -> m/s
        mm_per_day_to_m_per_s = 1 / 86400 / 1000
        precip_series = precip_df[PRECIPITATION_VAR] / counts_per_row * mm_per_day_to_m_per_s
        evap_series = evap_df[EVAPORATION_VAR] / counts_per_row * mm_per_day_to_m_per_s

        # Apply open-water evaporation factor
        times = evap_series.index.get_level_values("time")
        evap_factors = _open_water_factor(times.values)
        evap_series = evap_series * evap_factors

        # Build meteo DataFrame
        meteo_df = pd.DataFrame(
            {
                "node_id": precip_series.index.get_level_values("node_id"),
                "time": times,
                "precipitation": precip_series.values,
                "potential_evaporation": evap_series.values,
            }
        )

        new_model = self._add_meteo_to_model(meteo_df)
        return new_model

    def _add_meteo_to_model(self, meteo_means: pd.DataFrame) -> Model:
        """
        Add dynamic meteo information to an existing Ribasim model

        Add the meteo information (dynamic) to the model. If a basin.time.df is already specified, the precipitation and evaporation values are replaced.
        If a mode.basin.time.df is not present, it is created and the drainage and infiltration fluxes are set to 0
        """
        model = self.model
        if model.basin.time.df is None:
            if model.basin.static.df is not None:
                final_time_df = meteo_means.merge(
                    model.basin.static.df[["node_id", "drainage", "infiltration"]], on="node_id", how="left"
                )
            else:
                meteo_means["drainage"] = 0
                meteo_means["infiltration"] = 0
                final_time_df = meteo_means.copy()
            model.basin.time.df = final_time_df  # pyrefly: ignore[bad-assignment]
            model.basin.time.df.fillna(0, inplace=True)  # pyrefly: ignore[missing-attribute]
        else:
            current_df = model.basin.time.df
            current_df["conv_time"] = pd.to_datetime(current_df["time"])
            current_df = current_df.merge(
                meteo_means,
                left_on=["node_id", "conv_time"],
                right_on=["node_id", "time"],
                how="left",
                suffixes=("_existing", "_new"),
            )
            current_df.rename(
                columns={
                    "time_existing": "time",
                    "potential_evaporation_new": "potential_evaporation",
                    "precipitation_new": "precipitation",
                },
                inplace=True,
            )
            current_df.drop(
                columns=["time_new", "conv_time", "potential_evaporation_existing", "precipitation_existing"],
                inplace=True,
            )
            model.basin.time.df = current_df
            model.basin.time.df.fillna(0, inplace=True)

        # Reset the static information
        model.basin.static.df = None

        # Set the start and end date of the model
        model.starttime = self.startdate  # pyrefly: ignore[bad-assignment]
        model.endtime = self.enddate  # pyrefly: ignore[bad-assignment]
        print("Dynamic meteo added to model")
        return model
