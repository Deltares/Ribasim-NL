"""Assign dynamic precipitation and evaporation forcing from LHM zarr budgets to Ribasim Basin nodes."""

import imod
import numpy as np
import pandas as pd
import xarray as xr

from ribasim_nl import Model
from ribasim_nl.assign_offline_budgets import _compute_budgets_per_basin, _crop_to_gdf, split_basin_definitions

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

# Variable names in the LHM zarr store
PRECIPITATION_VAR = "precipitation_mmd"
EVAPORATION_VAR = "makkink_mmd"

# Conversion factor: summed mm/day per cell -> m/s (after dividing by cell count)
MM_PER_DAY_TO_M_PER_S = 1 / 86400 / 1000


def _open_water_factor(times: np.ndarray) -> np.ndarray:
    """Return an array of open-water evaporation correction factors per timestep (Makkink -> open water)."""
    ts = pd.DatetimeIndex(times)
    months = ts.month - 1  # 0-based row index
    decades = np.where(ts.day < 11, 0, np.where(ts.day < 21, 1, 2))
    return EVAP_FACTOR[months, decades]


def _count_cells_per_basin(mask: xr.DataArray, nodata: int = -999) -> pd.Series:
    """Count the number of valid raster cells per basin node_id in a rasterized basin mask.

    Parameters
    ----------
    mask : xr.DataArray
        Rasterized basin mask where each cell value is a node_id, and nodata cells are filled with ``nodata``.
    nodata : int, optional
        Fill value for cells outside any basin, by default -999.

    Returns
    -------
    pd.Series
        Series indexed by node_id with the number of raster cells assigned to each basin.
    """
    flat = mask.values.reshape(-1)
    valid = np.isfinite(flat) & (flat != nodata)
    ids = flat[valid].astype(np.int64)
    unique, counts = np.unique(ids, return_counts=True)
    return pd.Series(counts, index=unique, name="count")


class SetDynamicForcing:
    def __init__(
        self,
        model: Model,
        budgets: xr.Dataset,
        startdate: str,
        enddate: str,
    ) -> None:
        """Set up dynamic precipitation and evaporation forcing for a Ribasim model.

        Parameters
        ----------
        model : Model
            Ribasim model to add forcing to. Note: ``basin.time.df`` will be cleared on ``add()``.
        budgets : xr.Dataset
            LHM zarr dataset containing precipitation and evaporation variables.
        startdate : str
            Model start date (ISO format, e.g. "2000-01-01").
        enddate : str
            Model end date (ISO format, e.g. "2001-01-01").
        """
        self.model = model
        self.budgets = budgets
        self.startdate = startdate
        self.enddate = enddate

    def add(self) -> Model:
        """Compute basin-averaged precipitation and evaporation from LHM zarr and add to model.

        Basins are split into primary and secondary definitions to correctly handle overlapping
        basin areas. Cell counts are summed across both masks for node_ids present in both,
        ensuring correct spatial averaging.

        Returns
        -------
        Model
            Updated Ribasim model with dynamic meteo forcing in ``basin.time``.
        """
        self.model.basin.time.df = None  # reset any existing time forcing before rebuilding

        basins = self.model.basin.area.df
        assert basins is not None
        basin_definition = basins[["node_id", "geometry"]].copy()

        like = _crop_to_gdf(self.budgets[PRECIPITATION_VAR].isel(time=0, drop=True), basin_definition)
        assert isinstance(like, xr.DataArray)

        # Split basins into primary and secondary to handle overlapping basin areas.
        # Exclude node_ids already in primary from secondary so each node_id is counted exactly once.
        primary_basin_definition, secondary_basin_definition = split_basin_definitions(ribasim_model=self.model)
        primary_node_ids = set(primary_basin_definition["node_id"].unique())
        secondary_basin_definition = secondary_basin_definition[
            ~secondary_basin_definition["node_id"].isin(primary_node_ids)
        ]

        primary_basin_mask = imod.prepare.rasterize(
            primary_basin_definition,
            column="node_id",
            like=like,
            fill=-999,
            dtype=np.int32,
        )
        secondary_basin_mask = imod.prepare.rasterize(
            secondary_basin_definition,
            column="node_id",
            like=like,
            fill=-999,
            dtype=np.int32,
        )

        # Crop budgets to the basin extent
        precip_ds = _crop_to_gdf(self.budgets[[PRECIPITATION_VAR]], basin_definition)
        assert isinstance(precip_ds, xr.Dataset)
        evap_ds = _crop_to_gdf(self.budgets[[EVAPORATION_VAR]], basin_definition)
        assert isinstance(evap_ds, xr.Dataset)

        # Sum budgets over raster cells per basin, for primary and secondary masks (no overlap)
        print("compute precipitation per basin")
        primary_precip_df = _compute_budgets_per_basin(precip_ds, primary_basin_mask)
        secondary_precip_df = _compute_budgets_per_basin(precip_ds, secondary_basin_mask)

        print("compute evaporation per basin")
        primary_evap_df = _compute_budgets_per_basin(evap_ds, primary_basin_mask)
        secondary_evap_df = _compute_budgets_per_basin(evap_ds, secondary_basin_mask)

        # Concatenate — no duplicate (node_id, time) rows since secondary excludes primary node_ids
        precip_df = pd.concat([primary_precip_df, secondary_precip_df]).sort_index()
        evap_df = pd.concat([primary_evap_df, secondary_evap_df]).sort_index()

        # Cell counts per basin: combine primary and secondary (disjoint sets, no summing needed)
        cell_counts = pd.concat(
            [_count_cells_per_basin(primary_basin_mask), _count_cells_per_basin(secondary_basin_mask)]
        )

        # Divide summed mm/day by cell count to get the basin-mean, then convert to m/s
        node_ids = precip_df.index.get_level_values("node_id")
        counts_per_row = node_ids.map(cell_counts.to_dict()).values
        precip_series = precip_df[PRECIPITATION_VAR] / counts_per_row * MM_PER_DAY_TO_M_PER_S
        evap_series = evap_df[EVAPORATION_VAR] / counts_per_row * MM_PER_DAY_TO_M_PER_S

        # Apply open-water evaporation correction factor (Makkink -> open water)
        times = evap_series.index.get_level_values("time")
        evap_series = evap_series * _open_water_factor(times.values)

        meteo_df = pd.DataFrame(
            {
                "node_id": precip_series.index.get_level_values("node_id"),
                "time": times,
                "precipitation": precip_series.values,
                "potential_evaporation": evap_series.values,
            }
        )

        return self._add_meteo_to_model(meteo_df)

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
