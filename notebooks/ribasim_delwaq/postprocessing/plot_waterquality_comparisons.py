"""Create reusable observed-versus-DELWAQ water-quality comparison plots."""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import xarray as xr


def create_comparison_figure(
    dataset: xr.Dataset,
    observations: pd.DataFrame,
    segment_index: int,
    location_code: str,
    location_name: str | float,
    node_id: int,
    parameter: str,
    components: list[str],
) -> plt.Figure:
    """Create one observed-versus-DELWAQ concentration figure."""
    figure, axis = plt.subplots(figsize=(10, 3.5))
    display_name = location_name if pd.notna(location_name) else location_code
    modelled = sum(
        dataset[f"ribasim_{component}"].isel(ribasim_nNodes=segment_index) for component in components
    ).load()
    observed = observations.loc[observations["parameter"] == parameter]
    axis.plot(modelled["nTimesDlwq"], modelled, color="#1677b8", label="DELWAQ")
    axis.scatter(observed["datum"], observed["meetwaarde"], color="#5c4033", s=18, label="Monitoring", zorder=3)
    modelled_seasonal_means = seasonal_means(modelled.to_series())
    observed_seasonal_means = seasonal_means(observed.set_index("datum")["meetwaarde"])
    axis.scatter(
        modelled_seasonal_means.index,
        modelled_seasonal_means,
        color="#2e8b57",
        marker="D",
        s=25,
        label="DELWAQ seasonal mean",
        zorder=4,
    )
    axis.scatter(
        observed_seasonal_means.index,
        observed_seasonal_means,
        color="#f28e2b",
        marker="D",
        s=25,
        label="Monitoring seasonal mean",
        zorder=4,
    )
    axis.set_title(f"{parameter}: {display_name} ({location_code}, Ribasim node {node_id})")
    axis.set_xlabel("Time")
    axis.set_ylabel(f"Concentration [{observed['eenheid'].iloc[0]}]")
    axis.grid(alpha=0.25)
    axis.legend(loc="upper right")
    figure.tight_layout()
    return figure


def save_comparison_plot(
    output_path: Path,
    dataset: xr.Dataset,
    observations: pd.DataFrame,
    segment_index: int,
    location_code: str,
    location_name: str | float,
    node_id: int,
    parameter: str,
    components: list[str],
) -> None:
    """Create and save one observed-versus-DELWAQ concentration plot."""
    figure = create_comparison_figure(
        dataset,
        observations,
        segment_index,
        location_code,
        location_name,
        node_id,
        parameter,
        components,
    )
    figure.savefig(output_path, dpi=150, bbox_inches="tight")
    plt.close(figure)


def seasonal_means(values: pd.Series) -> pd.Series:
    """Return means for April--September and October--March periods."""
    values = pd.to_numeric(values, errors="coerce").dropna()
    timestamps = pd.DatetimeIndex(values.index)
    season_start_year = timestamps.year - (timestamps.month <= 3)
    season_start_month = pd.Series(10, index=values.index).where(
        ~timestamps.month.isin(range(4, 10)),
        4,
    )
    season_start = pd.DatetimeIndex(
        pd.to_datetime(
            {
                "year": season_start_year.to_numpy(),
                "month": season_start_month.to_numpy(),
                "day": 1,
            }
        )
    )
    return values.groupby(season_start).mean()
