"""Calculate observed-to-modelled water-quality validation factors."""

import pandas as pd


def calculate_validation_factor(modelled: pd.Series, observed: pd.Series) -> float | None:
    """Return the observed-to-modelled validation factor from annual means.

    Only calendar years with both a modelled and observed annual mean contribute
    to the factor. A factor below one indicates modelled concentrations exceed
    observed concentrations.
    """
    annual_means = pd.concat(
        {
            "modelled": _annual_means(modelled),
            "observed": _annual_means(observed),
        },
        axis="columns",
    ).dropna()
    annual_means = annual_means.loc[annual_means["modelled"] != 0]
    if annual_means.empty:
        return None
    return float(annual_means["observed"].mean() / annual_means["modelled"].mean())


def calculate_annual_validation_factors(modelled: pd.Series, observed: pd.Series) -> pd.Series:
    """Return observed-to-modelled validation factors indexed by calendar year."""
    annual_means = pd.concat(
        {
            "modelled": _annual_means(modelled),
            "observed": _annual_means(observed),
        },
        axis="columns",
    ).dropna()
    annual_means = annual_means.loc[annual_means["modelled"] != 0]
    return annual_means["observed"] / annual_means["modelled"]


def calculate_summer_validation_factor(modelled: pd.Series, observed: pd.Series) -> float | None:
    """Return the observed-to-modelled validation factor for April--September means."""
    summer_means = pd.concat(
        {
            "modelled": _annual_means(modelled, months=range(4, 10)),
            "observed": _annual_means(observed, months=range(4, 10)),
        },
        axis="columns",
    ).dropna()
    summer_means = summer_means.loc[summer_means["modelled"] != 0]
    if summer_means.empty:
        return None
    return float(summer_means["observed"].mean() / summer_means["modelled"].mean())


def _annual_means(values: pd.Series, months: range | None = None) -> pd.Series:
    """Return calendar-year means after dropping invalid timestamps and values."""
    values = pd.to_numeric(values, errors="coerce")
    timestamps = pd.to_datetime(values.index, errors="coerce")
    valid = values.notna() & timestamps.notna()
    if not valid.any():
        return pd.Series(dtype=float)
    if months is not None:
        valid &= timestamps.month.isin(months)
    if not valid.any():
        return pd.Series(dtype=float)
    return values.loc[valid].groupby(timestamps[valid].year).mean()
