"""Helpers for preparing daily flow and concentration time series."""

from collections.abc import Sequence

import matplotlib.pyplot as plt
import pandas as pd


def interpolate_timeseries(
    data: pd.DataFrame,
    value_column: str,
    group_columns: Sequence[str],
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
    interpolation_method: str = "ffill",
    before_method: str = "bfill",
    after_method: str = "ffill",
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Interpolate and extrapolate grouped data onto a daily time grid."""
    group_columns = list(group_columns)
    required_columns = {"time", value_column, *group_columns}
    missing_columns = required_columns - set(data.columns)
    if missing_columns:
        raise ValueError(f"Missing required columns: {sorted(missing_columns)}")

    start = pd.Timestamp(start_date).normalize()
    end = pd.Timestamp(end_date).normalize()
    if start > end:
        raise ValueError("end_date must be on or after start_date.")

    if interpolation_method not in {"linear", "ffill"}:
        raise ValueError("interpolation_method must be 'linear' or 'ffill'.")
    valid_fill_methods = {"zero", "bfill", "ffill", "mean"}
    if before_method not in valid_fill_methods or after_method not in valid_fill_methods:
        raise ValueError(f"Fill methods must be one of {sorted(valid_fill_methods)}.")

    daily_index = pd.date_range(start, end, freq="D")
    selected_columns = [*group_columns, "time", value_column]
    prepared = data[selected_columns].copy()
    prepared["time"] = pd.to_datetime(prepared["time"], errors="raise").dt.normalize()
    if prepared.isna().any().any():
        raise ValueError(f"{selected_columns} must not contain missing values.")

    daily_frames = []
    coverage_records = []
    for group_values, group_data in prepared.groupby(group_columns, sort=True, dropna=False):
        if not isinstance(group_values, tuple):
            group_values = (group_values,)

        group_data = group_data.sort_values("time").drop_duplicates("time", keep="last")
        times_in_range = group_data.loc[group_data["time"].between(start, end), "time"]
        records_in_range = len(times_in_range)
        span_fraction = (
            ((times_in_range.max() - times_in_range.min()).days + 1) / len(daily_index) if len(times_in_range) else 0.0
        )
        coverage_records.append(
            dict(
                zip(
                    group_columns,
                    group_values,
                    strict=True,
                ),
                record_count=records_in_range,
                coverage_fraction=records_in_range / len(daily_index),
                span_fraction=span_fraction,
            )
        )

        series = group_data.set_index("time")[value_column].reindex(
            pd.DatetimeIndex(group_data["time"]).union(daily_index)
        )
        series = series.interpolate(method="time") if interpolation_method == "linear" else series.ffill()

        series = _fill_missing(series, before_method, direction="before")
        series = _fill_missing(series, after_method, direction="after").reindex(daily_index)
        daily_frames.append(
            pd.DataFrame(
                {
                    **dict(zip(group_columns, group_values, strict=True)),
                    "time": daily_index,
                    value_column: series.to_numpy(),
                }
            )
        )

    if not daily_frames:
        raise ValueError("data contains no rows.")

    return pd.concat(daily_frames, ignore_index=True), pd.DataFrame(coverage_records)


def _fill_missing(series: pd.Series, method: str, direction: str) -> pd.Series:
    """Fill missing values on one side of a series."""
    filled = series.copy()
    valid_positions = series.notna().to_numpy().nonzero()[0]
    if len(valid_positions) == 0:
        return filled

    positions = range(valid_positions[0]) if direction == "before" else range(valid_positions[-1] + 1, len(series))

    if method == "zero":
        filled.iloc[list(positions)] = 0
    elif method == "mean":
        filled.iloc[list(positions)] = series.mean()
    elif method == "bfill" and direction == "before":
        filled = filled.bfill()
    elif method == "ffill" and direction == "after":
        filled = filled.ffill()
    return filled


def interpolate_flow_dynamic(
    flow_dynamic: pd.DataFrame,
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Interpolate and extrapolate dynamic flows onto a daily time grid."""
    return interpolate_timeseries(
        flow_dynamic,
        value_column="flow_rate",
        group_columns=["node_id"],
        start_date=start_date,
        end_date=end_date,
        interpolation_method="ffill",
        before_method="bfill",
        after_method="ffill",
    )


def interpolate_concentrations(
    concentrations: pd.DataFrame,
    start_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Interpolate and extrapolate concentrations per node and substance."""
    return interpolate_timeseries(
        concentrations,
        value_column="concentration",
        group_columns=["node_id", "substance"],
        start_date=start_date,
        end_date=end_date,
        interpolation_method="ffill",
        before_method="zero",
        after_method="ffill",
    )


def plot_boundary_timeseries(
    original_concentrations: pd.DataFrame,
    daily_concentrations: pd.DataFrame,
    lhm_flow: pd.DataFrame,
    daily_flow: pd.DataFrame,
    node_id: int,
    substance: str,
) -> None:
    """Plot observed/interpolated concentrations and Ribasim/input flows."""
    original_concentration = original_concentrations.loc[
        original_concentrations["node_id"].eq(node_id) & original_concentrations["substance"].eq(substance),
        ["time", "concentration"],
    ].sort_values("time")

    interpolated_concentration = daily_concentrations.loc[
        daily_concentrations["node_id"].eq(node_id) & daily_concentrations["substance"].eq(substance),
        ["time", "concentration"],
    ].sort_values("time")

    fig, ax = plt.subplots(figsize=(14, 5))
    original_concentration.plot(
        x="time",
        y="concentration",
        title=f"{substance} concentration at node {node_id}",
        xlabel="Time",
        ylabel="Concentration [mg/L]",
        marker="o",
        linestyle="",
        grid=True,
        label="Observation data",
        ax=ax,
    )
    interpolated_concentration.plot(
        x="time",
        y="concentration",
        marker="",
        linestyle="-",
        label="Interpolated/extrapolated",
        ax=ax,
    )
    ax.set_xlim(
        interpolated_concentration["time"].min(),
        interpolated_concentration["time"].max(),
    )
    ax.legend()
    fig.tight_layout()
    plt.show()

    ribasim_flow = (
        lhm_flow.loc[lhm_flow["from_node_id"].eq(node_id), ["time", "flow_rate"]]
        .sort_values("time")
        .reset_index(drop=True)
    )

    input_flow = (
        daily_flow.loc[daily_flow["node_id"].eq(node_id), ["time", "flow_rate"]]
        .sort_values("time")
        .reset_index(drop=True)
    )

    fig, ax = plt.subplots(figsize=(14, 6))
    ribasim_flow.plot(
        x="time",
        y="flow_rate",
        ax=ax,
        label="Ribasim output",
        color="tab:blue",
        linewidth=1.5,
        marker="o",
        linestyle="",
        markersize=3,
    )
    input_flow.plot(
        x="time",
        y="flow_rate",
        ax=ax,
        label="Interpolated/extrapolated",
        color="tab:orange",
        linewidth=1.5,
        linestyle="",
        marker=".",
        markersize=3,
    )
    ax.set_title(f"Flow from node {node_id}")
    ax.set_xlabel("Time")
    ax.set_ylabel("Flow rate [m³/s]")
    ax.grid(True)
    ax.legend()
    fig.tight_layout()
    plt.show()
