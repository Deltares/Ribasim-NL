"""Create annual measured-versus-modelled validation scatter plots."""

from math import ceil
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from matplotlib.ticker import FuncFormatter
from ribasim import Model

PARAMETER_COMPONENTS = {
    "Ntot": ["NO3", "NH4", "OON"],
    "Ptot": ["PO4", "AAP", "OOP"],
}
N_COLUMNS = 2


def create_validation_scatterplots(
    model_path: Path,
    validation_path: Path,
    output_folder: Path,
    location_limit: int | str,
) -> list[Path]:
    """Create one annual measured-versus-modelled scatter grid per parameter."""
    if location_limit != "all" and (not isinstance(location_limit, int) or location_limit < 1):
        raise ValueError('location_limit must be a positive integer or "all".')

    delwaq_map_path = model_path / "delwaq_all_sources" / "delwaq_map.nc"
    toml_path = model_path / "lhm_coupled.toml"
    mapping_path = validation_path / "output_validate" / "basins_doorgaand_joined_monlocs.shp"
    observations_path = validation_path / "KRWMeetwaarden_1990_2025_20260615_1432.parquet"
    for path, description in [
        (delwaq_map_path, "DELWAQ map output"),
        (toml_path, "Ribasim model"),
        (mapping_path, "Monitoring-to-node mapping"),
        (observations_path, "Monitoring data"),
    ]:
        if not path.is_file():
            raise FileNotFoundError(f"{description} not found: {path}")

    output_folder.mkdir(parents=True, exist_ok=True)
    dataset = xr.open_dataset(delwaq_map_path)
    try:
        model = Model.read(toml_path)
        mapping = gpd.read_file(mapping_path).dropna(subset=["loc_Code", "node_id"])
        observations = pd.read_parquet(observations_path)
        observations["datum"] = pd.to_datetime(observations["datum"], format="%d-%m-%Y", errors="coerce")
        model_start = pd.Timestamp(dataset["nTimesDlwq"].min().item())
        model_end = pd.Timestamp(dataset["nTimesDlwq"].max().item())
        observations = observations.loc[observations["datum"].between(model_start, model_end)].copy()
        values_by_parameter = _collect_annual_means(dataset, model, mapping, observations, location_limit)
    finally:
        dataset.close()

    output_paths = []
    for parameter, values_by_year in values_by_parameter.items():
        if not values_by_year:
            continue
        figure = _create_scatter_grid(values_by_year, parameter)
        output_path = output_folder / f"validation_scatterplots_{parameter}.png"
        figure.savefig(output_path, dpi=200, bbox_inches="tight")
        plt.close(figure)
        output_paths.append(output_path)
    if not output_paths:
        raise ValueError("No positive measured and modelled annual mean concentrations could be calculated.")
    return output_paths


def _collect_annual_means(
    dataset: xr.Dataset,
    model: Model,
    mapping: gpd.GeoDataFrame,
    observations: pd.DataFrame,
    location_limit: int | str,
) -> dict[str, dict[int, list[tuple[float, float]]]]:
    """Collect positive annual measured and modelled mean pairs by parameter."""
    values_by_parameter: dict[str, dict[int, list[tuple[float, float]]]] = {
        parameter: {} for parameter in PARAMETER_COMPONENTS
    }
    locations = mapping[["loc_Code", "node_id"]].drop_duplicates().sort_values("loc_Code")
    if location_limit != "all":
        locations = locations.head(location_limit)
    segment_cache: dict[int, int] = {}

    for location in locations.itertuples(index=False):
        node_id = int(location.node_id)
        if node_id not in model.node.df.index:
            continue
        segment_index = _get_segment_index(dataset, model, node_id, segment_cache)
        if segment_index is None:
            continue
        location_observations = observations.loc[observations["KRW_monitoringslocatie"] == location.loc_Code]
        for parameter, components in PARAMETER_COMPONENTS.items():
            observed = location_observations.loc[location_observations["parameter"] == parameter].set_index("datum")[
                "meetwaarde"
            ]
            modelled = sum(
                dataset[f"ribasim_{component}"].isel(ribasim_nNodes=segment_index) for component in components
            ).load()
            annual_means = pd.concat(
                {
                    "measured": _annual_means(observed),
                    "modelled": _annual_means(modelled.to_series()),
                },
                axis="columns",
            ).dropna()
            annual_means = annual_means.loc[(annual_means["measured"] > 0) & (annual_means["modelled"] > 0)]
            for year, pair in annual_means.iterrows():
                values_by_parameter[parameter].setdefault(int(year), []).append(
                    (float(pair["measured"]), float(pair["modelled"]))
                )
    return values_by_parameter


def _get_segment_index(dataset: xr.Dataset, model: Model, node_id: int, segment_cache: dict[int, int]) -> int | None:
    """Return the DELWAQ segment co-located with a Ribasim node."""
    if node_id in segment_cache:
        return segment_cache[node_id]
    node = model.node.df.loc[node_id]
    distance_squared = (dataset["ribasim_node_x"] - node.geometry.x) ** 2 + (
        dataset["ribasim_node_y"] - node.geometry.y
    ) ** 2
    segment_index = int(distance_squared.argmin().item())
    distance = float(distance_squared.isel(ribasim_nNodes=segment_index).item() ** 0.5)
    if distance > 1e-3:
        return None
    segment_cache[node_id] = segment_index
    return segment_index


def _annual_means(values: pd.Series) -> pd.Series:
    """Return calendar-year means after dropping invalid timestamps and values."""
    values = pd.to_numeric(values, errors="coerce")
    timestamps = pd.to_datetime(values.index, errors="coerce")
    valid = values.notna() & timestamps.notna()
    if not valid.any():
        return pd.Series(dtype=float)
    return values.loc[valid].groupby(timestamps[valid].year).mean()


def _create_scatter_grid(values_by_year: dict[int, list[tuple[float, float]]], parameter: str) -> plt.Figure:
    """Create one measured-versus-modelled scatter panel for every calendar year."""
    panels = [(str(year), values) for year, values in sorted(values_by_year.items())]
    n_rows = ceil(len(panels) / N_COLUMNS)
    figure, axes = plt.subplots(n_rows, N_COLUMNS, figsize=(5.2 * N_COLUMNS, 4.5 * n_rows), squeeze=False)
    figure.suptitle(f"Validation scatter plots ({parameter})", fontweight="bold", x=0.01, ha="left")
    for axis, (label, values) in zip(axes.flat, panels, strict=False):
        _plot_scatter(axis, np.asarray(values), label)
    for axis in axes.flat[len(panels) :]:
        axis.set_visible(False)
    figure.tight_layout(rect=(0, 0, 1, 0.97))
    return figure


def _plot_scatter(axis: plt.Axes, values: np.ndarray, label: str) -> None:
    """Plot measured values against modelled values with 95 percent agreement limits."""
    measured = values[:, 0]
    modelled = values[:, 1]
    log_ratios = np.log(modelled / measured)
    agreement_factor = float(np.exp(1.96 * np.std(log_ratios, ddof=0)))
    lower_limit = 1 / agreement_factor

    plot_min = float(min(measured.min(), modelled.min()) / 1.2)
    plot_max = float(max(measured.max(), modelled.max()) * 1.2)
    line_values = np.geomspace(plot_min, plot_max, 200)
    axis.scatter(measured, modelled, color="#2ca02c", s=20, alpha=0.8, edgecolors="none")
    axis.plot(line_values, line_values, color="black", linewidth=1.5, label="1:1")
    axis.plot(line_values, lower_limit * line_values, color="black", linestyle="--", linewidth=1.5)
    axis.plot(line_values, agreement_factor * line_values, color="black", linestyle="--", linewidth=1.5)
    axis.set(
        title=f"Validation scatter plot {label}\n95% agreement limits: / {agreement_factor:.2f}",
        xlabel="Measured concentration",
        ylabel="Modelled concentration",
        xscale="log",
        yscale="log",
        xlim=(plot_min, plot_max),
        ylim=(plot_min, plot_max),
        aspect="equal",
    )
    decimal_formatter = FuncFormatter(lambda value, _: f"{value:g}")
    axis.xaxis.set_major_formatter(decimal_formatter)
    axis.xaxis.set_minor_formatter(decimal_formatter)
    axis.yaxis.set_major_formatter(decimal_formatter)
    axis.yaxis.set_minor_formatter(decimal_formatter)
    axis.grid(which="both", alpha=0.25)
