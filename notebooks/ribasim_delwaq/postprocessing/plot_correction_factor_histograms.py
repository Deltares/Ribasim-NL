"""Create annual KRW validation-factor histograms for DELWAQ validation."""

from math import ceil
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from ribasim import Model
from validation_factors import calculate_annual_validation_factors

PARAMETER_COMPONENTS = {
    "Ntot": ["NO3", "NH4", "OON"],
    "Ptot": ["PO4", "AAP", "OOP"],
}
N_COLUMNS = 2


def create_correction_factor_histograms(
    model_path: Path,
    validation_path: Path,
    output_folder: Path,
    location_limit: int | str,
) -> list[Path]:
    """Create one annual histogram grid per parameter and return its paths.

    Factors are calculated as the annual mean monitored concentration divided
    by the annual mean DELWAQ concentration, once for every mapped location.
    The caller provides paths and either a positive location count or ``"all"``.
    Histogram values are the untransformed observed-to-modelled ratios.
    """
    if location_limit != "all" and (not isinstance(location_limit, int) or location_limit < 1):
        raise ValueError('location_limit must be a positive integer or "all".')
    delwaq_map_path = model_path / "delwaq" / "delwaq_map.nc"
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

        factors_by_parameter = _collect_annual_ratios(dataset, model, mapping, observations, location_limit)
    finally:
        dataset.close()

    output_paths = []
    for parameter, factors_by_year in factors_by_parameter.items():
        if not factors_by_year:
            continue
        figure = _create_histogram_grid(factors_by_year, parameter)
        output_path = output_folder / f"correction_factors_krw_tussenevaluatie_{parameter}.png"
        figure.savefig(output_path, dpi=200, bbox_inches="tight")
        plt.close(figure)
        output_paths.append(output_path)
    if not output_paths:
        raise ValueError("No positive validation factors could be calculated for the mapped monitoring locations.")
    return output_paths


def _collect_annual_ratios(
    dataset: xr.Dataset,
    model: Model,
    mapping: gpd.GeoDataFrame,
    observations: pd.DataFrame,
    location_limit: int | str,
) -> dict[str, dict[int, list[float]]]:
    """Collect observed-to-modelled ratios by parameter, year, and location."""
    factors_by_parameter: dict[str, dict[int, list[float]]] = {parameter: {} for parameter in PARAMETER_COMPONENTS}
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
            annual_factors = calculate_annual_validation_factors(modelled.to_series(), observed)
            for year, factor in annual_factors.loc[annual_factors > 0].items():
                factors_by_parameter[parameter].setdefault(int(year), []).append(float(factor))
    return factors_by_parameter


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


def _create_histogram_grid(factors_by_year: dict[int, list[float]], parameter: str) -> plt.Figure:
    """Create annual and grand-total histogram panels in the supplied layout."""
    panels = [(str(year), values) for year, values in sorted(factors_by_year.items())]
    panels.append(("Grand Total", [factor for values in factors_by_year.values() for factor in values]))
    n_rows = ceil(len(panels) / N_COLUMNS)
    figure, axes = plt.subplots(n_rows, N_COLUMNS, figsize=(4.8 * N_COLUMNS, 3.8 * n_rows), squeeze=False)
    figure.suptitle(f"{parameter}: Cobserved / Cmodelled KRW-tussenevaluatie", fontweight="bold")
    for axis, (label, factors) in zip(axes.flat, panels, strict=False):
        _plot_histogram(axis, np.asarray(factors), label)
    for axis in axes.flat[len(panels) :]:
        axis.set_visible(False)
    figure.tight_layout(rect=(0, 0, 1, 0.97))
    return figure


def _plot_histogram(axis: plt.Axes, values: np.ndarray, label: str) -> None:
    """Plot one validation-factor histogram with the KRW summary statistics."""
    rmse = float(np.sqrt(np.mean((values - 1) ** 2)))
    bin_width = 0.5
    bin_start = max(float(values.min()), np.floor(values.min() / bin_width) * bin_width)
    bin_end = np.ceil(values.max() / bin_width) * bin_width
    bins = np.arange(bin_start, bin_end + bin_width, bin_width)

    axis.hist(values, bins=bins, color="#4c93bd", edgecolor="white")
    axis.axvline(1, color="#2e8b57", linewidth=1.5)
    for threshold, color in [(0.25, "#3366cc"), (0.50, "#dc3545")]:
        axis.axvline(1 - threshold, color=color, linestyle="--", linewidth=1.2)
        axis.axvline(1 + threshold, color=color, linestyle="--", linewidth=1.2)
    axis.set(
        title=(f"Histogram of {label}\nRMSE: {rmse:.4f}"),
        xlabel="Cobserved / Cmodelled",
        ylabel="Frequency",
        xlim=(0, 10),
    )
    axis.grid(axis="y", alpha=0.25)
