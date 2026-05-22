from os import PathLike
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
import xarray as xr

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib

if TYPE_CHECKING:
    from ribasim_nl.model import Model


def _resolve_toml_and_results_dir(model: Model | str | PathLike[str]) -> tuple[Path, Path]:
    if isinstance(model, (str, PathLike)):
        toml_path = Path(model).resolve()
        if not toml_path.exists():
            raise FileNotFoundError(f"Model TOML file does not exist: {toml_path}")

        with toml_path.open("rb") as file:
            config = tomllib.load(file)

        return toml_path, Path(config.get("results_dir", "results"))

    toml_path = Path(model.toml_path).resolve()
    return toml_path, Path(model.results_dir)


def _compute_node_performance(basin_path: Path) -> pd.DataFrame:
    with xr.open_dataset(basin_path) as ds:
        convergence = ds["convergence"].load()

    return pd.DataFrame(
        {
            "node_id": convergence.node_id.values,
            "median_convergence": convergence.median(dim="time").values,
            "mean_convergence": convergence.mean(dim="time").values,
            "max_convergence": convergence.max(dim="time").values,
        }
    ).sort_values("mean_convergence", ascending=False)


def write_performance(
    model: Model | str | PathLike[str],
    output_path: str | PathLike[str] | None = None,
) -> Path:
    """Write performance.xlsx next to a model TOML file.

    Parameters
    ----------
    model : Model or path-like
        A ``ribasim_nl.Model`` instance or a path to a model TOML file.
    output_path : path-like, optional
        Output Excel path. Defaults to ``performance.xlsx`` next to the TOML.

    Returns
    -------
    Path
        The written Excel file path.
    """
    toml_path, results_dir = _resolve_toml_and_results_dir(model)
    results_path = toml_path.parent / results_dir

    solver_stats_path = results_path / "solver_stats.nc"
    basin_path = results_path / "basin.nc"

    if not solver_stats_path.exists():
        raise FileNotFoundError(f"solver_stats.nc not found: {solver_stats_path}")
    if not basin_path.exists():
        raise FileNotFoundError(f"basin.nc not found: {basin_path}")

    if output_path is None:
        output_path = toml_path.with_name("performance.xlsx")
    output_path = Path(output_path).resolve()

    with xr.open_dataset(solver_stats_path) as ds:
        time_df = ds.to_dataframe().reset_index()

    nodes_df = _compute_node_performance(basin_path)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path) as writer:
        time_df.to_excel(writer, sheet_name="time", index=False)
        nodes_df.to_excel(writer, sheet_name="nodes", index=False)

    return output_path
