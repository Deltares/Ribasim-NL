# %%
from pathlib import Path

import pandas as pd
import xarray as xr

from ribasim_nl import Model

DEFAULT_TOML_FILE = Path(r"d:\repositories\Ribasim-NL\data\Limburg\modellen\Limburg_performance_test\limburg.toml")


def infer_authority(toml_file: str | Path) -> str:
    """Infer the water authority from a model toml path under data/<authority>/modellen/..."""
    toml_file = Path(toml_file).resolve()
    try:
        data_index = toml_file.parts.index("data")
    except ValueError as exc:
        raise ValueError(f"Cannot infer authority from toml path: {toml_file}") from exc
    return toml_file.parts[data_index + 1]


def get_results_dir(toml_file: str | Path) -> Path:
    """Return the results directory for a Ribasim model toml file."""
    return Path(toml_file).resolve().parent / "results"


def get_solver_stats_path(toml_file: str | Path) -> Path:
    """Return the solver_stats.nc path for a model toml file."""
    return get_results_dir(toml_file) / "solver_stats.nc"


def get_basin_path(toml_file: str | Path) -> Path:
    """Return the basin.nc path for a model toml file."""
    return get_results_dir(toml_file) / "basin.nc"


def get_basin_state_path(toml_file: str | Path) -> Path:
    """Return the basin_state.nc path for a model toml file."""
    return get_results_dir(toml_file) / "basin_state.nc"


def compute_totals(authority: str, ds: xr.Dataset, basin_state_path: str | Path | None = None) -> dict[str, object]:
    """Compute summary statistics for a single model's solver stats."""
    total_minutes = ds["computation_time"].sum().item() / 1000 / 60
    mean_dt_sec = ds["dt"].mean().item()

    n_basins = pd.NA
    if basin_state_path is not None:
        basin_state_path = Path(basin_state_path)
        if basin_state_path.exists():
            with xr.open_dataset(basin_state_path) as basin_state_ds:
                n_basins = basin_state_ds.sizes["node_id"]

    return {
        "authority": authority,
        "computation_time_min": total_minutes,
        "mean_dt_sec": mean_dt_sec,
        "n_basins": n_basins,
    }


def compute_convergence(basin_path: str | Path) -> pd.DataFrame:
    """Compute min/median/mean/max convergence over time per node_id."""
    basin_path = Path(basin_path)
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


def write_model_performance(
    toml_file: str | Path,
    authority: str | None = None,
    output_path: str | Path | None = None,
) -> Path:
    """Write performance.xlsx next to a model toml file based on its results directory."""
    toml_file = Path(toml_file).resolve()
    authority = infer_authority(toml_file) if authority is None else authority
    solver_stats_path = get_solver_stats_path(toml_file)
    basin_path = get_basin_path(toml_file)
    basin_state_path = get_basin_state_path(toml_file)

    if not solver_stats_path.exists():
        raise FileNotFoundError(f"solver_stats.nc not found: {solver_stats_path}")

    if output_path is None:
        output_path = toml_file.with_name("performance.xlsx")
    output_path = Path(output_path).resolve()

    with xr.open_dataset(solver_stats_path) as ds:
        solver_stats_df = ds.to_dataframe().reset_index()
        solver_stats_df["authority"] = authority
        totals_df = pd.DataFrame([compute_totals(authority, ds, basin_state_path)])

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(output_path) as writer:
        totals_df.to_excel(writer, sheet_name="totals", index=False)
        solver_stats_df.to_excel(writer, sheet_name="solver_stats", index=False)
        if basin_path.exists():
            compute_convergence(basin_path).to_excel(writer, sheet_name=authority[:31], index=False)

    return output_path


def run_limburg_performance_test(
    toml_file: str | Path = DEFAULT_TOML_FILE,
    authority: str | None = None,
    output_model_dir: str = "Limburg_performmance_test",
) -> Path:
    """Patch the model, run it, and write performance.xlsx next to the patched toml."""
    model = Model.read(toml_file)
    model.run()
    performance_path = write_model_performance(toml_file, authority=authority)
    return performance_path


def main() -> None:
    performance_path = run_limburg_performance_test()
    print(f"Performance written to {performance_path}")


if __name__ == "__main__":
    main()
