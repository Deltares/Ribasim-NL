# %%
"""Aggregate solver performance and convergence statistics per water authority."""

from pathlib import Path

import pandas as pd
import xarray as xr

run_dir = Path("log/runs")

AUTHORITIES = [
    "AaenMaas",
    "AmstelGooienVecht",
    "BrabantseDelta",
    "Delfland",
    "DeDommel",
    "DrentsOverijsselseDelta",
    "HollandseDelta",
    "HollandsNoorderkwartier",
    "HunzeenAas",
    "Rijkswaterstaat",
    "Limburg",
    "Noorderzijlvest",
    "RijnenIJssel",
    "Rijnland",
    "Rivierenland",
    "Scheldestromen",
    "SchielandendeKrimpenerwaard",
    "StichtseRijnlanden",
    "ValleienVeluwe",
    "Vechtstromen",
    "WetterskipFryslan",
    "Zuiderzeeland",
]


def get_results_dir(authority: str) -> Path:
    """Return the results directory for a given authority."""
    return run_dir / authority / "model/results"


def compute_totals(authority: str, ds: xr.Dataset) -> dict:
    """Compute summary statistics for a single authority's solver stats."""
    total_minutes = ds["computation_time"].sum().item() / 1000 / 60
    mean_dt_sec = ds["dt"].mean().item()
    basin_state_path = get_results_dir(authority) / "basin_state.nc"
    n_basins = xr.open_dataset(basin_state_path).sizes["node_id"]
    return {
        "authority": authority,
        "computation_time_min": total_minutes,
        "mean_dt_sec": mean_dt_sec,
        "n_basins": n_basins,
    }


def compute_convergence(authority: str) -> pd.DataFrame:
    """Compute min/median/mean/max convergence over time per node_id."""
    basin_path = get_results_dir(authority) / "basin.nc"
    convergence = xr.open_dataset(basin_path)["convergence"]
    return pd.DataFrame(
        {
            "node_id": convergence.node_id.values,
            "median_convergence": convergence.median(dim="time").values,
            "mean_convergence": convergence.mean(dim="time").values,
            "max_convergence": convergence.max(dim="time").values,
        }
    ).sort_values("mean_convergence", ascending=False)


def main() -> None:
    """Aggregate solver stats and convergence data for all authorities."""
    dfs = []
    totals = []

    for authority in AUTHORITIES:
        solver_stats_path = get_results_dir(authority) / "solver_stats.nc"
        if not solver_stats_path.exists():
            continue
        ds = xr.open_dataset(solver_stats_path)
        df = ds.to_dataframe().reset_index()
        df["authority"] = authority
        dfs.append(df)
        totals.append(compute_totals(authority, ds))

    df_all = pd.concat(dfs, ignore_index=True)
    df_totals = pd.DataFrame(totals)

    # Write performance Excel
    output_path = run_dir / "performance.xlsx"
    with pd.ExcelWriter(output_path) as writer:
        df_totals.to_excel(writer, sheet_name="totals", index=False)
        df_all.to_excel(writer, sheet_name="solver_stats", index=False)
        for authority in AUTHORITIES:
            basin_path = get_results_dir(authority) / "basin.nc"
            if not basin_path.exists():
                continue
            df_conv = compute_convergence(authority)
            df_conv.to_excel(writer, sheet_name=authority[:31], index=False)

    print(df_totals)
    print(f"\nWritten to {output_path}")


if __name__ == "__main__":
    main()
