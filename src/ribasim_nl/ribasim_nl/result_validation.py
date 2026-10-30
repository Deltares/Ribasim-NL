"""Regression checks on the results of a model run, which supplement the input checks of `ribasim_nl.validation`.

`result_metrics` summarizes a run in a few numbers: solver statistics, how well the simulated discharges match the
measurements of the Observation nodes, see `ribasim_nl.goodness_of_fit`, and the discharges and levels at key points
of the main water system.
`validate_results` checks them against `LIMITS`. The limits are loose bounds around the results of a run of
`lhm_coupled`, so that they catch large unintended regressions rather than the noise of small changes. Change them
when a change of the results is intended, and explain why in the pull request.
"""

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from ribasim_nl.goodness_of_fit import compare_observations, read_table
from ribasim_nl.webmap import model_results_dir, read_config

# Metrics per group, as written to the metrics file of DVC
Metrics = dict[str, dict[str, float]]

MS_PER_HOUR = 3_600_000


@dataclass(frozen=True)
class Limit:
    """Bounds of a metric, outside of which it has regressed."""

    min: float = -np.inf
    max: float = np.inf

    def __post_init__(self) -> None:
        assert self.min < self.max, f"Empty limit {self}"

    def issue(self, name: str, value: float) -> str | None:
        """Describe how a metric is out of bounds, if it is."""
        if not np.isfinite(value):
            return f"{name} is {value}"
        if value < self.min:
            return f"{name} is {value:.4g}, below the minimum of {self.min:.4g}"
        if value > self.max:
            return f"{name} is {value:.4g}, above the maximum of {self.max:.4g}"
        return None


@dataclass(frozen=True)
class KeyLevel:
    """A Basin of the main water system, of which the 1st and 99th percentile of the levels should be in a range."""

    node_id: int
    name: str
    low: float  # m NAP
    high: float  # m NAP


# Basins of Rijkswaterstaat, with a range around the target levels of the Peilbesluiten
KEY_LEVELS = (
    KeyLevel(8006803, "IJsselmeer", -0.6, 0.1),  # -0.40 in winter, -0.10 to -0.40 in summer
    KeyLevel(8005792, "Markermeer", -0.65, 0.0),  # -0.40 in winter, -0.10 to -0.30 in summer
    KeyLevel(8008077, "Veluwemeer", -0.4, 0.2),  # -0.05
    KeyLevel(8004210, "Noordzeekanaal", -0.6, -0.2),  # -0.40
    KeyLevel(8005018, "Amsterdam-Rijnkanaal Breukelen", -0.6, -0.2),  # -0.40, the same section as the Noordzeekanaal
    KeyLevel(8002609, "Volkerak", -0.3, 0.3),  # 0.00
    KeyLevel(8001814, "Haringvliet", -0.3, 0.6),
    KeyLevel(8010592, "Twentekanaal (Kanaal Zutphen-Enschede)", 9.8, 10.4),  # 10.00 between Eefde and Delden
    KeyLevel(8008147, "Julianakanaal", 43.8, 44.3),  # 44.00 above Born
)

# Observation nodes of the main water system, by name, with the minimum Kling-Gupta efficiency of the discharge
KEY_DISCHARGES = {
    "Lobith": 0.99,  # the Rhine boundary, so it checks the forcing
    "Pannerdense Kop": 0.75,  # Waal
    "IJsselkop": 0.75,  # IJssel
    "Driel boven": 0.7,  # Nederrijn
    "Hagestein boven": 0.7,  # Lek
    "Tiel Waal": 0.75,
    "Haringvlietsluizen binnen": 0.6,
    "Maasmond": 0.1,
    "Den Oever buiten": 0.1,  # Afsluitdijk
    "Kornwerderzand buiten": 0.0,  # Afsluitdijk
    "IJmuiden binnen": 0.0,  # Noordzeekanaal
}

LIMITS: dict[str, dict[str, Limit]] = {
    # Around 2.2e6, 173 s, 100 s and 0.07 in October 2026, with DiscreteControl evaluated every 6 hours. The
    # computation time is not checked, since it depends on the machine and its load, while these counts hardly do.
    "solver": {
        "rhs_calls": Limit(max=2.8e6),
        "mean_dt": Limit(min=135.0),  # s
        "dt_p05": Limit(min=65.0),  # s, the 5th percentile of the mean timestep per day
        "rejected_fraction": Limit(max=0.15),  # of the timesteps
    },
    # 639 Observations with a median NSE of -0.05 and KGE of 0.06
    "observations": {
        "compared": Limit(min=600),
        "median_nse": Limit(min=-0.2),
        "median_kge": Limit(min=-0.1),
    },
    "discharge_kge": {name: Limit(min=min_kge) for name, min_kge in KEY_DISCHARGES.items()},
    "level_p01": {key.name: Limit(min=key.low) for key in KEY_LEVELS},
    "level_p99": {key.name: Limit(max=key.high) for key in KEY_LEVELS},
}


def solver_metrics(solver_stats: xr.Dataset) -> dict[str, float]:
    """Summarize the solver statistics of `solver_stats.nc`, with the computation time in hours."""
    stats = solver_stats.to_dataframe()
    assert len(stats) > 0, "No solver statistics"
    accepted = stats["accepted_timesteps"].sum()
    rejected = stats["rejected_timesteps"].sum()
    assert accepted > 0, "No accepted timesteps"
    # dt is the mean timestep per row, so dt times the accepted timesteps is the simulated time of the row
    simulated = (stats["dt"] * stats["accepted_timesteps"]).sum()
    return {
        # rounded, since it varies between runs, and the metrics file is tracked by git
        "computation_time": round(float(stats["computation_time"].sum() / MS_PER_HOUR), 1),
        "rhs_calls": float(stats["rhs_calls"].sum()),
        "mean_dt": float(simulated / accepted),
        "dt_p05": float(stats["dt"].quantile(0.05)),
        "rejected_fraction": float(rejected / (accepted + rejected)),
    }


def observation_metrics(comparison: pd.DataFrame) -> dict[str, float]:
    """Summarize `compare_observations` in the number of compared Observations and their median efficiencies."""
    compared = comparison.dropna(subset=["nse"])
    return {
        "compared": float(len(compared)),
        "median_nse": float(compared["nse"].median()),
        "median_kge": float(compared["kge"].median()),
    }


def key_discharge_metrics(comparison: pd.DataFrame) -> dict[str, float]:
    """The KGE of the `KEY_DISCHARGES` in `compare_observations`, which is NaN if it is missing."""
    by_name = comparison.reset_index().drop_duplicates("name", keep=False).set_index("name")["kge"]
    return {name: float(by_name.get(name, np.nan)) for name in KEY_DISCHARGES}


def key_level_metrics(database: Path, results_dir: Path) -> tuple[dict[str, float], dict[str, float]]:
    """The 1st and 99th percentile of the levels of the `KEY_LEVELS`, by name."""
    with closing(sqlite3.connect(database)) as con:
        names = read_table(con, "Node", "node_id, name").set_index("node_id")["name"]
    node_ids = [key.node_id for key in KEY_LEVELS]
    for key in KEY_LEVELS:
        assert names.get(key.node_id) == key.name, f"Basin #{key.node_id} is not named {key.name}"
    with xr.open_dataset(results_dir / "basin.nc") as ds:
        level = ds["level"].sel(node_id=node_ids).to_pandas()
    assert isinstance(level, pd.DataFrame)
    p01, p99 = level.quantile(0.01), level.quantile(0.99)
    return (
        {key.name: float(p01[key.node_id]) for key in KEY_LEVELS},
        {key.name: float(p99[key.node_id]) for key in KEY_LEVELS},
    )


def result_metrics(toml_path: Path) -> tuple[Metrics, pd.DataFrame]:
    """Summarize the results of a model run in the metrics of `LIMITS`, also return `compare_observations`."""
    config, _, database = read_config(toml_path)
    results_dir = model_results_dir(toml_path, config)
    assert results_dir.is_dir(), f"No results in {results_dir}"
    with xr.open_dataset(results_dir / "solver_stats.nc") as ds:
        solver = solver_metrics(ds)
    comparison = compare_observations(database, results_dir)
    level_p01, level_p99 = key_level_metrics(database, results_dir)
    metrics = {
        "solver": solver,
        "observations": observation_metrics(comparison),
        "discharge_kge": key_discharge_metrics(comparison),
        "level_p01": level_p01,
        "level_p99": level_p99,
    }
    return metrics, comparison


def result_issues(metrics: Metrics, limits: dict[str, dict[str, Limit]] = LIMITS) -> list[str]:
    """Describe the metrics that are out of their limits, or missing."""
    issues = []
    for group, group_limits in limits.items():
        for name, limit in group_limits.items():
            value = metrics.get(group, {}).get(name, np.nan)
            if issue := limit.issue(f"{group} {name}", value):
                issues.append(issue)
    return issues


def validate_results(metrics: Metrics, limits: dict[str, dict[str, Limit]] = LIMITS) -> None:
    """Check the metrics of `result_metrics` against their limits and report all regressions together."""
    issues = result_issues(metrics, limits)
    if issues:
        raise ValueError("Ribasim-NL result validation failed:\n- " + "\n- ".join(issues))
