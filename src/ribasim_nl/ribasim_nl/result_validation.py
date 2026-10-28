"""Regression checks on the results of a model run, which supplement the input checks of `ribasim_nl.validation`.

`result_metrics` summarizes a run in a few numbers: solver statistics, how well the simulated discharges match the
measurements of the Observation nodes, and the discharges and levels at key points of the main water system.
`validate_results` checks them against `LIMITS`. The limits are loose bounds around the results of a run of
`lhm_coupled`, so that they catch large unintended regressions rather than the noise of small changes. Change them
when a change of the results is intended, and explain why in the pull request.
"""

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import xarray as xr

from ribasim_nl.webmap import model_results_dir, read_config

# Metrics per group, as written to the metrics file of DVC
Metrics = dict[str, dict[str, float]]

# Observations need this many days with both a measured and a simulated value to be compared
MIN_COMPARED_DAYS = 365
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
    KeyLevel(8005792, "Markermeer", -0.6, 0.0),  # -0.40 in winter, -0.10 to -0.30 in summer
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


def compare(observed: pd.Series, simulated: pd.Series) -> dict[str, float]:
    """Goodness of fit of a simulated to an observed time series, on the times at which both have a value.

    The Nash-Sutcliffe (NSE) and Kling-Gupta efficiencies (KGE) are NaN if there are fewer than
    `MIN_COMPARED_DAYS` values, or if the observations are constant. The bias is relative to the mean absolute
    observation.
    """
    assert observed.index.is_unique and simulated.index.is_unique, "Duplicate times"
    both = pd.concat({"observed": observed, "simulated": simulated}, axis=1, join="inner").dropna()
    o, s = both["observed"], both["simulated"]
    result = {"days": float(len(both)), "nse": np.nan, "kge": np.nan, "bias": np.nan}
    if len(both) < MIN_COMPARED_DAYS or o.std() == 0:
        return result
    result["nse"] = float(1 - ((s - o) ** 2).sum() / ((o - o.mean()) ** 2).sum())
    if o.abs().mean() > 0:
        result["bias"] = float((s.mean() - o.mean()) / o.abs().mean())
    if o.mean() != 0:
        # a constant simulation does not correlate
        r = np.corrcoef(o, s)[0, 1] if s.std() > 0 else 0.0
        result["kge"] = float(1 - np.sqrt((r - 1) ** 2 + (s.std() / o.std() - 1) ** 2 + (s.mean() / o.mean() - 1) ** 2))
    return result


def _read_table(con: sqlite3.Connection, table: str, columns: str) -> pd.DataFrame:
    """Read columns of a table of a Ribasim database, which is empty if the table does not exist."""
    exists = con.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone()
    if exists is None:
        return pd.DataFrame(columns=[column.strip() for column in columns.split(",")])
    return pd.read_sql_query(f'SELECT {columns} FROM "{table}"', con)  # noqa: S608


def compare_observations(database: Path, results_dir: Path) -> pd.DataFrame:
    """Compare each Observation node with the simulated values of the node it observes, see `compare`.

    Like in the model viewer, the simulated flow rate of a node is its total outflow, which for a Junction is the
    flow through it, and other variables are those of an observed Basin.

    :return: a table by the node ID of the Observation, with its name, the observed node and the result of `compare`
    """
    with closing(sqlite3.connect(database)) as con:
        node = _read_table(con, "Node", "node_id, name, node_type").set_index("node_id")
        link = _read_table(con, "Link", "link_id, from_node_id, to_node_id, link_type")
        observed_values = _read_table(con, "Observation / time", "node_id, variable, time, value")
    observed_values["time"] = pd.to_datetime(observed_values["time"])

    observation_links = link[link["link_type"] == "observation"]
    observed = dict(zip(observation_links["from_node_id"], observation_links["to_node_id"], strict=True))
    assert len(observed) == len(observation_links), "Observation nodes must observe one node"
    node_types = dict(zip(node.index, node["node_type"], strict=True))
    names = dict(zip(node.index, node["name"], strict=True))
    flow_links = link[link["link_type"] == "flow"]
    outflows = flow_links.groupby("from_node_id")["link_id"].agg(list)

    flow_rate = pd.DataFrame()
    link_ids = sorted({link_id for node_id in observed.values() for link_id in outflows.get(node_id, [])})
    if link_ids:
        with xr.open_dataset(results_dir / "flow.nc") as ds:
            flow_rate = ds["flow_rate"].sel(link_id=link_ids).to_pandas()
        assert isinstance(flow_rate, pd.DataFrame)

    rows = []
    for key, values in observed_values.groupby(["node_id", "variable"]):
        observation_id, variable = cast(tuple[int, str], key)
        if observation_id not in observed:
            continue
        node_id = observed[observation_id]
        if node_types[node_id] == "Basin":
            with xr.open_dataset(results_dir / "basin.nc") as ds:
                simulated = ds[variable].sel(node_id=node_id).to_series()
        elif variable == "flow_rate":
            simulated = flow_rate[outflows[node_id]].sum(axis=1)
        else:
            raise ValueError(f"Cannot compare the {variable} of {node_types[node_id]} #{node_id}")
        rows.append(
            {
                "node_id": observation_id,
                "name": names[observation_id],
                "variable": variable,
                "observed_node_id": node_id,
                **compare(values.set_index("time")["value"], simulated),
            }
        )
    columns = ["node_id", "name", "variable", "observed_node_id", "days", "nse", "kge", "bias"]
    return pd.DataFrame(rows, columns=columns).set_index("node_id")


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
        names = _read_table(con, "Node", "node_id, name").set_index("node_id")["name"]
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
