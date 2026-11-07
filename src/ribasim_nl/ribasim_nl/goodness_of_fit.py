"""How well the simulated values of a model run fit the measurements of its Observation nodes.

`observed_and_simulated` pairs the measured values of each Observation node with the simulated values of the node it
observes, and `compare` summarizes how well they fit. `ribasim_nl.result_validation` checks the fit of a run of the
coupled LHM model, and `ribasim_nl.webmap` exports it for the model viewer.
"""

import sqlite3
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import numpy as np
import pandas as pd
import xarray as xr

# Observations need this many days with both a measured and a simulated value to be compared
MIN_COMPARED_DAYS = 365


@dataclass(frozen=True)
class ObservedSeries:
    """The measured values of a variable of an Observation node, and the simulated values of the node it observes."""

    node_id: int
    name: str | None
    variable: str
    observed_node_id: int
    observed: pd.Series
    simulated: pd.Series


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


def read_table(con: sqlite3.Connection, table: str, columns: str) -> pd.DataFrame:
    """Read columns of a table of a Ribasim database, which is empty if the table does not exist."""
    exists = con.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?", (table,)).fetchone()
    if exists is None:
        return pd.DataFrame(columns=[column.strip() for column in columns.split(",")])
    return pd.read_sql_query(f'SELECT {columns} FROM "{table}"', con)  # noqa: S608


def observed_and_simulated(database: Path, results_dir: Path) -> list[ObservedSeries]:
    """The measured and simulated values of each observed variable of each Observation node, by node ID and variable.

    Like in the model viewer, the simulated flow rate of a node is its total outflow, which for a Junction is the
    flow through it, and other variables are those of an observed Basin. Observation nodes without a link to the
    node they observe are left out.
    """
    with closing(sqlite3.connect(database)) as con:
        node = read_table(con, "Node", "node_id, name, node_type").set_index("node_id")
        link = read_table(con, "Link", "link_id, from_node_id, to_node_id, link_type")
        observed_values = read_table(con, "Observation / time", "node_id, variable, time, value")
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

    series = []
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
        series.append(
            ObservedSeries(
                node_id=observation_id,
                name=names[observation_id],
                variable=variable,
                observed_node_id=node_id,
                observed=values.set_index("time")["value"],
                simulated=simulated,
            )
        )
    return series


def compare_observations(database: Path, results_dir: Path) -> pd.DataFrame:
    """Compare each Observation node with the simulated values of the node it observes, see `compare`.

    :return: a table by the node ID of the Observation, with its name, the observed node and the result of `compare`
    """
    rows = [
        {"node_id": s.node_id, "name": s.name, "variable": s.variable, "observed_node_id": s.observed_node_id}
        | compare(s.observed, s.simulated)
        for s in observed_and_simulated(database, results_dir)
    ]
    columns = ["node_id", "name", "variable", "observed_node_id", "days", "nse", "kge", "bias"]
    return pd.DataFrame(rows, columns=columns).set_index("node_id")
