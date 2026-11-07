"""Compare the results of two runs of the coupled model, Basin by Basin and link by link.

    pixi run compare-runs --base <commit> [--head <commit>] > comparison.md

The metrics of `ribasim_nl.result_validation` are aggregates, which can hide large local changes. This writes the
largest differences as Markdown, for the `describe-run` skill: the network, levels, flows, exchange between
authorities, water balance, Basin areas, Observations and solver. Without `--head` it reads the working tree.

Node IDs can shift between runs, so nodes are matched by their type and location first, see `node_mapping`.
This finds where the runs differ, and what changed in the input next to it, but it does not trace a difference to
its cause. Runs at a commit are read with DVC, from the DVC cache or remote, into the cache of the model viewer.
"""

import argparse
import shutil
import sqlite3
import sys
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from dvc.api import DVCFileSystem

from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.webmap import read_features
from ribasim_nl.webmap_diff import read_basin_areas
from ribasim_nl.webmap_local import CACHE_DIR, REPO_DIR, resolve_commit

RUN_DIR = Path("data/Rijkswaterstaat/runs/lhm_coupled")
DATABASE = RUN_DIR / "input/database.gpkg"
BASIN_RESULTS = RUN_DIR / "results/basin.nc"
FLOW_RESULTS = RUN_DIR / "results/flow.nc"
SOLVER_STATS = RUN_DIR / "results/solver_stats.nc"
OBSERVATIONS = Path("data/Rijkswaterstaat/metrics/lhm_coupled_observations.csv")

AUTHORITY_FACTOR = 100_000
AUTHORITIES = {code: name for name, code in waterbeheercode.items()}
BALANCE_TERMS = ["precipitation", "evaporation", "drainage", "infiltration"]
DRY_PERIOD = slice("2018-08-01", "2018-11-30")
LEVEL_THRESHOLDS = (0.1, 0.5)
# Differences below these are not listed, or counted as a change of the input
MIN_LEVEL_CHANGE = 0.01
MIN_FLOW_CHANGE = 0.01
MIN_AREA_CHANGE = 0.01
MIN_BALANCE_CHANGE = 0.01
LEVEL_TOLERANCE = 0.001
# A Basin with a level this close to the bottom of its profile is counted as dry
DRY_DEPTH = 0.01
# Nodes closer than this are at the same location, see `GEOMETRY_TOLERANCE` in `ribasim_nl.webmap_diff`
LOCATION_DECIMALS = 2
LOCATION_TOLERANCE = 0.01
# The Basins with the largest differences of each group that are listed, and likewise for other tables
TOP = 15


@dataclass(frozen=True)
class Run:
    """The tables of a run that are compared.

    Nodes are indexed by node_id with node_type, name, meta_categorie and a point geometry. Links are the flow links
    with link_id as index and from_node_id, to_node_id, and the mean flow over the run and the dry period. The levels
    have a row per time and a column per Basin. Balance has the mean of `BALANCE_TERMS` per Basin. Bottom and
    streefpeil are per Basin, area is the area of the geometry in m2, and profile has a value per Basin that differs
    when its profile differs. Observations are indexed by node_id, and solver is the computation time per day in s.
    """

    nodes: gpd.GeoDataFrame
    links: pd.DataFrame
    levels: pd.DataFrame
    balance: pd.DataFrame
    bottom: pd.Series
    streefpeil: pd.Series
    area: pd.Series
    profile: pd.Series
    observations: pd.DataFrame
    solver: pd.Series


def fetch(path: Path, commit: str | None, cache_dir: Path = CACHE_DIR) -> Path:
    """The local path of a file of the repository, at a commit or if that is None in the working tree."""
    if commit is None:
        local = REPO_DIR / path
        assert local.is_file(), f"{local} not found, get it with `dvc checkout` or pass --head"
        return local
    # The same place as `checkout_model`, so the files of a preview of the model viewer are reused
    target = cache_dir / "revisions" / commit / path
    if not target.is_file():
        print(f"Getting {path} at {commit}", file=sys.stderr)
        target.parent.mkdir(parents=True, exist_ok=True)
        partial = target.with_name(f"{target.name}.partial")
        DVCFileSystem(str(REPO_DIR), rev=commit).get_file(path.as_posix(), str(partial))
        shutil.move(partial, target)
    return target


def profile_digests(profile: pd.DataFrame) -> pd.Series:
    """A value per node_id that differs when its profile differs by more than rounding, independent of row order."""
    columns = ["node_id", "level", "area"]
    rounded = profile[columns].round({"level": 3, "area": 0}).sort_values(columns, kind="stable")
    hashes = pd.util.hash_pandas_object(rounded[["level", "area"]], index=False)
    # Each row is weighted by its place in the profile, as a sum of the hashes alone ignores which level has which area
    position = rounded.groupby("node_id").cumcount().to_numpy().astype("uint64") + 1
    return pd.Series(hashes.to_numpy() * position, index=rounded["node_id"].to_numpy()).groupby(level=0).sum()


def read_run(commit: str | None) -> Run:
    """Read the tables of a run, at a commit or if that is None in the working tree."""
    database = fetch(DATABASE, commit)
    nodes = read_features(database, "Node", "node_id").set_index("node_id")
    assert nodes.index.is_unique, f"Duplicate node_id in {database}"

    with closing(sqlite3.connect(database)) as connection:
        profile = pd.read_sql('SELECT node_id, level, area FROM "Basin / profile"', connection)
        streefpeil = pd.read_sql('SELECT node_id, meta_streefpeil FROM "Basin / area"', connection)
    streefpeil = streefpeil.groupby("node_id")["meta_streefpeil"].first()

    with xr.open_dataset(fetch(BASIN_RESULTS, commit)) as ds:
        levels = pd.DataFrame(ds["level"].to_numpy(), index=ds["time"].to_numpy(), columns=ds["node_id"].to_numpy())
        balance = pd.DataFrame(
            {term: ds[term].mean("time").to_numpy() for term in BALANCE_TERMS}, index=ds["node_id"].to_numpy()
        )

    with xr.open_dataset(fetch(FLOW_RESULTS, commit)) as ds:
        links = pd.DataFrame(
            {
                "from_node_id": ds["from_node_id"].to_numpy(),
                "to_node_id": ds["to_node_id"].to_numpy(),
                "mean": ds["flow_rate"].mean("time").to_numpy(),
                "dry": ds["flow_rate"].sel(time=DRY_PERIOD).mean("time").to_numpy(),
            },
            index=ds["link_id"].to_numpy().astype("int64"),
        )

    with xr.open_dataset(fetch(SOLVER_STATS, commit)) as ds:
        # The computation time is in ms
        solver = pd.Series(ds["computation_time"].to_numpy() / 1000, index=ds["time"].to_numpy())

    return Run(
        nodes=nodes,
        links=links,
        levels=levels,
        balance=balance,
        bottom=profile.groupby("node_id")["level"].min(),
        streefpeil=streefpeil,
        area=read_basin_areas(database).area,
        profile=profile_digests(profile),
        observations=pd.read_csv(fetch(OBSERVATIONS, commit), index_col="node_id"),
        solver=solver,
    )


def node_mapping(base: gpd.GeoDataFrame, head: gpd.GeoDataFrame) -> pd.Series:
    """The node_id in `head` of each node_id in `base`, with the negated ID for a node that is only in `base`.

    Nodes of the same type at the same location are the same node, whatever their ID. Other Observations are the same
    node if they have the same name, since the layout moves them and their ID says nothing about the series they
    measure. The others are the same node if they have the same ID and type, so they moved.
    """

    def locations(nodes: gpd.GeoDataFrame) -> pd.DataFrame:
        table = pd.DataFrame(
            {
                "node_type": nodes["node_type"],
                "x": nodes.geometry.x.round(LOCATION_DECIMALS),
                "y": nodes.geometry.y.round(LOCATION_DECIMALS),
            },
            index=nodes.index,
        )
        return table.drop_duplicates(keep=False).rename_axis("node_id").reset_index()

    pairs = locations(base).merge(locations(head), on=["node_type", "x", "y"], suffixes=("_base", "_head"))
    mapping = pd.Series(-base.index.to_numpy(), index=base.index, name="node_id")
    mapping[pairs["node_id_base"].to_numpy()] = pairs["node_id_head"].to_numpy()

    def open_observations(nodes: gpd.GeoDataFrame, matched: pd.Series) -> pd.Series:
        """The node_id by name of the Observations that are not matched yet, without ambiguous names."""
        names = nodes.loc[(nodes["node_type"] == "Observation") & ~nodes.index.isin(matched), "name"]
        names = names[~names.duplicated(keep=False)]
        return pd.Series(names.index.to_numpy(), index=names.to_numpy())

    named = pd.concat(
        [open_observations(base, pairs["node_id_base"]), open_observations(head, pairs["node_id_head"])],
        axis=1,
        join="inner",
        keys=["node_id_base", "node_id_head"],
    )
    mapping[named["node_id_base"].to_numpy()] = named["node_id_head"].to_numpy()

    open_base = base.index.difference(pd.Index(pairs["node_id_base"])).difference(pd.Index(named["node_id_base"]))
    open_head = head.index.difference(pd.Index(pairs["node_id_head"])).difference(pd.Index(named["node_id_head"]))
    same_id = open_base.intersection(open_head)
    same_type = base.loc[same_id, "node_type"] == head.loc[same_id, "node_type"]
    moved = same_id[same_type & (base.loc[same_id, "node_type"] != "Observation")]
    mapping.loc[moved] = moved.to_numpy()
    matched = mapping[mapping > 0]
    assert matched.is_unique, "Multiple nodes of the base run are matched with the same node"
    return mapping


def renumber(run: Run, mapping: pd.Series) -> Run:
    """A run with the node IDs of `mapping`, so that its tables can be compared with those of the other run."""

    def nodes_of(index: pd.Index) -> pd.Index:
        assert index.isin(mapping.index).all(), "A table has nodes that are not in the Node table"
        return pd.Index(mapping.reindex(index).to_numpy(), name=index.name)

    def series(table: pd.Series) -> pd.Series:
        return table.set_axis(nodes_of(table.index), axis=0)

    links = run.links.assign(
        from_node_id=nodes_of(pd.Index(run.links["from_node_id"])),
        to_node_id=nodes_of(pd.Index(run.links["to_node_id"])),
    )
    return Run(
        nodes=run.nodes.set_axis(nodes_of(run.nodes.index), axis=0),
        links=links,
        levels=run.levels.set_axis(nodes_of(run.levels.columns), axis=1),
        balance=run.balance.set_axis(nodes_of(run.balance.index), axis=0),
        bottom=series(run.bottom),
        streefpeil=series(run.streefpeil),
        area=series(run.area),
        profile=series(run.profile),
        observations=run.observations.set_axis(nodes_of(run.observations.index), axis=0),
        solver=run.solver,
    )


def authority_code(node_ids: np.ndarray | pd.Index | pd.Series) -> np.ndarray:
    """The code of the authority of each node, also for the negated IDs of `node_mapping`."""
    return np.abs(np.asarray(node_ids)) // AUTHORITY_FACTOR


def authority(node_ids: pd.Index | pd.Series) -> np.ndarray:
    """The name of the authority of each node, see the model conventions in AGENTS.md."""
    return np.array([AUTHORITIES.get(int(code), str(code)) for code in authority_code(node_ids)], dtype=object)


def markdown_table(df: pd.DataFrame, digits: int = 3) -> str:
    """A Markdown table of a dataframe with its index as a column, or a note if it is empty."""
    if df.empty:
        return "None.\n"

    def cell(value: object) -> str:
        if isinstance(value, float | np.floating):
            return "" if np.isnan(value) else f"{value:.{digits}f}"
        return "" if value is None else str(value).replace("|", "/")

    table = df.reset_index()
    rows = [list(table.columns), ["---"] * table.shape[1], *table.itertuples(index=False)]
    return "".join("| " + " | ".join(cell(value) for value in row) + " |\n" for row in rows)


def moved_nodes(base: Run, head: Run) -> pd.Series:
    """The distance in m of the nodes in both runs that moved, for a `base` with the IDs of `head`."""
    common = base.nodes.index.intersection(head.nodes.index)
    distance = base.nodes.loc[common].geometry.distance(head.nodes.loc[common].geometry)
    return distance[distance > LOCATION_TOLERANCE]


def compare_network(base: Run, head: Run) -> pd.DataFrame:
    """The number of nodes per type that are only in one run, or moved, for a `base` with the IDs of `head`."""
    added = head.nodes.index.difference(base.nodes.index)
    removed = base.nodes.index.difference(head.nodes.index)
    counts = pd.DataFrame(
        {
            "added": head.nodes.loc[added, "node_type"].value_counts(),
            "removed": base.nodes.loc[removed, "node_type"].value_counts(),
            "moved": head.nodes.loc[moved_nodes(base, head).index, "node_type"].value_counts(),
        }
    )
    counts.index.name = "node_type"
    return counts.fillna(0).astype("int64").sort_index()


def connections(run: Run) -> set[tuple[int, int]]:
    """The nodes that connect two authorities, tracing through Junctions since their IDs can shift between runs."""
    junctions = set(run.nodes.index[run.nodes["node_type"] == "Junction"])
    downstream: dict[int, list[int]] = {}
    for from_id, to_id in zip(run.links["from_node_id"], run.links["to_node_id"], strict=True):
        downstream.setdefault(int(from_id), []).append(int(to_id))

    found = set()
    for start in downstream.keys() - junctions:
        stack, seen = list(downstream[start]), set()
        while stack:
            node = stack.pop()
            if node in seen:
                continue
            seen.add(node)
            if node in junctions:
                stack += downstream.get(node, [])
            elif abs(start) // AUTHORITY_FACTOR != abs(node) // AUTHORITY_FACTOR:
                found.add((start, node))
    return found


def input_changes(base: Run, head: Run, basins: pd.Index) -> pd.Series:
    """What changed in the input of each Basin: its location, area, target level, profile, and the flow links on it."""
    changes = pd.Series([[] for _ in basins], index=basins, dtype=object)
    area_change = (head.area.reindex(basins) / base.area.reindex(basins) - 1).abs()
    streefpeil_change = (head.streefpeil.reindex(basins) - base.streefpeil.reindex(basins)).abs()
    profile_differs = head.profile.reindex(basins) != base.profile.reindex(basins)
    moved = moved_nodes(base, head)

    def pairs(run: Run) -> set[tuple[int, int]]:
        return set(zip(run.links["from_node_id"], run.links["to_node_id"], strict=True))

    differing = pairs(base) ^ pairs(head)
    with_changed_link = {node for pair in differing for node in pair}

    for basin in basins:
        if basin in moved.index:
            changes[basin].append(f"moved {moved[basin]:.0f} m")
        if area_change[basin] > MIN_AREA_CHANGE:
            changes[basin].append(f"area {head.area[basin] / base.area[basin] - 1:+.0%}")
        if streefpeil_change[basin] > LEVEL_TOLERANCE:
            changes[basin].append(f"streefpeil {head.streefpeil[basin] - base.streefpeil[basin]:+.2f} m")
        if profile_differs[basin]:
            changes[basin].append("profile")
        if basin in with_changed_link:
            changes[basin].append("links")
    return changes.map(", ".join)


def level_statistics(levels: pd.DataFrame) -> pd.DataFrame:
    """The mean, 1st and 99th percentile of the level of each Basin."""
    return pd.DataFrame(
        {
            "mean": levels.mean(),
            "p1": levels.quantile(0.01),
            "p99": levels.quantile(0.99),
        }
    )


def compare_levels(base: Run, head: Run) -> pd.DataFrame:
    """The level statistics of the Basins in both runs, with the largest change of them in `change`."""
    basins = base.levels.columns.intersection(head.levels.columns)
    base_stats = level_statistics(base.levels[basins])
    head_stats = level_statistics(head.levels[basins])
    deltas = head_stats - base_stats
    largest = deltas.abs().to_numpy().argmax(axis=1)
    table = pd.DataFrame(
        {
            "name": head.nodes["name"].reindex(basins),
            "authority": authority(basins),
            "categorie": head.nodes["meta_categorie"].reindex(basins),
            "change": deltas.to_numpy()[np.arange(len(basins)), largest],
            "mean": head_stats["mean"],
            "mean_change": deltas["mean"],
            "p1_change": deltas["p1"],
            "p99_change": deltas["p99"],
            "above_streefpeil": head_stats["mean"] - head.streefpeil.reindex(basins),
        }
    )
    table.index.name = "node_id"
    return table


def level_counts(table: pd.DataFrame) -> pd.DataFrame:
    """The number of Basins per authority with a change over each of `LEVEL_THRESHOLDS`."""
    counts = {"basins": table.groupby("authority").size()}
    for threshold in LEVEL_THRESHOLDS:
        over = table["change"].abs() > threshold
        counts[f"over {threshold} m"] = table[over].groupby("authority").size()
    result = pd.DataFrame(counts).fillna(0).astype("int64")
    return result.sort_values(f"over {LEVEL_THRESHOLDS[0]} m", ascending=False, kind="stable")


def top_levels(table: pd.DataFrame, base: Run, head: Run) -> pd.DataFrame:
    """The Basins with the largest change in level, with what changed in their input."""
    over = table[table["change"].abs() > MIN_LEVEL_CHANGE]
    top = over.loc[over["change"].abs().sort_values(ascending=False, kind="stable").index[:TOP]]
    columns = ["name", "authority", "categorie", "mean_change", "p1_change", "p99_change", "above_streefpeil"]
    return top[columns].assign(input=input_changes(base, head, top.index))


def monthly_level_changes(base: Run, head: Run) -> pd.DataFrame:
    """Per month, the mean and largest absolute change in the level over the Basins and days."""
    basins = base.levels.columns.intersection(head.levels.columns)
    times = base.levels.index.intersection(head.levels.index)
    delta = (head.levels.loc[times, basins] - base.levels.loc[times, basins]).abs()
    monthly = delta.groupby(pd.DatetimeIndex(delta.index).to_period("M"))
    table = pd.DataFrame({"mean": monthly.mean().mean(axis=1), "largest": monthly.max().max(axis=1)})
    table.index = table.index.astype(str)
    table.index.name = "month"
    return table


def dry_basins(run: Run) -> pd.Series:
    """The number of days each Basin is within `DRY_DEPTH` of the bottom of its profile."""
    bottom = run.bottom.reindex(run.levels.columns)
    return (run.levels - bottom < DRY_DEPTH).sum()


def compare_dry_basins(base: Run, head: Run) -> pd.DataFrame:
    """The Basins with the largest change in the number of days they are dry."""
    basins = base.levels.columns.intersection(head.levels.columns)
    days = pd.DataFrame({"base": dry_basins(base)[basins], "head": dry_basins(head)[basins]})
    days["change"] = days["head"] - days["base"]
    days = days[days["change"] != 0]
    days = days.loc[days["change"].abs().sort_values(ascending=False, kind="stable").index[:TOP]]
    days.insert(0, "authority", authority(days.index))
    days.insert(0, "name", head.nodes["name"].reindex(days.index))
    days.index.name = "node_id"
    return days


def match_links(base: Run, head: Run) -> pd.DataFrame:
    """The flow links of both runs, matched by their nodes, since link IDs and the IDs of Junctions can shift."""
    key = ["from_node_id", "to_node_id"]
    for run in (base, head):
        assert not run.links.duplicated(key).any(), "Multiple flow links between the same nodes"
    return base.links.merge(head.links, on=key, how="outer", suffixes=("_base", "_head"), validate="one_to_one")


def top_one_run_flows(matched: pd.DataFrame, names: pd.Series) -> pd.DataFrame:
    """The links in only one run with the largest mean flow; negative node IDs are only in the base run."""
    only_base = matched["mean_head"].isna()
    only_head = matched["mean_base"].isna()
    table = pd.DataFrame(
        {
            "run": np.where(only_base, "base", "head"),
            "from_node_id": matched["from_node_id"].abs(),
            "from_name": matched["from_node_id"].map(names),
            "to_node_id": matched["to_node_id"].abs(),
            "to_name": matched["to_node_id"].map(names),
            "mean": matched["mean_base"].where(only_base, matched["mean_head"]),
            "dry": matched["dry_base"].where(only_base, matched["dry_head"]),
        }
    )[only_base | only_head]
    largest = table[["mean", "dry"]].abs().max(axis=1)
    return table.loc[
        largest[largest > MIN_FLOW_CHANGE].sort_values(ascending=False, kind="stable").index[:TOP]
    ].set_index("run")


def top_flows(matched: pd.DataFrame) -> pd.DataFrame:
    """The links in both runs with the largest change in the mean flow, over the run or the dry period."""
    both = matched.dropna(subset=["mean_base", "mean_head"]).copy()
    both["change"] = (both["mean_head"] - both["mean_base"]).abs()
    both["dry_change"] = (both["dry_head"] - both["dry_base"]).abs()
    largest = both[["change", "dry_change"]].max(axis=1)
    top = both.loc[largest[largest > MIN_FLOW_CHANGE].sort_values(ascending=False, kind="stable").index[:TOP]]
    columns = ["from_node_id", "to_node_id", "mean_base", "mean_head", "dry_base", "dry_head"]
    return top[columns].set_index(["from_node_id", "to_node_id"])


def net_exchange(run: Run, column: str) -> pd.Series:
    """The net flow between each pair of authorities, from the first to the second, in the order of their codes."""
    from_code = pd.Series(authority_code(run.links["from_node_id"]), index=run.links.index)
    to_code = pd.Series(authority_code(run.links["to_node_id"]), index=run.links.index)
    crossing = from_code != to_code
    forward = from_code[crossing] < to_code[crossing]
    first = from_code[crossing].where(forward, to_code[crossing])
    second = to_code[crossing].where(forward, from_code[crossing])
    flow = run.links.loc[crossing, column].where(forward, -run.links.loc[crossing, column])
    return flow.groupby([first.to_numpy(), second.to_numpy()]).sum()


def compare_exchange(base: Run, head: Run) -> pd.DataFrame:
    """The pairs of authorities with the largest change in net exchange, over the run or the dry period."""
    table = pd.DataFrame(
        {
            "mean_base": net_exchange(base, "mean"),
            "mean_head": net_exchange(head, "mean"),
            "dry_base": net_exchange(base, "dry"),
            "dry_head": net_exchange(head, "dry"),
        }
    ).fillna(0.0)
    largest = pd.concat(
        [(table["mean_head"] - table["mean_base"]).abs(), (table["dry_head"] - table["dry_base"]).abs()], axis=1
    ).max(axis=1)
    table = table.loc[largest[largest > MIN_FLOW_CHANGE].sort_values(ascending=False, kind="stable").index[:TOP]]
    names = [f"{AUTHORITIES.get(a, a)} - {AUTHORITIES.get(b, b)}" for a, b in table.index]
    table.index = pd.Index(names, name="from - to")
    return table


def node_names(base: Run, head: Run) -> pd.Series:
    """The name of each node of both runs, for a `base` with the IDs of `head`."""
    return pd.concat([base.nodes["name"], head.nodes["name"]]).groupby(level=0).last()


def compare_connections(base: Run, head: Run) -> pd.DataFrame:
    """The connections between authorities that are only in one run, with the names of their nodes."""
    base_connections, head_connections = connections(base), connections(head)
    names = node_names(base, head)
    rows = [
        {"run": run, "from_node_id": abs(a), "from_name": names.get(a), "to_node_id": abs(b), "to_name": names.get(b)}
        for run, pairs in (("base", base_connections - head_connections), ("head", head_connections - base_connections))
        for a, b in sorted(pairs)
    ]
    columns = ["run", "from_node_id", "from_name", "to_node_id", "to_name"]
    return pd.DataFrame(rows, columns=columns).set_index("run")


def compare_balance(base: Run, head: Run) -> pd.DataFrame:
    """The water balance terms and Basin area per authority where they differ, most changed Basin area first."""

    def per_authority(run: Run) -> pd.DataFrame:
        table = run.balance.groupby(authority(run.balance.index)).sum()
        return table.assign(area=run.area.groupby(authority(run.area.index)).sum() / 1e6)

    base_table, head_table = per_authority(base), per_authority(head)
    table = pd.concat(
        [
            df
            for term in [*BALANCE_TERMS, "area"]
            for df in (base_table[term].rename(f"{term}_base"), head_table[term].rename(f"{term}_head"))
        ],
        axis=1,
    )
    changes = (head_table - base_table).abs()
    changed = changes[(changes > MIN_BALANCE_CHANGE).any(axis=1)]
    table = table.loc[changed["area"].sort_values(ascending=False, kind="stable").index]
    table.index.name = "authority"
    return table


def compare_areas(base: Run, head: Run) -> pd.DataFrame:
    """The Basins with the largest relative change in area."""
    common = base.area.index.intersection(head.area.index)
    table = pd.DataFrame({"base_km2": base.area[common] / 1e6, "head_km2": head.area[common] / 1e6})
    table["relative"] = table["head_km2"] / table["base_km2"] - 1
    table = table[table["relative"].abs() > MIN_AREA_CHANGE]
    # A large relative change of a tiny Basin says little, so these are ordered by the change in area
    table = table.loc[
        (table["relative"].abs() * table["base_km2"]).sort_values(ascending=False, kind="stable").index[:TOP]
    ]
    table.insert(0, "authority", authority(table.index))
    table.insert(0, "name", head.nodes["name"].reindex(table.index))
    table.index.name = "node_id"
    return table


def one_run_basins(base: Run, head: Run) -> pd.DataFrame:
    """The Basins in only one run with the largest area, whose area is not necessarily given to another Basin."""
    rows = []
    for name, run, other in (("base", base, head), ("head", head, base)):
        only = run.area.index.difference(other.area.index)
        table = pd.DataFrame({"run": name, "area_km2": run.area[only] / 1e6, "authority": authority(only)})
        table.insert(1, "name", run.nodes["name"].reindex(only))
        table.insert(0, "node_id", np.abs(only))
        rows.append(table)
    table = pd.concat(rows)
    return table.sort_values("area_km2", ascending=False, kind="stable").head(TOP).set_index("node_id")


def compare_observations(base: Run, head: Run) -> pd.DataFrame:
    """The Observations with the largest change in KGE, for a `base` with the IDs of `head`."""
    joined = base.observations.merge(
        head.observations, left_index=True, right_index=True, how="inner", suffixes=("_base", "_head")
    )
    joined["change"] = joined["kge_head"] - joined["kge_base"]
    joined = joined.dropna(subset=["change"])
    top = joined.loc[joined["change"].abs().sort_values(ascending=False, kind="stable").index[:TOP]]
    columns = ["name_head", "kge_base", "kge_head", "nse_base", "nse_head", "bias_base", "bias_head"]
    return top[columns].rename(columns={"name_head": "name"})


def compare_solver(base: Run, head: Run) -> pd.DataFrame:
    """The days with the largest change in computation time, in s."""
    table = pd.DataFrame({"base": base.solver, "head": head.solver}).dropna()
    table["change"] = table["head"] - table["base"]
    table = table.loc[table["change"].abs().sort_values(ascending=False, kind="stable").index[:TOP]]
    table.index = pd.DatetimeIndex(table.index).strftime("%Y-%m-%d")
    table.index.name = "day"
    return table


def report(base: Run, head: Run, labels: tuple[str, str]) -> str:
    """The differences between two runs as Markdown, largest first in each table."""
    mapping = node_mapping(base.nodes, head.nodes)
    renumbered = int(((mapping > 0) & (mapping != mapping.index)).sum())
    base = renumber(base, mapping)
    counts = compare_network(base, head)
    moved = moved_nodes(base, head)
    levels = compare_levels(base, head)
    matched = match_links(base, head)
    only_one = matched["mean_base"].isna() | matched["mean_head"].isna()
    storage = levels["categorie"] == "bergend"
    rijkswaterstaat = levels["authority"] == "Rijkswaterstaat"

    sections = [
        ("Basins with a changed level, per authority", level_counts(levels), 3),
        ("Largest changes in level, excluding storage Basins (m)", top_levels(levels[~storage], base, head), 3),
        ("Largest changes in level of storage Basins (m)", top_levels(levels[storage], base, head), 3),
        ("Largest changes in level of Rijkswaterstaat (m)", top_levels(levels[rijkswaterstaat], base, head), 3),
        ("Mean and largest change in level per month (m)", monthly_level_changes(base, head), 3),
        ("Largest changes in the days a Basin is dry", compare_dry_basins(base, head), 0),
        ("Largest changes in mean flow, over the run and the dry period (m3/s)", top_flows(matched), 3),
        (
            "Flow links in only one run with the largest flow (m3/s)",
            top_one_run_flows(matched, node_names(base, head)),
            3,
        ),
        ("Largest changes in net exchange between authorities (m3/s)", compare_exchange(base, head), 3),
        ("Connections between authorities in only one run", compare_connections(base, head), 0),
        ("Water balance per authority (mean m3/s) and Basin area (km2)", compare_balance(base, head), 2),
        ("Largest changes in Basin area", compare_areas(base, head), 3),
        ("Largest Basins in only one run", one_run_basins(base, head), 3),
        ("Largest changes in the KGE of Observations", compare_observations(base, head), 3),
        ("Days with the largest change in computation time (s)", compare_solver(base, head), 1),
    ]
    lines = [
        f"Comparison of the results of `{labels[1]}` (head) with `{labels[0]}` (base).",
        f"The dry period is {DRY_PERIOD.start} to {DRY_PERIOD.stop}. The `input` column names what changed at that "
        "Basin itself, not what changed upstream, so it need not be the cause of the difference.",
        "",
        "## Nodes and links",
        "",
        f"{renumbered} nodes have another ID at the same location, which is not a difference: the tables below use "
        f"the IDs of head. {len(moved)} nodes moved, the median by {moved.median() if len(moved) else 0:.0f} m. "
        f"{int(only_one.sum())} of the {len(matched)} flow links are in only one run.",
        "",
        markdown_table(counts),
    ]
    for title, table, digits in sections:
        lines += [f"## {title}", "", markdown_table(table, digits)]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> None:
    """Command line entry point, see the module docstring."""
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", required=True, help="git revision of the run to compare with")
    parser.add_argument("--head", help="git revision of the run to compare, instead of the working tree")
    args = parser.parse_args(argv)

    base_commit = resolve_commit(args.base)
    assert base_commit is not None, f"Unknown git revision {args.base}"
    head_commit = None if args.head is None else resolve_commit(args.head)
    assert args.head is None or head_commit is not None, f"Unknown git revision {args.head}"

    labels = (base_commit[:7], "working tree" if head_commit is None else head_commit[:7])
    print(report(read_run(base_commit), read_run(head_commit), labels))


if __name__ == "__main__":
    main()
