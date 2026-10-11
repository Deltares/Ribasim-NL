"""List the Basins whose level oscillates in a model run, with the controlled nodes that listen to them.

Usage:
    pixi run python scripts/control_oscillation.py <model toml> [--reference <toml>] [--min-change 0.01] [--csv out.csv]

With a reference run, for instance with `solver.discrete_control_interval = 0`, only Basins that oscillate more than
three times as much as in the reference are listed. See issue #850.
"""

import argparse
import sqlite3
import tomllib
from pathlib import Path

import pandas as pd
import xarray as xr
from ribasim_nl.control_diagnostics import oscillating_basins


def read_level(toml_path: Path) -> pd.DataFrame:
    """Read the Basin levels of a model run, with a time index and a column per Basin node ID."""
    with toml_path.open("rb") as f:
        config = tomllib.load(f)
    basin_nc = toml_path.parent / config.get("results_dir", "results") / "basin.nc"
    assert basin_nc.is_file(), f"No Basin results at {basin_nc}"
    with xr.open_dataset(basin_nc) as ds:
        return ds["level"].to_pandas()


def database(toml_path: Path) -> Path:
    with toml_path.open("rb") as f:
        config = tomllib.load(f)
    path = toml_path.parent / config.get("input_dir", ".") / "database.gpkg"
    assert path.is_file(), f"No database at {path}"
    return path


def listening_nodes(gpkg: Path) -> pd.DataFrame:
    """Get the controlled Outlets and Pumps per listened Basin, with their capacity."""
    with sqlite3.connect(gpkg) as con:
        variable = pd.read_sql("select node_id, listen_node_id from `DiscreteControl / variable`", con)
        link = pd.read_sql("select from_node_id, to_node_id from Link where link_type = 'control'", con)
        static = pd.concat(
            [
                pd.read_sql(f"select node_id, flow_rate, max_flow_rate from `{t} / static`", con)  # noqa: S608
                for t in ("Outlet", "Pump")
            ]
        )
    # Ribasim limits the flow rate to the max_flow_rate
    capacity = static.set_index("node_id")[["flow_rate", "max_flow_rate"]].min(axis=1).groupby(level=0).max()
    controlled = variable.merge(link, left_on="node_id", right_on="from_node_id")[["listen_node_id", "to_node_id"]]
    controlled = controlled.drop_duplicates().rename(columns={"to_node_id": "controlled_node_id"})
    return controlled.assign(capacity=controlled["controlled_node_id"].map(capacity))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("toml", type=Path)
    parser.add_argument("--reference", type=Path)
    parser.add_argument("--min-change", type=float, default=0.01, help="minimum mean level change [m/d]")
    parser.add_argument("--csv", type=Path)
    args = parser.parse_args()

    reference = read_level(args.reference) if args.reference else None
    basins = oscillating_basins(read_level(args.toml), reference, min_change=args.min_change)
    gpkg = database(args.toml)
    with sqlite3.connect(gpkg) as con:
        node = pd.read_sql("select node_id, meta_waterbeheerder from Node", con).set_index("node_id")
        area = pd.read_sql("select node_id, max(area) as area from `Basin / profile` group by node_id", con)
    basins = basins.join(node).join(area.set_index("node_id"))
    result = basins.join(listening_nodes(gpkg).set_index("listen_node_id"), how="left").rename_axis("basin_node_id")

    print(f"{len(basins)} oscillating Basins")
    with pd.option_context("display.width", 200, "display.max_rows", 100, "display.max_columns", None):
        print(result.head(100).round(4))
    if args.csv:
        result.to_csv(args.csv)


if __name__ == "__main__":
    main()
