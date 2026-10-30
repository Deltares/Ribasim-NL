from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from ribasim import Node
from ribasim.nodes import basin, level_boundary, observation, outlet
from ribasim_nl.goodness_of_fit import MIN_COMPARED_DAYS
from shapely.geometry import Point

from ribasim_nl import Model


@pytest.fixture
def observed() -> pd.Series:
    """A seasonal discharge, measured on just enough days to be compared."""
    time = pd.date_range("2020-01-01", periods=MIN_COMPARED_DAYS + 1, name="time")
    return pd.Series(2.0 + np.sin(np.arange(len(time)) * 2 * np.pi / 365), index=time)


@pytest.fixture
def observation_run(tmp_path: Path, observed: pd.Series) -> tuple[Path, Path]:
    """The database and results directory of a model with Observations of a Basin level and of an Outlet flow rate.

    Basin 1 -> Outlet 2 -> LevelBoundary 3, observed by Observation 4 (flow rate) and 5 (level of Basin 1),
    and Observation 6 without a link. All measure `observed`; the simulated flow rate of the Outlet equals it, and
    the level of Basin 1 is 0.5 m higher.
    """
    time = observed.index
    model = Model(starttime=time[0], endtime=time[-1], crs="EPSG:28992")
    model.basin.add(
        Node(1, Point(0, 0)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])]
    )
    model.outlet.add(Node(2, Point(10, 0)), [outlet.Static(flow_rate=[1.0])])
    model.level_boundary.add(Node(3, Point(20, 0)), [level_boundary.Static(level=[0.0])])
    model.link.add(model.basin[1], model.outlet[2])
    model.link.add(model.outlet[2], model.level_boundary[3])
    for node_id, name, variable in ((4, "afvoer", "flow_rate"), (5, "peil", "level"), (6, "los", "flow_rate")):
        model.observation.add(
            Node(node_id, Point(10, 5 * node_id), name=name),
            [observation.Time(time=time, variable=variable, value=observed.to_numpy())],
        )
    model.link.add(model.observation[4], model.outlet[2])
    model.link.add(model.observation[5], model.basin[1])
    toml_path = tmp_path / "model.toml"
    model.write(toml_path)

    results_dir = tmp_path / "results"
    results_dir.mkdir()
    link = model.link.df
    flow_links = link.index[link["link_type"] == "flow"].to_numpy()
    # the inflow of the Outlet differs from its outflow
    flow_rate = np.column_stack([2 * observed.to_numpy(), observed.to_numpy()])
    xr.Dataset({"flow_rate": (("time", "link_id"), flow_rate)}, coords={"time": time, "link_id": flow_links}).to_netcdf(
        results_dir / "flow.nc"
    )
    xr.Dataset(
        {"level": (("time", "node_id"), (observed.to_numpy() + 0.5)[:, None], {"units": "m"})},
        coords={"time": time, "node_id": [1]},
    ).to_netcdf(results_dir / "basin.nc")
    return toml_path.parent / "input/database.gpkg", results_dir
