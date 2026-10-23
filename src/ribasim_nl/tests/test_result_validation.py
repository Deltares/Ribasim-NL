import numpy as np
import pandas as pd
import pytest
import xarray as xr
from ribasim import Node
from ribasim.nodes import basin, level_boundary, observation, outlet
from ribasim_nl.result_validation import (
    MIN_COMPARED_DAYS,
    Limit,
    compare,
    compare_observations,
    key_discharge_metrics,
    observation_metrics,
    result_issues,
    solver_metrics,
    validate_results,
)
from shapely.geometry import Point

from ribasim_nl import Model

TIME = pd.date_range("2020-01-01", periods=MIN_COMPARED_DAYS + 1, name="time")
# a seasonal discharge
OBSERVED = pd.Series(2.0 + np.sin(np.arange(len(TIME)) * 2 * np.pi / 365), index=TIME)


def test_limit():
    limit = Limit(min=0.0, max=1.0)
    assert limit.issue("x", 0.5) is None
    assert limit.issue("x", -1.0) == "x is -1, below the minimum of 0"
    assert limit.issue("x", 2.0) == "x is 2, above the maximum of 1"
    assert limit.issue("x", np.nan) == "x is nan"
    with pytest.raises(AssertionError, match="Empty limit"):
        Limit(min=1.0, max=1.0)


def test_compare():
    perfect = compare(OBSERVED, OBSERVED)
    assert perfect == {"days": len(TIME), "nse": 1.0, "kge": pytest.approx(1.0), "bias": 0.0}

    biased = compare(OBSERVED, 1.5 * OBSERVED)
    assert biased["bias"] == pytest.approx(0.5)
    assert biased["kge"] == pytest.approx(1 - np.sqrt(0.5**2 + 0.5**2))
    assert biased["nse"] < 1

    # a constant simulation does not correlate
    constant = compare(OBSERVED, pd.Series(OBSERVED.mean(), index=TIME))
    assert constant["nse"] == pytest.approx(0.0, abs=1e-12)
    assert constant["kge"] == pytest.approx(1 - np.sqrt(2))

    # only times with both values count
    short = compare(OBSERVED.iloc[:100], OBSERVED)
    assert short["days"] == 100
    assert np.isnan(short["nse"]) and np.isnan(short["kge"])


def test_solver_metrics():
    stats = xr.Dataset(
        {
            "computation_time": ("time", [3_600_000.0, 1_800_000.0]),
            "rhs_calls": ("time", [100, 300]),
            "accepted_timesteps": ("time", [10, 30]),
            "rejected_timesteps": ("time", [0, 10]),
            "dt": ("time", [8640.0, 2880.0]),
        },
        coords={"time": pd.date_range("2020-01-01", periods=2)},
    )
    metrics = solver_metrics(stats)
    assert metrics["computation_time"] == 1.5
    assert metrics["rhs_calls"] == 400
    assert metrics["mean_dt"] == 2 * 86400 / 40
    assert metrics["rejected_fraction"] == 10 / 50


@pytest.fixture
def run(tmp_path):
    """A model with Observations of a Basin level and of an Outlet flow rate, and its results.

    Basin 1 -> Outlet 2 -> LevelBoundary 3, observed by Observation 4 (flow rate) and 5 (level of Basin 1),
    and Observation 6 without a link.
    """
    model = Model(starttime=TIME[0], endtime=TIME[-1], crs="EPSG:28992")
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
            [observation.Time(time=TIME, variable=variable, value=OBSERVED.to_numpy())],
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
    flow_rate = np.column_stack([2 * OBSERVED.to_numpy(), OBSERVED.to_numpy()])
    xr.Dataset({"flow_rate": (("time", "link_id"), flow_rate)}, coords={"time": TIME, "link_id": flow_links}).to_netcdf(
        results_dir / "flow.nc"
    )
    xr.Dataset(
        {"level": (("time", "node_id"), (OBSERVED.to_numpy() + 1)[:, None])}, coords={"time": TIME, "node_id": [1]}
    ).to_netcdf(results_dir / "basin.nc")
    return toml_path.parent / "input/database.gpkg", results_dir


def test_compare_observations(run):
    comparison = compare_observations(*run)
    # the unlinked Observation is not compared
    assert comparison.index.tolist() == [4, 5]
    assert comparison["observed_node_id"].tolist() == [2, 1]
    # the flow rate of the Outlet is that of its outflow link
    assert comparison.at[4, "nse"] == 1.0
    assert comparison.at[5, "bias"] == pytest.approx(1 / OBSERVED.abs().mean())

    metrics = observation_metrics(comparison)
    assert metrics["compared"] == 2
    assert metrics["median_nse"] == pytest.approx(comparison["nse"].mean())
    # a missing key discharge has no KGE, which fails its limit
    assert np.isnan(key_discharge_metrics(comparison)["Lobith"])


def test_validate_results():
    limits = {"solver": {"rhs_calls": Limit(max=10.0), "mean_dt": Limit(min=1.0)}, "level_p99": {"IJsselmeer": Limit()}}
    metrics = {"solver": {"rhs_calls": 5.0, "mean_dt": 2.0}, "level_p99": {"IJsselmeer": 0.0}}
    assert result_issues(metrics, limits) == []
    validate_results(metrics, limits)

    metrics = {"solver": {"rhs_calls": 20.0}, "level_p99": {"IJsselmeer": 0.0}}
    assert result_issues(metrics, limits) == [
        "solver rhs_calls is 20, above the maximum of 10",
        "solver mean_dt is nan",
    ]
    with pytest.raises(ValueError, match="result validation failed"):
        validate_results(metrics, limits)
