import numpy as np
import pandas as pd
import pytest
import xarray as xr
from ribasim import Node
from ribasim.nodes import basin, level_boundary, observation, outlet
from ribasim_nl.goodness_of_fit import compare, compare_observations, observed_and_simulated
from shapely.geometry import Point

from ribasim_nl import Model


def test_compare(observed):
    perfect = compare(observed, observed)
    assert perfect == {"days": len(observed), "nse": 1.0, "kge": pytest.approx(1.0), "bias": 0.0}

    biased = compare(observed, 1.5 * observed)
    assert biased["bias"] == pytest.approx(0.5)
    assert biased["kge"] == pytest.approx(1 - np.sqrt(0.5**2 + 0.5**2))
    assert biased["nse"] < 1

    # a constant simulation does not correlate
    constant = compare(observed, pd.Series(observed.mean(), index=observed.index))
    assert constant["nse"] == pytest.approx(0.0, abs=1e-12)
    assert constant["kge"] == pytest.approx(1 - np.sqrt(2))

    # only times with both values count
    short = compare(observed.iloc[:100], observed)
    assert short["days"] == 100
    assert np.isnan(short["nse"]) and np.isnan(short["kge"])


def assert_values_equal(series: pd.Series, expected: pd.Series) -> None:
    """Compare the times and values of two series, also if their times have another resolution."""
    pd.testing.assert_series_equal(series, expected, check_names=False, check_freq=False, check_index_type=False)


def test_observed_and_simulated(observation_run, observed):
    flow, level = observed_and_simulated(*observation_run)
    # the unlinked Observation is left out
    assert (flow.node_id, flow.name, flow.variable, flow.observed_node_id) == (4, "afvoer", "flow_rate", 2)
    assert (level.node_id, level.name, level.variable, level.observed_node_id) == (5, "peil", "level", 1)
    assert_values_equal(flow.observed, observed)
    # the flow rate of the Outlet is that of its outflow link
    assert_values_equal(flow.simulated, observed)
    assert_values_equal(level.simulated, observed + 0.5)


def test_compare_observations(observation_run, observed):
    comparison = compare_observations(*observation_run)
    assert comparison.index.tolist() == [4, 5]
    assert comparison["observed_node_id"].tolist() == [2, 1]
    assert comparison.at[4, "nse"] == 1.0
    assert comparison.at[5, "bias"] == pytest.approx(0.5 / observed.abs().mean())


def test_observed_and_simulated_junction(tmp_path, observed):
    """The simulated flow rate of a Junction of a Basin tree is the net flow of its link towards the Basin.

    LevelBoundary 6 -> Outlet 5 -> Junction 4 -> Basin 1, and Junction 4 -> Outlet 2 -> LevelBoundary 7, with
    Observation 8 of Junction 4.
    """
    time = observed.index
    model = Model(starttime=time[0], endtime=time[-1], crs="EPSG:28992")
    model.basin.add(
        Node(1, Point(0, 0)), [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])]
    )
    for node_id, x in ((2, 20), (5, -20)):
        model.outlet.add(Node(node_id, Point(x, 10)), [outlet.Static(flow_rate=[1.0])])
    model.junction.add(Node(4, Point(0, 10)))
    for node_id, x in ((6, -30), (7, 30)):
        model.level_boundary.add(Node(node_id, Point(x, 10)), [level_boundary.Static(level=[0.0])])
    model.observation.add(
        Node(8, Point(0, 20), name="netto"),
        [observation.Time(time=time, variable="flow_rate", value=observed.to_numpy())],
    )
    for from_node, to_node in (
        (model.level_boundary[6], model.outlet[5]),
        (model.outlet[5], model.junction[4]),
        (model.junction[4], model.basin[1]),
        (model.junction[4], model.outlet[2]),
        (model.outlet[2], model.level_boundary[7]),
        (model.observation[8], model.junction[4]),
    ):
        model.link.add(from_node, to_node)
    toml_path = tmp_path / "model.toml"
    model.write(toml_path)

    # Outlet 5 lets in twice the observed flow and Outlet 2 lets out the observed flow, so the net flow is observed
    link = model.link.df
    flow_links = link.index[link["link_type"] == "flow"].to_numpy()
    flows = {(6, 5): 2.0, (5, 4): 2.0, (4, 1): 1.0, (4, 2): 1.0, (2, 7): 1.0}
    factors = [flows[(int(link.at[i, "from_node_id"]), int(link.at[i, "to_node_id"]))] for i in flow_links]
    results_dir = tmp_path / "results"
    results_dir.mkdir()
    flow_rate = observed.to_numpy()[:, None] * np.array(factors)[None, :]
    xr.Dataset({"flow_rate": (("time", "link_id"), flow_rate)}, coords={"time": time, "link_id": flow_links}).to_netcdf(
        results_dir / "flow.nc"
    )
    (series,) = observed_and_simulated(tmp_path / "input/database.gpkg", results_dir)
    assert series.observed_node_id == 4
    assert series.link_ids == (int(link.index[(link["from_node_id"] == 4) & (link["to_node_id"] == 1)][0]),)
    assert_values_equal(series.simulated, observed)
