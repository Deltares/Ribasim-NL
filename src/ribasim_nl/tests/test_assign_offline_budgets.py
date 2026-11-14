import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from ribasim import Node
from ribasim.nodes import basin
from ribasim_nl.assign_offline_budgets import AssignOfflineBudgets
from ribasim_nl.set_forcing import (
    EVAPORATION_VAR,
    MM_PER_DAY_TO_M_PER_S,
    PRECIPITATION_VAR,
    SetDynamicForcing,
    _sample_cell_per_basin,
)
from shapely.geometry import MultiPolygon, Point, box

from ribasim_nl import Model

CELL_SIZE = 250.0
TIME = pd.date_range("2017-06-01", periods=2, name="time")


def budgets(values: dict[str, float]) -> xr.Dataset:
    """Budgets on 3 rows of 4 cells with the lower left corner at the origin, twice as large in the second column."""
    x = (np.arange(4) + 0.5) * CELL_SIZE
    y = (np.arange(3)[::-1] + 0.5) * CELL_SIZE
    data = {}
    for key, value in values.items():
        array = np.full((len(TIME), len(y), len(x)), value)
        array[:, :, 1] *= 2
        data[key] = (("time", "y", "x"), array)
    return xr.Dataset(data, coords={"time": TIME, "y": y, "x": x})


def test_sample_cell_per_basin():
    """A Basin takes the cell that a point on its surface lies in, or the nearest cell outside the raster."""
    polygons = {7: box(260, 10, 300, 50), 3: box(-900, 0, -800, 100), 5: box(0, 0, 100, 100)}
    basin_definition = gpd.GeoDataFrame({"node_id": list(polygons)}, geometry=list(polygons.values()))
    ds = budgets({"a": 4.0, "b": 1.0})
    ds["a"].loc[{"x": 125.0}] = np.nan

    df = _sample_cell_per_basin(ds, basin_definition)

    assert df.index.names == ["node_id", "time"]
    assert df.index.get_level_values("node_id").unique().tolist() == [3, 5, 7]
    # cells without data count as 0, like in the sum over the cells of a Basin
    assert df.xs(TIME[0], level="time").to_dict() == {"a": {3: 0.0, 5: 0.0, 7: 8.0}, "b": {3: 1.0, 5: 1.0, 7: 2.0}}
    assert _sample_cell_per_basin(ds, basin_definition.iloc[:0]).empty


def forcing_model() -> Model:
    """Basin 1 (doorgaand) and its storage Basin 2 contain the center of the lower left cell, storage Basin 3 none."""
    model = Model(starttime=TIME[0], endtime=TIME[-1], crs="EPSG:28992")
    areas = {1: box(100, 0, 260, 250), 2: box(100, 0, 260, 250), 3: box(260, 0, 350, 250)}
    categories = {1: "doorgaand", 2: "bergend", 3: "bergend"}
    for node_id, area in areas.items():
        model.basin.add(
            Node(node_id, Point(area.centroid.x, 10 * node_id), meta_categorie=categories[node_id]),
            [
                basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]),
                basin.State(level=[0.5]),
                basin.Area(geometry=[MultiPolygon([area])]),
            ],
        )
    return model


def test_forcing_of_basins_without_cell_center():
    """A Basin that contains no cell center gets the meteo of the cell it lies in, but no budgets."""
    ds = budgets({PRECIPITATION_VAR: 4.0, EVAPORATION_VAR: 2.0, "bdgriv_sys1": -80.0, "bdgriv_sys2": 16.0})

    model = SetDynamicForcing(forcing_model(), ds, startdate=TIME[0], enddate=TIME[-1]).add()
    model, _ = AssignOfflineBudgets(ds).compute_budgets(
        model, primary_budgets={"bdgriv_sys1"}, secondary_budgets={"bdgriv_sys2"}, surface_runoff_budgets=set()
    )

    df = model.basin.time.df
    assert df is not None
    df = df[df["time"] == TIME[0]].set_index("node_id")
    assert df.index.tolist() == [1, 2, 3]
    # Basin 3 lies in the second column; in June the open water evaporation is 1.3 times Makkink
    np.testing.assert_allclose(df["precipitation"], np.array([4, 4, 8]) * MM_PER_DAY_TO_M_PER_S)
    np.testing.assert_allclose(df["potential_evaporation"], np.array([2, 2, 4]) * 1.3 * MM_PER_DAY_TO_M_PER_S)
    # Basin 1 drains the primary budget of the lower left cell, of which its storage Basin 2 has the center
    np.testing.assert_allclose(df["drainage"], np.array([80, 0, 0]) / 86400)
    np.testing.assert_allclose(df["infiltration"], np.array([0, 16, 0]) / 86400)
    np.testing.assert_array_equal(df["surface_runoff"], 0)
