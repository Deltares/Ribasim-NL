import pandas as pd
import pytest
from ribasim import Node
from ribasim.nodes import basin, level_boundary, manning_resistance, outlet, pump
from ribasim_nl.design_capacities import design_capacities, set_design_capacities
from shapely.geometry import MultiPolygon, Point, box

from ribasim_nl import Model

MM_PER_DAY = 1e-3 / 86400


def polder_model() -> Model:
    """Supply Outlet 2 from LevelBoundary 1 into Basin 3 (100 ha), drained by Pump 5 into Basin 6 (10 ha).

    Bergend Basin 4 drains into Basin 3 through ManningResistance 9, and has the same area.
    Basin 6 drains by Outlet 7 into LevelBoundary 8.
    """
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    model.level_boundary.add(Node(1, Point(0, 0)), [level_boundary.Static(level=[0.0])])
    model.level_boundary.add(Node(8, Point(400, 0)), [level_boundary.Static(level=[0.0])])
    model.outlet.add(Node(2, Point(50, 0)), [outlet.Static(flow_rate=[1.0], meta_known_flow_rate=[False])])
    model.outlet.add(Node(7, Point(350, 0)), [outlet.Static(flow_rate=[1.0], meta_known_flow_rate=[False])])
    model.pump.add(
        Node(5, Point(200, 0)), [pump.Static(flow_rate=[3.0], max_flow_rate=[3.0], meta_known_flow_rate=[True])]
    )
    for node_id, x, category, polygon in (
        (3, 100, "doorgaand", box(0, 0, 1000, 1000)),
        (4, 100, "bergend", box(0, 0, 1000, 1000)),
        (6, 300, "doorgaand", box(1000, 0, 2000, 100)),
    ):
        model.basin.add(
            Node(node_id, Point(x, 10 * node_id), meta_categorie=category),
            [
                basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]),
                basin.State(level=[0.5]),
                basin.Area(geometry=[MultiPolygon([polygon])]),
            ],
        )
    model.manning_resistance.add(
        Node(9, Point(100, 35)),
        [manning_resistance.Static(length=[100.0], manning_n=[0.04], profile_width=[1.0], profile_slope=[1.0])],
    )
    model.link.add(model.basin[4], model.manning_resistance[9])
    model.link.add(model.manning_resistance[9], model.basin[3])
    model.link.add(model.level_boundary[1], model.outlet[2])
    model.link.add(model.outlet[2], model.basin[3])
    model.link.add(model.basin[3], model.pump[5])
    model.link.add(model.pump[5], model.basin[6])
    model.link.add(model.basin[6], model.outlet[7])
    model.link.add(model.outlet[7], model.level_boundary[8])
    return model


def function_table(functions: dict[int, str]) -> pd.DataFrame:
    """Function table of the Pumps and Outlets of `polder_model`, indexed by node_id."""
    return pd.DataFrame({"function": functions.values()}, index=pd.Index(list(functions), name="node_id"))


def test_design_capacities():
    model = polder_model()
    capacities = design_capacities(model, function_table({2: "supply", 5: "drain", 7: "drain"}))
    # the supply stops at the drain Pump, and the bergend Basin 4 is not counted twice
    assert capacities.at[2, "supplied_area"] == pytest.approx(1e6)
    assert capacities.at[2, "capacity"] == pytest.approx(1e6 * 4 * MM_PER_DAY, rel=1e-2)
    assert capacities.at[5, "drained_area"] == pytest.approx(1e6)
    # the drainage stops at the supply Outlet, and continues through the drain Pump
    assert capacities.at[7, "drained_area"] == pytest.approx(1.1e6)
    assert capacities.at[7, "capacity"] == pytest.approx(1.1e6 * 50 * MM_PER_DAY, rel=1e-2)

    # a flow_control node gets the larger of drainage and supply
    capacities = design_capacities(model, function_table({2: "supply", 5: "flow_control", 7: "drain"}))
    assert capacities.at[5, "supplied_area"] == pytest.approx(1e5)
    assert capacities.at[5, "design"] == pytest.approx(1e6 * 15 * MM_PER_DAY)

    # supplying out of the model supplies the area upstream
    capacities = design_capacities(model, function_table({2: "supply", 5: "drain", 7: "supply"}))
    assert capacities.at[7, "supplied_area"] == pytest.approx(1.1e6)

    with pytest.raises(AssertionError, match="Unknown functions"):
        design_capacities(model, function_table({2: "inlet"}))


def test_design_capacities_split():
    model = polder_model()
    # a second Outlet from Basin 6 to a boundary, parallel to Outlet 7
    model.level_boundary.add(Node(11, Point(400, 100)), [level_boundary.Static(level=[0.0])])
    model.outlet.add(Node(10, Point(350, 100)), [outlet.Static(flow_rate=[1.0], meta_known_flow_rate=[False])])
    model.link.add(model.basin[6], model.outlet[10])
    model.link.add(model.outlet[10], model.level_boundary[11])
    capacities = design_capacities(model, function_table({2: "supply", 5: "drain", 7: "drain", 10: "drain"}))
    # parallel drains share the drainage of Basin 6, and together drain all the area upstream
    assert capacities.at[7, "drained_area"] == capacities.at[10, "drained_area"] == pytest.approx(0.55e6)

    # a known drain carries up to its capacity, and the unknown drains share the rest
    outlet_df = model.outlet.static.df
    assert outlet_df is not None
    outlet_df.loc[outlet_df["node_id"] == 10, ["max_flow_rate", "meta_known_flow_rate"]] = [
        0.2e6 * 50 * MM_PER_DAY,
        True,
    ]
    capacities = design_capacities(model, function_table({2: "supply", 5: "drain", 7: "drain", 10: "drain"}))
    assert capacities.at[10, "drained_area"] == pytest.approx(0.2e6)
    assert capacities.at[7, "drained_area"] == pytest.approx(0.9e6)


def test_design_capacities_trapped_loop():
    model = polder_model()
    # an unknown Outlet back from Basin 6 to Basin 3 closes a loop with the unknown Pump 5
    model.outlet.add(Node(12, Point(250, 50)), [outlet.Static(flow_rate=[1.0], meta_known_flow_rate=[False])])
    model.link.add(model.basin[6], model.outlet[12])
    model.link.add(model.outlet[12], model.basin[3])
    pump_df, outlet_df = model.pump.static.df, model.outlet.static.df
    assert pump_df is not None and outlet_df is not None
    pump_df["meta_known_flow_rate"] = False
    outlet_df.loc[outlet_df["node_id"] == 7, ["max_flow_rate", "meta_known_flow_rate"]] = [0.01, True]
    capacities = design_capacities(model, function_table({2: "supply", 5: "drain", 7: "drain", 12: "drain"}))
    # only the known Outlet 7 leaves the loop, so it carries all, and Outlet 12 does not pass the drainage back
    assert capacities.at[7, "drained_area"] == pytest.approx(1.1e6)
    assert capacities.at[5, "drained_area"] == pytest.approx(1e6)
    assert capacities.at[12, "drained_area"] == 0.0
    # Outlet 12 keeps its share of the own area of Basin 6
    assert capacities.at[12, "design"] == pytest.approx(1e5 / 2 * 50 * MM_PER_DAY)


def test_design_capacities_downstream_limit():
    model = polder_model()
    pump_df, outlet_df = model.pump.static.df, model.outlet.static.df
    assert pump_df is not None and outlet_df is not None
    pump_df["meta_known_flow_rate"] = False
    outlet_df.loc[outlet_df["node_id"] == 7, ["max_flow_rate", "meta_known_flow_rate"]] = [0.01, True]
    capacities = design_capacities(model, function_table({2: "supply", 5: "drain", 7: "drain"}))
    # the drain Pump delivers no more than the supply of Basin 6 and the known Outlet out of it
    assert capacities.at[5, "design"] == pytest.approx(1e6 * 15 * MM_PER_DAY)
    assert capacities.at[5, "capacity"] == pytest.approx(1e5 * 4 * MM_PER_DAY + 0.01, rel=1e-2)
    # the supply Outlet into Basin 3 needs less than the supply of Basin 3 and the Pump out of it
    assert capacities.at[2, "capacity"] == pytest.approx(capacities.at[2, "design"], rel=1e-2)


def test_set_design_capacities():
    model = polder_model()
    capacities = set_design_capacities(model, function_table({2: "supply", 5: "drain", 7: "drain"}))
    assert model.outlet.static.df is not None and model.pump.static.df is not None
    outlet_df = model.outlet.static.df.set_index("node_id")
    assert outlet_df.at[2, "max_flow_rate"] == outlet_df.at[2, "flow_rate"] == capacities.at[2, "capacity"]
    # the known Pump capacity is kept
    assert capacities.at[5, "known"]
    assert model.pump.static.df[["flow_rate", "max_flow_rate"]].eq(3.0).all().all()


def test_set_design_capacities_between_boundaries():
    model = polder_model()
    model.level_boundary.add(Node(10, Point(0, 100)), [level_boundary.Static(level=[0.0])])
    model.level_boundary.add(Node(12, Point(100, 100)), [level_boundary.Static(level=[0.0])])
    model.outlet.add(
        Node(11, Point(50, 100)),
        [outlet.Static(flow_rate=[2.0], max_flow_rate=[2.5], meta_known_flow_rate=[False])],
    )
    model.link.add(model.level_boundary[10], model.outlet[11])
    model.link.add(model.outlet[11], model.level_boundary[12])
    set_design_capacities(model, function_table({2: "supply", 5: "drain", 7: "drain", 11: "supply"}))
    # an Outlet that supplies no Basin keeps its capacity
    assert model.outlet.static.df is not None
    outlet_df = model.outlet.static.df.set_index("node_id")
    assert outlet_df.at[11, "max_flow_rate"] == outlet_df.at[11, "flow_rate"] == 2.5
