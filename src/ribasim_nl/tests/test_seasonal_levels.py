from datetime import datetime

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from ribasim import Node
from ribasim.nodes import basin, discrete_control, level_boundary, outlet
from ribasim_nl.peilbeheer import DEFAULT_TRANSITIONS, SEASONS, TRANSITION_COLUMNS, complete_peilbeheer
from ribasim_nl.seasonal_levels import (
    apply_seasonal_levels,
    assign_peilbeheer,
    remove_seasonal_levels,
    season_switch,
    seasonal_differences,
    seasonal_thresholds,
)
from shapely.geometry import MultiPolygon, Point, Polygon, box

from ribasim_nl import Model

# Nodes of Aa en Maas in the coupled model, see `ribasim_nl.aquo`
AAM = 38 * 100_000
TARGET_LEVEL = -0.5


def square(x: float) -> MultiPolygon:
    return MultiPolygon([Polygon([(x - 500, -500), (x + 500, -500), (x + 500, 500), (x - 500, 500)])])


def control_tables(listen_node_id: int) -> list:
    """A DiscreteControl that switches to `afvoer` above the target level, with the default band."""
    return [
        discrete_control.Variable(compound_variable_id=[1], listen_node_id=[listen_node_id], variable=["level"]),
        discrete_control.Condition(
            compound_variable_id=[1],
            condition_id=[1],
            threshold_low=[TARGET_LEVEL - 0.025],
            threshold_high=[TARGET_LEVEL + 0.025],
        ),
        discrete_control.Logic(truth_state=["F", "T"], control_state=["aanvoer", "afvoer"]),
    ]


def build_model(offset: int) -> Model:
    """LevelBoundary 1 -> inlet Outlet 2 -> Basin 3 -> drain Outlet 4 -> LevelBoundary 5.

    DiscreteControl 6 controls the inlet and 7 the drain, both listening to Basin 3. The node IDs are offset by `offset`.
    """
    model = Model(starttime="2017-01-01", endtime="2018-01-01", crs="EPSG:28992")
    model.level_boundary.add(Node(offset + 1, Point(0, 0)), [level_boundary.Static(level=[1.0])])
    model.outlet.add(
        Node(offset + 2, Point(1000, 0)),
        [
            outlet.Static(
                control_state=["aanvoer", "afvoer"],
                flow_rate=[1.0, 0.0],
                max_downstream_level=[TARGET_LEVEL, np.inf],
            )
        ],
    )
    model.basin.add(
        Node(offset + 3, Point(2000, 0)),
        [
            basin.Profile(level=[-2.0, 1.0], area=[100.0, 100.0]),
            basin.State(level=[TARGET_LEVEL]),
            basin.Area(geometry=[square(2000)], meta_streefpeil=[TARGET_LEVEL]),
        ],
    )
    model.outlet.add(
        Node(offset + 4, Point(3000, 0)),
        [
            outlet.Static(
                control_state=["aanvoer", "afvoer"],
                flow_rate=[0.0, 10.0],
                min_upstream_level=[TARGET_LEVEL, TARGET_LEVEL],
            )
        ],
    )
    model.level_boundary.add(Node(offset + 5, Point(4000, 0)), [level_boundary.Static(level=[-2.0])])
    model.discrete_control.add(Node(offset + 6, Point(1000, 100)), control_tables(offset + 3))
    model.discrete_control.add(Node(offset + 7, Point(3000, 100)), control_tables(offset + 3))
    for from_node, to_node in (
        (model.level_boundary[offset + 1], model.outlet[offset + 2]),
        (model.outlet[offset + 2], model.basin[offset + 3]),
        (model.basin[offset + 3], model.outlet[offset + 4]),
        (model.outlet[offset + 4], model.level_boundary[offset + 5]),
        (model.discrete_control[offset + 6], model.outlet[offset + 2]),
        (model.discrete_control[offset + 7], model.outlet[offset + 4]),
    ):
        model.link.add(from_node, to_node)
    return model


@pytest.fixture
def model() -> Model:
    """The model of `build_model` as part of the coupled model, with the node IDs of Aa en Maas."""
    return build_model(AAM)


def peilgebied(
    summer: float, winter: float, transitions: tuple = (pd.NA,) * 4, geometry=None, **bounds: float
) -> gpd.GeoDataFrame:
    """A peilgebied of Aa en Maas that covers half of Basin 3, with the default transitions unless given."""
    df = pd.DataFrame(
        {
            "authority": ["AaenMaas"],
            "code": ["P1"],
            "soort_peilbeheer": ["zomer- en winterpeil"],
            "streefpeil_zomer": [summer],
            "streefpeil_winter": [winter],
            **{f"{b}_{s}": [bounds.get(f"{b}_{s}", np.nan)] for b in ("ondergrens", "bovengrens") for s in SEASONS},
            **{c: pd.array([t], dtype="string") for c, t in zip(TRANSITION_COLUMNS, transitions, strict=True)},
        }
    )
    geometry = geometry if geometry is not None else box(2000, -1000, 3000, 1000)
    return gpd.GeoDataFrame(complete_peilbeheer(df), geometry=[geometry], crs="EPSG:28992")


def control_tables_of(model: Model) -> dict[str, pd.DataFrame]:
    """The tables that `apply_seasonal_levels` changes, sorted, without their index and the `meta_fixed_` columns."""
    tables = {
        "variable": model.discrete_control.variable.df,
        "condition": model.discrete_control.condition.df,
        "logic": model.discrete_control.logic.df,
        "outlet": model.outlet.static.df,
        "node": model.node.df[["cyclic_time"]].reset_index(),
    }
    sorted_tables = {}
    for name, df in tables.items():
        assert df is not None
        df = df.drop(columns=[c for c in df.columns if c.startswith("meta_fixed_")]).reset_index(drop=True)
        sorted_tables[name] = df.sort_values(list(df.columns[:3])).reset_index(drop=True)
    return sorted_tables


def test_season_switch():
    switch = season_switch(("01-03", "01-05", "01-09", "01-11"), 2017)
    assert switch.index.tolist() == [
        datetime(2017, 1, 1),
        datetime(2017, 3, 1),
        datetime(2017, 9, 1),
        datetime(2018, 1, 1),
    ]
    assert switch.tolist() == [0.0, 1.0, 0.0, 0.0]
    # 1 April in a leap year
    assert season_switch(DEFAULT_TRANSITIONS, 2020).index[1] == datetime(2020, 4, 1)


def test_assign_peilbeheer(model):
    # the target level of the Basin is closest to the summer level, which it keeps
    regimes = assign_peilbeheer(model, peilgebied(summer=-0.45, winter=-0.65))
    regime = regimes.loc[AAM + 3]
    assert regime["peilgebied"] == "P1"
    assert regime["overlap"] == pytest.approx(0.5)
    assert regime[["streefpeil_zomer", "streefpeil_winter"]].tolist() == pytest.approx([-0.5, -0.7])
    assert regime[["ondergrens_winter", "bovengrens_winter"]].tolist() == pytest.approx([-0.725, -0.675])
    # a peilgebied of another authority, or that covers too little of the Basin
    assert assign_peilbeheer(model, peilgebied(-0.45, -0.65).assign(authority="DeDommel")).empty
    assert assign_peilbeheer(model, peilgebied(-0.45, -0.65), min_overlap=0.6).empty
    # a peilgebied of which both target levels are far from that of the Basin
    assert assign_peilbeheer(model, peilgebied(0.3, 0.1)).empty
    assert len(assign_peilbeheer(model, peilgebied(0.3, 0.1), max_offset=0.6)) == 1


def test_assign_regional_model():
    # in a regional model, the authority is given instead of following from the node IDs
    model = build_model(0)
    assert assign_peilbeheer(model, peilgebied(-0.45, -0.65)).empty
    assert len(assign_peilbeheer(model, peilgebied(-0.45, -0.65), authority="AaenMaas")) == 1


def test_assign_weirs(model):
    # weirs within 100 m of the Basin, of which the one with the closest target level counts
    weirs = pd.concat(
        [peilgebied(-0.3, -0.4, geometry=Point(2550, 0)), peilgebied(-0.52, -0.62, geometry=Point(2550, 0))]
    )
    weirs["code"] = ["W1", "W2"]
    regime = assign_peilbeheer(model, weirs).loc[AAM + 3]
    assert regime["peilgebied"] == "W2"
    assert regime[["streefpeil_zomer", "streefpeil_winter"]].tolist() == pytest.approx([-0.5, -0.6])
    assert assign_peilbeheer(model, peilgebied(-0.52, -0.62, geometry=Point(2650, 0))).empty


def test_seasonal_thresholds():
    regimes = pd.DataFrame(
        {
            "meta_streefpeil": [-0.5],
            "streefpeil_zomer": [-0.5],
            "ondergrens_zomer": [-0.6],
            "bovengrens_zomer": [-0.45],
            "streefpeil_winter": [-0.7],
            "ondergrens_winter": [-0.725],
            "bovengrens_winter": [-0.675],
        },
        index=[1],
    )
    conditions = pd.DataFrame(
        {
            "basin": [1, 1, 1],
            "weight": [1.0, -1.0, 1.0],
            # the default band around the target level, around minus the target level, and another band
            "threshold_low": [-0.525, 0.475, -0.51],
            "threshold_high": [-0.475, 0.525, -0.49],
        }
    )
    season = pd.Series([0.0, 1.0])
    low, high = seasonal_thresholds(conditions, seasonal_differences(regimes), season)
    # in winter, and in summer with the bounds of the target level
    np.testing.assert_allclose(low, [[-0.725, -0.6], [0.675, 0.45], [-0.71, -0.51]])
    np.testing.assert_allclose(high, [[-0.675, -0.45], [0.725, 0.6], [-0.69, -0.49]])


def test_apply_seasonal_levels(model, tmp_path):
    apply_seasonal_levels(model, peilgebied(summer=-0.5, winter=-0.7))

    # the thresholds switch at the begin of the transitions
    condition = model.discrete_control.condition.df.set_index(["node_id", "compound_variable_id", "time"])
    drain = condition.loc[(AAM + 7, 1)]
    assert drain.index.tolist() == [
        datetime(2017, 1, 1),
        datetime(2017, 4, 1),
        datetime(2017, 10, 1),
        datetime(2018, 1, 1),
    ]
    assert drain["threshold_low"].tolist() == pytest.approx([-0.725, -0.525, -0.725, -0.725])
    assert drain["threshold_high"].tolist() == pytest.approx([-0.675, -0.475, -0.675, -0.675])
    assert drain["meta_fixed_threshold_low"].tolist() == pytest.approx([-0.525] * 4)
    # the condition on the season, of a variable with weight 0
    season = condition.loc[(AAM + 7, 2)]
    assert season.index.tolist() == drain.index.tolist()
    assert season["threshold_high"].tolist() == [2.0, -1.0, 2.0, 2.0]
    variable = model.discrete_control.variable.df.set_index(["node_id", "compound_variable_id"])
    assert variable.loc[(AAM + 7, 2), ["listen_node_id", "weight"]].tolist() == [AAM + 3, 0.0]
    logic = model.discrete_control.logic.df
    logic = logic.loc[logic.node_id == AAM + 7]
    assert dict(zip(logic["truth_state"], logic["control_state"], strict=True)) == {
        "FF": "aanvoer winter",
        "TF": "afvoer winter",
        "FT": "aanvoer zomer",
        "TT": "afvoer zomer",
    }
    assert model.node.df.loc[[AAM + 6, AAM + 7], "cyclic_time"].all()

    # the levels of the control states per season
    static = model.outlet.static.df.set_index(["node_id", "control_state"])
    assert static.loc[(AAM + 4, "afvoer winter"), "min_upstream_level"] == pytest.approx(-0.7)
    assert static.loc[(AAM + 4, "afvoer zomer"), "min_upstream_level"] == pytest.approx(-0.5)
    assert static.loc[(AAM + 2, "aanvoer winter"), "max_downstream_level"] == pytest.approx(-0.7)
    assert static.loc[(AAM + 2, "aanvoer zomer"), "max_downstream_level"] == pytest.approx(-0.5)
    assert static.loc[(AAM + 2, "afvoer zomer"), "max_downstream_level"] == np.inf
    assert static.loc[(AAM + 4, "afvoer winter"), "meta_fixed_min_upstream_level"] == pytest.approx(-0.5)

    # the initial level is the winter level, and the level management is metadata of the Basin
    assert model.basin.state.df["level"].tolist() == pytest.approx([-0.7])
    area = model.basin.area.df.iloc[0]
    assert area["meta_peilgebied"] == "P1"
    assert area[["meta_streefpeil", "meta_streefpeil_zomer", "meta_streefpeil_winter"]].tolist() == pytest.approx(
        [-0.5, -0.5, -0.7]
    )

    model.validate_ribasim_nl(layout=False)
    model.write(tmp_path / "ribasim.toml")
    Model.read(tmp_path / "ribasim.toml")


def test_apply_transitions(model):
    apply_seasonal_levels(model, peilgebied(summer=-0.5, winter=-0.7, transitions=("01-03", "01-05", "01-09", "01-11")))
    condition = model.discrete_control.condition.df.set_index(["node_id", "compound_variable_id", "time"])
    drain = condition.loc[(AAM + 7, 1)]
    assert drain.index.tolist() == [
        datetime(2017, 1, 1),
        datetime(2017, 3, 1),
        datetime(2017, 9, 1),
        datetime(2018, 1, 1),
    ]
    assert drain["threshold_high"].tolist() == pytest.approx([-0.675, -0.475, -0.675, -0.675])


def test_apply_fixed_levels(model):
    # a fixed level with wider bounds only widens the band of the thresholds
    apply_seasonal_levels(model, peilgebied(summer=-0.5, winter=-0.5, ondergrens_zomer=-0.6, ondergrens_winter=-0.6))
    condition = model.discrete_control.condition.df
    assert condition["time"].isna().all()
    assert condition["threshold_low"].tolist() == pytest.approx([-0.6, -0.6])
    assert condition["threshold_high"].tolist() == pytest.approx([-0.475, -0.475])
    assert set(model.outlet.static.df["control_state"]) == {"aanvoer", "afvoer"}
    assert not model.node.df["cyclic_time"].any()


@pytest.mark.parametrize("bounds", [{}, {"ondergrens_zomer": -0.6, "ondergrens_winter": -0.8}])
def test_remove_seasonal_levels(model, bounds):
    fixed = control_tables_of(model)
    apply_seasonal_levels(model, peilgebied(summer=-0.5, winter=-0.7, **bounds))
    seasonal = control_tables_of(model)
    # applying again gives the same model, since it removes the seasons first
    apply_seasonal_levels(model, peilgebied(summer=-0.5, winter=-0.7, **bounds), shift_state=False)
    for name, df in control_tables_of(model).items():
        pd.testing.assert_frame_equal(df, seasonal[name], check_dtype=False)
    remove_seasonal_levels(model)
    for name, df in control_tables_of(model).items():
        pd.testing.assert_frame_equal(df, fixed[name], check_dtype=False)
    assert "meta_peilgebied" not in model.basin.area.df.columns
