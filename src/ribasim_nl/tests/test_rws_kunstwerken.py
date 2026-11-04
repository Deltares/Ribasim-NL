import numpy as np
import pandas as pd
import pytest
from ribasim_nl.rws_kunstwerken import (
    PARALLEL_KWK,
    SUBTRACT_PROPERTY,
    discrete_control_tables,
    flow_demand_tables,
    has_discrete_control,
    has_flow_demand,
    line_along,
    rating_curve_from_bottom,
    read_control_thresholds,
    read_kwk_properties,
    read_static,
    repeat_yearly,
)
from shapely.geometry import LineString, Point

STARTTIME = pd.Timestamp("2017-01-01")
ENDTIME = pd.Timestamp("2019-01-01")


def sheet(rows: list[list]) -> pd.DataFrame:
    """A structure sheet as read by `pd.read_excel`, with four columns."""
    rows = [row + [np.nan] * (4 - len(row)) for row in rows]
    return pd.DataFrame(rows, columns=["Eigenschap", "Waarde", "beschrijving", "bron"])


def sluis_born() -> pd.DataFrame:
    """Like Sluis Born in Maas.xlsx: an afvoer Outlet with DiscreteControl and a FlowDemand."""
    return sheet(
        [
            ["Ribasim type", "Outlet"],
            ["Kunstwerkcode", "60A-001-06"],
            ["Aanvoer Capaciteit (m3/s)", 0],
            ["Afvoer Capaciteit (m3/s)", 15.7, "schutverliezen + lekverliezen"],
            ["Minimale capaciteit (m3/s)", 0.5],
            ["Maximale capaciteit (m3/s)", 15.7],
            ["Gereviewed door", np.nan],
            ["Meetlocatiecode", 8008147],
            ["Type sturing", "DiscreteControl"],
            ["Meetlocatiecode", 8008147],
            ["FlowDemand", "ja"],
            ["Review datum", pd.Timestamp("2026-10-23")],
            [np.nan],
            ["Toestand (control_state)", "Bovengrens (threshold_high)", "Ondergrens (threshold_low)"],
            ["afvoer", 44.075, 44.025],
            [np.nan],
            ["FlowDemand (m3/s)", "Prioriteit"],
            [15, 9, np.nan, "Schut+Lekverlies"],
        ]
    )


def test_rating_curve_from_bottom():
    """Like the Waal at Tiel, a calculated Q(h) table gets a first row without flow at the bottom of the Basin."""
    qh = pd.DataFrame({"Q": [347.7, 370.1, 394.8], "H": [1.0, 1.1, 1.2]})
    static = rating_curve_from_bottom(qh, bottom=-3.7).df
    assert static is not None
    assert static["level"].to_list() == [-3.7, 1.0, 1.1, 1.2]
    assert static["flow_rate"].to_list() == [0.0, 347.7, 370.1, 394.8]


@pytest.mark.parametrize(
    ("q", "h", "bottom"),
    [
        ([1.0, 2.0], [1.0, 1.0], 0.0),  # repeated level
        ([2.0, 1.0], [1.0, 2.0], 0.0),  # decreasing flow
        ([0.0, 1.0], [1.0, 2.0], 0.0),  # no flow already in the table
        ([1.0, 2.0], [1.0, 2.0], 1.0),  # bottom not below the table
        ([1.0, np.nan], [1.0, 2.0], 0.0),  # missing flow
    ],
)
def test_rating_curve_from_bottom_invalid(q: list[float], h: list[float], bottom: float):
    with pytest.raises(AssertionError):
        rating_curve_from_bottom(pd.DataFrame({"Q": q, "H": h}), bottom)


def test_read_kwk_properties():
    properties = read_kwk_properties(sluis_born())
    assert properties["Kunstwerkcode"] == "60A-001-06"
    assert properties["Meetlocatiecode"] == 8008147
    assert "Gereviewed door" not in properties
    # the tables are not properties
    assert "afvoer" not in properties
    assert has_discrete_control(properties)
    assert has_flow_demand(properties)


def test_read_kwk_properties_conflict():
    df = sheet([["Ribasim type", "Outlet"], ["Meetlocatiecode", 1], ["Meetlocatiecode", 2]])
    with pytest.raises(ValueError, match="Conflicting"):
        read_kwk_properties(df)


def test_read_static_local():
    """Without DiscreteControl, the afvoer capacity is the flow rate, and it caps the flow."""
    df = sheet(
        [
            ["Ribasim type", "Outlet"],
            ["Kunstwerkcode", "45D-358"],
            ["Streefpeil bovenstrooms (m +NAP)", 4.7],
            ["Afvoer Capaciteit (m3/s)", 18],
            ["Minimale capaciteit (m3/s)", 0.5],
            ["Maximale capaciteit (m3/s)", 28],
            ["Type sturing", "lokaal"],
        ]
    )
    static = read_static(df, "Outlet").df
    assert static is not None
    assert static[["flow_rate", "max_flow_rate", "min_flow_rate", "min_upstream_level"]].iloc[0].to_list() == [
        18,
        18,
        0.5,
        4.7,
    ]
    assert not has_flow_demand(read_kwk_properties(df))


def test_read_static_flow_demand_level():
    """Like Sluis 15, a lock with a FlowDemand stops at its Streefpeil, so it does not empty the pound above it."""
    df = sheet(
        [
            ["Ribasim type", "Outlet"],
            ["Streefpeil (m +NAP)", 33.61],
            ["Capaciteit (m3/s)", 15],
            ["Type sturing", "lokaal"],
            ["FlowDemand", "ja"],
        ]
    )
    static = read_static(df, "Outlet").df
    assert static is not None
    assert static["min_upstream_level"].to_list() == [33.61]
    # without a FlowDemand the Streefpeil is not used
    static = read_static(df[df["Eigenschap"] != "FlowDemand"], "Outlet").df
    assert static is not None
    assert "min_upstream_level" not in static or static["min_upstream_level"].isna().all()


def test_read_static_flow_demand_at_sea_level():
    """Like the Goereese sluis, a level of 0 m NAP is a level, and the afvoer capacity without control is the flow."""
    df = sheet(
        [
            ["Ribasim type", "Outlet"],
            ["Kunstwerkcode", "37C-001-01"],
            ["Aanvoer Capaciteit (m3/s)", 0],
            ["Afvoer Capaciteit (m3/s)", 5],
            ["Streefpeil bovenstrooms (m +NAP)", 0],
            ["FlowDemand", "ja"],
            [np.nan],
            ["FlowDemand (m3/s)", "Prioriteit"],
            [5, 2],
        ]
    )
    static = read_static(df, "Outlet").df
    assert static is not None
    assert static[["flow_rate", "min_upstream_level"]].iloc[0].to_list() == [5, 0]
    assert has_flow_demand(read_kwk_properties(df))
    demand = flow_demand_tables(df, STARTTIME, ENDTIME)[0].df
    assert demand is not None
    assert demand[["demand", "demand_priority"]].iloc[0].to_list() == [5, 2]


def test_read_static_discrete_control():
    static = read_static(sluis_born(), "Outlet").df
    assert static is not None
    assert static["control_state"].to_list() == ["aanvoer", "afvoer"]
    assert static["flow_rate"].to_list() == [0.0, 15.7]
    assert static["max_flow_rate"].to_list() == [15.7, 15.7]
    assert static["min_flow_rate"].to_list() == [0.5, 0.5]


@pytest.mark.parametrize(
    ("node_type", "listen_side", "levels"),
    [
        # Sluis Born drains the Julianakanaal it listens to, down to the lower threshold
        ("Outlet", "upstream", {"min_upstream_level": [44.025] * 2}),
        # a structure that only drains does not limit the level of the Basin downstream
        ("Outlet", "downstream", {}),
        ("Outlet", None, {}),
    ],
)
def test_read_static_levels(node_type, listen_side, levels):
    static = read_static(sluis_born(), node_type, listen_side).df
    assert static is not None
    for column in ("min_upstream_level", "max_downstream_level"):
        if column in levels:
            assert static[column].to_list() == levels[column]
        else:
            assert column not in static or static[column].isna().all()


def test_read_static_supply_levels():
    """Like Gemaal Born, a pump that supplies the Basin downstream stops at the upper threshold."""
    df = sheet(
        [
            ["Ribasim type", "Pump"],
            ["Aanvoer Capaciteit (m3/s)", 12],
            ["Afvoer Capaciteit (m3/s)", 0],
            ["Streefpeil benedenstrooms (m +NAP)", 44.05],
            ["Type sturing", "DiscreteControl"],
            [np.nan],
            ["Toestand (control_state)", "Bovengrens (threshold_high)", "Ondergrens (threshold_low)"],
            ["aanvoer", 44.075, 44.025],
        ]
    )
    static = read_static(df, "Pump", "downstream").df
    assert static is not None
    assert static["flow_rate"].to_list() == [12.0, 0.0]
    assert static["max_downstream_level"].to_list() == [44.075, 44.075]


def test_discrete_control_tables():
    variable, condition, logic = discrete_control_tables(sluis_born(), 8147, STARTTIME, ENDTIME)
    assert variable.df["listen_node_id"].to_list() == [8147]
    assert condition.df[["threshold_high", "threshold_low"]].iloc[0].to_list() == [44.075, 44.025]
    assert condition.df["time"].to_list() == [STARTTIME]
    # afvoer above the thresholds, also for an aanvoer structure
    assert dict(zip(logic.df["truth_state"], logic.df["control_state"], strict=True)) == {"T": "afvoer", "F": "aanvoer"}


def test_read_control_thresholds_invalid():
    df = sheet(
        [
            ["Ribasim type", "Pump"],
            ["Type sturing", "DiscreteControl"],
            ["Toestand (control_state)", "Bovengrens (threshold_high)", "Ondergrens (threshold_low)"],
            ["aanvoer", 25.025, 25.975],
        ]
    )
    with pytest.raises(AssertionError, match="threshold_low should be below threshold_high"):
        read_control_thresholds(df)


def test_dated_tables_repeat_yearly():
    """Dated thresholds and demands, like the Nijkerkersluis, repeat in every year of the simulation."""
    df = sheet(
        [
            ["Ribasim type", "Outlet"],
            ["Type sturing", "DiscreteControl"],
            ["FlowDemand", "ja"],
            [np.nan],
            ["FlowDemand (m3/s)", "Prioriteit", "Datum"],
            [0, 6, pd.Timestamp("2026-01-01")],
            [2, 6, pd.Timestamp("2026-05-01")],
            [0, 6, pd.Timestamp("2026-10-01")],
            [np.nan],
            ["Toestand (control_state)", "Bovengrens (threshold_high)", "Ondergrens (threshold_low)", "Datum"],
            ["afvoer", -0.28, -0.30, pd.Timestamp("2024-01-01")],
            ["afvoer", -0.04, -0.06, pd.Timestamp("2024-04-01")],
        ]
    )
    (demand,) = flow_demand_tables(df, STARTTIME, ENDTIME)
    assert demand.df is not None
    assert demand.df["demand"].to_list() == [0, 2, 0] * 3
    assert demand.df["time"].iloc[3] == pd.Timestamp("2018-01-01")
    assert demand.df["time"].iloc[-1] == pd.Timestamp("2019-10-01")

    _, condition, _ = discrete_control_tables(df, 7030, STARTTIME, ENDTIME)
    assert condition.df is not None
    assert condition.df["threshold_high"].to_list() == [-0.28, -0.04] * 3
    assert condition.df["time"].iloc[1] == pd.Timestamp("2017-04-01")


def test_repeat_yearly_sorts_by_day_of_year():
    time = pd.Series(pd.to_datetime(["2024-10-01", "2025-04-01"]))
    repeated = repeat_yearly(time, STARTTIME, pd.Timestamp("2017-12-31"))
    assert repeated["index"].to_list() == [1, 0]
    assert repeated["time"].to_list() == [pd.Timestamp("2017-04-01"), pd.Timestamp("2017-10-01")]


def test_line_along():
    """Like the channel and the fairway from the Goereese sluis to the sea, which connect end to end."""
    lines = [LineString([(10, 10), (0, 10), (0, 0)]), LineString([(10, 10), (20, 10)])]
    line = line_along(lines, Point(-1, 5), Point(15, 11))
    assert list(line.coords) == [(-1, 5), (0, 5), (0, 10), (10, 10), (15, 10), (15, 11)]
    # the other way around
    assert list(line_along(lines, Point(15, 11), Point(-1, 5)).coords) == list(line.coords)[::-1]


def test_line_along_needs_connected_lines():
    lines = [LineString([(0, 0), (1, 0)]), LineString([(5, 0), (6, 0)])]
    with pytest.raises(AssertionError, match="connect end to end"):
        line_along(lines, Point(0, 0), Point(6, 0))


def test_parallel_kwk_next_to_a_structure_in_the_network():
    """A structure next to another one needs that one in the network, so it cannot be next to such a structure itself."""
    assert not {parallel.main for parallel in PARALLEL_KWK.values()} & set(PARALLEL_KWK)


def test_discrete_control_on_level_difference():
    """A threshold on a difference between two Basins, which says nothing about the level to stop at."""
    kwk_df = sheet(
        [
            ["Ribasim type", "Outlet"],
            ["Kunstwerkcode", "44F-002-01"],
            ["Aanvoer Capaciteit (m3/s)", 0.1],
            ["Afvoer Capaciteit (m3/s)", 0],
            ["Meetlocatiecode", 8004941],
            [SUBTRACT_PROPERTY, 8005265],
            ["Type sturing", "DiscreteControl"],
            [np.nan],
            ["Toestand (control_state)", "Bovengrens (threshold_high)", "Ondergrens (threshold_low)"],
            ["aanvoer", 1.8, 1.75],
        ]
    )
    variable, condition, logic = discrete_control_tables(kwk_df, 4941, STARTTIME, ENDTIME, 5265)
    assert variable.df["listen_node_id"].to_list() == [4941, 5265]
    assert variable.df["weight"].to_list() == [1.0, -1.0]
    assert variable.df["compound_variable_id"].to_list() == [1, 1]
    assert len(condition.df) == 1
    # no flow above the threshold, in the afvoer state
    static = read_static(kwk_df, "Outlet", "upstream").df
    assert static.set_index("control_state")["flow_rate"].to_dict() == {"aanvoer": 0.1, "afvoer": 0.0}
    assert static["min_upstream_level"].isna().all() and static["max_downstream_level"].isna().all()
    assert dict(zip(logic.df["truth_state"], logic.df["control_state"], strict=True)) == {"T": "afvoer", "F": "aanvoer"}
