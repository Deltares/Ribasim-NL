import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from ribasim import Node
from ribasim.nodes import basin, manning_resistance, outlet, tabulated_rating_curve
from ribasim_nl.rws_kunstwerken import (
    PARALLEL_KWK,
    RWS_NODE_ID_OFFSET,
    SUBTRACT_PROPERTY,
    ParallelKwk,
    discrete_control_tables,
    flow_demand_tables,
    has_discrete_control,
    has_flow_demand,
    line_along,
    listen_node,
    listen_variable,
    main_route,
    rating_curve_from_bottom,
    read_control_thresholds,
    read_kwk_properties,
    read_static,
    repeat_yearly,
)
from shapely.geometry import LineString, Point

from ribasim_nl import Model, Network

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
    qh = pd.DataFrame({"Q": q, "H": h})
    with pytest.raises(AssertionError):
        rating_curve_from_bottom(qh, bottom)


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


def test_read_static_seasonal_thresholds():
    """Like Sluis Borgharen, a lock with summer and winter thresholds and a FlowDemand stops at the lowest threshold."""
    df = sheet(
        [
            ["Ribasim type", "Outlet"],
            ["Kunstwerkcode", "61F-004-02"],
            ["Streefpeil bovenstrooms (m +NAP)", 44.05],
            ["Aanvoer Capaciteit (m3/s)", 0],
            ["Afvoer Capaciteit (m3/s)", 0.1],
            ["Maximale capaciteit (m3/s)", 0.1],
            ["FlowDemand", "ja"],
            ["Type sturing", "DiscreteControl"],
            ["Meetlocatiecode", 8007826],
            [np.nan],
            ["FlowDemand (m3/s)", "Prioriteit"],
            [0.1, 2],
            [np.nan],
            ["Toestand (control_state)", "Bovengrens (threshold_high)", "Ondergrens (threshold_low)", "Datum"],
            ["afvoer", 44.075, 44.025, pd.Timestamp("2026-01-01")],
            ["afvoer", 44.175, 44.125, pd.Timestamp("2026-04-01")],
            ["afvoer", 44.075, 44.025, pd.Timestamp("2026-10-01")],
        ]
    )
    static = read_static(df, "Outlet", "upstream").df
    assert static is not None
    assert static["flow_rate"].to_list() == [0.0, 0.1]
    assert static["max_flow_rate"].to_list() == [0.1, 0.1]
    # not at the Streefpeil, and in both states at the winter threshold, also in summer
    assert static["min_upstream_level"].to_list() == [44.025, 44.025]
    _, condition, _ = discrete_control_tables(df, 7826, STARTTIME, ENDTIME)
    assert condition.df is not None
    assert condition.df["threshold_low"].to_list() == [44.025, 44.125, 44.025] * 3


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
    start, end = Point(0, 0), Point(6, 0)
    with pytest.raises(AssertionError, match="connect end to end"):
        line_along(lines, start, end)


def gap_network() -> tuple[Network, tuple[int, int]]:
    """A network with a gap from x = 10 to 20, like at a lock chamber, and the nodes at both ends of the gap."""
    network = Network(gpd.GeoDataFrame(geometry=[LineString([(0, 0), (10, 0)]), LineString([(20, 0), (30, 0)])]))
    nodes = network.nodes
    ends = tuple(int(nodes.index[nodes.distance(Point(x, 0)) < 1e-6][0]) for x in (10, 20))
    return network, (ends[0], ends[1])


def test_main_route_bridges_lock_chambers():
    """Like Sluis Roermond, a lock in a gap of the network gets a line through its chamber."""
    network, chamber = gap_network()
    route = main_route(network, {"lock": ParallelKwk("weir", xy=(15.0, 1.0), chamber=chamber)})
    assert len(route) == 3
    assert list(route.geometry.iloc[-1].coords) == [(10, 0), (20, 0)]
    # without a chamber, the network as it is
    assert len(main_route(network, {"lock": ParallelKwk("weir", xy=(15.0, 1.0))})) == 2


def test_main_route_lock_in_its_chamber():
    network, chamber = gap_network()
    with pytest.raises(AssertionError, match="not in its chamber"):
        main_route(network, {"lock": ParallelKwk("weir", xy=(15.0, 10.0), chamber=chamber)})


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


def test_read_static_supply_without_control():
    """Like the Prs. Beatrixsluis, a supply pump without control has its aanvoer capacity as flow rate."""
    df = sheet(
        [
            ["Ribasim type", "Pump"],
            ["Kunstwerkcode", "38F-352-01"],
            ["Aanvoer Capaciteit (m3/s)", 10],
            ["Afvoer Capaciteit (m3/s)", 0],
            ["Maximale capaciteit (m3/s)", 13],
            ["Streefpeil bovenstrooms (m +NAP)", -0.4],
            ["FlowDemand", "ja"],
        ]
    )
    static = read_static(df, "Pump").df
    assert static is not None
    assert static[["flow_rate", "max_flow_rate", "min_upstream_level"]].iloc[0].to_list() == [10, 10, -0.4]
    # without control, a structure cannot switch between two capacities
    df.loc[df["Eigenschap"] == "Afvoer Capaciteit (m3/s)", "Waarde"] = 5
    with pytest.raises(AssertionError, match="without a DiscreteControl"):
        read_static(df, "Pump")


def pr_bernhardsluis() -> pd.DataFrame:
    """Like the Pr. Bernhardsluis: an aanvoer Outlet with DiscreteControl on the Waal upstream, and a FlowDemand."""
    return sheet(
        [
            ["Ribasim type", "Outlet"],
            ["Kunstwerkcode", "39D-001-02"],
            ["Afvoer Capaciteit (m3/s)", 0],
            ["Aanvoer Capaciteit (m3/s)", 73],
            ["Streefpeil bovenstrooms (m +NAP)", 0],
            ["FlowDemand", "ja"],
            ["Meetlocatiecode", 8007291],
            ["Type sturing", "DiscreteControl"],
            [np.nan],
            ["Toestand (control_state)", "Bovengrens (threshold_high)", "Ondergrens (threshold_low)"],
            ["aanvoer", 3.125, 3.075],
            [np.nan],
            ["FlowDemand (m3/s)", "Prioriteit"],
            [1, 3],
        ]
    )


def test_read_static_flow_demand_level_with_control():
    """A structure that supplies from the Basin it listens to has no level from its thresholds to stop its FlowDemand.

    It then stops at its Streefpeil bovenstrooms, like the Pr. Bernhardsluis.
    """
    static = read_static(pr_bernhardsluis(), "Outlet", "upstream").df
    assert static is not None
    assert static.set_index("control_state")["flow_rate"].to_dict() == {"aanvoer": 73.0, "afvoer": 0.0}
    assert static["min_upstream_level"].to_list() == [0.0, 0.0]
    # without a FlowDemand the Streefpeil is not used
    df = pr_bernhardsluis()
    static = read_static(df[df["Eigenschap"] != "FlowDemand"], "Outlet", "upstream").df
    assert static is not None
    assert "min_upstream_level" not in static or static["min_upstream_level"].isna().all()


def prs_irenesluis_doorspoeling() -> pd.DataFrame:
    """Like the Prs. Irenesluis-Doorspoeling, which supplies 25 m3/s while less flows at Nigtevecht."""
    return sheet(
        [
            ["Ribasim type", "Outlet"],
            ["Kunstwerkcode", "39B-002-01"],
            ["Aanvoer Capaciteit (m3/s)", 25],
            ["Afvoer Capaciteit (m3/s)", 0],
            ["Minimale capaciteit (m3/s)", 10],
            ["Meetlocatiecode", 8010764],
            ["Meetlocatievariabele", "flow_rate"],
            ["Type sturing", "DiscreteControl"],
            [np.nan],
            ["Toestand (control_state)", "Bovengrens (threshold_high)", "Ondergrens (threshold_low)"],
            ["aanvoer", 27.5, 22.5],
        ]
    )


def test_discrete_control_on_flow_rate():
    df = prs_irenesluis_doorspoeling()
    variable, condition, logic = discrete_control_tables(df, 10764, STARTTIME, ENDTIME)
    assert variable.df["listen_node_id"].to_list() == [10764]
    assert variable.df["variable"].to_list() == ["flow_rate"]
    assert condition.df[["threshold_high", "threshold_low"]].iloc[0].to_list() == [27.5, 22.5]
    assert dict(zip(logic.df["truth_state"], logic.df["control_state"], strict=True)) == {"T": "afvoer", "F": "aanvoer"}
    # the minimum capacity holds in both states, and a flow rate says nothing about the level to stop at
    static = read_static(df, "Outlet").df
    assert static is not None
    assert static.set_index("control_state")["flow_rate"].to_dict() == {"aanvoer": 25.0, "afvoer": 0.0}
    assert static["min_flow_rate"].to_list() == [10.0, 10.0]
    for column in ("min_upstream_level", "max_downstream_level"):
        assert column not in static or static[column].isna().all()
    # only a level difference is implemented
    with pytest.raises(AssertionError, match="level difference"):
        discrete_control_tables(df, 10764, STARTTIME, ENDTIME, subtract_node_id=10763)


def test_listen_variable():
    assert listen_variable(read_kwk_properties(sluis_born())) == "level"
    assert listen_variable(read_kwk_properties(prs_irenesluis_doorspoeling())) == "flow_rate"
    df = prs_irenesluis_doorspoeling()
    df.loc[df["Eigenschap"] == "Meetlocatievariabele", "Waarde"] = "storage"
    with pytest.raises(AssertionError, match="Meetlocatievariabele"):
        listen_variable(read_kwk_properties(df))


def waal_model() -> Model:
    """A structure between Basins, with a TabulatedRatingCurve upstream and a ManningResistance downstream.

    Basin 1 flows through TabulatedRatingCurve 2 into Basin 3, from which Outlet 4 takes water to Basin 5, and
    ManningResistance 6 drains Basin 5 to Basin 7.
    """
    model = Model(starttime="2020-01-01", endtime="2021-01-01", crs="EPSG:28992")
    for node_id in (1, 3, 5, 7):
        model.basin.add(
            Node(node_id, Point(node_id * 100, 0)),
            [basin.Profile(level=[0.0, 1.0], area=[100.0, 100.0]), basin.State(level=[0.5])],
        )
    model.tabulated_rating_curve.add(
        Node(2, Point(200, 0)), [tabulated_rating_curve.Static(level=[0.0, 1.0], flow_rate=[0.0, 1.0])]
    )
    model.outlet.add(Node(4, Point(400, 0)), [outlet.Static(flow_rate=[1.0])])
    model.manning_resistance.add(
        Node(6, Point(600, 0)),
        [manning_resistance.Static(length=[100.0], manning_n=[0.04], profile_width=[10.0], profile_slope=[1.0])],
    )
    for from_id, to_id in ((1, 2), (2, 3), (3, 4), (4, 5), (5, 6), (6, 7)):
        model.link.add(model.get_node(from_id), model.get_node(to_id))
    return model


def outlet_sheet(meetlocatiecode: int, variable: str = "level") -> pd.DataFrame:
    """An aanvoer Outlet with DiscreteControl on the `variable` of node `meetlocatiecode` of the coupled model."""
    return sheet(
        [
            ["Ribasim type", "Outlet"],
            ["Aanvoer Capaciteit (m3/s)", 1],
            ["Meetlocatiecode", meetlocatiecode],
            ["Meetlocatievariabele", variable],
            ["Type sturing", "DiscreteControl"],
            [np.nan],
            ["Toestand (control_state)", "Bovengrens (threshold_high)", "Ondergrens (threshold_low)"],
            ["aanvoer", 0.6, 0.5],
        ]
    )


def test_listen_node():
    model = waal_model()
    # next to it, or connected to the Basin on one side through nodes that do not control the flow
    assert listen_node(model, outlet_sheet(RWS_NODE_ID_OFFSET + 3), 4) == (3, "upstream")
    assert listen_node(model, outlet_sheet(RWS_NODE_ID_OFFSET + 7), 4) == (7, "downstream")
    # like the Prs. Irenesluis-Doorspoeling, the flow rate of a connector node anywhere
    assert listen_node(model, outlet_sheet(RWS_NODE_ID_OFFSET + 6, "flow_rate"), 4) == (6, None)
    with pytest.raises(AssertionError, match="not a Basin"):
        listen_node(model, outlet_sheet(RWS_NODE_ID_OFFSET + 6), 4)
    with pytest.raises(AssertionError, match="flow rate of Basin"):
        listen_node(model, outlet_sheet(RWS_NODE_ID_OFFSET + 3, "flow_rate"), 4)
    # a TabulatedRatingCurve controls the level upstream of it
    with pytest.raises(AssertionError, match="not on one side"):
        listen_node(model, outlet_sheet(RWS_NODE_ID_OFFSET + 1), 4)
