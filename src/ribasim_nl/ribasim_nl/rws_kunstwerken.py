"""Read the properties of Rijkswaterstaat structures (kunstwerken) from their Excel sheets.

A sheet has `Eigenschap` (property) and `Waarde` (value) columns, with general properties in the first
rows, optionally followed by tables like a Q(h) or QQ relation, DiscreteControl thresholds or a FlowDemand.

A structure with `Type sturing` DiscreteControl switches between the control states aanvoer (supply) and afvoer
(drainage), with the flow rates `Aanvoer Capaciteit (m3/s)` and `Afvoer Capaciteit (m3/s)`. It is in the afvoer
state while the level of the listened Basin is above the thresholds, and in the aanvoer state while it is below
them. The `Toestand (control_state)` column of the thresholds names the function of the structure, and does not
change this. A pump that supplies a canal therefore pumps while the canal is below its thresholds. With
`Meetlocatiecode aftrek` the structure switches on the level difference with that Basin, rather than on a level.

A structure in `PARALLEL_KWK` is next to another structure, between the same Basins, so it has no place of its own in
the network. `5_model_netwerk` leaves it out, and `6_model_sturing` adds it.
"""

from collections.abc import Iterable, Mapping
from typing import Any, Literal, NamedTuple, cast

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
import shapely.ops
from ribasim.nodes import discrete_control, flow_demand, outlet, pump, tabulated_rating_curve
from shapely.geometry import LineString, Point

from ribasim_nl.network import Network

FLOW_KWARGS = {
    "Capaciteit (m3/s)": "flow_rate",
    "Minimale capaciteit (m3/s)": "min_flow_rate",
    "Maximale capaciteit (m3/s)": "max_flow_rate",
    "Streefpeil bovenstrooms (m +NAP)": "min_upstream_level",
    "Streefpeil benedenstrooms (m +NAP)": "max_downstream_level",
}
CONTROL_STATES = {"aanvoer": "Aanvoer Capaciteit (m3/s)", "afvoer": "Afvoer Capaciteit (m3/s)"}
CONTROL_TABLE = "Toestand (control_state)"
FLOW_DEMAND_TABLE = "FlowDemand (m3/s)"
TABLE_HEADERS = {CONTROL_TABLE, FLOW_DEMAND_TABLE, "QQ relatie", "Q(h) relatie", "QHQ relatie", "PidControl"}
# The Basin of which the level is subtracted from that of the Meetlocatiecode, in a DiscreteControl on a difference
SUBTRACT_PROPERTY = "Meetlocatiecode aftrek"
# properties that are read, and so cannot have conflicting duplicates in a sheet
USED_PROPERTIES = {
    *FLOW_KWARGS,
    *CONTROL_STATES.values(),
    "Ribasim type",
    "Kunstwerkcode",
    "Meetlocatiecode",
    SUBTRACT_PROPERTY,
    "Meetlocatievariabele",
    "Type sturing",
    "FlowDemand",
}
# `Type sturing` values without a control node
LOCAL_CONTROL = {"lokaal", "geen"}
YES_NO = {"ja": True, "nee": False}


class ParallelKwk(NamedTuple):
    """A structure next to the structure with code `main`, flowing the other way with `reverse`, at `offset` or `xy`.

    With `downstream_network_node`, the link out of it starts along the network from this node. It then shares the
    route of the main structure from where their channels meet, so a Junction there combines the flow of both, as is
    measured upstream of a weir with a lock next to it. `8b_update_bc` keeps such an observed Junction as it is, and
    routes the other links.

    With `chamber`, the network nodes at both ends of its lock chamber. The network has a gap there, since it is clipped
    to the Basin areas, which leave out the chamber, so `main_route` bridges it.
    """

    main: str
    reverse: bool = False
    offset: float = 30.0  # m
    xy: tuple[float, float] | None = None
    downstream_network_node: int | None = None
    chamber: tuple[int, int] | None = None


# The structures of a complex that is one place in the network, by their code
PARALLEL_KWK = {
    "14E-001-27": ParallelKwk("14E-001-33"),  # Gemaal Den Oever, next to the Stevinsluizen
    "37C-001-01": ParallelKwk("37C-350-09", xy=(62091.0, 426984.0)),  # Goereese sluis, next to the Haringvlietsluizen
    "20A-001-04": ParallelKwk("20A-001-02", reverse=True),  # Krabbersgatsluis, supply of the Markermeer
    "20D-001-04": ParallelKwk("20D-001-08", reverse=True),  # Houtribsluizen, supply of the Markermeer
    # The locks and fish passages next to the weirs of the Nederrijn and the Lek, in their own channels, see #996
    # The network has a gap at Sluis Hagestein, which is placed 33 m from the NIS location to be within 100 m of the
    # network upstream of it, so `8b_update_bc` routes the link towards it
    "38F-001-02": ParallelKwk("38F-001-01", xy=(137650.0, 444919.0), downstream_network_node=5309),  # Sluis Hagestein
    "39B-003-01": ParallelKwk("39B-003-02", xy=(156499.0, 443114.0)),  # Sluis Amerongen
    "40A-004-01": ParallelKwk("40A-004-02", xy=(183700.0, 442130.0), downstream_network_node=8654),  # Sluis Driel
    # The locks and the fish passage next to the weirs of the Maas, in their own channels, at their NIS location, see
    # #1011. The fish passage of Linne is placed on its channel, 63 m north of the bridge over it in NIS. The series
    # Lith measures the whole Maas, so the link out of its lock joins that of the weir where their channels meet.
    "61F-004-02": ParallelKwk("61F-004-01", xy=(176846.0, 320008.0)),  # Sluis Borgharen
    "58D-353-03": ParallelKwk("58D-353-01", xy=(192413.0, 353227.0)),  # Vistrap Linne
    "58D-350-01": ParallelKwk("58D-354-01", xy=(196500.0, 358250.0), chamber=(9191, 9193)),  # Sluis Roermond
    "58E-350-01": ParallelKwk("58E-350-04", xy=(205579.0, 370472.0)),  # Sluis Belfeld
    "46D-350-01": ParallelKwk("46D-350-04", xy=(196224.0, 405947.0), chamber=(9226, 9231)),  # Sluis Sambeek
    "45F-001-01": ParallelKwk("45F-001-02", xy=(179267.0, 420087.0)),  # Sluis Grave
    # Sluis Lith
    "45B-351-03": ParallelKwk(
        "45B-351-02", xy=(159614.0, 424475.0), downstream_network_node=6575, chamber=(6576, 6575)
    ),
    "46A-001-01": ParallelKwk("46A-001-06"),  # Schutsluis Heumen, next to the Keersluis
    "46A-001-02": ParallelKwk("46A-001-06", reverse=True, offset=-30.0),  # Gemaal Heumen, from the canal to the Maas
    # Dunea pumps Maas water to the Waal side of the Wilhelminasluis, see #979
    "Brakel, Maaswaterbel": ParallelKwk("44F-002-01", reverse=True, xy=(131922.0, 423045.0)),
}


def main_route(network: Network, structures: Mapping[str, ParallelKwk] = PARALLEL_KWK) -> gpd.GeoDataFrame:
    """The lines of the network, and a straight line through each lock chamber of `structures` that it lacks.

    Links are routed along these lines, see `ribasim_nl.flow_paths.rebuild_rws_junction_trees`, so they pass
    through the chamber of such a lock, rather than straight from the nearest line.
    """
    links, nodes = network.links, network.nodes
    chambers = []
    for code, structure in structures.items():
        if structure.chamber is None:
            continue
        for node_id in structure.chamber:
            ends = ((links["node_from"] == node_id) | (links["node_to"] == node_id)).sum()
            assert ends == 1, f"Network node {node_id} at the chamber of {code} is not the end of a gap"
        line = LineString([cast(Point, nodes.at[node_id, "geometry"]) for node_id in structure.chamber])
        assert structure.xy is not None and line.distance(Point(structure.xy)) < 5.0, f"{code} is not in its chamber"
        chambers.append(line)
    return pd.concat([links, gpd.GeoDataFrame(geometry=chambers, crs=links.crs)], ignore_index=True)


def line_along(lines: Iterable[LineString], start: Point, end: Point) -> LineString:
    """The line from `start` to `end` along `lines` that connect end to end, such as waterways of OpenStreetMap."""
    merged = shapely.line_merge(shapely.union_all(list(lines)))
    assert isinstance(merged, LineString), f"Expected lines that connect end to end, got a {merged.geom_type}"
    first, last = merged.project(start), merged.project(end)
    part = shapely.ops.substring(merged, min(first, last), max(first, last))
    assert isinstance(part, LineString) and part.length > 0, f"No line from {start} to {end}"
    coords = list(part.coords) if first < last else list(part.coords)[::-1]
    return shapely.remove_repeated_points(LineString([*start.coords, *coords, *end.coords]))


def read_rating_curve(kwk_df: pd.DataFrame) -> tabulated_rating_curve.Static:
    """Q(h) relation of a structure as TabulatedRatingCurve."""
    qh_df = kwk_df.loc[kwk_df.Eigenschap.to_list().index("Q(h) relatie") + 2 :][["Eigenschap", "Waarde"]].rename(
        columns={"Eigenschap": "level", "Waarde": "flow_rate"}
    )
    qh_df.dropna(inplace=True)
    return tabulated_rating_curve.Static(level=qh_df["level"].to_list(), flow_rate=qh_df["flow_rate"].to_list())


def rating_curve_from_bottom(qh: pd.DataFrame, bottom: float) -> tabulated_rating_curve.Static:
    """Rating curve from a table with columns `H` and `Q` that does not reach no flow, which starts at `bottom`."""
    assert qh[["H", "Q"]].notna().all(axis=None), "Missing values in the Q(h) table"
    assert qh["H"].is_monotonic_increasing and qh["H"].is_unique, "H should increase"
    assert qh["Q"].is_monotonic_increasing and qh["Q"].iloc[0] > 0, "Q should be positive and not decrease"
    assert bottom < qh["H"].iloc[0], f"Bottom {bottom} should be below the lowest level {qh['H'].iloc[0]}"
    return tabulated_rating_curve.Static(level=[bottom, *qh["H"]], flow_rate=[0.0, *qh["Q"]])


def read_qq_curve(kwk_df: pd.DataFrame) -> pd.DataFrame:
    """QQ relation of a structure: flow rate as function of a condition flow rate."""
    return kwk_df.loc[kwk_df.Eigenschap.to_list().index("QQ relatie") + 2 :][["Eigenschap", "Waarde"]].rename(
        columns={"Eigenschap": "condition_flow_rate", "Waarde": "flow_rate"}
    )


def read_kwk_properties(kwk_df: pd.DataFrame) -> pd.Series:
    """General properties of a structure, in the rows above its first table.

    A property can occur twice, as long as the values of the properties that are read agree.
    """
    eigenschap = kwk_df["Eigenschap"]
    # a table starts at a known header, or at the first number in the Eigenschap column
    is_table = eigenschap.isin(TABLE_HEADERS) | eigenschap.map(lambda x: pd.notna(x) and not isinstance(x, str))
    end = int(is_table.to_numpy().argmax()) if is_table.any() else len(kwk_df)
    rows = kwk_df.iloc[:end][["Eigenschap", "Waarde"]].dropna()

    for key, values in rows.groupby("Eigenschap")["Waarde"]:
        if key in USED_PROPERTIES and len(set(values.astype(str))) > 1:
            raise ValueError(f"Conflicting values for {key!r}: {values.to_list()}")
    properties = rows.drop_duplicates("Eigenschap", keep="first").set_index("Eigenschap")["Waarde"]

    if "Kunstwerkcode" in properties:
        properties["Kunstwerkcode"] = str(properties["Kunstwerkcode"])
    return properties


def read_flow_kwargs(kwk_properties: pd.Series) -> dict[str, Any]:
    """Flow rates and levels of a structure, as keyword arguments for Outlet or Pump static tables.

    Without `Capaciteit (m3/s)`, a structure without control uses `Afvoer Capaciteit (m3/s)` as flow rate.
    """
    kwk_properties = kwk_properties.copy()
    if "Capaciteit (m3/s)" not in kwk_properties and CONTROL_STATES["afvoer"] in kwk_properties:
        aanvoer = kwk_properties.get(CONTROL_STATES["aanvoer"], 0.0)
        assert aanvoer == 0, f"Aanvoer capacity {aanvoer} without a DiscreteControl that switches to it"
        kwk_properties["Capaciteit (m3/s)"] = kwk_properties[CONTROL_STATES["afvoer"]]
    kwargs = kwk_properties.rename(FLOW_KWARGS).to_dict()
    if "flow_rate" in kwargs:
        kwargs["max_flow_rate"] = kwargs["flow_rate"]
    return {str(k): [v] for k, v in kwargs.items() if k in FLOW_KWARGS.values()}


def read_pump(kwk_properties: pd.Series) -> pump.Static:
    """Static table of a pump."""
    return pump.Static(**read_flow_kwargs(kwk_properties))


def has_discrete_control(kwk_properties: pd.Series) -> bool:
    """Whether the `Type sturing` of a structure is DiscreteControl."""
    control_type = kwk_properties.get("Type sturing", "lokaal")
    assert control_type in {*LOCAL_CONTROL, "DiscreteControl"}, f"Type sturing {control_type!r} not implemented"
    return control_type == "DiscreteControl"


def has_flow_demand(kwk_properties: pd.Series) -> bool:
    """Whether a structure gets a FlowDemand, for instance for the losses of a navigation lock."""
    value = kwk_properties.get("FlowDemand", "nee")
    assert value in YES_NO, f"FlowDemand should be one of {list(YES_NO)}, not {value!r}"
    return YES_NO[value]


def _read_table(kwk_df: pd.DataFrame, header: str, columns: list[str]) -> pd.DataFrame:
    """The rows below `header` in the first columns, until the first empty row."""
    positions = np.flatnonzero(kwk_df["Eigenschap"].to_numpy() == header)
    assert len(positions) == 1, f"Expected one {header!r} table, found {len(positions)}"
    start = int(positions[0]) + 1
    table = kwk_df.iloc[start:, : len(columns)].copy()
    table.columns = columns
    empty = table[columns[0]].isna().to_numpy()
    return table.iloc[: int(empty.argmax()) if empty.any() else len(table)].reset_index(drop=True)


def _times(table: pd.DataFrame) -> pd.Series | None:
    """The dates of a table, if any, as timestamps."""
    if table["time"].isna().all():
        return None
    assert table["time"].notna().all(), "Give a date in all rows, or in none"
    time = pd.to_datetime(table["time"])
    assert time.is_monotonic_increasing and time.is_unique, f"Dates should increase: {time.to_list()}"
    return time


def repeat_yearly(time: pd.Series, starttime: pd.Timestamp, endtime: pd.Timestamp) -> pd.DataFrame:
    """Repeat the dates of a yearly pattern in every year from `starttime` to `endtime`.

    Returns `index`, the row of `time` that each date repeats, and `time`. The years of `time` are ignored, and it
    should span at most one year, without 29 February.
    """
    assert (time.max() - time.min()) <= pd.Timedelta(days=366), "A yearly pattern spans at most one year"
    assert not ((time.dt.month == 2) & (time.dt.day == 29)).any(), "29 February does not repeat yearly"
    # sort by day of the year, so a pattern starting in another month also works
    order = (time.dt.month * 100 + time.dt.day).sort_values(kind="stable").index
    rows = [
        {"index": i, "time": pd.Timestamp(year=year, month=time[i].month, day=time[i].day)}
        for year in range(starttime.year, endtime.year + 1)
        for i in order
    ]
    return pd.DataFrame(rows)


def read_static(
    kwk_df: pd.DataFrame, node_type: str, listen_side: Literal["upstream", "downstream"] | None = None
) -> outlet.Static | pump.Static:
    """Static table of an Outlet or Pump, with the aanvoer and afvoer states if it has DiscreteControl.

    The capacity, levels and minimum capacity hold in both states, the flow rate differs per state. A missing
    aanvoer or afvoer capacity is 0.

    Without allocation, a FlowDemand is a minimum flow also from an empty Basin, so a structure with a FlowDemand
    stops at `Streefpeil bovenstrooms (m +NAP)`, or else at its `Streefpeil (m +NAP)`.

    The levels of a structure with DiscreteControl follow from its thresholds and `listen_side`, the side of the
    structure where the listened Basin is, rather than from the Streefpeil properties. A structure that drains the
    listened Basin stops at the lowest `threshold_low`, and one that supplies it stops at the highest
    `threshold_high`. This holds the level between the evaluations of the DiscreteControl, also against a FlowDemand.
    """
    kwk_properties = read_kwk_properties(kwk_df)
    table = {"Outlet": outlet.Static, "Pump": pump.Static}[node_type]
    if not has_discrete_control(kwk_properties):
        upstream_level = "Streefpeil bovenstrooms (m +NAP)"
        if (
            has_flow_demand(kwk_properties)
            and upstream_level not in kwk_properties
            and "Streefpeil (m +NAP)" in kwk_properties
        ):
            # the Streefpeil of a lock is its upstream level, at which its FlowDemand stops
            kwk_properties = kwk_properties.copy()
            kwk_properties[upstream_level] = kwk_properties["Streefpeil (m +NAP)"]
        return table(**read_flow_kwargs(kwk_properties))

    assert "Capaciteit (m3/s)" not in kwk_properties, "Use the aanvoer and afvoer capacities with DiscreteControl"
    # a missing capacity is 0, like the aanvoer of a sluice that only drains
    flow_rate = dict.fromkeys(CONTROL_STATES, 0.0) | {
        state: float(kwk_properties[key]) for state, key in CONTROL_STATES.items() if key in kwk_properties
    }
    assert max(flow_rate.values()) > 0, f"DiscreteControl needs a positive {' or '.join(CONTROL_STATES.values())}"
    max_flow_rate = float(kwk_properties.get("Maximale capaciteit (m3/s)", max(flow_rate.values())))
    assert all(0 <= i <= max_flow_rate for i in flow_rate.values()), f"Flow rates {flow_rate} above {max_flow_rate}"
    kwargs: dict[str, Any] = {
        "control_state": list(flow_rate),
        "flow_rate": list(flow_rate.values()),
        "max_flow_rate": [max_flow_rate] * 2,
    }
    if "Minimale capaciteit (m3/s)" in kwk_properties:
        kwargs["min_flow_rate"] = [float(kwk_properties["Minimale capaciteit (m3/s)"])] * 2

    # a threshold on a level difference says nothing about the level to stop at
    if SUBTRACT_PROPERTY in kwk_properties:
        return table(**kwargs)

    thresholds = read_control_thresholds(kwk_df)
    # above the thresholds a structure is in the afvoer state, so it drains an upstream Basin in that state, and it
    # supplies a downstream Basin in the aanvoer state
    if listen_side == "upstream" and flow_rate["afvoer"] > 0:
        kwargs["min_upstream_level"] = [thresholds["threshold_low"].min()] * 2
    elif listen_side == "downstream" and flow_rate["aanvoer"] > 0:
        kwargs["max_downstream_level"] = [thresholds["threshold_high"].max()] * 2
    return table(**kwargs)


def read_control_thresholds(kwk_df: pd.DataFrame) -> pd.DataFrame:
    """DiscreteControl thresholds of a structure: `control_state`, `threshold_high`, `threshold_low` and `time`."""
    table = _read_table(kwk_df, CONTROL_TABLE, ["control_state", "threshold_high", "threshold_low", "time"])
    assert len(table) > 0, f"No rows in {CONTROL_TABLE!r}"
    states = set(table["control_state"])
    assert len(states) == 1 and states <= set(CONTROL_STATES), f"Expected aanvoer or afvoer, got {states}"
    table[["threshold_high", "threshold_low"]] = table[["threshold_high", "threshold_low"]].astype(float)
    # a band between the thresholds prevents switching back and forth, see ribasim_nl.validation
    invalid = table[table["threshold_low"] >= table["threshold_high"]]
    assert invalid.empty, f"threshold_low should be below threshold_high:\n{invalid}"
    table["time"] = _times(table)
    return table


def discrete_control_tables(
    kwk_df: pd.DataFrame,
    listen_node_id: int,
    starttime: pd.Timestamp,
    endtime: pd.Timestamp,
    subtract_node_id: int | None = None,
) -> list[Any]:
    """DiscreteControl tables that switch a structure to afvoer above the thresholds, and to aanvoer below them.

    Dated thresholds are a yearly pattern, repeated over the simulation period. With `subtract_node_id`, the thresholds
    are on the level of `listen_node_id` minus that of this Basin.
    """
    kwk_properties = read_kwk_properties(kwk_df)
    variable = kwk_properties.get("Meetlocatievariabele", "level")
    assert variable == "level", f"Only DiscreteControl on a level is implemented, not {variable!r}"
    thresholds = read_control_thresholds(kwk_df)
    time = thresholds["time"]
    if time.notna().all():
        repeated = repeat_yearly(time, starttime, endtime)
        thresholds = thresholds.loc[repeated["index"]].assign(time=repeated["time"].to_numpy())
    else:
        assert len(thresholds) == 1, "Give a date with each of multiple thresholds"
        thresholds = thresholds.assign(time=starttime)
    n = len(thresholds)
    listen_node_ids = [listen_node_id] if subtract_node_id is None else [listen_node_id, subtract_node_id]
    return [
        discrete_control.Variable(
            compound_variable_id=[1] * len(listen_node_ids),
            listen_node_id=listen_node_ids,
            variable=["level"] * len(listen_node_ids),
            weight=[1.0, -1.0][: len(listen_node_ids)],
        ),
        discrete_control.Condition(
            compound_variable_id=[1] * n,
            condition_id=[1] * n,
            threshold_high=thresholds["threshold_high"].to_list(),
            threshold_low=thresholds["threshold_low"].to_list(),
            time=thresholds["time"].to_list(),
        ),
        discrete_control.Logic(truth_state=["F", "T"], control_state=["aanvoer", "afvoer"]),
    ]


def flow_demand_tables(
    kwk_df: pd.DataFrame, starttime: pd.Timestamp, endtime: pd.Timestamp
) -> list[flow_demand.Static | flow_demand.Time]:
    """FlowDemand of a structure; dated demands are a yearly pattern, repeated over the simulation period."""
    table = _read_table(kwk_df, FLOW_DEMAND_TABLE, ["demand", "demand_priority", "time"])
    assert len(table) > 0, f"No rows in {FLOW_DEMAND_TABLE!r}"
    table["demand"] = table["demand"].astype(float)
    table["demand_priority"] = table["demand_priority"].astype(int)
    assert (table["demand"] >= 0).all(), f"Negative demand:\n{table}"
    time = _times(table)
    if time is None:
        assert len(table) == 1, "Give a date with each of multiple demands"
        return [
            flow_demand.Static(demand=table["demand"].to_list(), demand_priority=table["demand_priority"].to_list())
        ]
    repeated = repeat_yearly(time, starttime, endtime)
    table = table.loc[repeated["index"]]
    return [
        flow_demand.Time(
            time=repeated["time"].to_list(),
            demand=table["demand"].to_list(),
            demand_priority=table["demand_priority"].to_list(),
        )
    ]
