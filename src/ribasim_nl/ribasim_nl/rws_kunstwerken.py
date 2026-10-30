"""Read the properties of Rijkswaterstaat structures (kunstwerken) from their Excel sheets.

A sheet has `Eigenschap` (property) and `Waarde` (value) columns, with general properties in the first
rows, optionally followed by tables like a Q(h) or QQ relation, DiscreteControl thresholds or a FlowDemand.

A structure with `Type sturing` DiscreteControl switches between the control states aanvoer (supply) and afvoer
(drainage), with the flow rates `Aanvoer Capaciteit (m3/s)` and `Afvoer Capaciteit (m3/s)`. It is in the afvoer
state while the level of the listened Basin is above the thresholds, and in the aanvoer state while it is below
them. The `Toestand (control_state)` column of the thresholds names the function of the structure, and does not
change this. A pump that supplies a canal therefore pumps while the canal is below its thresholds.
"""

from typing import Any, Literal

import numpy as np
import pandas as pd
from ribasim.nodes import discrete_control, flow_demand, outlet, pump, tabulated_rating_curve

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
# properties that are read, and so cannot have conflicting duplicates in a sheet
USED_PROPERTIES = {
    *FLOW_KWARGS,
    *CONTROL_STATES.values(),
    "Ribasim type",
    "Kunstwerkcode",
    "Meetlocatiecode",
    "Meetlocatievariabele",
    "Type sturing",
    "FlowDemand",
}
# `Type sturing` values without a control node
LOCAL_CONTROL = {"lokaal", "geen"}
YES_NO = {"ja": True, "nee": False}


def read_rating_curve(kwk_df: pd.DataFrame) -> tabulated_rating_curve.Static:
    """Q(h) relation of a structure as TabulatedRatingCurve."""
    qh_df = kwk_df.loc[kwk_df.Eigenschap.to_list().index("Q(h) relatie") + 2 :][["Eigenschap", "Waarde"]].rename(
        columns={"Eigenschap": "level", "Waarde": "flow_rate"}
    )
    qh_df.dropna(inplace=True)
    return tabulated_rating_curve.Static(level=qh_df["level"].to_list(), flow_rate=qh_df["flow_rate"].to_list())


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

    The levels of a structure with DiscreteControl follow from its thresholds and `listen_side`, the side of the
    structure where the listened Basin is, rather than from the Streefpeil properties. A structure that drains the
    listened Basin stops at the lowest `threshold_low`, and one that supplies it stops at the highest
    `threshold_high`. This holds the level between the evaluations of the DiscreteControl, also against a FlowDemand.
    """
    kwk_properties = read_kwk_properties(kwk_df)
    table = {"Outlet": outlet.Static, "Pump": pump.Static}[node_type]
    if not has_discrete_control(kwk_properties):
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
    kwk_df: pd.DataFrame, listen_node_id: int, starttime: pd.Timestamp, endtime: pd.Timestamp
) -> list[Any]:
    """DiscreteControl tables that switch a structure to afvoer above the thresholds, and to aanvoer below them.

    Dated thresholds are a yearly pattern, repeated over the simulation period.
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
    return [
        discrete_control.Variable(compound_variable_id=[1], listen_node_id=[listen_node_id], variable=["level"]),
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
