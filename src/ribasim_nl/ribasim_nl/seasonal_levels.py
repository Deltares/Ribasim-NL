"""Summer and winter target levels (zomer- en winterpeil) in a model with one target level per Basin.

The models are built with one target level per Basin, `meta_streefpeil` in the `Basin / area` table, from which the
levels of their control are derived: the thresholds of the DiscreteControl conditions on its level, and the
`min_upstream_level` and `max_downstream_level` of the Outlets and Pumps around it. `apply_seasonal_levels` varies
these over the year with the level management of the peilgebieden of `ribasim_nl.peilbeheer`, see
https://github.com/Deltares/Ribasim-NL/issues/893:

1. `assign_peilbeheer` gives a Basin the level management of the peilgebied that covers most of its area, or of the
   weir near it with the closest target level. Its target level stays the same in the season of which the target
   level of the peilgebied is closest to it, since the target levels of the models are checked with the water boards.
   The peilgebied gives the difference with the other season, and the bounds of the target level. A peilgebied of
   which both target levels are far from that of the Basin does not describe its water, and is not assigned.
   `join_peilbeheer` gives the Basins that are joined by resistance nodes, and so are one water in the model, the
   level management of the largest area of them, also none.
2. The thresholds of the DiscreteControl conditions on the level of a Basin follow its target level. Thresholds with
   the default band of `DEFAULT_LEVEL_THRESHOLD_RANGE` around a level get the bounds of the target level instead.
3. The Outlets and Pumps controlled by a DiscreteControl node get a winter and a summer variant of each control state,
   with the levels of that season. A condition on the season switches between them.
4. The initial level of a Basin follows its target level at the start of the model.

The levels change at once, at the begin of the transitions between summer and winter. A gradual transition needs
Ribasim to support parameters of control states that change over time. The DiscreteControl nodes that change over the
year get cyclic timeseries, over the year of the start of the model.

The regional models get the seasons in `ribasim_nl.regional_model.finish_regional_model`. `koppelen` removes them with
`remove_seasonal_levels`, since the coupling works with the fixed levels, and applies them again to the coupled model.
The fixed levels and thresholds are kept in the `meta_fixed_` columns for this.
"""

import logging
from collections.abc import Sequence
from datetime import datetime

import geopandas as gpd
import networkx as nx
import numpy as np
import pandas as pd
import shapely

from ribasim_nl.aquo import waterbeheercode
from ribasim_nl.control_layout import DEFAULT_LEVEL_THRESHOLD_RANGE
from ribasim_nl.coupling_level_common import as_int
from ribasim_nl.coupling_level_network import first_non_junction, flow_link_graphs
from ribasim_nl.model import Model
from ribasim_nl.peilbeheer import LEVEL_COLUMNS, SEASONS, TRANSITION_COLUMNS, month_day
from ribasim_nl.settings import settings

LOG = logging.getLogger(__name__)

# The level management of the peilgebieden, written by pipeline/peilbeheer.py
PEILBEHEER = settings.ribasim_nl_data_dir / "Rijkswaterstaat/verwerkt/peilbeheer.gpkg"
# The node IDs of the coupled model are the waterbeheercode of their authority times this, plus their own node ID
NODE_ID_FACTOR = 100_000
# The suffix of the control states of the Outlets and Pumps in each season
STATE_SUFFIX = {"winter": " winter", "zomer": " zomer"}
# The condition on the season has a variable with weight 0, which is always 0, and these thresholds, so it is true
# in summer, as (threshold_low, threshold_high)
SEASON_THRESHOLDS = {"winter": (1.0, 2.0), "zomer": (-2.0, -1.0)}
# Tolerance to recognize the default band of thresholds
BAND_TOLERANCE = 1e-6  # m
# The peilgebied of a Basin covers at least this fraction of it
MIN_OVERLAP = 0.5
# The target level of the peilgebied of a Basin is at most this far from that of the Basin, in one of the seasons
MAX_OFFSET = 0.5  # m
# Peilgebieden that are points, such as weirs, belong to the Basins within this distance
POINT_DISTANCE = 100.0  # m
# The Basins joined by these nodes get the same level management, see `join_peilbeheer`
JOINING_NODE_TYPES = ["ManningResistance", "LinearResistance"]
LEVEL_LIMITS = ["min_upstream_level", "max_downstream_level"]
THRESHOLDS = ["threshold_low", "threshold_high"]
# The fixed levels and thresholds, to remove the seasons again
FIXED_LEVELS = [f"meta_fixed_{column}" for column in LEVEL_LIMITS]
FIXED_THRESHOLDS = [f"meta_fixed_{column}" for column in THRESHOLDS]

META_COLUMNS = {
    "peilgebied": "meta_peilgebied",
    "soort_peilbeheer": "meta_soort_peilbeheer",
    **{column: f"meta_{column}" for column in LEVEL_COLUMNS + TRANSITION_COLUMNS},
}


def node_authority(node_ids: pd.Series) -> pd.Series:
    """The authority of nodes of the coupled model, from their node ID, see `ribasim_nl.aquo`."""
    authority_by_code = {code: authority for authority, code in waterbeheercode.items()}
    return (node_ids // NODE_ID_FACTOR).map(authority_by_code)


def _offset(target_level: pd.Series, summer: pd.Series, winter: pd.Series) -> pd.Series:
    """The difference of a target level with the closest of the summer and winter level."""
    to_summer, to_winter = target_level - summer, target_level - winter
    return to_summer.where(to_summer.abs() <= to_winter.abs(), to_winter)


def assign_peilbeheer(
    model: Model,
    peilgebieden: gpd.GeoDataFrame,
    authority: str | None = None,
    min_overlap: float = MIN_OVERLAP,
    max_offset: float = MAX_OFFSET,
) -> pd.DataFrame:
    """Assign the level management of the peilgebieden to the Basins with a target level.

    A Basin gets the peilgebied of its authority that covers most of its area, if that is at least `min_overlap` of
    it. A Basin without such a peilgebied gets the peilgebied that is a point within `POINT_DISTANCE` of it with the
    closest target level, such as the weir that it drains over. The levels of the peilgebied are shifted such that its
    target level in the season of which it is closest to the target level of the Basin, is that target level. A
    peilgebied of which both target levels are further than `max_offset` from that of the Basin does not describe its
    water, and is not assigned.

    :param model: model
    :param peilgebieden: the level management of the peilgebieden, see `ribasim_nl.peilbeheer.read_peilbeheer`
    :param authority: the authority of all Basins of a regional model, or else their authority follows from their
        node ID, as in the coupled model
    :param min_overlap: the minimum fraction of the area of a Basin that its peilgebied covers
    :param max_offset: the maximum difference between the target levels of the Basin and its peilgebied [m]
    :return: the level management per Basin node_id, with the target level `meta_streefpeil` of the Basin, the code of
        its peilgebied as `peilgebied`, the fraction of the Basin that it covers as `overlap`, and the shift of the
        levels of the peilgebied as `offset`
    """
    area_df = model.basin.area.df
    assert area_df is not None
    area_df = area_df.loc[np.isfinite(area_df["meta_streefpeil"].astype(float))]
    basins = gpd.GeoDataFrame(
        area_df[["node_id", "meta_streefpeil", "geometry"]], geometry="geometry", crs=model.crs
    ).dissolve(by="node_id", aggfunc="first")
    basins["authority"] = authority if authority is not None else node_authority(basins.index.to_series())
    assert basins.crs is not None
    peilgebieden = peilgebieden.to_crs(basins.crs).reset_index(drop=True)
    peilgebieden = peilgebieden.loc[peilgebieden["authority"].isin(basins["authority"].unique())]
    is_point = peilgebieden.geom_type.eq("Point")

    # The area that covers most of a Basin
    pairs = gpd.sjoin(basins, peilgebieden.loc[~is_point], predicate="intersects", rsuffix="peilgebied")
    pairs = pairs.loc[pairs["authority_left"] == pairs["authority_peilgebied"]].copy()
    basin_geometry = basins.geometry.loc[pairs.index].to_numpy()
    peilgebied_geometry = peilgebieden.geometry.loc[pairs["index_peilgebied"]].to_numpy()
    intersection = shapely.area(shapely.intersection(basin_geometry, peilgebied_geometry))
    pairs["overlap"] = intersection / shapely.area(basin_geometry)
    areas = pairs.sort_values("overlap", kind="stable").groupby(level=0).tail(1)
    areas = areas.loc[areas["overlap"] >= min_overlap]

    # The point near a Basin with the closest target level
    pairs = gpd.sjoin(
        basins.loc[~basins.index.isin(areas.index)],
        peilgebieden.loc[is_point],
        predicate="dwithin",
        distance=POINT_DISTANCE,
        rsuffix="peilgebied",
    )
    pairs = pairs.loc[pairs["authority_left"] == pairs["authority_peilgebied"]].copy()
    point = peilgebieden.loc[pairs["index_peilgebied"]].set_axis(pairs.index)
    offset = _offset(pairs["meta_streefpeil"], point["streefpeil_zomer"], point["streefpeil_winter"])
    pairs["distance"] = offset.abs()
    points = pairs.sort_values("distance", kind="stable").groupby(level=0).head(1)

    best = pd.concat([areas, points]).sort_index()
    columns = ["code", "soort_peilbeheer", *LEVEL_COLUMNS, *TRANSITION_COLUMNS]
    regimes = pd.DataFrame(
        peilgebieden.loc[best["index_peilgebied"], columns].to_numpy(), index=best.index, columns=columns
    )
    regimes = regimes.rename(columns={"code": "peilgebied"}).astype(dict.fromkeys(LEVEL_COLUMNS, float))
    regimes["meta_streefpeil"] = best["meta_streefpeil"].astype(float)
    regimes["overlap"] = best["overlap"]
    # The target level of the Basin stays the same in the season of the closest target level of the peilgebied
    regimes["offset"] = _offset(regimes["meta_streefpeil"], regimes["streefpeil_zomer"], regimes["streefpeil_winter"])
    regimes[LEVEL_COLUMNS] = regimes[LEVEL_COLUMNS].add(regimes["offset"], axis=0).round(4)
    regimes.index.name = "node_id"
    return regimes.loc[regimes["offset"].abs() <= max_offset]


def joined_basins(model: Model) -> pd.Series:
    """Group the Basins that are joined by ManningResistance or LinearResistance nodes, also through Junctions.

    :return: the number of the group per Basin node_id
    """
    node_df, link_df = model.node.df, model.link.df
    node_type = node_df["node_type"]
    joining = node_type.index[node_type.isin(["Basin", "Junction", *JOINING_NODE_TYPES])]
    links = link_df.loc[
        link_df["link_type"].fillna("flow").eq("flow")
        & link_df["from_node_id"].isin(joining)
        & link_df["to_node_id"].isin(joining)
    ]
    graph = nx.Graph()
    basins = node_type.index[node_type.eq("Basin")]
    graph.add_nodes_from(basins)
    graph.add_edges_from(zip(links["from_node_id"], links["to_node_id"], strict=True))
    group = {node_id: i for i, component in enumerate(nx.connected_components(graph)) for node_id in component}
    return pd.Series(group).loc[basins].rename_axis("node_id")


def join_peilbeheer(model: Model, regimes: pd.DataFrame) -> pd.DataFrame:
    """Give the Basins that are joined by resistance nodes the same level management.

    The flow between these Basins follows their difference in level, so they are one water in the model, of which the
    seasons should not change the levels relative to each other. Of their level management, also none, the one of the
    largest area applies to all of them, relative to their own target level.

    :param model: model
    :param regimes: the level management of the Basins, see `assign_peilbeheer`
    :return: the level management of the Basins, see `assign_peilbeheer`, with `overlap` and `offset` NaN for the
        Basins that get it from the Basins they are joined to
    """
    if regimes.empty:
        return regimes
    area_df = model.basin.area.df
    assert area_df is not None
    area_df = area_df.loc[np.isfinite(area_df["meta_streefpeil"].astype(float))]
    node_ids = area_df["node_id"].to_numpy()
    basins = pd.DataFrame(
        {
            "area": pd.Series(shapely.area(area_df["geometry"].to_numpy()), index=node_ids).groupby(level=0).sum(),
            "meta_streefpeil": area_df.groupby("node_id")["meta_streefpeil"].first().astype(float),
        }
    ).rename_axis("node_id")
    basins["group"] = joined_basins(model).loc[basins.index]
    # The level management of a Basin relative to its target level, which is empty for a Basin without one
    relative = pd.concat([seasonal_differences(regimes).round(4), regimes[TRANSITION_COLUMNS]], axis=1)
    basins["key"] = relative.astype(str).agg("|".join, axis=1).reindex(basins.index, fill_value="")

    largest = basins.groupby(["group", "key"])["area"].sum().sort_values(ascending=False, kind="stable")
    winner = largest.reset_index().drop_duplicates("group").set_index("group")["key"]
    changed = basins.loc[basins["key"].ne(basins["group"].map(winner))]
    # The largest Basin with the level management of its group, from which the changed Basins get it
    winning = basins.loc[basins["key"].eq(basins["group"].map(winner))].sort_values(
        "area", ascending=False, kind="stable"
    )
    source = changed["group"].map(winning.reset_index().drop_duplicates("group").set_index("group")["node_id"])
    source = source.loc[source.isin(regimes.index)]

    joined = regimes.loc[source.to_numpy()].set_axis(source.index)
    shift = basins.loc[source.index, "meta_streefpeil"] - joined["meta_streefpeil"]
    joined[LEVEL_COLUMNS] = joined[LEVEL_COLUMNS].add(shift, axis=0).round(4)
    joined["meta_streefpeil"] = basins.loc[source.index, "meta_streefpeil"]
    joined[["overlap", "offset"]] = np.nan
    lost = changed.index.isin(regimes.index) & ~changed.index.isin(source.index)
    LOG.info(
        "Of the Basins joined by resistance nodes, %d get the level management of the others, %d lose theirs",
        len(joined),
        lost.sum(),
    )
    result = pd.concat([regimes.drop(index=changed.index, errors="ignore"), joined]).sort_index()
    result.index.name = "node_id"
    return result


def season_switch(transitions: Sequence[str], year: int) -> pd.Series:
    """The season over a year, indexed by the time from which it applies.

    It is 1 in summer, from the begin of the transition to summer until the begin of the transition to winter, and 0 in
    winter.

    :param transitions: the begin and end of the transition to summer, and of the transition to winter, as "dd-mm"
    :param year: the year
    """
    dates = []
    for day_month in transitions:
        parsed = month_day(day_month)
        assert parsed is not None, f"Invalid transitions {transitions}"
        dates.append(datetime(year, *parsed))
    begin_summer, end_summer, begin_winter, end_winter = dates
    start, end = datetime(year, 1, 1), datetime(year + 1, 1, 1)
    assert start < begin_summer <= end_summer < begin_winter <= end_winter < end, f"Invalid transitions {transitions}"
    return pd.Series({start: 0.0, begin_summer: 1.0, begin_winter: 0.0, end: 0.0})


def seasonal_differences(regimes: pd.DataFrame) -> pd.DataFrame:
    """The difference of the target level with `meta_streefpeil`, and of the bounds with the target level, per season.

    :param regimes: the level management of the Basins, see `assign_peilbeheer`
    :return: per Basin node_id, `delta`, `below` and `above`, each with the suffix `_zomer` and `_winter`
    """
    differences = {}
    for season in SEASONS:
        target = regimes[f"streefpeil_{season}"]
        differences[f"delta_{season}"] = target - regimes["meta_streefpeil"]
        differences[f"below_{season}"] = target - regimes[f"ondergrens_{season}"]
        differences[f"above_{season}"] = regimes[f"bovengrens_{season}"] - target
    return pd.DataFrame(differences, index=regimes.index)


def seasonal_thresholds(
    conditions: pd.DataFrame, differences: pd.DataFrame, season: pd.Series
) -> tuple[np.ndarray, np.ndarray]:
    """The thresholds over the year of conditions on the level of one Basin each, with the same transitions.

    The thresholds move with the target level of their Basin. Thresholds with the default band around a level get
    the bounds of the target level around that level instead.

    :param conditions: the conditions, with the `basin` and the `weight` of their variable, which is 1 or -1
    :param differences: see `seasonal_differences`
    :param season: the `season_switch` of the Basins
    :return: `threshold_low` and `threshold_high`, with a row per condition and a column per time of `season`
    """
    basin = differences.loc[conditions["basin"]]

    def over_year(column: str) -> np.ndarray:
        winter = basin[f"{column}_winter"].to_numpy()[:, None]
        summer = basin[f"{column}_zomer"].to_numpy()[:, None]
        return winter + (summer - winter) * season.to_numpy()[None, :]

    delta, below, above = over_year("delta"), over_year("below"), over_year("above")
    weight = conditions["weight"].to_numpy()[:, None]
    low = conditions["threshold_low"].to_numpy(dtype=float)[:, None]
    high = conditions["threshold_high"].to_numpy(dtype=float)[:, None]
    # A weight of -1 negates the level, so the bound below the level becomes the threshold above it
    center = (low + high) / 2 + weight * delta
    default_band = np.abs(high - low - DEFAULT_LEVEL_THRESHOLD_RANGE) < BAND_TOLERANCE
    threshold_low = np.where(default_band, center - np.where(weight > 0, below, above), low + weight * delta)
    threshold_high = np.where(default_band, center + np.where(weight > 0, above, below), high + weight * delta)
    # Keep the thresholds that do not change exactly as they were
    threshold_low = np.where(np.abs(threshold_low - low) < BAND_TOLERANCE, low, threshold_low.round(4))
    threshold_high = np.where(np.abs(threshold_high - high) < BAND_TOLERANCE, high, threshold_high.round(4))
    return threshold_low, threshold_high


def _over_year(rows: pd.DataFrame, times: pd.Index) -> pd.DataFrame:
    """Repeat each row for each time."""
    over_year = rows.loc[rows.index.repeat(len(times))].copy()
    over_year["time"] = np.tile(times.to_numpy(), len(rows))
    return over_year


def _neighbors(model: Model, node_ids: Sequence[int]) -> pd.DataFrame:
    """The first nodes upstream and downstream of connector nodes that are not a Junction."""
    outgoing, incoming = flow_link_graphs(model.link.df.rename_axis("link_id").reset_index())
    node_type = model.node.df["node_type"].to_dict()
    neighbors = {
        node_id: (
            first_non_junction(node_id, incoming, node_type)[0],
            first_non_junction(node_id, outgoing, node_type)[0],
        )
        for node_id in node_ids
    }
    return pd.DataFrame.from_dict(neighbors, orient="index", columns=["upstream", "downstream"], dtype="Int64")


def season_levels(
    static: pd.DataFrame, neighbors: pd.DataFrame, differences: pd.DataFrame, bottoms: pd.Series
) -> pd.DataFrame:
    """The `min_upstream_level` and `max_downstream_level` of static rows in each season.

    The finite levels move with the target level of the Basin upstream and downstream of their node respectively.
    Ribasim requires a `min_upstream_level` above the bottom of the Basin upstream, so it stays above it.

    :param static: static rows of Outlets or Pumps
    :param neighbors: the `upstream` and `downstream` node per node_id
    :param differences: see `seasonal_differences`
    :param bottoms: the lowest level of the profile per Basin
    :return: the levels with the suffix `_zomer` and `_winter`, with the index of `static`
    """
    levels = {}
    bottom = static["node_id"].map(neighbors["upstream"]).map(bottoms).astype(float)
    for season in SEASONS:
        delta = differences[f"delta_{season}"]
        for column, side in zip(LEVEL_LIMITS, ("upstream", "downstream"), strict=True):
            shift = static["node_id"].map(neighbors[side]).map(delta).astype(float).fillna(0.0)
            level = static[column].astype(float)
            shifted = (level + shift).round(4)
            if column == "min_upstream_level":
                shifted = shifted.where(~(shifted < bottom), bottom)
            levels[f"{column}_{season}"] = level.where(~np.isfinite(level) | (shift == 0), shifted)
    return pd.DataFrame(levels, index=static.index)


def remove_seasonal_levels(model: Model) -> None:
    """Restore the fixed levels and thresholds of the control, the inverse of `apply_seasonal_levels`.

    The initial levels of the Basins stay as they are.
    """
    variable_df = model.discrete_control.variable.df
    condition_df = model.discrete_control.condition.df
    logic_df = model.discrete_control.logic.df
    node_df = model.node.df
    area_df = model.basin.area.df
    if area_df is not None:
        model.basin.area.df = area_df.drop(columns=list(META_COLUMNS.values()), errors="ignore")
    if variable_df is None or condition_df is None or logic_df is None:
        return

    keys = ["node_id", "compound_variable_id"]
    is_season = variable_df["weight"].eq(0)
    season_nodes = set(variable_df.loc[is_season, "node_id"])
    season_keys = pd.MultiIndex.from_frame(variable_df.loc[is_season, keys])
    condition_df = condition_df.loc[~pd.MultiIndex.from_frame(condition_df[keys]).isin(season_keys)]
    changed_nodes = set()
    if set(FIXED_THRESHOLDS) <= set(condition_df.columns):
        fixed = condition_df[FIXED_THRESHOLDS[0]].notna()
        changed_nodes = set(condition_df.loc[fixed & condition_df["time"].notna(), "node_id"])
        restored = condition_df.loc[fixed].copy()
        restored[THRESHOLDS] = restored[FIXED_THRESHOLDS].to_numpy()
        restored["time"] = pd.NaT
        restored = restored.drop_duplicates([*keys, "condition_id"])
        condition_df = pd.concat([condition_df.loc[~fixed], restored]).drop(columns=FIXED_THRESHOLDS)
    model.discrete_control.condition.df = condition_df.sort_values([*keys, "condition_id"], ignore_index=True)

    in_season = logic_df["node_id"].isin(season_nodes)
    winter = in_season & logic_df["control_state"].str.endswith(STATE_SUFFIX["winter"])
    assert logic_df.loc[winter, "truth_state"].str.endswith("F").all(), "Winter logic without season condition"
    restored = logic_df.loc[winter].assign(
        truth_state=logic_df.loc[winter, "truth_state"].str[:-1],
        control_state=logic_df.loc[winter, "control_state"].str.removesuffix(STATE_SUFFIX["winter"]),
    )
    model.discrete_control.logic.df = pd.concat([logic_df.loc[~in_season], restored], ignore_index=True)
    model.discrete_control.variable.df = variable_df.loc[~is_season].reset_index(drop=True)

    for node_type in ("Outlet", "Pump"):
        table = model.get_component(node_type).static
        static = table.df
        if static is None or not set(FIXED_LEVELS) <= set(static.columns):
            continue
        state = static["control_state"].astype("string")
        winter = state.str.endswith(STATE_SUFFIX["winter"]).fillna(False).to_numpy(dtype=bool)
        summer = state.str.endswith(STATE_SUFFIX["zomer"]).fillna(False).to_numpy(dtype=bool)
        restored = static.loc[winter].copy()
        restored["control_state"] = state[winter].str.removesuffix(STATE_SUFFIX["winter"])
        restored[LEVEL_LIMITS] = restored[FIXED_LEVELS].to_numpy()
        table.df = (
            pd.concat([static.loc[~winter & ~summer], restored])
            .drop(columns=FIXED_LEVELS)
            .sort_values(["node_id", "control_state"], kind="stable", ignore_index=True)
        )

    node_df.loc[node_df.index.isin(list(changed_nodes | season_nodes)), "cyclic_time"] = False


def apply_seasonal_levels(
    model: Model,
    peilgebieden: gpd.GeoDataFrame,
    *,
    authority: str | None = None,
    shift_state: bool = True,
    min_overlap: float = MIN_OVERLAP,
    max_offset: float = MAX_OFFSET,
) -> pd.DataFrame:
    """Vary the target levels of the Basins and their control between summer and winter, see the module docstring.

    The seasons that the model already has are removed first, with `remove_seasonal_levels`.

    :param model: model
    :param peilgebieden: the level management of the peilgebieden, see `ribasim_nl.peilbeheer.read_peilbeheer`
    :param authority: the authority of all Basins of a regional model, see `assign_peilbeheer`
    :param shift_state: let the initial levels follow the target levels, not when they already do, as the final state
        of a model with the seasons
    :param min_overlap: see `assign_peilbeheer`
    :param max_offset: see `assign_peilbeheer`
    :return: the level management of the Basins, see `assign_peilbeheer`
    """
    remove_seasonal_levels(model)
    regimes = assign_peilbeheer(
        model, peilgebieden, authority=authority, min_overlap=min_overlap, max_offset=max_offset
    )
    regimes = join_peilbeheer(model, regimes)
    LOG.info(
        "Assigned the level management of %d peilgebieden to %d Basins", regimes["peilgebied"].nunique(), len(regimes)
    )
    variable_df = model.discrete_control.variable.df
    condition_df = model.discrete_control.condition.df
    logic_df = model.discrete_control.logic.df
    node_df, link_df = model.node.df, model.link.df
    if regimes.empty or variable_df is None or condition_df is None or logic_df is None:
        return regimes

    year = model.starttime.year
    # The Basins with the same transitions have the same calendar
    transitions = regimes[TRANSITION_COLUMNS]
    calendar = transitions.groupby(TRANSITION_COLUMNS, sort=False).ngroup()
    seasons = {
        calendar_id: season_switch(list(map(str, row)), year)
        for calendar_id, row in transitions.groupby(calendar).first().iterrows()
    }
    differences = seasonal_differences(regimes)

    # The conditions on the level of one Basin with a level management
    keys = ["node_id", "compound_variable_id"]
    variable = variable_df.assign(weight=variable_df["weight"].astype(float).fillna(1.0))
    on_basin = (
        variable.groupby(keys)["listen_node_id"].transform("size").eq(1)
        & variable["variable"].eq("level")
        & variable["weight"].abs().eq(1.0)
        & variable["listen_node_id"].isin(regimes.index)
    )
    basin_variables = variable.loc[on_basin, [*keys, "listen_node_id", "weight"]]
    conditions = condition_df.merge(basin_variables.rename(columns={"listen_node_id": "basin"}), on=keys, how="left")
    assert len(conditions) == len(condition_df)
    conditions.index = condition_df.index
    # DiscreteControl nodes that already change over time are kept as they are
    rows_per_condition = conditions.groupby([*keys, "condition_id"])["condition_id"].transform("size")
    cyclic_time = node_df["cyclic_time"].fillna(False).astype(bool)
    timed = set(conditions.loc[rows_per_condition > 1, "node_id"]) | set(node_df.index[cyclic_time])
    on_seasonal_basin = conditions["basin"].notna() & ~conditions["node_id"].isin(timed)
    seasonal_conditions = conditions.loc[on_seasonal_basin].astype({"basin": int})

    # Conditions of which the thresholds change over the year get a row per time, the others keep one row
    condition_df = condition_df.assign(**dict.fromkeys(FIXED_THRESHOLDS, np.nan))
    columns = list(condition_df.columns)
    changing_rows = []
    for calendar_id, group in seasonal_conditions.groupby(seasonal_conditions["basin"].map(calendar)):
        season = seasons[as_int(calendar_id)]
        threshold_low, threshold_high = seasonal_thresholds(group, differences, season)
        changes = (threshold_low != threshold_low[:, :1]).any(axis=1) | (threshold_high != threshold_high[:, :1]).any(
            axis=1
        )
        constant = group.index[~changes]
        condition_df.loc[constant, FIXED_THRESHOLDS] = group.loc[constant, THRESHOLDS].to_numpy()
        condition_df.loc[constant, "threshold_low"] = threshold_low[~changes, 0]
        condition_df.loc[constant, "threshold_high"] = threshold_high[~changes, 0]
        rows = _over_year(condition_df.loc[group.index[changes], columns], season.index)
        rows[FIXED_THRESHOLDS] = rows[THRESHOLDS].to_numpy()
        rows["threshold_low"], rows["threshold_high"] = threshold_low[changes].ravel(), threshold_high[changes].ravel()
        changing_rows.append(rows)
    changing = pd.concat(changing_rows) if changing_rows else condition_df.iloc[:0]

    # The Outlets and Pumps controlled by a DiscreteControl node, their Basins, and their levels per season
    node_type = node_df["node_type"]
    control_links = link_df.loc[
        link_df["link_type"].eq("control")
        & link_df["from_node_id"].map(node_type).eq("DiscreteControl")
        & link_df["to_node_id"].map(node_type).isin(["Outlet", "Pump"])
    ]
    assert control_links["to_node_id"].is_unique, "An Outlet or Pump is controlled by more than one DiscreteControl"
    controller = control_links.set_index("to_node_id")["from_node_id"]
    controller = controller.loc[~controller.isin(timed)]
    neighbors = _neighbors(model, controller.index.tolist())
    profile_df = model.basin.profile.df
    assert profile_df is not None
    bottoms = profile_df.groupby("node_id")["level"].min()

    levels: dict[str, pd.DataFrame] = {}
    season_states: set[int] = set()
    for node_type_name in ("Outlet", "Pump"):
        static = model.get_component(node_type_name).static.df
        if static is None:
            continue
        has_state = static["control_state"].notna() & static["node_id"].isin(controller.index)
        state = static.loc[has_state, "control_state"].astype(str)
        assert not state.str.endswith(tuple(STATE_SUFFIX.values())).any(), "Control states with a season suffix"
        node_levels = season_levels(static.loc[has_state], neighbors, differences, bottoms)
        differs = pd.Series(False, index=node_levels.index)
        for column in LEVEL_LIMITS:
            winter, summer = node_levels[f"{column}_winter"], node_levels[f"{column}_zomer"]
            differs |= winter.ne(summer) & ~(winter.isna() & summer.isna())
        season_states |= set(static.loc[differs.index[differs], "node_id"].map(controller))
        levels[node_type_name] = node_levels

    # The nodes of DiscreteControl nodes with seasonal control states get a variant of each state per season
    for node_type_name, node_levels in levels.items():
        table = model.get_component(node_type_name).static
        static = table.df
        assert static is not None
        in_season = static.index.isin(node_levels.index) & static["node_id"].map(controller).isin(season_states)
        season_rows = []
        for season_name in SEASONS:
            rows = static.loc[in_season].copy()
            rows[FIXED_LEVELS] = rows[LEVEL_LIMITS].to_numpy()
            rows["control_state"] = rows["control_state"] + STATE_SUFFIX[season_name]
            for column in LEVEL_LIMITS:
                rows[column] = node_levels.loc[rows.index, f"{column}_{season_name}"]
            season_rows.append(rows)
        table.df = pd.concat([static.loc[~in_season], *season_rows], ignore_index=True).sort_values(
            ["node_id", "control_state"], kind="stable", ignore_index=True
        )

    # DiscreteControl nodes with seasonal control states get a condition on the season, and the logic of both seasons
    season_variables, switch_rows = [], []
    # The condition on the season reuses the first variable of its node, with weight 0
    first_variable = variable_df.sort_index().groupby("node_id")[["listen_node_id", "variable"]].first()
    max_compound_variable_id = variable_df.groupby("node_id")["compound_variable_id"].max()
    listen_basins = variable.loc[on_basin].groupby("node_id")["listen_node_id"].agg(list)
    controlled_ids = pd.Series(controller.index, index=controller.to_numpy()).groupby(level=0).agg(list)
    changing_basins = set(differences.index[differences["delta_zomer"].ne(differences["delta_winter"])])
    for control_id in sorted(season_states):
        # The season follows the transitions of a Basin around its nodes of which the target level changes over the
        # year, since the levels of its control states follow it, preferably one that it listens to
        neighbor_basins = neighbors.loc[controlled_ids[control_id], ["upstream", "downstream"]].stack().dropna()
        seasonal = [as_int(basin_id) for basin_id in neighbor_basins if as_int(basin_id) in changing_basins]
        candidates = [basin_id for basin_id in listen_basins.get(control_id, []) if basin_id in seasonal] + seasonal
        assert candidates, f"DiscreteControl #{control_id} has seasonal control states without a seasonal Basin"
        basin_id = candidates[0]
        compound_variable_id = as_int(max_compound_variable_id[control_id]) + 1
        season_variables.append(
            {
                "node_id": control_id,
                "compound_variable_id": compound_variable_id,
                "listen_node_id": as_int(first_variable.at[control_id, "listen_node_id"]),
                "variable": first_variable.at[control_id, "variable"],
                "weight": 0.0,
            }
        )
        for time, summer in seasons[as_int(calendar[basin_id])].items():
            threshold_low, threshold_high = SEASON_THRESHOLDS["zomer" if summer else "winter"]
            switch_rows.append(
                {
                    "node_id": control_id,
                    "compound_variable_id": compound_variable_id,
                    "condition_id": 1,
                    "threshold_low": threshold_low,
                    "threshold_high": threshold_high,
                    "time": time,
                }
            )
    if season_states:
        in_season = logic_df["node_id"].isin(season_states)
        season_logic = [
            logic_df.loc[in_season].assign(
                truth_state=logic_df.loc[in_season, "truth_state"] + truth,
                control_state=logic_df.loc[in_season, "control_state"] + STATE_SUFFIX[season_name],
            )
            for season_name, truth in (("winter", "F"), ("zomer", "T"))
        ]
        model.discrete_control.logic.df = pd.concat([logic_df.loc[~in_season], *season_logic], ignore_index=True)
        model.discrete_control.variable.df = pd.concat(  # ty: ignore[invalid-assignment]
            [variable_df, pd.DataFrame(season_variables)], ignore_index=True
        )

    # All conditions of a DiscreteControl node that changes over the year are cyclic, also those that do not change
    cyclic = set(changing["node_id"]) | season_states
    unchanged = condition_df.loc[condition_df["node_id"].isin(cyclic) & ~condition_df.index.isin(changing.index)].copy()
    fixed = unchanged[FIXED_THRESHOLDS].to_numpy(dtype=float)
    unchanged[FIXED_THRESHOLDS] = np.where(np.isnan(fixed), unchanged[THRESHOLDS].to_numpy(dtype=float), fixed)
    year_bounds = pd.Index([datetime(year, 1, 1), datetime(year + 1, 1, 1)])
    model.discrete_control.condition.df = (  # ty: ignore[invalid-assignment]
        pd.concat(
            [
                condition_df.loc[~condition_df["node_id"].isin(cyclic)],
                _over_year(unchanged, year_bounds),
                changing,
                pd.DataFrame(switch_rows, columns=columns),
            ],
            ignore_index=True,
        )
        .sort_values([*keys, "condition_id", "time"], kind="stable", ignore_index=True)
        .reindex(columns=columns)
    )
    node_df.loc[sorted(cyclic), "cyclic_time"] = True

    # The initial level of a Basin follows its target level at the start, if that stays above its bottom
    state_df = model.basin.state.df
    if shift_state and state_df is not None:
        start = pd.Timestamp(model.starttime)
        season_at_start = pd.Series({i: season.loc[:start].iloc[-1] for i, season in seasons.items()})
        winter, summer = differences["delta_winter"], differences["delta_zomer"]
        delta = winter + (summer - winter) * calendar.map(season_at_start)
        shift = state_df["node_id"].map(delta).astype(float).fillna(0.0)
        level = (state_df["level"] + shift).round(4)
        shifted = (shift != 0) & (level > state_df["node_id"].map(bottoms))
        state_df["level"] = state_df["level"].where(~shifted, level)

    # The level management of the Basins as metadata
    area_df = model.basin.area.df
    assert area_df is not None
    for column, meta_column in META_COLUMNS.items():
        area_df[meta_column] = area_df["node_id"].map(regimes[column])

    LOG.info(
        "%d DiscreteControl nodes change over the year, %d with seasonal control states",
        len(cyclic),
        len(season_states),
    )
    return regimes
