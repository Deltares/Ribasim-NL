"""Foreign inflows (buitenlandse aanvoer) on the FlowBoundaries of the regional models.

`data/buitenlandse_aanvoer.csv` lists each foreign inflow once: its water authority, the name of its FlowBoundary,
its location and hydro-object, an optional measured timeseries, and three constant flow rates [m3/s]. For inflows with
measurements, the constants are the medians over all measurements, over the summer (April to September) and over the
winter (October to March), see `pipeline/_preprocess_buitenlandse_aanvoer.py`.

The inflows are matched to the FlowBoundaries of the `meta_categorie` "buitenlandse aanvoer" by name, and checked
against their location. The hydro-objects are recorded to later match the inflows independently of the
schematization.
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Literal

import geopandas as gpd
import numpy as np
import pandas as pd
from pandera.typing import DataFrame
from ribasim.schemas import FlowBoundaryStaticSchema, FlowBoundaryTimeSchema

from ribasim_nl.model import Model
from ribasim_nl.settings import settings

logger = logging.getLogger(__name__)

OVERVIEW_CSV = Path(__file__).parent / "data" / "buitenlandse_aanvoer.csv"
# the timeseries of the overview are relative to this directory
TIMESERIES_DIR = settings.ribasim_nl_data_dir / "Basisgegevens/BuitenlandseAanvoer"
CATEGORY = "buitenlandse aanvoer"
# distance [m] within which the FlowBoundary of an inflow must lie from its location
MAX_DISTANCE = 10.0
SUMMER_MONTHS = range(4, 10)

Constant = Literal["constant_global", "constant_summer", "constant_winter"]
CONSTANTS: tuple[Constant, ...] = ("constant_global", "constant_summer", "constant_winter")


def read_overview(authority: str | None = None) -> pd.DataFrame:
    """Read and check the foreign inflows of `OVERVIEW_CSV`, indexed by their `id`.

    :param authority: only the inflows of this water authority
    :return: the inflows
    """
    path = OVERVIEW_CSV
    df = pd.read_csv(path, index_col="id", dtype={"timeseries": "string", "timeseries_column": "string"})
    assert df.index.is_unique, f"Duplicate inflow ids in {path}"
    assert not df.duplicated(["authority", "name"]).any(), f"Duplicate inflow names of an authority in {path}"
    constants = df[list(CONSTANTS)]
    assert (np.isfinite(constants) & (constants >= 0)).all(axis=None), f"Constants must be non-negative in {path}"
    assert (df["timeseries"].isna() == df["timeseries_column"].isna()).all(), (
        f"Inflows with a timeseries need a timeseries_column, and only those, in {path}"
    )
    if authority is not None:
        df = df.loc[df["authority"] == authority]
    return df


def read_timeseries(timeseries: str, column: str) -> pd.Series:
    """Read the daily mean flow rates [m3/s] of a measured inflow, without the days without measurements.

    The timestamps in the column `Datum` are local wall-clock time; the hour repeated by the autumn DST fall-back is
    averaged into its day. Values that are not numeric are ignored, and negative flow rates are clipped to zero.

    :param timeseries: CSV file, relative to `TIMESERIES_DIR`
    :param column: column of the inflow
    :return: daily mean flow rates
    """
    path = TIMESERIES_DIR / timeseries
    df = pd.read_csv(path, usecols=["Datum", column], dtype={"Datum": "string"}, encoding="utf-8-sig")
    # round timestamps such as 23:59:59.971 that were rounded off in Excel
    time = pd.to_datetime(df["Datum"], format="ISO8601").dt.round("s")
    flow_rate = pd.Series(pd.to_numeric(df[column], errors="coerce").to_numpy(), index=time, name=column)
    flow_rate = flow_rate[flow_rate.index.notna()].dropna().sort_index()
    daily = flow_rate.resample("D").mean().dropna()
    if (negative := int((daily < 0).sum())) > 0:
        logger.warning(f"{column} in {path} has {negative} negative daily flow rate(s), set to 0.")
    return daily.clip(lower=0)


def median_flow_rates(flow_rate: pd.Series) -> dict[Constant, float]:
    """The constants of an inflow: the median flow rates of all days, of the summer and of the winter [m3/s].

    :param flow_rate: daily flow rates
    :return: the constants
    """
    summer = pd.DatetimeIndex(flow_rate.index).month.isin(SUMMER_MONTHS)
    assert summer.any() and not summer.all(), f"{flow_rate.name} needs measurements in summer and winter"
    medians = (flow_rate.median(), flow_rate[summer].median(), flow_rate[~summer].median())
    return {constant: round(float(median), 3) for constant, median in zip(CONSTANTS, medians, strict=True)}


def match_flow_boundaries(model: Model, inflows: pd.DataFrame) -> pd.Series:
    """Match the inflows to the FlowBoundaries of the `meta_categorie` "buitenlandse aanvoer", by name.

    Each such FlowBoundary must be matched by exactly one inflow, within `MAX_DISTANCE` of its location.

    :param model: model
    :param inflows: the inflows of the water authority of the model, see `read_overview`
    :return: the node IDs of the FlowBoundaries, by inflow id
    """
    nodes = model.flow_boundary.node.df
    if "meta_categorie" not in nodes:
        nodes = gpd.GeoDataFrame({"name": [], "meta_categorie": []}, geometry=[], crs=model.crs)
    nodes = nodes.loc[nodes["meta_categorie"] == CATEGORY]
    assert nodes["name"].is_unique, f"Names of the {CATEGORY} FlowBoundaries are not unique"

    unmatched_inflows = inflows.loc[~inflows["name"].isin(nodes["name"]), "name"].tolist()
    unmatched_nodes = nodes.loc[~nodes["name"].isin(inflows["name"]), "name"].to_dict()
    if unmatched_inflows or unmatched_nodes:
        raise ValueError(
            f"Each {CATEGORY} FlowBoundary must be an inflow of {OVERVIEW_CSV.name}; inflows without FlowBoundary: "
            f"{unmatched_inflows}; FlowBoundaries (node ID: name) without inflow: {unmatched_nodes}"
        )

    node_ids = inflows["name"].map(pd.Series(nodes.index, index=nodes["name"]))
    locations = gpd.GeoSeries(
        gpd.points_from_xy(inflows["coordinate_x"], inflows["coordinate_y"]), index=inflows.index, crs=nodes.crs
    )
    geometry = gpd.GeoSeries(nodes.geometry.reindex(node_ids).to_numpy(), index=inflows.index, crs=nodes.crs)
    distance = locations.distance(geometry)
    if (far := distance > MAX_DISTANCE).any():
        raise ValueError(f"FlowBoundaries further than {MAX_DISTANCE} m from their inflow: {distance[far].to_dict()}")
    return node_ids.astype("int32")


def _replace_rows(df: pd.DataFrame | None, node_ids: pd.Series, new: pd.DataFrame | None) -> pd.DataFrame | None:
    """Replace the rows of `node_ids` in a table by `new`, or None if the table becomes empty."""
    frames = [] if df is None else [df.loc[~df["node_id"].isin(node_ids)]]
    if new is not None:
        frames.append(new)
    df = pd.concat(frames, ignore_index=True) if frames else None
    return None if df is None or df.empty else df


def _replace_forcing(
    model: Model, node_ids: pd.Series, static: pd.DataFrame | None, time: pd.DataFrame | None = None
) -> None:
    """Replace the static and time tables of the FlowBoundaries `node_ids`, keeping the other FlowBoundaries."""
    static = _replace_rows(model.flow_boundary.static.df, node_ids, static)
    time = _replace_rows(model.flow_boundary.time.df, node_ids, time)
    model.flow_boundary.static.df = None if static is None else DataFrame[FlowBoundaryStaticSchema](static)
    model.flow_boundary.time.df = None if time is None else DataFrame[FlowBoundaryTimeSchema](time)


def set_constant_inflow(model: Model, authority: str, constant: Constant = "constant_global") -> None:
    """Set the foreign inflows of the water authority to one of their constants.

    :param model: model of the water authority
    :param authority: water authority
    :param constant: the constant of the inflows
    """
    inflows = read_overview(authority=authority)
    node_ids = match_flow_boundaries(model, inflows)
    if node_ids.empty:
        return
    static = pd.DataFrame({"node_id": node_ids, "flow_rate": inflows[constant]}).reset_index(drop=True)
    _replace_forcing(model, node_ids, static)


def add_transboundary_inflow(model: Model, authority: str, starttime: datetime, endtime: datetime) -> None:
    """Set the measured daily flow rates of the foreign inflows of the water authority, from start to end time.

    Gaps in the measurements are interpolated linearly. Before the first and after the last measurement, the flow rate
    is `constant_global`. Inflows without measurements get `constant_global` as static flow rate.

    :param model: model of the water authority
    :param authority: water authority
    :param starttime: first day
    :param endtime: last day
    """
    inflows = read_overview(authority=authority)
    node_ids = match_flow_boundaries(model, inflows)
    if node_ids.empty:
        return
    measured = inflows["timeseries"].notna()
    days = pd.date_range(starttime, endtime, freq="D")
    time = []
    columns = ["timeseries", "timeseries_column", "constant_global"]
    for node_id, (timeseries, column, constant) in zip(
        node_ids[measured], inflows.loc[measured, columns].itertuples(index=False), strict=True
    ):
        flow_rate = read_timeseries(timeseries, column)
        if not flow_rate.index.isin(days).any():
            logger.warning(f"{column} has no measurements from {starttime} to {endtime}.")
        flow_rate = flow_rate.asfreq("D").interpolate().reindex(days).fillna(constant)
        time.append(pd.DataFrame({"node_id": node_id, "time": days, "flow_rate": flow_rate.to_numpy()}))
    static = pd.DataFrame(
        {"node_id": node_ids[~measured], "flow_rate": inflows.loc[~measured, "constant_global"]}
    ).reset_index(drop=True)
    logger.info(f"Measured foreign inflows of {authority}: {inflows.loc[measured, 'name'].tolist()}")
    _replace_forcing(model, node_ids, static, pd.concat(time, ignore_index=True) if time else None)
