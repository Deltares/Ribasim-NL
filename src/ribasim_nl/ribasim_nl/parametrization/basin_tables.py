# %%
from datetime import datetime

import pandas as pd

from ribasim_nl.model import Model
from ribasim_nl.parametrization.conversions import round_to_precision
from ribasim_nl.parametrization.empty_table import empty_table_df


def _get_basin_average_forcing(
    model: Model, precipitation_mm_per_day: float | None = None, evaporation_mm_per_day: float | None = None
):
    static_df = empty_table_df(model, node_type="Basin", table_type="Static", fill_value=0)
    static_df["precipitation"] = static_df["precipitation"].astype(float)
    static_df["potential_evaporation"] = static_df["potential_evaporation"].astype(float)

    area = model.basin.area.df.dissolve("node_id").geometry.area
    max_profile_area = model.basin.profile.df.set_index("node_id")["area"].groupby("node_id").max()
    multi_factor = (area / max_profile_area).astype(float) * 0.001 / 86400

    if precipitation_mm_per_day is not None:
        precipitation = multi_factor * precipitation_mm_per_day  # m/s
        static_df.loc[:, "precipitation"] = precipitation[static_df.node_id].to_numpy()
    if evaporation_mm_per_day is not None:
        evaporation = multi_factor * evaporation_mm_per_day  # m/s
        static_df.loc[:, "potential_evaporation"] = evaporation[static_df.node_id].to_numpy()
    return static_df


# %%
def update_basin_static(
    model: Model, precipitation_mm_per_day: float | None = None, evaporation_mm_per_day: float | None = None
):
    """Add precipitation and/or evaporation to the model.basin.static table from basin.area

    Args:
        model (Model): Ribasim Model
        precipitation_mm_per_day (float | None, optional): Precipitation in mm/day. Defaults to None.
        evaporation_mm_per_day (float | None, optional): Evaporation in mm/day. Defaults to None.
    """
    # get basin average forcing
    static_df = _get_basin_average_forcing(
        model=model, precipitation_mm_per_day=precipitation_mm_per_day, evaporation_mm_per_day=evaporation_mm_per_day
    )

    # add to static df
    model.basin.static.df = static_df


def update_basin_profile(
    model: Model,
    percentages_map: dict = {"hoofdwater": 90, "doorgaand": 10, "bergend": 3},
    default_percentage: int = 10,
    profile_depth=3,
):
    # read profile from basin-table
    profile = model.basin.area.df.copy()

    # determine the profile area, which is also used for the profile
    # profile["area"] = profile["geometry"].area * (default_percentage / 100)
    profile = profile[["node_id", "meta_streefpeil", "geometry"]]
    profile = profile.rename(columns={"meta_streefpeil": "level"})

    # get open-water percentages per category
    profile["percentage"] = default_percentage
    for category, percentage in percentages_map.items():
        node_ids = model.basin.node.df.loc[model.basin.node.df.meta_categorie == category].index.to_numpy()
        profile.loc[profile.node_id.isin(node_ids), "percentage"] = percentage

    # calculate area at invert from percentage
    profile["area"] = profile["geometry"].area * profile["percentage"] / 100
    profile.drop(columns=["geometry", "percentage"], inplace=True)

    # define the profile-bottom
    profile_bottom = profile.copy()
    profile_bottom["area"] = 0.1
    profile_bottom["level"] -= profile_depth

    # define the profile slightly above the bottom of the bakje
    profile_slightly_above_bottom = profile.copy()
    profile_slightly_above_bottom["level"] -= profile_depth - 0.01  # remain one centimeter above the bottom

    # combine all profiles by concatenating them, and sort on node_id, level and area.
    profile_df = pd.concat([profile_bottom, profile_slightly_above_bottom, profile])
    profile_df = profile_df.sort_values(by=["node_id", "level", "area"], ascending=True).reset_index(drop=True)
    profile_df.loc[:, "area"] = profile_df["area"].apply(round_to_precision, args=(0.1,))

    model.basin.profile.df = profile_df


def update_basin_state(model: Model):
    """Update basin state by max profile-level

    Args:
        model (Model): Ribasim Model
    """
    model.basin.state.df = model.basin.profile.df.groupby("node_id").max().reset_index()[["node_id", "level"]]


def add_basin_time_synthetic(
    model: Model,
    precipitation_mm_per_day: float,
    evaporation_mm_per_day: float,
    start_time: datetime,
    end_time: datetime,
):
    # define time-variables
    half_time = start_time + (end_time - start_time) // 2
    time = start_time, half_time, end_time

    static_df = _get_basin_average_forcing(model, precipitation_mm_per_day, evaporation_mm_per_day)

    import numpy as np

    time_df = pd.DataFrame(
        {"node_id": np.repeat(static_df.node_id, len(time)), "time": np.tile(time, len(static_df.node_id))}
    ).set_index("node_id")

    for column in ["precipitation", "potential_evaporation", "drainage", "infiltration"]:
        time_df[column] = float(0)

    time_df.loc[time_df["time"] == start_time, "precipitation"] = static_df["precipitation"].to_numpy()
    time_df.loc[time_df["time"] == half_time, "potential_evaporation"] = static_df["potential_evaporation"].to_numpy()
    time_df.loc[time_df["time"] == end_time, "potential_evaporation"] = static_df["potential_evaporation"].to_numpy()

    model.basin.static.df = None
    model.basin.time.df = time_df.reset_index()
    model.starttime = start_time
    model.endtime = end_time
