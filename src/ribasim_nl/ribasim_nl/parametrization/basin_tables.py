# %%
from datetime import datetime

import pandas as pd

from ribasim_nl.model import Model
from ribasim_nl.parametrization.conversions import round_to_precision
from ribasim_nl.parametrization.empty_table import empty_table_df


def _get_basin_average_forcing(
    model: Model, precipitation_mm_per_day: float | None = None, evaporation_mm_per_day: float | None = None
) -> pd.DataFrame:
    static_df = empty_table_df(model, node_type="Basin", table_type="Static", fill_value=0)
    static_df["precipitation"] = static_df["precipitation"].astype(float)
    static_df["potential_evaporation"] = static_df["potential_evaporation"].astype(float)

    assert model.basin.area.df is not None
    area = model.basin.area.df.dissolve("node_id").geometry.area
    assert model.basin.profile.df is not None
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
) -> None:
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
    percentages_map: dict[str, int] | None = None,
    default_percentage: int = 10,
    profile_depth: int = 3,
) -> None:
    # read profile from basin-table
    if percentages_map is None:
        percentages_map = {"hoofdwater": 25, "doorgaand": 5, "bergend": 2}
    assert model.basin.area.df is not None
    profile = model.basin.area.df.copy()

    # determine the profile area, which is also used for the profile
    # profile["area"] = profile["geometry"].area * (default_percentage / 100)
    profile = profile[["node_id", "meta_streefpeil", "geometry"]]
    profile = profile.rename(columns={"meta_streefpeil": "level"})

    # get open-water percentages per category
    profile["percentage"] = default_percentage
    assert model.basin.node is not None
    assert model.basin.node.df is not None
    for category, percentage in percentages_map.items():
        node_ids = model.basin.node.df.loc[model.basin.node.df.meta_categorie == category].index.to_numpy()
        profile.loc[profile.node_id.isin(node_ids), "percentage"] = percentage
        print(percentage)

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


def update_basin_state(model: Model) -> None:
    """Update basin state by max profile-level

    Args:
        model (Model): Ribasim Model
    """
    # pyrefly: ignore[missing-attribute]
    model.basin.state.df = model.basin.profile.df.groupby("node_id").max().reset_index()[["node_id", "level"]]


def apply_basin_level_overrides(
    model: Model,
    basin_level_overrides: list[tuple[list[int], float]],
    *,
    target_level_column: str = "meta_streefpeil",
    update_profile: bool = True,
    update_state: bool = True,
) -> list[int]:
    """Apply manual Basin target levels and keep derived Basin tables consistent."""
    assert model.basin.area.df is not None

    protected_basin_node_ids = [int(node_id) for node_ids, _ in basin_level_overrides for node_id in node_ids]

    if update_profile and model.basin.profile.df is not None and not model.basin.profile.df.empty:
        profile_top = model.basin.profile.df.groupby("node_id")["level"].max()
        for node_ids, target_level in basin_level_overrides:
            for node_id in node_ids:
                node_id = int(node_id)
                if node_id not in profile_top.index:
                    continue

                level_shift = float(target_level) - float(profile_top.at[node_id])
                if level_shift == 0:
                    continue

                mask = model.basin.profile.df["node_id"].eq(node_id)
                model.basin.profile.df.loc[mask, "level"] = (
                    model.basin.profile.df.loc[mask, "level"].astype(float) + level_shift
                )

    for node_ids, target_level in basin_level_overrides:
        mask = model.basin.area.df["node_id"].isin(node_ids)
        model.basin.area.df.loc[mask, target_level_column] = float(target_level)

    if update_state:
        model.basin.state.df = model.basin.area.df[["node_id", target_level_column]].rename(
            columns={target_level_column: "level"}
        )

    return protected_basin_node_ids


def sync_min_upstream_levels_with_profile_bottoms(
    model: Model,
    basin_node_ids: list[int] | set[int] | None = None,
    *,
    verbose: bool = True,
) -> pd.DataFrame:
    """Keep Outlet/Pump min_upstream_level above direct upstream Basin profile bottoms."""
    assert model.basin.profile.df is not None

    basin_bottom = model.basin.profile.df.groupby("node_id")["level"].min().astype(float)
    if basin_node_ids is not None:
        basin_node_ids = {int(node_id) for node_id in basin_node_ids}
        basin_bottom = basin_bottom[basin_bottom.index.astype(int).isin(basin_node_ids)]
    if basin_bottom.empty:
        return pd.DataFrame()

    node_type_by_id = model.node.df["node_type"].to_dict()
    link_df = model.link.df.copy()
    if "link_type" in link_df.columns:
        link_df = link_df[link_df["link_type"].fillna("flow").eq("flow")]

    requirements: list[dict[str, object]] = []
    for row in link_df.itertuples(index=False):
        upstream_node_id = int(row.from_node_id)
        target_node_id = int(row.to_node_id)
        target_node_type = node_type_by_id.get(target_node_id)
        if node_type_by_id.get(upstream_node_id) != "Basin":
            continue
        if target_node_type not in {"Outlet", "Pump"}:
            continue
        if upstream_node_id not in basin_bottom.index:
            continue

        requirements.append(
            {
                "node_type": target_node_type,
                "node_id": target_node_id,
                "upstream_basin_id": upstream_node_id,
                "required_min_upstream_level": float(basin_bottom.at[upstream_node_id]),
            }
        )

    requirements_df = pd.DataFrame(requirements)
    if requirements_df.empty:
        return requirements_df

    required_level_by_node = (
        requirements_df.groupby(["node_type", "node_id"])["required_min_upstream_level"].max().to_dict()
    )
    update_records: list[dict[str, object]] = []

    for node_type in ["Outlet", "Pump"]:
        static_df = getattr(model, node_type.lower()).static.df
        if static_df is None or "min_upstream_level" not in static_df.columns:
            continue

        node_requirements = {
            node_id: required_level
            for (required_node_type, node_id), required_level in required_level_by_node.items()
            if required_node_type == node_type
        }
        if not node_requirements:
            continue

        for node_id, required_level in node_requirements.items():
            mask = static_df["node_id"].eq(int(node_id)) & static_df["min_upstream_level"].notna()
            if not mask.any():
                continue

            current_values = static_df.loc[mask, "min_upstream_level"].astype(float)
            update_index = current_values[current_values.lt(float(required_level))].index
            if len(update_index) == 0:
                continue

            update_records.extend(
                {
                    "node_type": node_type,
                    "node_id": int(node_id),
                    "source_fid": int(static_df.at[index, "fid"]) if "fid" in static_df.columns else None,
                    "old_min_upstream_level": float(static_df.at[index, "min_upstream_level"]),
                    "new_min_upstream_level": float(required_level),
                }
                for index in update_index
            )
            static_df.loc[update_index, "min_upstream_level"] = float(required_level)

    update_df = pd.DataFrame(update_records)
    if verbose and not update_df.empty:
        print(f"min_upstream_level gesynchroniseerd met profielbodem voor {len(update_df)} kunstwerk-rijen")

    return update_df


def add_basin_time_synthetic(
    model: Model,
    precipitation_mm_per_day: float,
    evaporation_mm_per_day: float,
    start_time: datetime,
    end_time: datetime,
) -> None:
    # define time-variables
    half_time = start_time + (end_time - start_time) // 2
    time = start_time, half_time, end_time

    static_df = _get_basin_average_forcing(model, precipitation_mm_per_day, evaporation_mm_per_day)

    import numpy as np

    time_df = pd.DataFrame(
        {"node_id": np.repeat(static_df.node_id, len(time)), "time": np.tile(list(time), len(static_df.node_id))}
    ).set_index("node_id")

    for column in ["precipitation", "potential_evaporation", "drainage", "infiltration"]:
        time_df[column] = float(0)

    time_df.loc[time_df["time"] == start_time, "precipitation"] = static_df["precipitation"].to_numpy()
    time_df.loc[time_df["time"] == half_time, "potential_evaporation"] = static_df["potential_evaporation"].to_numpy()
    time_df.loc[time_df["time"] == end_time, "potential_evaporation"] = static_df["potential_evaporation"].to_numpy()

    model.basin.static.df = None
    model.basin.time.df = time_df.reset_index()  # pyrefly: ignore[bad-assignment]
    model.starttime = start_time
    model.endtime = end_time
