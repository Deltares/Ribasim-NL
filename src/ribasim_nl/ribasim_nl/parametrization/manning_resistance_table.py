from pathlib import Path
from typing import cast

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from pandas import Timestamp
from pandera.typing import DataFrame
from ribasim.schemas import ManningResistanceStaticSchema

from ribasim_nl.model import Model
from ribasim_nl.parametrization.conversions import round_to_precision
from ribasim_nl.parametrization.empty_table import empty_table_df

# Half the width across a ManningResistance that is searched for water, and the distance from the node that a water
# area may lie, for nodes just outside its polygon [m]
MAX_WATER_HALF_WIDTH = 150.0
WATER_TOLERANCE = 5.0


def water_widths(model: Model, water_area_df: gpd.GeoDataFrame) -> pd.Series:
    """Width of the water area across each ManningResistance, perpendicular to its links.

    The links of a ManningResistance follow the watercourse, so the line through the node perpendicular to them
    crosses it. The width is the length of the part of that line within the water area at the node, up to
    `MAX_WATER_HALF_WIDTH` on both sides. Nodes without water area at them, such as on watercourses that are too
    narrow to be mapped as an area, get a width of 0.

    Args:
        model (Model): Ribasim Model with ManningResistance nodes and their links
        water_area_df (gpd.GeoDataFrame): Polygons of the water area, like the `top10nl_waterdeel_vlak` of Top10NL

    Returns
    -------
        pd.Series: width of the water area [m] by ManningResistance node_id
    """
    assert model.link.df is not None and model.manning_resistance.node is not None
    node_df = model.manning_resistance.node.df
    assert node_df is not None
    water = np.asarray(shapely.make_valid(water_area_df.geometry.to_numpy()))
    tree = shapely.STRtree(water)
    link_df = model.link.df[model.link.df.link_type == "flow"]
    widths = pd.Series(0.0, index=node_df.index, name="water_width")
    for node_id, point in node_df.geometry.items():
        # direction of the watercourse at the node, from the ends of its links next to it
        links = link_df[(link_df.from_node_id == node_id) | (link_df.to_node_id == node_id)].geometry
        neighbours = [
            np.asarray(line.interpolate(min(line.project(point) + side, line.length)).coords[0][:2])
            for line in links
            for side in (-10, 10)
            if line.length > 0
        ]
        if len(neighbours) < 2:
            continue
        neighbours = np.array(neighbours)
        # the principal direction of the points around the node
        centred = neighbours - neighbours.mean(axis=0)
        direction = np.linalg.svd(centred, full_matrices=False)[2][0]
        across = np.array([-direction[1], direction[0]])
        xy = np.asarray(point.coords[0][:2])
        cross = shapely.LineString([xy - across * MAX_WATER_HALF_WIDTH, xy + across * MAX_WATER_HALF_WIDTH])
        nearby = tree.query(cross, predicate="intersects")
        if len(nearby) == 0:
            continue
        inside = cross.intersection(shapely.union_all(water[nearby]))
        parts = [g for g in getattr(inside, "geoms", [inside]) if g.length > 0 and g.distance(point) <= WATER_TOLERANCE]
        widths.at[node_id] = max((g.length for g in parts), default=0.0)
    return widths


def widen_to_water_width(
    static_df: pd.DataFrame, water_width: pd.Series, depth: float, profile_slope: float
) -> pd.DataFrame:
    """Widen the profiles that are narrower at `depth` than the water area across them, see `water_widths`.

    Profiles of a watercourse can be missing, or be taken from a smaller watercourse nearby, so a ManningResistance
    on a large watercourse gets a much too small profile. Those become a trapezoid with the width of the water area
    at `depth` and sides of `profile_slope`, horizontal over vertical, with a bottom of at least a third of that width.

    Args:
        static_df (pd.DataFrame): ManningResistance static table with node_id, profile_width and profile_slope
        water_width (pd.Series): width of the water area [m] by node_id
        depth (float): depth of the profile at the water level [m], like that of the Basin profiles
        profile_slope (float): slope of the sides of widened profiles, horizontal over vertical

    Returns
    -------
        pd.DataFrame: static table with the widened profiles
    """
    assert depth > 0 and profile_slope >= 0
    static_df = static_df.copy()
    width = static_df["node_id"].map(water_width).fillna(0.0)
    top_width = static_df["profile_width"] + 2 * static_df["profile_slope"] * depth
    widen = width > top_width
    bottom = np.maximum(width - 2 * depth * profile_slope, width / 3)
    static_df.loc[widen, "profile_width"] = bottom[widen].round(2)
    static_df.loc[widen, "profile_slope"] = ((width - bottom) / 2 / depth)[widen].round(2)
    return static_df


def update_manning_resistance_static(
    model: Model,
    profiles_gpkg: Path | None = None,
    profile_slope: float = 1,
    profile_width: float = 25,
    manning_n: float = 0.04,
    water_area_df: gpd.GeoDataFrame | None = None,
    depth: float = 2.0,
    water_profile_slope: float = 2.0,
) -> None:
    """Generate a default manning-table.

    Args:
        model (Model): Ribasim Model
        profile_slope (float, optional): Slope of the cross section talud. Defaults to 1.
        profile_width (float, optional): _description_. Defaults to 25.
        manning_n (float, optional): _description_. Defaults to 0.04.
        water_area_df (gpd.GeoDataFrame, optional): Water area; profiles narrower than it are widened, see
            `widen_to_water_width`. Defaults to None.
        depth (float, optional): Depth of the profiles at the water level, see `widen_to_water_width`. Defaults to 2.
        water_profile_slope (float, optional): Slope of the widened profiles. Defaults to 2.

    Returns
    -------
        pd.DataFrame: dataframe for static Manning-table in Ribasim model
    """
    # empty dataframe
    static_df = empty_table_df(model=model, node_type="ManningResistance", table_type="Static")

    # length from length links
    assert model.link.df is not None
    length = [
        round_to_precision(
            model.link.df[(model.link.df.from_node_id == node_id) | (model.link.df.to_node_id == node_id)].length.sum(),
            precision=10,
        )
        for node_id in static_df.node_id
    ]
    static_df.loc[:, "length"] = length

    # slope and width from profiles geopackage else defaults
    if profiles_gpkg:
        profiles_df = gpd.read_file(profiles_gpkg).set_index("profiel_id")
        profile_ids = [
            model.link.df.set_index("to_node_id").at[i, "meta_profielid_waterbeheerder"] for i in static_df.node_id
        ]
        static_df.loc[:, "profile_slope"] = profiles_df.loc[profile_ids]["profile_slope"].to_numpy()
        static_df.loc[:, "profile_width"] = profiles_df.loc[profile_ids]["profile_width"].to_numpy()
    else:
        static_df.loc[:, "profile_slope"] = profile_slope
        static_df.loc[:, "profile_width"] = profile_width

    # manning_n
    static_df.loc[:, "manning_n"] = manning_n

    if water_area_df is not None:
        static_df = widen_to_water_width(static_df, water_widths(model, water_area_df), depth, water_profile_slope)

    model.manning_resistance.static.df = DataFrame[ManningResistanceStaticSchema](static_df)


def calculate_flow_rate(
    depth: float, profile_width: float, profile_slope: float, manning_n: float, slope: float
) -> float:
    """Calculate expeted discharge

    Args:
        depth (float): depth in profile [m]
        profile_width (float): bottom-width of profile [m]
        profile_slope (float): profile embankment slope [-]
        manning_n (float): manning roughness coefficient [s/m^(1/3)]
        slope (float): longitudinal slope as part of manning formula [-]

    Returns
    -------
        float: discharge [m3/s]
    """
    if depth == 0:
        return 0  # No flow at zero depth
    A = (profile_width + profile_slope * depth) * depth  # Cross-sectional area
    P = profile_width + 2 * depth * (1 + profile_slope**2) ** 0.5  # Wetted perimeter
    R = A / P  # Hydraulic radius
    K = (1 / manning_n) * A * (R ** (2 / 3))  # Conveyance
    Q = K * (slope**0.5)  # Discharge
    return Q


def manning_flow_rate(model: Model, manning_node_id: int, at_timestamp: Timestamp | None = None) -> float:
    """Calculate the expected discharge at a timestamp

    Args:
        model (ribasim_nl.Model): ribasim-model including manning_node_id
        manning_node_id (int): node_id of manning-node
        at_timestamp (Timestamp | None, optional): timestamp, defaults to final timestamp in simulation. Defaults to None.

    Returns
    -------
        float: discharge [m3/s]
    """
    # get at_timestamp if not defined
    if at_timestamp is None:
        at_timestamp = model.basin_results.df.index.max()

    # get upstream and downstream basins
    us_basin_node_id = model.upstream_node_id(manning_node_id)
    ds_basin_node_id = model.downstream_node_id(manning_node_id)

    # get slope
    assert model.manning_resistance.static.df is not None
    df = model.basin_results.df.loc[at_timestamp].set_index("node_id")
    us_level = cast(float, df.at[us_basin_node_id, "level"])
    ds_level = cast(float, df.at[ds_basin_node_id, "level"])
    delta_h = us_level - ds_level
    slope = abs(
        delta_h / cast(float, model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "length"])
    )

    # get depth as in https://github.com/Deltares/Ribasim/blob/1773acf71857ab05390a60626fbf96dd4ccae740/core/src/solve.jl#L529-L532
    water_level = (us_level + ds_level) / 2
    assert model.basin.profile.df is not None
    bottom_level = (
        model.basin.profile.df.set_index("node_id").loc[us_basin_node_id, "level"].min()
        + model.basin.profile.df.set_index("node_id").loc[ds_basin_node_id, "level"].min()
    ) / 2
    depth = water_level - bottom_level

    q = calculate_flow_rate(
        depth=depth,
        profile_width=float(
            cast(float, model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "profile_width"])
        ),
        profile_slope=float(
            cast(float, model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "profile_slope"])
        ),
        manning_n=float(
            cast(float, model.manning_resistance.static.df.set_index("node_id").at[manning_node_id, "manning_n"])
        ),
        slope=slope,
    )

    return q
