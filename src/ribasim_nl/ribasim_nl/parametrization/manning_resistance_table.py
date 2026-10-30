from pathlib import Path
from typing import cast

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from pandas import Timestamp
from pandera.typing import DataFrame
from ribasim.schemas import ManningResistanceStaticSchema

from ribasim_nl.bgt_water import BGT_WATER, read_bgt_water
from ribasim_nl.model import Model
from ribasim_nl.parametrization.conversions import round_to_precision
from ribasim_nl.parametrization.empty_table import empty_table_df

# Half the width across a watercourse that is searched for water [m]
MAX_WATER_HALF_WIDTH = 150.0
# Distance between the cross sections along the links of a ManningResistance, and from their ends, which lie at
# nodes, junctions and bends where a cross section is not representative [m]
CROSS_SECTION_STEP = 25.0
CROSS_SECTION_MARGIN = 30.0
# Distance from a link within which water counts as its watercourse: links follow hydro-objects, which can lie a few
# metres beside the water of a narrow watercourse [m]
WATER_TOLERANCE = 5.0


def crosses_link(profile: shapely.LineString, link: shapely.LineString, max_angle: float = 45.0) -> bool:
    """Whether a profile line lies across a link: crossing it near its middle, at most `max_angle` from square.

    A profile of a side ditch can touch the link where the ditch joins, but lies along it.
    """
    point = profile.intersection(link)
    if point.is_empty:
        return False
    point = cast(
        shapely.Point, shapely.get_point(shapely.get_parts(point)[0], 0) if point.geom_type != "Point" else point
    )
    if not 0.2 <= profile.project(point) / profile.length <= 0.8:
        return False
    across = np.subtract(profile.coords[-1][:2], profile.coords[0][:2])
    at = link.project(point)
    a, b = link.interpolate(max(at - 5, 0)), link.interpolate(min(at + 5, link.length))
    along = np.array([b.x - a.x, b.y - a.y])
    cosine = abs(across @ along) / (np.linalg.norm(across) * np.linalg.norm(along))
    return cosine < np.sin(np.radians(max_angle))


def reach_water_width(
    links: list[shapely.LineString], water: np.ndarray, tree: shapely.STRtree, tolerance: float = WATER_TOLERANCE
) -> float:
    """Median width of the water area across links, or 0 if less than half of the cross sections lie in water.

    The cross sections lie every `CROSS_SECTION_STEP` along the links, perpendicular to them, except within
    `CROSS_SECTION_MARGIN` of their ends.
    """
    widths = []
    for line in links:
        for at in np.arange(CROSS_SECTION_MARGIN, line.length - CROSS_SECTION_MARGIN + 1e-9, CROSS_SECTION_STEP):
            point = line.interpolate(at)
            a, b = line.interpolate(at - 5), line.interpolate(at + 5)
            along = np.array([b.x - a.x, b.y - a.y])
            across = np.array([-along[1], along[0]]) / np.linalg.norm(along)
            xy = np.array([point.x, point.y])
            cross = shapely.LineString([xy - across * MAX_WATER_HALF_WIDTH, xy + across * MAX_WATER_HALF_WIDTH])
            nearby = tree.query(cross, predicate="intersects")
            inside = cross.intersection(shapely.union_all(water[nearby])) if len(nearby) else shapely.LineString()
            parts = [g for g in shapely.get_parts(inside) if g.length > 0 and g.distance(point) <= tolerance]
            widths.append(max((g.length for g in parts), default=0.0))
    widths = np.array(widths)
    if len(widths) == 0 or (widths > 0).mean() < 0.5:
        return 0.0
    return float(np.median(widths[widths > 0]))


def trapezoid(top_width: float, depth: float, profile_slope: float) -> tuple[float, float]:
    """Bottom width and slope of a trapezoid with `top_width` at `depth`, with sides of `profile_slope`.

    The bottom is at least a third of the top width, with steeper sides if needed. Slopes are horizontal over vertical.
    """
    assert top_width > 0 and depth > 0 and profile_slope >= 0
    bottom_width = max(top_width - 2 * depth * profile_slope, top_width / 3)
    return bottom_width, (top_width - bottom_width) / 2 / depth


# Default profile of ManningResistances without DAMO profile or BGT water, see `update_manning_resistance_static`
DEFAULT_PROFILE_WIDTH = 25.0
DEFAULT_PROFILE_SLOPE = 1.0
# Depth at which BGT widths apply, that of the primary Basin profiles, and the slope of the sides of their profile
WATER_PROFILE_DEPTH = 2.0
WATER_PROFILE_SLOPE = 2.0
# Sources of profiles that depend on the course of the links, see `update_manning_resistance_reach`
REACH_SOURCES = {"BGT", "default"}


def _links_by_node(model: Model, node_ids: pd.Series) -> dict[int, list[shapely.LineString]]:
    """The flow links of each node."""
    assert model.link.df is not None
    link_df = model.link.df[model.link.df.link_type == "flow"]
    return {
        int(node_id): list(link_df[(link_df.from_node_id == node_id) | (link_df.to_node_id == node_id)].geometry)
        for node_id in node_ids.unique()
    }


def _reach_length(links: list[shapely.LineString]) -> float:
    """Length of the links of a ManningResistance, rounded to 10 m."""
    return round_to_precision(sum(link.length for link in links), precision=10)


def _set_water_profiles(
    static_df: pd.DataFrame,
    rows: list[int],
    links: dict[int, list[shapely.LineString]],
    water_area_df: gpd.GeoDataFrame | None,
) -> None:
    """Set the profile of `rows` from the width of the BGT water along their links, or else the default."""
    static_df.loc[rows, ["profile_width", "profile_slope", "meta_profile_source"]] = [
        DEFAULT_PROFILE_WIDTH,
        DEFAULT_PROFILE_SLOPE,
        "default",
    ]
    if water_area_df is None or not rows:
        return
    water = np.asarray(shapely.make_valid(water_area_df.geometry.to_numpy()))
    tree = shapely.STRtree(water)
    node_ids = static_df["node_id"].astype(int)
    for row in rows:
        width = reach_water_width(links[node_ids[row]], water, tree)
        if width > 0:
            bottom_width, slope = trapezoid(width, WATER_PROFILE_DEPTH, WATER_PROFILE_SLOPE)
            static_df.loc[row, ["profile_width", "profile_slope", "meta_profile_source"]] = [
                round(bottom_width, 2),
                round(slope, 2),
                "BGT",
            ]


def read_manning_water(model: Model, path: Path | None = BGT_WATER) -> gpd.GeoDataFrame | None:
    """The BGT water around the ManningResistances of a model, or None without a path or ManningResistances."""
    node = model.manning_resistance.node
    if path is None or node is None or node.df is None or node.df.empty:
        return None
    xmin, ymin, xmax, ymax = node.df.total_bounds
    return read_bgt_water(
        shapely.box(xmin - 5000, ymin - 5000, xmax + 5000, ymax + 5000), path=path, columns=["geometry"]
    )


def update_manning_resistance_reach(model: Model, water_area_df: gpd.GeoDataFrame | None) -> None:
    """Update the BGT profiles of ManningResistances whose links were routed after `update_manning_resistance_static`.

    Routing the links along the waterways, see `ribasim_nl.regional_model.finish_regional_model`, changes their
    course and can move the ManningResistance. So the profiles that depend on the course of the links, those from the
    BGT or the default, are determined again. The length is kept: routing moves Basin and ManningResistance nodes
    closer together, and shorter ManningResistances made the coupled model a third slower.
    """
    static_df = model.manning_resistance.static.df
    if static_df is None or "meta_profile_source" not in static_df.columns:
        return
    static_df = static_df.copy()
    rows = [row for row in static_df.index if static_df.at[row, "meta_profile_source"] in REACH_SOURCES]
    links = _links_by_node(model, static_df.loc[rows, "node_id"])
    _set_water_profiles(static_df, rows, links, water_area_df)
    model.manning_resistance.static.df = DataFrame[ManningResistanceStaticSchema](static_df)


def update_manning_resistance_static(
    model: Model,
    profiles_gpkg: Path | None = None,
    manning_n: float = 0.04,
    water_area_df: gpd.GeoDataFrame | None = None,
) -> None:
    """Generate the ManningResistance static table, with the profile from the first source that has one.

    1. The DAMO profile of its link, from `profiles_gpkg`, if it lies across one of the links of the node, see
       `crosses_link`. Otherwise it is of another watercourse, as watercourses can have no profiles at all.
    2. The width of the BGT water along its links, see `reach_water_width` and `ribasim_nl.bgt_water`.
    3. The default `DEFAULT_PROFILE_WIDTH` and `DEFAULT_PROFILE_SLOPE`.

    Widths from the BGT are the top width at `WATER_PROFILE_DEPTH`, with sides of `WATER_PROFILE_SLOPE`, see
    `trapezoid`. The source is written to `meta_profile_source`. As the links are routed later, the profiles from the
    BGT are updated then, see `update_manning_resistance_reach`.

    Args:
        model (Model): Ribasim Model
        profiles_gpkg (Path, optional): DAMO profiles with profile lines, by `profiel_id`. Defaults to None.
        manning_n (float, optional): Manning roughness. Defaults to 0.04.
        water_area_df (gpd.GeoDataFrame, optional): BGT water, see `ribasim_nl.bgt_water`. Defaults to None.
    """
    # empty dataframe
    static_df = empty_table_df(model=model, node_type="ManningResistance", table_type="Static")

    links = _links_by_node(model, static_df["node_id"])
    static_df.loc[:, "length"] = [_reach_length(links[int(n)]) for n in static_df["node_id"]]
    static_df.loc[:, "meta_profile_source"] = None

    # 1. DAMO profile across the links
    remaining = list(static_df.index)
    node_ids = static_df["node_id"].astype(int)
    if profiles_gpkg:
        assert model.link.df is not None
        profiles_df = gpd.read_file(profiles_gpkg).set_index("profiel_id")
        profile_ids = model.link.df.set_index("to_node_id")["meta_profielid_waterbeheerder"]
        for row in list(remaining):
            profile_id = profile_ids.get(static_df.at[row, "node_id"])
            if profile_id not in profiles_df.index:
                continue
            profile = profiles_df.loc[profile_id]
            if any(crosses_link(profile.geometry, link) for link in links[node_ids[row]]):
                static_df.loc[row, ["profile_width", "profile_slope", "meta_profile_source"]] = [
                    profile["profile_width"],
                    profile["profile_slope"],
                    "DAMO",
                ]
                remaining.remove(row)

    # 2. width of the BGT water along the links, or else 3. the default
    _set_water_profiles(static_df, remaining, links, water_area_df)

    # manning_n
    static_df.loc[:, "manning_n"] = manning_n

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
