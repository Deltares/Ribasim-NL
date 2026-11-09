# %%
from typing import Any, cast

import geopandas as gpd
import numpy as np
import pandas as pd
import shapely
from pydantic import BaseModel, ConfigDict, PrivateAttr
from shapely.geometry import LineString, Point

from ribasim_nl.model import Model
from ribasim_nl.network import Network


class DAMOProfiles(BaseModel):
    model: Model
    profile_line_df: gpd.GeoDataFrame
    profile_point_df: gpd.GeoDataFrame
    water_area_df: gpd.GeoDataFrame | None = None
    network: Network | None = None
    profile_id_col: str = "meta_profielid_waterbeheerder"
    profile_line_id_col: str = "globalid"

    model_config = ConfigDict(arbitrary_types_allowed=True)

    _points_by_profile: gpd.GeoDataFrame | None = PrivateAttr(default=None)
    _points_by_profile_source: gpd.GeoDataFrame | None = PrivateAttr(default=None)

    def model_post_init(self, __context) -> None:
        if self.network is None:
            self.network = Network(lines_gdf=self.model.link.df)

        # drop duplicated profile-lines
        self.profile_line_df.drop_duplicates(self.profile_line_id_col, inplace=True)

        # drop NA geometries
        self.profile_line_df = self.profile_line_df[self.profile_line_df.geometry.notna()]
        self.profile_point_df = self.profile_point_df[self.profile_point_df.geometry.notna()]

        # in principle globalid in line should be in profiellijnid of point. In case they don't match we clean in 2 directions
        self.profile_line_df = self.profile_line_df[
            self.profile_line_df[self.profile_line_id_col].isin(self.profile_point_df.profiellijnid.to_numpy())
        ]

        # explode multipoints
        if "MultiPoint" in self.profile_point_df.geometry.type.unique():
            self.profile_point_df = self.profile_point_df.explode()

        self.profile_point_df = self.profile_point_df[
            self.profile_point_df.profiellijnid.isin(self.profile_line_df[self.profile_line_id_col].to_numpy())
        ]

        # drop nan profile.geometries
        self.profile_line_df = self.profile_line_df[~self.profile_line_df.geometry.isna()]

        # clip points by water area's
        if self.water_area_df is not None:
            sjoined = self.profile_point_df.sjoin(self.water_area_df, how="left", predicate="within")
            self.profile_point_df["within_water"] = ~sjoined.index_right.isna()

    @property
    def points_by_profile(self) -> gpd.GeoDataFrame:
        """Profile points indexed by sorted `profiellijnid`, for fast lookups of the points of one profile."""
        if self._points_by_profile is None or self._points_by_profile_source is not self.profile_point_df:
            # a stable sort keeps the order of points within a profile
            self._points_by_profile = self.profile_point_df.set_index("profiellijnid").sort_index(kind="stable")
            self._points_by_profile_source = self.profile_point_df
        return self._points_by_profile

    def get_profile_level(self, profile_id, statistic="max"):
        profile_points = self.points_by_profile.loc[profile_id]

        if isinstance(profile_points, pd.Series):
            z_values = [profile_points.geometry.z]
        else:
            profile_points_in_water = (
                profile_points[profile_points.within_water] if "within_water" in profile_points else pd.DataFrame()
            )
            z_values = (
                profile_points_in_water.geometry.apply(lambda g: g.z)
                if not profile_points_in_water.empty
                else profile_points.geometry.apply(lambda g: g.z)
            ).tolist()

        if len(z_values) == 1:
            return z_values[0]

        return getattr(pd.Series(z_values), statistic)()

    def get_profile_id(self, node_id, statistic="max"):
        try:
            node_type = self.model.get_node_type(node_id)
            if node_type == "Basin":
                profile_ids = self.model.link.df[
                    (self.model.link.df.from_node_id == node_id) | (self.model.link.df.to_node_id == node_id)
                ][self.profile_id_col].to_numpy()
                levels = [self.get_profile_level(profile_id, statistic) for profile_id in profile_ids]
                return pd.Series(levels, index=profile_ids).idxmin()
            else:
                return self.model.link.df[self.model.link.df.to_node_id == node_id].iloc[0][self.profile_id_col]
        except Exception as e:
            print(f"Fout bij node_id {node_id}: {e}")
            raise  # eventueel doorgeven zodat je de fout ook buiten kunt afhandelen

    def get_node_level(self, node_id, statistic="max"):
        node_type = self.model.get_node_type(node_id)
        if node_type == "Basin":
            profile_ids = self.model.link.df[
                (self.model.link.df.from_node_id == node_id) | (self.model.link.df.to_node_id == node_id)
            ][self.profile_id_col].to_numpy()
            return min(self.get_profile_level(profile_id, statistic) for profile_id in profile_ids)
        else:
            profile_id = self.model.link.df[self.model.link.df.to_node_id == node_id].iloc[0][self.profile_id_col]
            return self.get_profile_level(profile_id, statistic)

    def process_profiles(
        self,
        elevation_col: str | None = None,
        default_profile_slope: float = 2.0,
        min_profile_width: float = 1,
        min_profile_depth: float = 0.5,
    ) -> gpd.GeoDataFrame:
        """Derive a trapezoidal profile (levels, width and slope) for every profile line from its points.

        The width and slope are those of the trapezoid with the same top width and area as the points, see
        `fit_trapezoid`. Profiles without such a shape get them estimated from the width at water level instead.

        Profiles with a unique, numeric and complete `codevolgnummer` are processed vectorized. Others are
        processed per profile, renumbering the points along the profile line if `codevolgnummer` is duplicated.
        """
        points_df = self.profile_point_df.reset_index(drop=True)
        assert "within_water" in points_df.columns, "process_profiles requires a water_area_df"
        elevation = points_df.geometry.z if elevation_col is None else points_df[elevation_col]
        points_df = points_df.assign(elevation=elevation.to_numpy())
        profile_line_geometry = self.profile_line_df.set_index(self.profile_line_id_col)["geometry"]

        # profiles whose points we can't simply order by codevolgnummer
        if pd.api.types.is_numeric_dtype(points_df["codevolgnummer"]):
            duplicated = points_df.duplicated(["profiellijnid", "codevolgnummer"], keep=False)
            slow_ids = set(points_df.loc[duplicated | points_df["codevolgnummer"].isna(), "profiellijnid"])
        else:
            slow_ids = set(points_df["profiellijnid"])
        is_slow = points_df["profiellijnid"].isin(slow_ids)

        # vectorized: levels per profile, and width between the first and last point in water
        fast_df = points_df[~is_slow]
        grouped = fast_df.groupby("profiellijnid")["elevation"]
        profiles_df = pd.DataFrame({"bottom_level": grouped.min(), "invert_level": grouped.max()})
        in_water = fast_df[fast_df["within_water"]]
        profiles_df["water_level"] = in_water.groupby("profiellijnid")["elevation"].max()
        first = in_water.loc[in_water.groupby("profiellijnid")["codevolgnummer"].idxmin()].set_index("profiellijnid")
        last = in_water.loc[in_water.groupby("profiellijnid")["codevolgnummer"].idxmax()].set_index("profiellijnid")
        profiles_df["width_at_water_level"] = pd.Series(
            shapely.distance(np.asarray(first.geometry.array), np.asarray(last.loc[first.index].geometry.array)),
            index=first.index,
        )
        profiles_df["has_water"] = profiles_df.index.isin(first.index)
        # Python floats, like pandas' Series.min() and max() in the slow path,
        # since Python's round() differs from numpy's
        bottom_levels = profiles_df["bottom_level"].tolist()
        invert_levels = profiles_df["invert_level"].tolist()
        water_levels = profiles_df["water_level"].tolist()
        widths = profiles_df["width_at_water_level"].tolist()
        data = [
            _profile_dimensions(
                profiel_id=profiel_id,
                geometry=profile_line_geometry.at[profiel_id],
                bottom_level=bottom_levels[i],
                invert_level=invert_levels[i],
                water_level=water_levels[i],
                has_water=has_water,
                width_at_water_level=widths[i],
                default_profile_slope=default_profile_slope,
                min_profile_width=min_profile_width,
                min_profile_depth=min_profile_depth,
            )
            for i, (profiel_id, has_water) in enumerate(zip(profiles_df.index, profiles_df["has_water"], strict=True))
        ]

        # per profile, renumbering points along the profile line if codevolgnummer is duplicated
        for profiel_id, df in points_df[is_slow].groupby("profiellijnid"):
            geometry = cast(LineString, profile_line_geometry.at[profiel_id])
            water_level = df[df.within_water]["elevation"].max()
            df = df.copy()
            if df.codevolgnummer.duplicated().any():
                df["distance_on_line"] = [geometry.project(i) for i in df.geometry]
                df.sort_values("distance_on_line", inplace=True)
                df.loc[:, "codevolgnummer"] = [i + 1 for i in range(len(df))]
            df.set_index("codevolgnummer", inplace=True)
            has_water = df.within_water.any()
            width_at_water_level = (
                cast(Point, df.at[df[df.within_water].index.min(), "geometry"]).distance(
                    cast(Point, df.at[df[df.within_water].index.max(), "geometry"])
                )
                if has_water
                else np.nan
            )
            data.append(
                _profile_dimensions(
                    profiel_id=profiel_id,
                    geometry=geometry,
                    bottom_level=df["elevation"].min(),
                    invert_level=df["elevation"].max(),
                    water_level=water_level,
                    has_water=has_water,
                    width_at_water_level=width_at_water_level,
                    default_profile_slope=default_profile_slope,
                    min_profile_width=min_profile_width,
                    min_profile_depth=min_profile_depth,
                )
            )

        # the trapezoid with the same top width and area as the points, where they have such a shape
        lines = profile_line_geometry.reindex(points_df["profiellijnid"]).to_numpy()
        offsets = shapely.line_locate_point(lines, np.asarray(points_df.geometry.array))
        elevations = points_df["elevation"].to_numpy()
        point_indices = points_df.groupby("profiellijnid").indices
        for profile in data:
            indices = point_indices[profile["profiel_id"]]
            fit = fit_trapezoid(offsets[indices], elevations[indices])
            if fit is not None:
                profile_width, profile_slope = fit
                profile["profile_width"] = round(max(profile_width, min_profile_width), 2)
                profile["profile_slope"] = round(profile_slope, 2)

        # same order as a groupby over all profiles
        data.sort(key=lambda profile: profile["profiel_id"])
        return gpd.GeoDataFrame(data, crs=self.profile_line_df.crs)


def fit_trapezoid(offsets: np.ndarray, elevations: np.ndarray) -> tuple[float, float] | None:
    """The trapezoid with the same top width and area as a cross section, like `ribasim_nl.tables.manning_profile`.

    The cross section is the line through the points, ordered by their offset along the profile line, up to the
    lower of its two ends. The slope is horizontal over vertical, like the `profile_slope` of a ManningResistance.

    Args:
        offsets: distance of each point along the profile line [m]
        elevations: elevation of each point [m]

    Returns
    -------
        bottom width [m] and slope [-], or None if the points do not form a cross section with a depth
    """
    order = np.argsort(offsets, kind="stable")
    x, z = np.asarray(offsets, dtype=float)[order], np.asarray(elevations, dtype=float)[order]
    if len(x) < 3 or not np.isfinite(z).all() or x[-1] - x[0] <= 0:
        return None
    top = min(z[0], z[-1])
    depth = top - z.min()
    if depth <= 0:
        return None

    # exact area and width below the top of the piecewise linear cross section, per segment
    area = top_width = 0.0
    for x0, x1, d0, d1 in zip(x[:-1], x[1:], top - z[:-1], top - z[1:], strict=True):
        if d0 <= 0 and d1 <= 0:
            continue
        length = x1 - x0
        if d0 < 0 or d1 < 0:
            # the part of the segment below the top
            length *= max(d0, d1) / (abs(d0) + abs(d1))
            d0, d1 = max(d0, 0.0), max(d1, 0.0)
        area += (d0 + d1) / 2 * length
        top_width += length
    bottom_width = max(2 * area / depth - top_width, 0.0)
    return bottom_width, max(top_width - bottom_width, 0.0) / 2 / depth


def profile_level(profiles_df: pd.DataFrame, profile_ids: list, divisor: float = 2) -> np.ndarray:
    """Level of profiles between their bottom and invert level: bottom + (invert - bottom) / divisor.

    Args:
        profiles_df: profiles indexed by profile id, with bottom_level and invert_level
        profile_ids: profile ids to get the level of
        divisor: the level is at 1/divisor of the depth above the bottom level

    Returns
    -------
        level per profile id
    """
    profiles = profiles_df.loc[profile_ids]
    return (profiles["bottom_level"] + (profiles["invert_level"] - profiles["bottom_level"]) / divisor).to_numpy()


def profile_lines_from_points(
    profile_point_df: gpd.GeoDataFrame, profile_id_col: str, line_id_col: str = "globalid"
) -> gpd.GeoDataFrame:
    """Create a profile line per profile, from its first to its last point by `codevolgnummer`.

    Args:
        profile_point_df: profile points with a `codevolgnummer` and the profile they belong to
        profile_id_col: column in `profile_point_df` with the profile id
        line_id_col: name of the profile id column in the result

    Returns
    -------
        GeoDataFrame with a LineString per profile, sorted by profile id
    """
    points_df = profile_point_df.reset_index(drop=True)
    assert "codevolgnummer" in points_df.columns, "profile points need a codevolgnummer"
    codevolgnummer = points_df["codevolgnummer"]
    grouped = codevolgnummer.groupby(points_df[profile_id_col])

    # profiles where the first or last point is ambiguous are ordered like a (non-stable) sort_values
    is_first, is_last = codevolgnummer == grouped.transform("min"), codevolgnummer == grouped.transform("max")
    ambiguous_ids = set(points_df.loc[is_first, profile_id_col][points_df.loc[is_first, profile_id_col].duplicated()])
    ambiguous_ids |= set(points_df.loc[is_last, profile_id_col][points_df.loc[is_last, profile_id_col].duplicated()])
    if not pd.api.types.is_numeric_dtype(codevolgnummer) or codevolgnummer.isna().any():
        ambiguous_ids = set(points_df[profile_id_col])

    lines: dict[Any, LineString] = {}
    is_ambiguous = points_df[profile_id_col].isin(ambiguous_ids)
    unambiguous_df = points_df[~is_ambiguous]
    first = unambiguous_df[is_first[~is_ambiguous]].set_index(profile_id_col).geometry
    last = unambiguous_df[is_last[~is_ambiguous]].set_index(profile_id_col).geometry
    for profile_id, first_point, last_point in zip(first.index, first, last.loc[first.index], strict=True):
        lines[profile_id] = LineString([first_point, last_point])
    for profile_id, df in points_df[is_ambiguous].groupby(profile_id_col):
        geometries = df.sort_values("codevolgnummer")["geometry"]
        lines[profile_id] = LineString([geometries.iloc[0], geometries.iloc[-1]])

    profile_ids = sorted(lines)
    return gpd.GeoDataFrame(
        {line_id_col: profile_ids}, geometry=[lines[i] for i in profile_ids], crs=profile_point_df.crs
    )


def _profile_dimensions(
    profiel_id,
    geometry,
    bottom_level: float,
    invert_level: float,
    water_level: float,
    has_water: bool | np.bool_,
    width_at_water_level: float,
    default_profile_slope: float,
    min_profile_width: float,
    min_profile_depth: float,
) -> dict:
    """Profile levels, width and slope of a trapezoidal profile.

    The slope is horizontal over vertical, like the `profile_slope` of a Ribasim ManningResistance.
    If no profile point lies in water (`has_water` is False), `water_level` and `width_at_water_level` are NaN.
    """
    assert default_profile_slope >= 0, f"default_profile_slope should be non-negative, not {default_profile_slope}"
    if has_water:
        depth = max(water_level - bottom_level, min_profile_depth)
    else:
        # no points in water: estimate width from the profile line, depth from the invert level
        width_at_water_level = geometry.length / 3
        depth = max(invert_level - bottom_level, min_profile_depth)

    # estimate profile_width from width at water_level, depth and slope
    profile_width = width_at_water_level - 2 * depth * default_profile_slope

    # we assume profile_width is more than 1/3 of width at water_level. Correct values accordingly
    if profile_width < width_at_water_level / 3:
        profile_width = max(width_at_water_level / 3, min_profile_width)
        profile_slope = max(width_at_water_level - profile_width, 0) / 2 / depth
    else:
        profile_slope = default_profile_slope

    return {
        "profiel_id": profiel_id,
        "bottom_level": round(bottom_level, 2),
        "water_level": round(water_level, 2),
        "invert_level": round(invert_level, 2),
        "profile_slope": round(profile_slope, 2),
        "profile_width": round(profile_width, 2),
        "geometry": geometry,
    }
