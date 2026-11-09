"""Set representative profile depth based on width-hydrotopes or measurements."""

import logging

import geopandas as gpd
import numpy as np
import shapely
import shapely.affinity

from ribasim_nl.geodataframe import sorted_sjoin
from ribasim_nl.profiles import hydrotopes as ht

LOG = logging.getLogger(__name__)


def make_depth_profiles(
    profile_points: gpd.GeoDataFrame, col_profile_id: str = "profiellijnid", col_z: str = "hoogte"
) -> gpd.GeoDataFrame:
    """Translate profile point data to profile line data.

    :param profile_points: geospatial dataset with profile points
    :param col_profile_id: column profile ID, defaults 'profiellijnid'
    :param col_z: column z-coordinate, defaults to 'hoogte'

    :type profile_points: geopandas.GeoDataFrame
    :type col_profile_id: str, optional
    :type col_z: str, optional

    :return: geospatial dataset with profile lines
    :rtype: geopandas.GeoDataFrame
    """
    if not all(profile_points.has_z):
        profile_points["geometry"] = profile_points.apply(
            lambda row: shapely.Point(row.geometry.x, row.geometry.y, row[col_z]), axis=1
        )
    profile_lines = profile_points.groupby(col_profile_id)["geometry"].apply(
        lambda g: shapely.LineString(g) if len(g) > 1 else None
    )
    profile_lines.dropna(inplace=True)
    return gpd.GeoDataFrame({"line_id": profile_lines.index}, geometry=profile_lines.values, crs=profile_points.crs)


def depth_from_hydrotopes(
    hydro_objects: gpd.GeoDataFrame, hydrotope_map: gpd.GeoDataFrame, hydrotopes: ht.HydrotopeTable, **kwargs
) -> gpd.GeoDataFrame:
    """Determine representative depths of hydro-objects based on their representative width and hydrotope.

    :param hydro_objects: geospatial data of hydro-objects
    :param hydrotope_map: geospatial data of hydrotopes
    :param hydrotopes: table with hydrotope-data
    :param kwargs: optional arguments

    :key col_fid: column-name with hydrotope ID data (in `hydrotope_map`), defaults to 'HYDROTOPE2'
    :key drop_na: drop hydro-objects for which depth cannot be determined, defaults to True
    :key min_map: minimal joining of `hydrotope_map` to the `hydro_objects`, defaults to True

    :type hydro_objects: geopandas.GeoDataFrame
    :type hydrotope_map: geopandas.GeoDataFrame
    :type hydrotopes: profiles.hydrotopes.HydrotopeTable

    :return: hydro-objects with depth-estimates
    :rtype: geopandas.GeoDataFrame

    :raises ValueError: if hydro-objects are missing width-estimates
    """

    def depth_calculator(fid: int, width: float) -> float | None:
        """Calculation of depth based on overlapping hydrotope."""
        hydrotope = hydrotopes.get_by_fid(fid)
        if hydrotope is None:
            return None
        return hydrotope.depth(width)

    # verify required width estimations of hydro-objects
    if "width" not in hydro_objects.columns:
        msg = "Hydro-objects are missing width estimates"
        raise ValueError(msg)

    # optional arguments
    col_fid: str = kwargs.get("col_fid", "HYDROTYPE2")
    drop_na: bool = kwargs.get("drop_na", True)
    min_map: bool = kwargs.get("min_map", True)

    # verify definition of all occurring hydrotopes
    unique_fids = list(hydrotope_map[col_fid].unique())
    if not all(i in unique_fids for i in hydrotopes):
        missing = [i for i in unique_fids if i not in hydrotopes]
        msg = f"Not all occurring hydrotopes initiated: {missing=}"
        raise ValueError(msg)

    # minimise map-columns
    if min_map:
        hydrotope_map = hydrotope_map[[col_fid, "geometry"]]

    # align CRSs
    if hydro_objects.crs != hydrotope_map.crs:
        assert hydro_objects.crs is not None
        hydrotope_map.to_crs(hydro_objects.crs)

    # find overlapping hydrotopes per hydro-object
    temp = sorted_sjoin(hydro_objects, hydrotope_map, how="left", predicate="intersects", rsuffix="map")
    mask = temp["index_map"].isna()
    if mask.any():
        LOG.warning(f"Hydro-objects outside hydrotope map ({sum(mask)}) linked to nearest hydrotope")
        _temp = gpd.sjoin_nearest(hydro_objects.loc[temp.index[mask]], hydrotope_map, how="left", rsuffix="map")
        LOG.debug(temp[mask])
        temp.update(_temp)
        LOG.debug(temp[mask])

    temp["overlap"] = temp.apply(
        lambda row: row["geometry"].intersection(hydrotope_map.loc[row["index_map"], "geometry"]).length, axis=1
    )
    temp["depth"] = np.array(
        [depth_calculator(fid, width) for fid, width in zip(temp[col_fid], temp["width"], strict=True)], dtype=float
    )
    temp["has_depth"] = temp["depth"].notna()

    # assign the hydrotope with the largest overlap per hydro-object (`temp` is indexed by hydro-object), preferring
    # a hydrotope with a defined depth on equal overlap, e.g. for hydro-objects on the boundary between hydrotopes
    temp = temp.sort_values(["overlap", "has_depth"], ascending=False, kind="stable")
    best = temp[~temp.index.duplicated(keep="first")]
    assert hydro_objects.index.is_unique and best.index.sort_values().equals(hydro_objects.index.sort_values())
    hydro_objects["ht_code"] = best[col_fid]
    hydro_objects["depth"] = best["depth"]
    if drop_na:
        hydro_objects.dropna(subset="depth", inplace=True, ignore_index=True)

    # return updated hydro-objects
    return hydro_objects


# Measured bed levels outside this range, in m NAP, are errors or no-data values. In the level controlled authorities
# these are -9999 m in Hollandse Delta, about -99 m in Delfland, -278 m and up to +95 m in Amstel, Gooi en Vecht, and
# -2876 m to +210 m in Rijnland. The deepest other levels are -48 m in Amstel, Gooi en Vecht and -19 m in Scheldestromen.
MIN_BED_LEVEL = -50.0
MAX_BED_LEVEL = 50.0


def drop_implausible_levels(
    cross_sections: gpd.GeoDataFrame, min_level: float = MIN_BED_LEVEL, max_level: float = MAX_BED_LEVEL
) -> gpd.GeoDataFrame:
    """Remove the points of cross-sections with a level outside [`min_level`, `max_level`] m NAP.

    Cross-sections with fewer than two points left are removed. The others keep their index.

    :param cross_sections: measured cross-sectional profiles, with z-coordinates
    :param min_level: lowest plausible bed level, defaults to `MIN_BED_LEVEL`
    :param max_level: highest plausible bed level, defaults to `MAX_BED_LEVEL`

    :return: cross-sections without implausible levels
    """
    assert all(cross_sections.has_z), "cross-sections must have z-coordinates"
    lines, kept = [], []
    removed = 0
    for i, line in zip(cross_sections.index, cross_sections.geometry, strict=True):
        coords = np.asarray(line.coords)
        plausible = (coords[:, 2] >= min_level) & (coords[:, 2] <= max_level)
        removed += int((~plausible).sum())
        if plausible.sum() > 1:
            lines.append(shapely.LineString(coords[plausible]))
            kept.append(i)
    out = cross_sections.loc[kept]
    out = out.set_geometry(gpd.GeoSeries(lines, index=out.index, crs=cross_sections.crs))
    LOG.info(f"Removed {removed} implausible levels, and {len(cross_sections) - len(out)} cross-sections entirely")
    return out


def normalise_measured_cross_sections(
    cross_sections: gpd.GeoDataFrame, target_levels: gpd.GeoDataFrame, *, col_target_level: str = "meta_streefpeil"
) -> gpd.GeoDataFrame:
    """Normalise cross-sections z-coordinates to the target level within which they lay.

    A cross-section within several overlapping polygons gets the level of the smallest, which is the local one, such as
    a KRW water body within its polder in Hollandse Delta. A cross-section within none gets no level.

    :param cross_sections: measured cross-sectional profiles
    :param target_levels: regional target levels
    :param col_target_level: column-name with target level data in `target_levels`, defaults to "meta_streefpeil"

    :type cross_sections: geopandas.GeoDataFrame
    :type target_levels: geopandas.GeoDataFrame
    :type col_target_level: str, optional

    :return: measured cross-sectional profiles with normalised z-coordinates, with the index of `cross_sections`
    :rtype: geopandas.GeoDataFrame
    """
    assert cross_sections.index.is_unique, "cross-sections must have a unique index"
    polygons = target_levels[[col_target_level, "geometry"]].assign(_polygon_area=target_levels.area)
    joined = cross_sections[["geometry"]].sjoin(polygons, how="left", predicate="within")
    joined = joined.sort_values("_polygon_area", kind="stable")
    level = joined.loc[~joined.index.duplicated(), col_target_level].reindex(cross_sections.index)
    geometry = [shapely.affinity.translate(g, zoff=-z) for g, z in zip(cross_sections.geometry, level, strict=True)]
    out = cross_sections.assign(**{col_target_level: level})
    return out.set_geometry(gpd.GeoSeries(geometry, index=out.index, crs=out.crs))


def depth_from_measurements(
    hydro_objects: gpd.GeoDataFrame, cross_sections: gpd.GeoDataFrame, *, only_main_route: bool = True
) -> gpd.GeoDataFrame:
    """Determine representative depths of hydro-objects based on measured profiles (if present).

    :param hydro_objects: geospatial data of hydro-objects
    :param cross_sections: geospatial data of measured cross-sections
    :param only_main_route: only use measured cross-sectional profiles on main route hydro-objects, defaults to True

    :type hydro_objects: geopandas.GeoDataFrame
    :type cross_sections: geopandas.GeoDataFrame
    :type only_main_route: bool, optional

    :return: hydro-objects with depth-estimates
    :rtype: geopandas.GeoDataFrame

    :raises AssertionError: if `cross_sections` does not have z-coordinates
    """

    def depth_calculator(line: shapely.LineString) -> float:
        """Depth equals maximum depth, i.e., minimum z-coordinate."""
        return -np.min(line.coords, axis=0)[2]

    def representative_depth(indices: tuple[int, ...]) -> float | None:
        """Take the mean of estimated depths of all connected cross-sections, by their index."""
        if indices and -1 not in indices:
            return float(np.nanmean([depth_calculator(cross_sections.geometry.loc[i]) for i in indices]))
        return None

    # assure cross-sections contain z-coordinates, and can be found by their index
    assert all(cross_sections.has_z)
    assert cross_sections.index.is_unique and (cross_sections.index >= 0).all()

    # couple hydro-objects to cross-sections
    temp = sorted_sjoin(
        hydro_objects[hydro_objects["main-route"]] if only_main_route else hydro_objects,
        cross_sections,
        how="left",
        predicate="intersects",
        rsuffix="xs",
    )
    temp["index_xs"] = temp["index_xs"].fillna(-1).astype(int)
    temp = temp.groupby(level=0)["index_xs"].apply(list).to_frame()

    # flag hydro-objects: use of measurements
    hydro_objects["depth_measured"] = ~temp["index_xs"].isin([[-1]])
    hydro_objects.loc[hydro_objects["depth_measured"].isna(), "depth_measured"] = False
    if only_main_route:
        assert np.all(~hydro_objects.loc[~hydro_objects["main-route"], "depth_measured"])

    # update depth estimate
    temp["depth"] = temp["index_xs"].apply(representative_depth)
    temp.dropna(subset="depth", inplace=True)
    hydro_objects.update(temp[temp["depth"] > 0])

    # add cross-section coupling
    hydro_objects["index_xs"] = temp["index_xs"]

    # return updated hydro-objects
    return hydro_objects
