import geopandas as gpd
import numpy as np
import shapely

from profiles import hydrotopes as ht


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
    unique_fids = hydrotope_map[col_fid].unique()
    if not all(i in unique_fids for i in hydrotopes):
        missing = [i for i in unique_fids if i not in hydrotopes]
        msg = f"Not all occurring hydrotopes initiated: {missing=}"
        raise ValueError(msg)

    # minimise map-columns
    if min_map:
        hydrotope_map = hydrotope_map[[col_fid, "geometry"]]

    # align CRSs
    if hydro_objects.crs != hydrotope_map.crs:
        hydrotope_map.to_crs(hydro_objects.crs)

    # find overlapping hydrotopes per hydro-object
    temp = gpd.sjoin(hydro_objects, hydrotope_map, how="left", predicate="intersects", rsuffix="map").reset_index(
        drop=False
    )
    temp["overlap"] = temp.apply(
        lambda row: row["geometry"].intersection(hydrotope_map.loc[row["index_map"], "geometry"]).length, axis=1
    )

    # assign the hydrotope with the largest overlap per hydro-object
    idx = temp.groupby("index")["overlap"].idxmax()
    hydro_objects["ht_code"] = temp.loc[idx, col_fid]

    # calculate depth
    hydro_objects["depth"] = hydro_objects.apply(lambda row: depth_calculator(row["ht_code"], row["width"]), axis=1)
    if drop_na:
        hydro_objects.dropna(subset="depth", inplace=True, ignore_index=True)

    # return updated hydro-objects
    return hydro_objects


def depth_from_measurements(hydro_objects: gpd.GeoDataFrame, cross_sections: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Determine representative depths of hydro-objects based on measured profiles (if present).

    :param hydro_objects: geospatial data of hydro-objects
    :param cross_sections: geospatial data of measured cross-sections

    :type hydro_objects: geopandas.GeoDataFrame
    :type cross_sections: geopandas.GeoDataFrame

    :return: hydro-objects with depth-estimates
    :rtype: geopandas.GeoDataFrame

    :raises AssertionError: if `cross_sections` does not have z-coordinates
    """

    def depth_calculator(line: shapely.LineString) -> float:
        """Depth equals maximum depth, i.e., minimum z-coordinate."""
        return -np.min(line.coords, axis=0)[2]

    def representative_depth(indices: (int, ...)) -> float | None:
        """Take the mean of estimated depths of all connected cross-sections."""
        if indices and -1 not in indices:
            return float(np.nanmean([depth_calculator(cross_sections.loc[i, "geometry"]) for i in indices]))
        return None

    # assure cross-sections contain z-coordinates
    assert all(cross_sections.has_z)

    # couple hydro-objects to cross-sections
    temp = hydro_objects.sjoin(cross_sections, how="left", predicate="intersects", rsuffix="xs")
    temp["index_xs"] = temp["index_xs"].fillna(-1).astype(int)
    temp = temp.groupby(level=0)["index_xs"].apply(list).to_frame()

    # flag hydro-objects: use of measurements
    hydro_objects["depth_measured"] = ~temp["index_xs"].isin([[-1]])

    # update depth estimate
    temp["depth"] = temp["index_xs"].apply(representative_depth)
    temp.dropna(subset="depth", inplace=True)
    hydro_objects.update(temp)

    # add cross-section coupling
    hydro_objects["index_xs"] = temp["index_xs"]
    hydro_objects.loc[~hydro_objects["depth_measured"], "index_xs"] = np.empty(
        sum(~hydro_objects["depth_measured"]), dtype=list
    )

    # return updated hydro-objects
    return hydro_objects
